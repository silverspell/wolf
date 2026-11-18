use bytes::Bytes;
use wolf_broker::queue::{ConnectionId, QueueConfig, QueueRegistry};
use wolf_broker::{
    CancelReason, ChannelId, ConsumerCancel, ConsumerDispatcher, ConsumerId, CreditSignal,
    Delivery, DeliverySink, DeliveryTag, DomainError, Message,
};

#[derive(Debug, Default)]
struct RecordingSink {
    deliveries: Vec<Delivery>,
    cancels: Vec<ConsumerCancel>,
}

impl DeliverySink for RecordingSink {
    fn deliver(&mut self, delivery: Delivery) {
        self.deliveries.push(delivery);
    }

    fn cancel(&mut self, cancel: ConsumerCancel) {
        self.cancels.push(cancel);
    }
}

fn message(body: &'static str) -> Message {
    Message::new(Bytes::from_static(body.as_bytes()))
}

fn setup(
    prefetch: u32,
) -> (
    QueueRegistry,
    ConsumerDispatcher,
    ConnectionId,
    ChannelId,
    ConsumerId,
) {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);

    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");
    dispatcher
        .register(&mut queues, connection, channel, consumer, "jobs")
        .expect("register consumer");
    dispatcher.set_prefetch(channel, prefetch, false);

    (queues, dispatcher, connection, channel, consumer)
}

fn publish(queues: &mut QueueRegistry, connection: ConnectionId, bodies: &[&'static str]) {
    for body in bodies {
        queues
            .publish("jobs", connection, message(body))
            .expect("publish");
    }
}

fn queued_bodies(queues: &QueueRegistry) -> Vec<Bytes> {
    queues
        .queue("jobs")
        .expect("queue")
        .messages()
        .map(|message| message.body().clone())
        .collect()
}

fn delivered_bodies(sink: &RecordingSink) -> Vec<Bytes> {
    sink.deliveries
        .iter()
        .map(|delivery| delivery.message.body().clone())
        .collect()
}

#[test]
fn ack_multiple_acknowledges_all_tags_up_to_cut() {
    let (mut queues, mut dispatcher, connection, channel, consumer) = setup(3);
    publish(&mut queues, connection, &["m1", "m2", "m3"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        3
    );
    let last_tag = sink.deliveries[2].delivery_tag;

    let outcome = dispatcher
        .ack(channel, last_tag, true)
        .expect("ack multiple");

    assert_eq!(outcome.settled, 3);
    assert_eq!(outcome.requeued, 0);
    assert_eq!(
        outcome.credit,
        vec![CreditSignal::Consumer {
            consumer,
            credit: 3,
        }]
    );
    assert_eq!(dispatcher.unacked_count(channel), 0);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
}

#[test]
fn ack_single_leaves_other_deliveries_unacked() {
    let (mut queues, mut dispatcher, connection, channel, _consumer) = setup(3);
    publish(&mut queues, connection, &["m1", "m2", "m3"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        3
    );
    let first_tag = sink.deliveries[0].delivery_tag;

    let outcome = dispatcher
        .ack(channel, first_tag, false)
        .expect("ack single");

    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 0);
    assert_eq!(dispatcher.unacked_count(channel), 2);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
}

#[test]
fn ack_restores_prefetch_credit() {
    let (mut queues, mut dispatcher, connection, channel, consumer) = setup(1);
    publish(&mut queues, connection, &["m1", "m2"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("first dispatch"),
        1
    );
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 1);

    let outcome = dispatcher
        .ack(channel, sink.deliveries[0].delivery_tag, false)
        .expect("ack");
    assert_eq!(
        outcome.credit,
        vec![CreditSignal::Consumer {
            consumer,
            credit: 1,
        }]
    );

    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("second dispatch"),
        1
    );
    assert_eq!(sink.deliveries[0].message.body().as_ref(), b"m2");
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
}

#[test]
fn nack_multiple_with_requeue_requeues_in_order_with_redelivered() {
    let (mut queues, mut dispatcher, connection, channel, _consumer) = setup(3);
    publish(&mut queues, connection, &["m1", "m2", "m3"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        3
    );
    let last_tag = sink.deliveries[2].delivery_tag;

    let outcome = dispatcher
        .nack(&mut queues, channel, last_tag, true, true)
        .expect("nack multiple");

    assert_eq!(outcome.settled, 3);
    assert_eq!(outcome.requeued, 3);
    assert_eq!(dispatcher.unacked_count(channel), 0);
    assert_eq!(
        queued_bodies(&queues),
        vec![
            Bytes::from_static(b"m1"),
            Bytes::from_static(b"m2"),
            Bytes::from_static(b"m3"),
        ]
    );

    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("redispatch"),
        3
    );
    assert!(sink.deliveries.iter().all(|delivery| delivery.redelivered));
    assert_eq!(
        delivered_bodies(&sink),
        vec![
            Bytes::from_static(b"m1"),
            Bytes::from_static(b"m2"),
            Bytes::from_static(b"m3"),
        ]
    );
}

#[test]
fn reject_with_requeue_requeues_one_with_redelivered() {
    let (mut queues, mut dispatcher, connection, channel, _consumer) = setup(2);
    publish(&mut queues, connection, &["m1", "m2"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        2
    );
    let second_tag = sink.deliveries[1].delivery_tag;

    let outcome = dispatcher
        .reject(&mut queues, channel, second_tag, true)
        .expect("reject with requeue");

    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert_eq!(dispatcher.unacked_count(channel), 1);
    assert_eq!(queued_bodies(&queues), vec![Bytes::from_static(b"m2")]);

    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("redispatch"),
        1
    );
    assert!(sink.deliveries[0].redelivered);
    assert_eq!(sink.deliveries[0].message.body().as_ref(), b"m2");
}

#[test]
fn reject_without_requeue_discards() {
    let (mut queues, mut dispatcher, connection, channel, _consumer) = setup(2);
    publish(&mut queues, connection, &["m1", "m2"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        2
    );
    let first_tag = sink.deliveries[0].delivery_tag;

    let outcome = dispatcher
        .reject(&mut queues, channel, first_tag, false)
        .expect("reject without requeue");

    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 0);
    assert_eq!(dispatcher.unacked_count(channel), 1);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
}

#[test]
fn unknown_delivery_tag_is_rejected() {
    let (_queues, mut dispatcher, _connection, channel, _consumer) = setup(1);

    let error = dispatcher
        .ack(channel, DeliveryTag::new(99), false)
        .expect_err("unknown tag must fail");

    assert!(matches!(error, DomainError::PreconditionFailed { .. }));
    assert_eq!(dispatcher.unacked_count(channel), 0);
}

#[test]
fn connection_close_requeues_unacked_with_redelivered() {
    let (mut queues, mut dispatcher, connection, channel, _consumer) = setup(2);
    publish(&mut queues, connection, &["m1", "m2"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        2
    );

    let outcome = dispatcher.close_connection(&mut queues, connection);

    assert!(outcome.deleted_queues.is_empty());
    assert!(outcome.cancels.is_empty());
    assert_eq!(dispatcher.unacked_count(channel), 0);
    assert_eq!(
        queues
            .queue("jobs")
            .expect("queue survives")
            .consumer_count(),
        0
    );
    assert_eq!(
        queued_bodies(&queues),
        vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")]
    );

    let new_connection = ConnectionId::new(2);
    let new_channel = ChannelId::new(2);
    let new_consumer = ConsumerId::new(2);
    dispatcher
        .register(
            &mut queues,
            new_connection,
            new_channel,
            new_consumer,
            "jobs",
        )
        .expect("register new consumer");
    dispatcher.set_prefetch(new_channel, 2, false);

    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("redispatch"),
        2
    );
    assert!(sink.deliveries.iter().all(|delivery| delivery.redelivered));
    assert_eq!(
        delivered_bodies(&sink),
        vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")]
    );
}

#[test]
fn connection_close_still_applies_queue_lifecycle() {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);
    let config = QueueConfig {
        exclusive: true,
        ..QueueConfig::default()
    };
    let name = queues
        .declare("owned", config, connection)
        .expect("declare exclusive queue");
    dispatcher
        .register(&mut queues, connection, channel, consumer, name.as_str())
        .expect("register consumer");
    dispatcher.set_prefetch(channel, 1, false);
    queues
        .publish(name.as_str(), connection, message("m1"))
        .expect("publish");

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, name.as_str(), &mut sink)
            .expect("dispatch"),
        1
    );

    let outcome = dispatcher.close_connection(&mut queues, connection);

    assert_eq!(outcome.deleted_queues, vec![name.clone()]);
    assert_eq!(
        outcome.cancels,
        vec![ConsumerCancel {
            consumer,
            reason: CancelReason::QueueDeleted,
        }]
    );
    assert!(!queues.contains(name.as_str()));
    assert_eq!(dispatcher.unacked_count(channel), 0);
}

#[test]
fn recover_requeues_all_unacked_with_redelivered() {
    let (mut queues, mut dispatcher, connection, channel, _consumer) = setup(3);
    publish(&mut queues, connection, &["m1", "m2", "m3"]);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        3
    );

    let outcome = dispatcher.recover(&mut queues, channel).expect("recover");

    assert_eq!(outcome.settled, 3);
    assert_eq!(outcome.requeued, 3);
    assert_eq!(dispatcher.unacked_count(channel), 0);
    assert_eq!(
        queued_bodies(&queues),
        vec![
            Bytes::from_static(b"m1"),
            Bytes::from_static(b"m2"),
            Bytes::from_static(b"m3"),
        ]
    );

    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("redispatch"),
        3
    );
    assert!(sink.deliveries.iter().all(|delivery| delivery.redelivered));
    assert_eq!(
        delivered_bodies(&sink),
        vec![
            Bytes::from_static(b"m1"),
            Bytes::from_static(b"m2"),
            Bytes::from_static(b"m3"),
        ]
    );
}
