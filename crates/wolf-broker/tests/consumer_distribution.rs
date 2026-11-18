use std::future::Future;

use bytes::Bytes;
use wolf_broker::queue::{ConnectionId, QueueConfig, QueueRegistry};
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerDispatcher, ConsumerId, CreditSignal, DeliverOutcome,
    Delivery, DeliverySink, Message,
};

#[derive(Debug, Default)]
struct RecordingSink {
    deliveries: Vec<Delivery>,
}

impl DeliverySink for RecordingSink {
    fn deliver(&mut self, delivery: Delivery) -> impl Future<Output = DeliverOutcome> + Send {
        self.deliveries.push(delivery);
        std::future::ready(DeliverOutcome::Delivered)
    }

    fn cancel(&mut self, _cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

fn message(body: &'static str) -> Message {
    Message::new(Bytes::from_static(body.as_bytes()))
}

fn setup_two_consumers() -> (
    QueueRegistry,
    ConsumerDispatcher,
    ConnectionId,
    ChannelId,
    ConsumerId,
    ConsumerId,
) {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);

    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");
    dispatcher
        .register(&mut queues, connection, channel, first, "jobs")
        .expect("register first consumer");
    dispatcher
        .register(&mut queues, connection, channel, second, "jobs")
        .expect("register second consumer");

    (queues, dispatcher, connection, channel, first, second)
}

#[tokio::test]
async fn prefetch_one_alternates_and_caps_each_consumer() {
    let (mut queues, mut dispatcher, connection, channel, first, second) = setup_two_consumers();
    let signals = dispatcher.set_prefetch(channel, 1, false);
    assert_eq!(signals.len(), 2);

    for body in ["m1", "m2", "m3", "m4"] {
        queues
            .publish("jobs", connection, message(body))
            .expect("publish");
    }

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .await
            .expect("first dispatch"),
        2
    );
    assert_eq!(
        sink.deliveries
            .iter()
            .map(|delivery| delivery.consumer)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    assert_eq!(sink.deliveries[0].message.body().as_ref(), b"m1");
    assert_eq!(sink.deliveries[1].message.body().as_ref(), b"m2");
    assert_eq!(dispatcher.unacked_count(channel), 2);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 2);

    let first_tag = sink.deliveries[0].delivery_tag;
    let second_tag = sink.deliveries[1].delivery_tag;

    dispatcher
        .ack(channel, first_tag, false)
        .expect("ack first delivery");
    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .await
            .expect("second dispatch"),
        1
    );
    assert_eq!(sink.deliveries[0].consumer, first);
    assert_eq!(sink.deliveries[0].message.body().as_ref(), b"m3");
    let third_tag = sink.deliveries[0].delivery_tag;

    dispatcher
        .ack(channel, third_tag, false)
        .expect("ack third delivery");
    dispatcher
        .ack(channel, second_tag, false)
        .expect("ack second delivery");
    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .await
            .expect("third dispatch"),
        1
    );
    assert_eq!(sink.deliveries[0].consumer, second);
    assert_eq!(sink.deliveries[0].message.body().as_ref(), b"m4");
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
}

#[tokio::test]
async fn prefetch_zero_means_unlimited() {
    let (mut queues, mut dispatcher, connection, channel, first, second) = setup_two_consumers();
    dispatcher.set_prefetch(channel, 0, false);

    for body in ["m1", "m2", "m3", "m4"] {
        queues
            .publish("jobs", connection, message(body))
            .expect("publish");
    }

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .await
            .expect("dispatch"),
        4
    );
    assert_eq!(
        sink.deliveries
            .iter()
            .map(|delivery| delivery.consumer)
            .collect::<Vec<_>>(),
        vec![first, second, first, second]
    );
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
    assert_eq!(dispatcher.unacked_count(channel), 4);
}

#[tokio::test]
async fn credit_signals_are_per_consumer_when_global_false() {
    let (_queues, mut dispatcher, _connection, channel, first, second) = setup_two_consumers();

    let signals = dispatcher.set_prefetch(channel, 2, false);

    assert_eq!(
        signals,
        vec![
            CreditSignal::Consumer {
                consumer: first,
                credit: 2,
            },
            CreditSignal::Consumer {
                consumer: second,
                credit: 2,
            },
        ]
    );
}

#[tokio::test]
async fn credit_signal_is_channel_scoped_when_global_true() {
    let (_queues, mut dispatcher, _connection, channel, _first, _second) = setup_two_consumers();

    let signals = dispatcher.set_prefetch(channel, 4, true);

    assert_eq!(signals, vec![CreditSignal::Channel { channel, credit: 4 }]);
}

#[tokio::test]
async fn global_prefetch_caps_total_inflight_across_consumers() {
    let (mut queues, mut dispatcher, connection, channel, first, second) = setup_two_consumers();
    dispatcher.set_prefetch(channel, 2, true);

    for body in ["m1", "m2", "m3", "m4"] {
        queues
            .publish("jobs", connection, message(body))
            .expect("publish");
    }

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .await
            .expect("first dispatch"),
        2
    );
    assert_eq!(
        sink.deliveries
            .iter()
            .map(|delivery| delivery.consumer)
            .collect::<Vec<_>>(),
        vec![first, second]
    );
    assert_eq!(dispatcher.unacked_count(channel), 2);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 2);

    dispatcher
        .ack(channel, sink.deliveries[0].delivery_tag, false)
        .expect("ack first delivery");
    sink.deliveries.clear();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .await
            .expect("second dispatch"),
        1
    );
    assert_eq!(dispatcher.unacked_count(channel), 2);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 1);
}
