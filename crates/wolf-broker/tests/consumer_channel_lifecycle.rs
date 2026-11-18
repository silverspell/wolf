use bytes::Bytes;
use wolf_broker::queue::{ConnectionId, QueueConfig, QueueRegistry};
use wolf_broker::{
    CancelReason, ChannelId, ConsumerCancel, ConsumerDispatcher, ConsumerId, ConsumerOptions,
    Delivery, DeliverySink, Message, MessageMetadata, Vhost, VhostId, VhostName,
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

fn bodies(queues: &QueueRegistry, queue: &str) -> Vec<Bytes> {
    queues
        .queue(queue)
        .expect("queue")
        .messages()
        .map(|message| message.body().clone())
        .collect()
}

#[test]
fn message_metadata_is_optional_and_shared_across_clones() {
    let body = Bytes::from_static(b"payload");
    let properties = Bytes::from_static(b"\x80\x00");
    let message = Message::with_metadata(
        body.clone(),
        MessageMetadata::new("events", "jobs", properties.clone()),
    );

    let metadata = message.metadata().expect("metadata");
    assert_eq!(metadata.exchange, "events");
    assert_eq!(metadata.routing_key, "jobs");
    assert_eq!(metadata.properties, properties);

    let cloned = message.clone();
    assert_eq!(cloned.body().as_ptr(), body.as_ptr());
    assert_eq!(
        cloned.metadata().map(|value| value as *const _),
        Some(metadata as *const _)
    );

    let plain = Message::new(body);
    assert!(plain.metadata().is_none());
}

#[test]
fn no_ack_dispatch_does_not_track_or_consume_prefetch_credit() {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare");
    dispatcher
        .register_with(
            &mut queues,
            connection,
            channel,
            consumer,
            "jobs",
            ConsumerOptions { no_ack: true },
        )
        .expect("register");
    dispatcher.set_prefetch(channel, 1, false);
    for body in ["m1", "m2", "m3"] {
        queues
            .publish("jobs", connection, message(body))
            .expect("publish");
    }

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        3
    );
    assert_eq!(sink.deliveries.len(), 3);
    assert_eq!(dispatcher.unacked_count(channel), 0);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
}

#[test]
fn get_allocates_delivery_tags_and_tracks_unacked_unless_no_ack() {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare");
    queues
        .publish("jobs", connection, message("first"))
        .expect("publish first");
    queues
        .publish("jobs", connection, message("second"))
        .expect("publish second");

    let first = dispatcher
        .get(&mut queues, connection, channel, "jobs", false)
        .expect("get first");
    let wolf_broker::GetOutcome::Message(first) = first else {
        panic!("expected a message");
    };
    let first_tag = first.delivery_tag.expect("acknowledged get has a tag");
    assert_eq!(first.remaining, 1);
    assert_eq!(dispatcher.unacked_count(channel), 1);

    let second = dispatcher
        .get(&mut queues, connection, channel, "jobs", true)
        .expect("get second");
    let wolf_broker::GetOutcome::Message(second) = second else {
        panic!("expected a message");
    };
    assert!(second.delivery_tag.is_some());
    assert_eq!(dispatcher.unacked_count(channel), 1);

    dispatcher.ack(channel, first_tag, false).expect("ack get");
    assert_eq!(dispatcher.unacked_count(channel), 0);

    let empty = dispatcher
        .get(&mut queues, connection, channel, "jobs", false)
        .expect("get empty");
    assert_eq!(empty, wolf_broker::GetOutcome::Empty);
}

#[test]
fn cancel_removes_the_consumer_and_suppresses_delivery() {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare");
    dispatcher
        .register(&mut queues, connection, channel, consumer, "jobs")
        .expect("register");
    queues
        .publish("jobs", connection, message("m1"))
        .expect("publish");

    assert!(dispatcher.cancel(&mut queues, consumer).expect("cancel"));
    assert!(
        !dispatcher
            .cancel(&mut queues, consumer)
            .expect("idempotent")
    );
    assert_eq!(queues.queue("jobs").expect("queue").consumer_count(), 0);

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        0
    );
    assert!(sink.deliveries.is_empty());
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 1);
}

#[test]
fn close_channel_requeues_unacked_with_redelivered_and_removes_consumers() {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare");
    dispatcher
        .register(&mut queues, connection, channel, consumer, "jobs")
        .expect("register");
    for body in ["m1", "m2"] {
        queues
            .publish("jobs", connection, message(body))
            .expect("publish");
    }

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .expect("dispatch"),
        2
    );

    let outcome = dispatcher
        .close_channel(&mut queues, channel)
        .expect("close channel");
    assert_eq!(outcome.settled, 2);
    assert_eq!(outcome.requeued, 2);
    assert_eq!(dispatcher.unacked_count(channel), 0);
    assert_eq!(queues.queue("jobs").expect("queue").consumer_count(), 0);
    assert_eq!(
        bodies(&queues, "jobs"),
        vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")]
    );

    let new_channel = ChannelId::new(2);
    dispatcher
        .register(
            &mut queues,
            connection,
            new_channel,
            ConsumerId::new(2),
            "jobs",
        )
        .expect("register new consumer");
    sink.deliveries.clear();
    dispatcher
        .dispatch(&mut queues, "jobs", &mut sink)
        .expect("redispatch");
    assert!(sink.deliveries.iter().all(|delivery| delivery.redelivered));
}

#[test]
fn close_connection_requeues_basic_get_unacked_as_well() {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare");
    queues
        .publish("jobs", connection, message("fetched"))
        .expect("publish");

    let got = dispatcher
        .get(&mut queues, connection, channel, "jobs", false)
        .expect("get");
    assert!(matches!(got, wolf_broker::GetOutcome::Message(_)));
    assert_eq!(dispatcher.unacked_count(channel), 1);

    let outcome = dispatcher.close_connection(&mut queues, connection);
    assert!(outcome.deleted_queues.is_empty());
    assert_eq!(dispatcher.unacked_count(channel), 0);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 1);

    let connection = ConnectionId::new(2);
    let got = dispatcher
        .get(&mut queues, connection, channel, "jobs", false)
        .expect("get again");
    let wolf_broker::GetOutcome::Message(got) = got else {
        panic!("expected a message");
    };
    assert!(got.redelivered);
}

#[test]
fn deleting_a_queue_forgets_its_consumers_in_the_vhost_dispatcher() {
    let mut queues = QueueRegistry::new();
    let vhost = Vhost::with_routing(
        VhostId::new(1),
        VhostName::new("/").expect("valid vhost"),
        Default::default(),
        {
            queues
                .declare("jobs", QueueConfig::default(), ConnectionId::new(1))
                .expect("declare");
            queues
        },
    );
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);
    vhost
        .consume(
            connection,
            channel,
            consumer,
            "jobs",
            ConsumerOptions::default(),
        )
        .expect("consume");

    let outcome = vhost
        .delete_queue("jobs", connection, false, false)
        .expect("delete");
    assert_eq!(
        outcome.cancels,
        vec![ConsumerCancel {
            consumer,
            reason: CancelReason::QueueDeleted,
        }]
    );

    vhost
        .declare_queue("jobs", QueueConfig::default(), connection)
        .expect("redeclare");
    let mut sink = RecordingSink::default();
    assert_eq!(vhost.dispatch_all(&mut sink).expect("dispatch"), 0);
}
