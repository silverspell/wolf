use std::future::Future;

use bytes::Bytes;
use wolf_broker::publish::{AckState, GetOutcome, get_message};
use wolf_broker::queue::{ConnectionId, QueueConfig, QueueRegistry};
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerDispatcher, ConsumerId, DeliverOutcome, Delivery,
    DeliverySink, DomainError, Message,
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

fn declare(queues: &mut QueueRegistry, connection: ConnectionId) {
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");
}

#[tokio::test]
async fn get_message_returns_message_and_required_ack_state() {
    let mut queues = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    declare(&mut queues, connection);
    queues
        .publish("jobs", connection, message("first"))
        .expect("publish first");
    queues
        .publish("jobs", connection, message("second"))
        .expect("publish second");

    let outcome = get_message(&mut queues, connection, "jobs", false).expect("get message");

    match outcome {
        GetOutcome::Message(got) => {
            assert_eq!(got.message.body().as_ref(), b"first");
            assert!(!got.redelivered);
            assert_eq!(got.ack_state, AckState::Required);
            assert_eq!(got.remaining, 1);
        }
        other => panic!("expected a message, got {other:?}"),
    }
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 1);
}

#[tokio::test]
async fn get_message_reports_not_required_when_no_ack_is_true() {
    let mut queues = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    declare(&mut queues, connection);
    queues
        .publish("jobs", connection, message("only"))
        .expect("publish");

    let outcome = get_message(&mut queues, connection, "jobs", true).expect("get message");

    match outcome {
        GetOutcome::Message(got) => {
            assert_eq!(got.message.body().as_ref(), b"only");
            assert!(!got.redelivered);
            assert_eq!(got.ack_state, AckState::NotRequired);
            assert_eq!(got.remaining, 0);
        }
        other => panic!("expected a message, got {other:?}"),
    }
}

#[tokio::test]
async fn get_message_on_empty_queue_is_empty() {
    let mut queues = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    declare(&mut queues, connection);

    let outcome = get_message(&mut queues, connection, "jobs", false).expect("get message");

    assert_eq!(outcome, GetOutcome::Empty);
    assert_eq!(queues.queue("jobs").expect("queue").message_count(), 0);
}

#[tokio::test]
async fn get_message_preserves_redelivered_flag() {
    let mut queues = QueueRegistry::new();
    let mut dispatcher = ConsumerDispatcher::new();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);
    declare(&mut queues, connection);
    dispatcher
        .register(&mut queues, connection, channel, consumer, "jobs")
        .expect("register consumer");
    queues
        .publish("jobs", connection, message("again"))
        .expect("publish");

    let mut sink = RecordingSink::default();
    assert_eq!(
        dispatcher
            .dispatch(&mut queues, "jobs", &mut sink)
            .await
            .expect("dispatch"),
        1
    );
    let tag = sink.deliveries[0].delivery_tag;
    let settled = dispatcher
        .nack(&mut queues, channel, tag, false, true)
        .expect("nack with requeue");
    assert_eq!(settled.requeued, 1);

    let outcome = get_message(&mut queues, connection, "jobs", false).expect("get message");

    match outcome {
        GetOutcome::Message(got) => {
            assert_eq!(got.message.body().as_ref(), b"again");
            assert!(got.redelivered);
            assert_eq!(got.remaining, 0);
        }
        other => panic!("expected a message, got {other:?}"),
    }
}

#[tokio::test]
async fn get_message_checks_queue_existence_and_exclusive_access() {
    let mut queues = QueueRegistry::new();
    let owner = ConnectionId::new(1);
    let intruder = ConnectionId::new(2);

    let error = get_message(&mut queues, owner, "missing", false).expect_err("missing queue");
    assert_eq!(
        error,
        DomainError::QueueNotFound {
            vhost: String::new(),
            queue: "missing".to_string(),
        }
    );

    let config = QueueConfig {
        exclusive: true,
        ..QueueConfig::default()
    };
    let name = queues
        .declare("private", config, owner)
        .expect("declare exclusive queue");
    queues
        .publish(name.as_str(), owner, message("secret"))
        .expect("publish to exclusive queue");

    let error = get_message(&mut queues, intruder, name.as_str(), false)
        .expect_err("exclusive access must be denied");
    assert!(matches!(error, DomainError::ResourceLocked { .. }));
    assert_eq!(
        queues.queue(name.as_str()).expect("queue").message_count(),
        1
    );
}
