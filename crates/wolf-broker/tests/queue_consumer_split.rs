use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, DeliverOutcome, Delivery, DeliverySink,
    DeliveryTag, DomainError, GetOutcome, Message, PublishRequest, QueueConfig, Vhost, VhostId,
    VhostName,
};

#[derive(Default)]
struct RecordingSink {
    deliveries: Arc<Mutex<Vec<Delivery>>>,
}

impl DeliverySink for RecordingSink {
    async fn deliver(&mut self, delivery: Delivery) -> DeliverOutcome {
        self.deliveries
            .lock()
            .expect("delivery record lock is not poisoned")
            .push(delivery);
        DeliverOutcome::Delivered
    }

    fn cancel(&mut self, _cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

fn message(body: &'static [u8]) -> Message {
    Message::new(Bytes::from_static(body))
}

fn vhost() -> Vhost {
    Vhost::new(VhostId::new(1), VhostName::new("/").expect("valid vhost"))
}

async fn declare(vhost: &Vhost, connection: ConnectionId, queue: &str) {
    vhost
        .declare_queue(queue, QueueConfig::default(), connection)
        .await
        .expect("declare queue");
}

async fn consume(
    vhost: &Vhost,
    connection: ConnectionId,
    channel: ChannelId,
    consumer: ConsumerId,
    queue: &str,
) {
    vhost
        .consume(
            connection,
            channel,
            consumer,
            queue,
            ConsumerOptions::default(),
        )
        .await
        .expect("consume queue");
}

async fn publish(vhost: &Vhost, connection: ConnectionId, queue: &str, body: &'static [u8]) {
    vhost
        .publish(
            connection,
            PublishRequest {
                exchange: "",
                routing_key: queue,
                mandatory: false,
                message: message(body),
                properties: (),
            },
        )
        .await
        .expect("publish succeeds");
}

async fn dispatch(vhost: &Vhost, queue: &str) -> Vec<Delivery> {
    let record = Arc::new(Mutex::new(Vec::new()));
    let sink = RecordingSink {
        deliveries: Arc::clone(&record),
    };
    vhost
        .dispatch(queue, sink)
        .await
        .expect("dispatch succeeds");
    record
        .lock()
        .expect("delivery record lock is not poisoned")
        .clone()
}

fn consumers(deliveries: &[Delivery]) -> Vec<ConsumerId> {
    deliveries
        .iter()
        .map(|delivery| delivery.consumer)
        .collect()
}

fn bodies(deliveries: &[Delivery]) -> Vec<Bytes> {
    deliveries
        .iter()
        .map(|delivery| delivery.message.body().clone())
        .collect()
}

#[tokio::test]
async fn multiple_consumers_on_one_queue_round_robin_and_settle() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);

    declare(&vhost, connection, "jobs").await;
    consume(&vhost, connection, channel, first, "jobs").await;
    consume(&vhost, connection, channel, second, "jobs").await;
    for body in [b"m1".as_slice(), b"m2", b"m3", b"m4"] {
        publish(&vhost, connection, "jobs", body).await;
    }

    let deliveries = dispatch(&vhost, "jobs").await;
    assert_eq!(consumers(&deliveries), vec![first, second, first, second]);
    assert_eq!(
        bodies(&deliveries),
        vec![
            Bytes::from_static(b"m1"),
            Bytes::from_static(b"m2"),
            Bytes::from_static(b"m3"),
            Bytes::from_static(b"m4"),
        ]
    );

    let last_tag = deliveries[3].delivery_tag;
    let outcome = vhost.ack(channel, last_tag, true).await.expect("ack all");
    assert_eq!(outcome.settled, 4);
    assert_eq!(outcome.requeued, 0);
    assert!(dispatch(&vhost, "jobs").await.is_empty());
}

#[tokio::test]
async fn queues_keep_independent_round_robin_cursors() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);
    let third = ConsumerId::new(3);
    let fourth = ConsumerId::new(4);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, first, "first").await;
    consume(&vhost, connection, channel, second, "first").await;
    consume(&vhost, connection, channel, third, "second").await;
    consume(&vhost, connection, channel, fourth, "second").await;
    for body in [b"a1".as_slice(), b"a2"] {
        publish(&vhost, connection, "first", body).await;
    }
    for body in [b"b1".as_slice(), b"b2"] {
        publish(&vhost, connection, "second", body).await;
    }

    let first_deliveries = dispatch(&vhost, "first").await;
    assert_eq!(consumers(&first_deliveries), vec![first, second]);
    assert_eq!(
        bodies(&first_deliveries),
        vec![Bytes::from_static(b"a1"), Bytes::from_static(b"a2")]
    );

    let second_deliveries = dispatch(&vhost, "second").await;
    assert_eq!(consumers(&second_deliveries), vec![third, fourth]);
    assert_eq!(
        bodies(&second_deliveries),
        vec![Bytes::from_static(b"b1"), Bytes::from_static(b"b2")]
    );

    let outcome = vhost
        .ack(channel, first_deliveries[1].delivery_tag, false)
        .await
        .expect("ack first queue delivery");
    assert_eq!(outcome.settled, 1);

    let outcome = vhost
        .ack(channel, second_deliveries[1].delivery_tag, false)
        .await
        .expect("ack second queue delivery");
    assert_eq!(outcome.settled, 1);
    assert_eq!(vhost.queue_message_count("first"), 0);
    assert_eq!(vhost.queue_message_count("second"), 0);
}

#[tokio::test]
async fn channel_global_prefetch_spans_queues() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;

    let signals = vhost
        .set_prefetch(channel, 1, true)
        .await
        .expect("set global prefetch");
    assert_eq!(signals.len(), 1);

    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    let first_deliveries = dispatch(&vhost, "first").await;
    assert_eq!(first_deliveries.len(), 1);
    assert!(dispatch(&vhost, "second").await.is_empty());

    vhost
        .ack(channel, first_deliveries[0].delivery_tag, false)
        .await
        .expect("ack first queue delivery");

    let second_deliveries = dispatch(&vhost, "second").await;
    assert_eq!(second_deliveries.len(), 1);
    assert_eq!(second_deliveries[0].message.body().as_ref(), b"b");
}

#[tokio::test]
async fn ack_multiple_settles_tags_across_queues() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    let first_deliveries = dispatch(&vhost, "first").await;
    let second_deliveries = dispatch(&vhost, "second").await;
    let first_tag = first_deliveries[0].delivery_tag;
    let second_tag = second_deliveries[0].delivery_tag;

    let outcome = vhost
        .ack(channel, first_tag, true)
        .await
        .expect("ack multiple through first tag");
    assert_eq!(outcome.settled, 1);

    let outcome = vhost
        .ack(channel, second_tag, true)
        .await
        .expect("ack multiple through second tag");
    assert_eq!(outcome.settled, 1);

    assert!(dispatch(&vhost, "first").await.is_empty());
    assert!(dispatch(&vhost, "second").await.is_empty());
}

#[tokio::test]
async fn unknown_tag_fails_without_partial_settlement() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    let first_deliveries = dispatch(&vhost, "first").await;
    let second_deliveries = dispatch(&vhost, "second").await;

    let error = vhost
        .ack(channel, DeliveryTag::new(999), true)
        .await
        .expect_err("unknown tag must fail");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let outcome = vhost
        .ack(channel, first_deliveries[0].delivery_tag, true)
        .await
        .expect("first delivery is still unacked");
    assert_eq!(outcome.settled, 1);

    let outcome = vhost
        .ack(channel, second_deliveries[0].delivery_tag, true)
        .await
        .expect("second delivery is still unacked");
    assert_eq!(outcome.settled, 1);
}

#[tokio::test]
async fn recover_requeues_unacked_across_queues_with_redelivered() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    assert_eq!(dispatch(&vhost, "first").await.len(), 1);
    assert_eq!(dispatch(&vhost, "second").await.len(), 1);

    let outcome = vhost.recover(channel).await.expect("recover");
    assert_eq!(outcome.settled, 2);
    assert_eq!(outcome.requeued, 2);
    assert_eq!(vhost.queue_message_count("first"), 1);
    assert_eq!(vhost.queue_message_count("second"), 1);

    let first_deliveries = dispatch(&vhost, "first").await;
    let second_deliveries = dispatch(&vhost, "second").await;
    assert!(first_deliveries[0].redelivered);
    assert!(second_deliveries[0].redelivered);
}

#[tokio::test]
async fn basic_get_unacked_is_isolated_per_queue() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    let GetOutcome::Message(first) = vhost
        .get(connection, channel, "first", false)
        .await
        .expect("get first")
    else {
        panic!("expected a first message");
    };
    let GetOutcome::Message(second) = vhost
        .get(connection, channel, "second", false)
        .await
        .expect("get second")
    else {
        panic!("expected a second message");
    };

    vhost
        .ack(channel, first.delivery_tag.expect("first tag"), false)
        .await
        .expect("ack first get");

    let outcome = vhost.recover(channel).await.expect("recover");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert_eq!(vhost.queue_message_count("first"), 0);
    assert_eq!(vhost.queue_message_count("second"), 1);

    let GetOutcome::Message(again) = vhost
        .get(connection, channel, "second", false)
        .await
        .expect("get second again")
    else {
        panic!("expected a requeued second message");
    };
    assert!(again.delivery_tag.is_some());
    assert_ne!(again.delivery_tag, second.delivery_tag);
    assert!(again.redelivered);
}

#[tokio::test]
async fn nack_and_reject_requeue_only_their_queue() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    let first_deliveries = dispatch(&vhost, "first").await;
    let second_deliveries = dispatch(&vhost, "second").await;

    let outcome = vhost
        .reject(channel, first_deliveries[0].delivery_tag, true)
        .await
        .expect("reject first queue delivery");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert_eq!(vhost.queue_message_count("first"), 1);
    assert_eq!(vhost.queue_message_count("second"), 0);

    let outcome = vhost
        .nack(channel, second_deliveries[0].delivery_tag, false, true)
        .await
        .expect("nack second queue delivery");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert_eq!(vhost.queue_message_count("second"), 1);
}

#[tokio::test]
async fn cancel_is_isolated_per_queue_and_keeps_unacked_settleable() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, first, "first").await;
    consume(&vhost, connection, channel, second, "second").await;
    publish(&vhost, connection, "first", b"a").await;

    let first_deliveries = dispatch(&vhost, "first").await;
    assert_eq!(first_deliveries.len(), 1);

    assert!(vhost.cancel(first).await.expect("cancel first consumer"));

    publish(&vhost, connection, "first", b"a2").await;
    publish(&vhost, connection, "second", b"b").await;
    assert!(dispatch(&vhost, "first").await.is_empty());

    let second_deliveries = dispatch(&vhost, "second").await;
    assert_eq!(second_deliveries.len(), 1);
    assert_eq!(second_deliveries[0].consumer, second);

    let outcome = vhost
        .ack(channel, first_deliveries[0].delivery_tag, false)
        .await
        .expect("unacked delivery survives the cancel");
    assert_eq!(outcome.settled, 1);
}

#[tokio::test]
async fn deleting_queue_discards_its_unacked_without_affecting_other_queues() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, first, "first").await;
    consume(&vhost, connection, channel, second, "second").await;
    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    let first_deliveries = dispatch(&vhost, "first").await;
    let second_deliveries = dispatch(&vhost, "second").await;

    let outcome = vhost
        .delete_queue("first", connection, false, false)
        .await
        .expect("delete first queue");
    assert_eq!(outcome.cancels.len(), 1);

    let error = vhost
        .ack(channel, first_deliveries[0].delivery_tag, false)
        .await
        .expect_err("deleted queue unacked delivery is gone");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let outcome = vhost
        .ack(channel, second_deliveries[0].delivery_tag, false)
        .await
        .expect("other queue delivery still settles");
    assert_eq!(outcome.settled, 1);
    assert!(dispatch(&vhost, "second").await.is_empty());
}
