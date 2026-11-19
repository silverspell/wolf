use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, DeliverOutcome, Delivery, DeliverySink,
    DeliveryTag, DomainError, Message, PublishRequest, QueueConfig, Vhost, VhostId, VhostName,
};

struct RecordingSink {
    deliveries: Arc<Mutex<Vec<Delivery>>>,
}

impl DeliverySink for RecordingSink {
    fn deliver(&mut self, delivery: Delivery) -> impl Future<Output = DeliverOutcome> + Send {
        self.deliveries
            .lock()
            .expect("delivery record lock is not poisoned")
            .push(delivery);
        std::future::ready(DeliverOutcome::Delivered)
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

fn bodies(deliveries: &[Delivery]) -> Vec<Bytes> {
    deliveries
        .iter()
        .map(|delivery| delivery.message.body().clone())
        .collect()
}

#[tokio::test]
async fn ack_multiple_settles_in_increasing_tag_order_across_queues() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a1").await;
    publish(&vhost, connection, "first", b"a2").await;
    publish(&vhost, connection, "second", b"b1").await;
    publish(&vhost, connection, "second", b"b2").await;

    let first = dispatch(&vhost, "first").await;
    let second = dispatch(&vhost, "second").await;
    let cut = second[0].delivery_tag;

    let outcome = vhost
        .ack(channel, cut, true)
        .await
        .expect("ack multiple through first second-queue tag");
    assert_eq!(outcome.settled, 3);
    assert_eq!(outcome.requeued, 0);

    assert_eq!(vhost.queue_message_count("first"), 0);
    assert_eq!(vhost.queue_message_count("second"), 0);

    let remaining = vhost
        .ack(channel, second[1].delivery_tag, false)
        .await
        .expect("last delivery still settles");
    assert_eq!(remaining.settled, 1);

    let error = vhost
        .ack(channel, first[0].delivery_tag, false)
        .await
        .expect_err("already settled tag is unknown");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));
}

#[tokio::test]
async fn ack_multiple_unknown_cut_tag_settles_nothing() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a").await;
    publish(&vhost, connection, "second", b"b").await;

    let first = dispatch(&vhost, "first").await;
    let second = dispatch(&vhost, "second").await;

    let error = vhost
        .ack(channel, DeliveryTag::new(999), true)
        .await
        .expect_err("unknown cut tag must fail");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let outcome = vhost
        .ack(channel, first[0].delivery_tag, true)
        .await
        .expect("first delivery was not partially settled");
    assert_eq!(outcome.settled, 1);
    let outcome = vhost
        .ack(channel, second[0].delivery_tag, true)
        .await
        .expect("second delivery was not partially settled");
    assert_eq!(outcome.settled, 1);
}

#[tokio::test]
async fn ack_twice_second_ack_is_unknown_delivery_tag() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "jobs").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "jobs").await;
    publish(&vhost, connection, "jobs", b"m").await;
    let deliveries = dispatch(&vhost, "jobs").await;
    let tag = deliveries[0].delivery_tag;

    let outcome = vhost.ack(channel, tag, false).await.expect("first ack");
    assert_eq!(outcome.settled, 1);

    let error = vhost
        .ack(channel, tag, false)
        .await
        .expect_err("second ack is unknown");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let error = vhost
        .ack(channel, tag, true)
        .await
        .expect_err("multiple ack is unknown too");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));
}

#[tokio::test]
async fn nack_multiple_requeues_in_ascending_tag_order_per_queue() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a1").await;
    publish(&vhost, connection, "first", b"a2").await;
    publish(&vhost, connection, "second", b"b1").await;

    assert_eq!(dispatch(&vhost, "first").await.len(), 2);
    let second = dispatch(&vhost, "second").await;
    let cut = second[0].delivery_tag;

    let outcome = vhost
        .nack(channel, cut, true, true)
        .await
        .expect("nack multiple with requeue");
    assert_eq!(outcome.settled, 3);
    assert_eq!(outcome.requeued, 3);
    assert_eq!(vhost.queue_message_count("first"), 2);
    assert_eq!(vhost.queue_message_count("second"), 1);

    let first = dispatch(&vhost, "first").await;
    assert_eq!(
        bodies(&first),
        vec![Bytes::from_static(b"a1"), Bytes::from_static(b"a2")]
    );
    assert!(first.iter().all(|delivery| delivery.redelivered));
    let second = dispatch(&vhost, "second").await;
    assert_eq!(bodies(&second), vec![Bytes::from_static(b"b1")]);
    assert!(second[0].redelivered);
}

#[tokio::test]
async fn reject_without_requeue_removes_only_one_tag_from_the_channel_index() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "jobs").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "jobs").await;
    publish(&vhost, connection, "jobs", b"m1").await;
    publish(&vhost, connection, "jobs", b"m2").await;
    let deliveries = dispatch(&vhost, "jobs").await;
    let first = deliveries[0].delivery_tag;
    let second = deliveries[1].delivery_tag;

    let outcome = vhost
        .reject(channel, first, false)
        .await
        .expect("reject without requeue");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 0);

    let error = vhost
        .ack(channel, first, false)
        .await
        .expect_err("rejected tag is gone");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let outcome = vhost
        .ack(channel, second, false)
        .await
        .expect("other tag is still present");
    assert_eq!(outcome.settled, 1);
    assert_eq!(vhost.queue_message_count("jobs"), 0);
}

#[tokio::test]
async fn recover_requeues_all_channel_tags_with_redelivered() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    publish(&vhost, connection, "first", b"a1").await;
    publish(&vhost, connection, "first", b"a2").await;
    publish(&vhost, connection, "second", b"b1").await;

    assert_eq!(dispatch(&vhost, "first").await.len(), 2);
    assert_eq!(dispatch(&vhost, "second").await.len(), 1);

    let outcome = vhost.recover(channel).await.expect("recover");
    assert_eq!(outcome.settled, 3);
    assert_eq!(outcome.requeued, 3);
    assert_eq!(vhost.queue_message_count("first"), 2);
    assert_eq!(vhost.queue_message_count("second"), 1);

    let first = dispatch(&vhost, "first").await;
    assert_eq!(
        bodies(&first),
        vec![Bytes::from_static(b"a1"), Bytes::from_static(b"a2")]
    );
    assert!(first.iter().all(|delivery| delivery.redelivered));
    let second = dispatch(&vhost, "second").await;
    assert_eq!(bodies(&second), vec![Bytes::from_static(b"b1")]);
    assert!(second[0].redelivered);
}
