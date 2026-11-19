use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Barrier, oneshot};
use tokio::time::timeout;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, DeliverOutcome, Delivery, DeliverySink,
    DomainError, GetOutcome, Message, PublishRequest, QueueConfig, Vhost, VhostId, VhostName,
};

const SAFETY: Duration = Duration::from_secs(10);

struct BlockingSink {
    barrier: Arc<Barrier>,
    entered: Option<oneshot::Sender<()>>,
}

impl DeliverySink for BlockingSink {
    async fn deliver(&mut self, _delivery: Delivery) -> DeliverOutcome {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        self.barrier.wait().await;
        DeliverOutcome::Delivered
    }

    fn cancel(&mut self, _cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

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

async fn block_delivery(
    vhost: &Arc<Vhost>,
    queue: &str,
) -> (
    oneshot::Receiver<()>,
    tokio::task::JoinHandle<Result<usize, DomainError>>,
    Arc<Barrier>,
) {
    publish(vhost, ConnectionId::new(1), queue, b"blocked").await;
    let barrier = Arc::new(Barrier::new(2));
    let (entered_tx, entered_rx) = oneshot::channel();
    let sink = BlockingSink {
        barrier: Arc::clone(&barrier),
        entered: Some(entered_tx),
    };
    let vhost = Arc::clone(vhost);
    let queue = queue.to_string();
    let task = tokio::spawn(async move { vhost.dispatch(queue, sink).await });
    (entered_rx, task, barrier)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recover_on_one_channel_does_not_wait_for_a_blocked_delivery_on_another() {
    let vhost = Arc::new(vhost());
    let connection = ConnectionId::new(1);
    let first_channel = ChannelId::new(1);
    let second_channel = ChannelId::new(2);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(
        &vhost,
        connection,
        first_channel,
        ConsumerId::new(1),
        "first",
    )
    .await;
    consume(
        &vhost,
        connection,
        second_channel,
        ConsumerId::new(2),
        "second",
    )
    .await;
    publish(&vhost, connection, "first", b"a").await;
    assert_eq!(dispatch(&vhost, "first").await.len(), 1);

    let (entered, task, barrier) = block_delivery(&vhost, "second").await;
    timeout(SAFETY, entered)
        .await
        .expect("second channel reaches its blocked delivery")
        .expect("blocked delivery signalled");

    let outcome = timeout(SAFETY, vhost.recover(first_channel))
        .await
        .expect("recover does not wait for the other channel")
        .expect("recover succeeds");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert!(!task.is_finished(), "the other channel is still blocked");

    barrier.wait().await;
    timeout(SAFETY, task)
        .await
        .expect("blocked dispatch finishes after the barrier is released")
        .expect("blocked dispatch did not panic")
        .expect("blocked dispatch succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_channel_does_not_wait_for_another_channels_blocked_delivery() {
    let vhost = Arc::new(vhost());
    let connection = ConnectionId::new(1);
    let first_channel = ChannelId::new(1);
    let second_channel = ChannelId::new(2);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(
        &vhost,
        connection,
        first_channel,
        ConsumerId::new(1),
        "first",
    )
    .await;
    consume(
        &vhost,
        connection,
        second_channel,
        ConsumerId::new(2),
        "second",
    )
    .await;
    publish(&vhost, connection, "first", b"a").await;
    assert_eq!(dispatch(&vhost, "first").await.len(), 1);

    let (entered, task, barrier) = block_delivery(&vhost, "second").await;
    timeout(SAFETY, entered)
        .await
        .expect("second channel reaches its blocked delivery")
        .expect("blocked delivery signalled");

    let outcome = timeout(SAFETY, vhost.close_channel(first_channel))
        .await
        .expect("close_channel does not wait for the other channel")
        .expect("close_channel succeeds");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert!(!task.is_finished(), "the other channel is still blocked");

    barrier.wait().await;
    timeout(SAFETY, task)
        .await
        .expect("blocked dispatch finishes after the barrier is released")
        .expect("blocked dispatch did not panic")
        .expect("blocked dispatch succeeds");
}

#[tokio::test]
async fn queue_delete_removes_only_its_tags_from_the_channel_index() {
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

    let outcome = vhost
        .delete_queue("first", connection, false, false)
        .await
        .expect("delete first queue");
    assert_eq!(outcome.cancels.len(), 1);

    let error = vhost
        .ack(channel, first[0].delivery_tag, false)
        .await
        .expect_err("deleted queue delivery is gone");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let outcome = vhost
        .ack(channel, second[0].delivery_tag, false)
        .await
        .expect("other queue delivery still settles");
    assert_eq!(outcome.settled, 1);
}

#[tokio::test]
async fn cancel_keeps_unacked_delivery_settleable_and_does_not_remove_other_queue_state() {
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

    assert!(vhost.cancel(first).await.expect("cancel first consumer"));
    assert!(!vhost.cancel(first).await.expect("idempotent cancel"));

    publish(&vhost, connection, "first", b"a2").await;
    publish(&vhost, connection, "second", b"b2").await;
    assert!(dispatch(&vhost, "first").await.is_empty());

    let deliveries = dispatch(&vhost, "second").await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].consumer, second);
    assert_eq!(deliveries[0].message.body().as_ref(), b"b2");

    let outcome = vhost
        .ack(channel, first_deliveries[0].delivery_tag, false)
        .await
        .expect("unacked delivery survives cancel");
    assert_eq!(outcome.settled, 1);

    let outcome = vhost
        .ack(channel, second_deliveries[0].delivery_tag, false)
        .await
        .expect("other queue delivery survives cancel");
    assert_eq!(outcome.settled, 1);
}

#[tokio::test]
async fn basic_get_no_ack_does_not_enter_the_channel_index() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "jobs").await;
    publish(&vhost, connection, "jobs", b"m1").await;
    publish(&vhost, connection, "jobs", b"m2").await;

    let GetOutcome::Message(no_ack) = vhost
        .get(connection, channel, "jobs", true)
        .await
        .expect("no_ack get")
    else {
        panic!("expected a message");
    };
    let no_ack_tag = no_ack.delivery_tag.expect("no_ack get still has a tag");

    let outcome = vhost.recover(channel).await.expect("recover empty index");
    assert_eq!(outcome.settled, 0);
    assert_eq!(outcome.requeued, 0);
    assert_eq!(vhost.queue_message_count("jobs"), 1);

    let error = vhost
        .ack(channel, no_ack_tag, false)
        .await
        .expect_err("no_ack delivery never entered the index");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let GetOutcome::Message(ack_required) = vhost
        .get(connection, channel, "jobs", false)
        .await
        .expect("ack-required get")
    else {
        panic!("expected a message");
    };
    let tag = ack_required
        .delivery_tag
        .expect("ack-required get has a tag");
    assert_ne!(tag, no_ack_tag);

    let outcome = vhost.recover(channel).await.expect("recover tracked get");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert_eq!(vhost.queue_message_count("jobs"), 1);
}
