use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc::{self, Receiver};
use tokio::time::timeout;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, DeliverOutcome, Delivery,
    DeliveryEvent, DeliverySink, GetOutcome, Message, OutboxSender, PublishRequest, QueueConfig,
    Vhost, VhostId, VhostName,
};

const SAFETY: Duration = Duration::from_secs(10);

#[derive(Default)]
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

fn vhost() -> Vhost {
    Vhost::new(VhostId::new(1), VhostName::new("/").expect("valid vhost"))
}

fn outbox(capacity: usize) -> (OutboxSender, Receiver<DeliveryEvent>) {
    mpsc::channel(capacity)
}

fn recording() -> (RecordingSink, Arc<Mutex<Vec<Delivery>>>) {
    let record = Arc::new(Mutex::new(Vec::new()));
    (
        RecordingSink {
            deliveries: Arc::clone(&record),
        },
        record,
    )
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
    outbox: OutboxSender,
) {
    vhost
        .consume_with_outbox(
            connection,
            channel,
            consumer,
            queue,
            ConsumerOptions::default(),
            outbox,
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
                message: Message::new(Bytes::from_static(body)),
                properties: (),
            },
        )
        .await
        .expect("publish succeeds");
}

async fn dispatch(vhost: &Vhost, queue: &str, sink: RecordingSink) -> usize {
    vhost
        .dispatch(queue, sink)
        .await
        .expect("dispatch succeeds")
}

async fn next_event(receiver: &mut Receiver<DeliveryEvent>) -> DeliveryEvent {
    timeout(SAFETY, receiver.recv())
        .await
        .expect("an outbox event arrives within the safety timeout")
        .expect("the outbox stays open")
}

#[tokio::test]
async fn captured_outbox_receives_delivery_without_calling_the_sink() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let consumer = ConsumerId::new(1);

    declare(&vhost, connection, "jobs").await;
    let (sender, mut receiver) = outbox(4);
    consume(
        &vhost,
        connection,
        ChannelId::new(1),
        consumer,
        "jobs",
        sender,
    )
    .await;
    publish(&vhost, connection, "jobs", b"payload").await;

    let (sink, recorded) = recording();
    let delivered = dispatch(&vhost, "jobs", sink).await;
    assert_eq!(delivered, 1);
    assert!(
        recorded
            .lock()
            .expect("delivery record lock is not poisoned")
            .is_empty(),
        "the captured outbox must bypass the generic sink"
    );

    let DeliveryEvent::Delivery(delivery) = next_event(&mut receiver).await else {
        panic!("expected a delivery event");
    };
    assert_eq!(delivery.consumer, consumer);
    assert_eq!(delivery.message.body().as_ref(), b"payload");
}

#[tokio::test]
async fn closed_outbox_send_returns_closed_and_requeues_on_close_channel() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "jobs").await;
    let (sender, receiver) = outbox(1);
    drop(receiver);
    consume(
        &vhost,
        connection,
        channel,
        ConsumerId::new(1),
        "jobs",
        sender,
    )
    .await;
    publish(&vhost, connection, "jobs", b"requeue").await;

    let (sink, recorded) = recording();
    let delivered = dispatch(&vhost, "jobs", sink).await;
    assert_eq!(
        delivered, 0,
        "a closed outbox stops dispatching before counting the delivery"
    );
    assert!(
        recorded
            .lock()
            .expect("delivery record lock is not poisoned")
            .is_empty(),
        "a captured outbox is not replaced by the generic sink"
    );
    assert_eq!(vhost.queue_message_count("jobs"), 0);

    let outcome = vhost.close_channel(channel).await.expect("close channel");
    assert_eq!(outcome.settled, 1);
    assert_eq!(outcome.requeued, 1);
    assert_eq!(vhost.queue_message_count("jobs"), 1);

    let GetOutcome::Message(message) = vhost
        .get(connection, ChannelId::new(2), "jobs", true)
        .await
        .expect("get requeued message")
    else {
        panic!("expected the requeued message");
    };
    assert!(message.redelivered);
    assert_eq!(message.message.body().as_ref(), b"requeue");
}

#[tokio::test]
async fn cancel_removes_captured_outbox_and_stops_dispatch() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);

    declare(&vhost, connection, "jobs").await;
    let (sender, mut receiver) = outbox(4);
    consume(&vhost, connection, channel, consumer, "jobs", sender).await;

    publish(&vhost, connection, "jobs", b"first").await;
    let (sink, _recorded) = recording();
    assert_eq!(dispatch(&vhost, "jobs", sink).await, 1);
    let DeliveryEvent::Delivery(first) = next_event(&mut receiver).await else {
        panic!("expected the first delivery");
    };
    assert_eq!(first.message.body().as_ref(), b"first");

    assert!(vhost.cancel(consumer).await.expect("cancel consumer"));

    publish(&vhost, connection, "jobs", b"second").await;
    let (sink, recorded) = recording();
    assert_eq!(dispatch(&vhost, "jobs", sink).await, 0);
    assert!(
        recorded
            .lock()
            .expect("delivery record lock is not poisoned")
            .is_empty()
    );
    assert_eq!(vhost.queue_message_count("jobs"), 1);
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
}

#[tokio::test]
async fn captured_outbox_prefetch_and_ack_round_trip() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let consumer = ConsumerId::new(1);

    declare(&vhost, connection, "jobs").await;
    let (sender, mut receiver) = outbox(4);
    consume(&vhost, connection, channel, consumer, "jobs", sender).await;
    vhost
        .set_prefetch(channel, 1, false)
        .await
        .expect("set consumer prefetch");

    publish(&vhost, connection, "jobs", b"first").await;
    publish(&vhost, connection, "jobs", b"second").await;

    let (sink, _recorded) = recording();
    assert_eq!(dispatch(&vhost, "jobs", sink).await, 1);
    let DeliveryEvent::Delivery(first) = next_event(&mut receiver).await else {
        panic!("expected the first delivery");
    };
    assert_eq!(first.consumer, consumer);
    assert_eq!(first.message.body().as_ref(), b"first");

    let outcome = vhost
        .ack(channel, first.delivery_tag, false)
        .await
        .expect("ack first delivery");
    assert_eq!(outcome.settled, 1);

    let (sink, _recorded) = recording();
    assert_eq!(dispatch(&vhost, "jobs", sink).await, 1);
    let DeliveryEvent::Delivery(second) = next_event(&mut receiver).await else {
        panic!("expected the second delivery");
    };
    assert_eq!(second.consumer, consumer);
    assert_eq!(second.message.body().as_ref(), b"second");
    assert_ne!(second.delivery_tag, first.delivery_tag);

    let outcome = vhost
        .ack(channel, second.delivery_tag, false)
        .await
        .expect("ack second delivery");
    assert_eq!(outcome.settled, 1);
}

#[tokio::test]
async fn close_connection_drops_captured_outbox_and_requeues() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "jobs").await;
    let (sender, mut receiver) = outbox(4);
    consume(
        &vhost,
        connection,
        channel,
        ConsumerId::new(1),
        "jobs",
        sender,
    )
    .await;
    publish(&vhost, connection, "jobs", b"first").await;
    publish(&vhost, connection, "jobs", b"second").await;

    let (sink, _recorded) = recording();
    assert_eq!(dispatch(&vhost, "jobs", sink).await, 2);
    let DeliveryEvent::Delivery(first) = next_event(&mut receiver).await else {
        panic!("expected the first delivery");
    };
    let DeliveryEvent::Delivery(second) = next_event(&mut receiver).await else {
        panic!("expected the second delivery");
    };
    assert_eq!(first.message.body().as_ref(), b"first");
    assert_eq!(second.message.body().as_ref(), b"second");

    vhost.close_connection(connection).await;
    assert_eq!(vhost.queue_message_count("jobs"), 2);

    let (sink, recorded) = recording();
    assert_eq!(dispatch(&vhost, "jobs", sink).await, 0);
    assert!(
        recorded
            .lock()
            .expect("delivery record lock is not poisoned")
            .is_empty()
    );
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
}
