//! Bounded per-connection outbox and dispatcher backpressure contract.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::timeout;
use wolf_broker::{
    ChannelId, ConnectionId, ConsumerCancel, ConsumerId, ConsumerOptions, DeliverOutcome, Delivery,
    DeliverySink, Message, PublishRequest, QueueConfig, Vhost, VhostId, VhostName,
};
use wolf_server::delivery::{DeliveryHub, DeliveryHubSink, ServerEvent};

const SAFETY: Duration = Duration::from_secs(10);

struct SignalSink {
    hub: DeliveryHubSink,
    signal: Option<oneshot::Sender<()>>,
    deliveries: usize,
    signal_at: usize,
}

impl SignalSink {
    fn new(hub: DeliveryHubSink, signal: oneshot::Sender<()>, signal_at: usize) -> Self {
        Self {
            hub,
            signal: Some(signal),
            deliveries: 0,
            signal_at,
        }
    }
}

impl DeliverySink for SignalSink {
    fn deliver(&mut self, delivery: Delivery) -> impl Future<Output = DeliverOutcome> + Send {
        self.deliveries += 1;
        if self.deliveries == self.signal_at {
            if let Some(signal) = self.signal.take() {
                let _ = signal.send(());
            }
        }
        self.hub.deliver(delivery)
    }

    fn cancel(&mut self, cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        self.hub.cancel(cancel)
    }
}

fn vhost() -> Vhost {
    Vhost::new(VhostId::new(1), VhostName::new("/").expect("valid vhost"))
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
        .expect("consume succeeds");
}

#[tokio::test]
async fn bounded_outbox_isolates_queues() {
    let vhost = Arc::new(vhost());
    let connection = ConnectionId::new(1);
    vhost
        .declare_queue("a", QueueConfig::default(), connection)
        .await
        .expect("declare a");
    vhost
        .declare_queue("b", QueueConfig::default(), connection)
        .await
        .expect("declare b");

    let hub = DeliveryHub::with_capacity(2);
    let consumer_a = ConsumerId::new(101);
    let consumer_b = ConsumerId::new(102);
    consume(&vhost, connection, ChannelId::new(1), consumer_a, "a").await;
    consume(&vhost, connection, ChannelId::new(2), consumer_b, "b").await;

    let (a_sender, a_rx) = hub.subscribe();
    let (b_sender, mut b_rx) = hub.subscribe();
    hub.register(consumer_a, a_sender);
    hub.register(consumer_b, b_sender);

    for body in [b"1".as_slice(), b"2", b"3", b"4", b"5"] {
        publish(&vhost, connection, "a", body).await;
    }

    let (signal_tx, signal_rx) = oneshot::channel();
    let a_task = tokio::spawn({
        let vhost = Arc::clone(&vhost);
        let sink = SignalSink::new(hub.sink(), signal_tx, 3);
        async move { vhost.dispatch("a", sink).await }
    });

    timeout(SAFETY, signal_rx)
        .await
        .expect("the third delivery reaches the sink within the safety timeout")
        .expect("the third delivery signal");
    assert!(
        !a_task.is_finished(),
        "A must stay blocked while its bounded outbox is full"
    );
    assert_eq!(
        a_rx.len(),
        2,
        "the bounded outbox holds exactly its configured capacity"
    );

    publish(&vhost, connection, "b", b"b").await;
    let (b_signal_tx, b_signal_rx) = oneshot::channel();
    let b_sink = SignalSink::new(hub.sink(), b_signal_tx, 1);
    let dispatched = timeout(SAFETY, vhost.dispatch("b", b_sink))
        .await
        .expect("B dispatch does not wait for the blocked A queue")
        .expect("dispatch b");
    assert_eq!(dispatched, 1);
    timeout(SAFETY, b_signal_rx)
        .await
        .expect("B is delivered within the safety timeout")
        .expect("B delivery signal");
    assert!(matches!(
        b_rx.try_recv().expect("B outbox event"),
        ServerEvent::Delivery(_)
    ));
    assert!(
        !a_task.is_finished(),
        "A is still blocked after B completes"
    );

    a_task.abort();
    let _ = a_task.await;
}

#[tokio::test]
async fn bounded_outbox_preserves_order() {
    let vhost = Arc::new(vhost());
    let connection = ConnectionId::new(1);
    vhost
        .declare_queue("a", QueueConfig::default(), connection)
        .await
        .expect("declare a");

    let hub = DeliveryHub::with_capacity(2);
    let consumer = ConsumerId::new(201);
    consume(&vhost, connection, ChannelId::new(1), consumer, "a").await;

    let (sender, mut receiver) = hub.subscribe();
    hub.register(consumer, sender);

    let bodies = [b"1".as_slice(), b"2", b"3", b"4", b"5"];
    for body in bodies {
        publish(&vhost, connection, "a", body).await;
    }

    let (signal_tx, signal_rx) = oneshot::channel();
    let task = tokio::spawn({
        let vhost = Arc::clone(&vhost);
        let sink = SignalSink::new(hub.sink(), signal_tx, 3);
        async move { vhost.dispatch("a", sink).await }
    });

    timeout(SAFETY, signal_rx)
        .await
        .expect("the third delivery reaches the sink within the safety timeout")
        .expect("the third delivery signal");

    for (index, expected) in bodies.iter().enumerate() {
        let event = timeout(SAFETY, receiver.recv())
            .await
            .expect("each delivery arrives within the safety timeout")
            .expect("the outbox stays open");
        let ServerEvent::Delivery(delivery) = event else {
            panic!("expected a delivery event, got {event:?}");
        };
        assert_eq!(
            delivery.message.body().as_ref(),
            *expected,
            "delivery {} preserves publish order",
            index + 1
        );
    }

    let dispatched = timeout(SAFETY, task)
        .await
        .expect("the dispatch task finishes within the safety timeout")
        .expect("the dispatch task did not panic")
        .expect("the dispatch succeeds");
    assert_eq!(dispatched, 5);
}
