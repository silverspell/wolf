use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Barrier, oneshot};
use tokio::time::timeout;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, DeliverOutcome, Delivery, DeliverySink,
    Message, PublishRequest, QueueConfig, Vhost, VhostId, VhostName,
};

const SAFETY: Duration = Duration::from_secs(10);

struct BlockingSink {
    barrier: Arc<Barrier>,
    entered: Option<oneshot::Sender<()>>,
    delivered: Option<oneshot::Sender<Delivery>>,
}

impl DeliverySink for BlockingSink {
    async fn deliver(&mut self, delivery: Delivery) -> DeliverOutcome {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        self.barrier.wait().await;
        if let Some(delivered) = self.delivered.take() {
            let _ = delivered.send(delivery);
        }
        DeliverOutcome::Delivered
    }

    fn cancel(&mut self, _cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        std::future::ready(())
    }
}

struct SignalSink {
    delivered: Option<oneshot::Sender<Delivery>>,
}

impl DeliverySink for SignalSink {
    async fn deliver(&mut self, delivery: Delivery) -> DeliverOutcome {
        if let Some(delivered) = self.delivered.take() {
            let _ = delivered.send(delivery);
        }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blocked_delivery_does_not_block_another_queue_or_router_reads() {
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

    let consumer_a = ConsumerId::new(1);
    let consumer_b = ConsumerId::new(2);
    vhost
        .consume(
            connection,
            ChannelId::new(1),
            consumer_a,
            "a",
            ConsumerOptions::default(),
        )
        .await
        .expect("consume a");
    vhost
        .consume(
            connection,
            ChannelId::new(2),
            consumer_b,
            "b",
            ConsumerOptions::default(),
        )
        .await
        .expect("consume b");

    publish(&vhost, connection, "a", b"message-a").await;

    let barrier = Arc::new(Barrier::new(2));
    let (a_entered_tx, a_entered_rx) = oneshot::channel();
    let (a_delivered_tx, a_delivered_rx) = oneshot::channel();
    let a_sink = BlockingSink {
        barrier: Arc::clone(&barrier),
        entered: Some(a_entered_tx),
        delivered: Some(a_delivered_tx),
    };

    let vhost_a = Arc::clone(&vhost);
    let a_task = tokio::spawn(async move { vhost_a.dispatch("a", a_sink).await });

    timeout(SAFETY, a_entered_rx)
        .await
        .expect("A reached the barrier within the safety timeout")
        .expect("A entered its delivery");

    let (b_delivered_tx, b_delivered_rx) = oneshot::channel();
    let b_sink = SignalSink {
        delivered: Some(b_delivered_tx),
    };

    timeout(SAFETY, async {
        publish(&vhost, connection, "b", b"message-b").await;
        vhost.dispatch("b", b_sink).await.expect("dispatch b")
    })
    .await
    .expect("router resolution and B delivery do not wait for A");

    let delivered_b = timeout(SAFETY, b_delivered_rx)
        .await
        .expect("B is signalled while A is still blocked")
        .expect("B delivery signal");
    assert_eq!(delivered_b.consumer, consumer_b);
    assert_eq!(delivered_b.message.body().as_ref(), b"message-b");
    assert!(
        !a_task.is_finished(),
        "A must still be blocked at the barrier when B completes"
    );

    barrier.wait().await;

    let delivered_a = timeout(SAFETY, a_delivered_rx)
        .await
        .expect("A is delivered after the barrier is released")
        .expect("A delivery signal");
    assert_eq!(delivered_a.consumer, consumer_a);
    assert_eq!(delivered_a.message.body().as_ref(), b"message-a");

    let delivered = timeout(SAFETY, a_task)
        .await
        .expect("A dispatch finishes within the safety timeout")
        .expect("A dispatch task did not panic")
        .expect("A dispatch succeeds");
    assert_eq!(delivered, 1);
}
