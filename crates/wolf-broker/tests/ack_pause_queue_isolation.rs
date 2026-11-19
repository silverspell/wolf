#![cfg(feature = "test-hooks")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::timeout;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    AckPauseHook, ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, DeliverOutcome, Delivery,
    DeliverySink, Message, PublishRequest, QueueConfig, Vhost, VhostId, VhostName,
};

const SAFETY: Duration = Duration::from_secs(10);

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

async fn dispatch(vhost: &Vhost, queue: &str) -> oneshot::Receiver<Delivery> {
    let (delivered_tx, delivered_rx) = oneshot::channel();
    let sink = SignalSink {
        delivered: Some(delivered_tx),
    };
    timeout(SAFETY, vhost.dispatch(queue, sink))
        .await
        .expect("dispatch within the safety timeout")
        .expect("dispatch succeeds");
    delivered_rx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_ack_on_a_does_not_block_b_delivery() {
    let connection = ConnectionId::new(1);
    let channel_a = ChannelId::new(1);
    let channel_b = ChannelId::new(2);
    let consumer_a = ConsumerId::new(1);
    let consumer_b = ConsumerId::new(2);

    let (hook, mut handle) = AckPauseHook::pair();
    let vhost = Arc::new(vhost().with_ack_pause_hook(hook));

    vhost
        .declare_queue("a", QueueConfig::default(), connection)
        .await
        .expect("declare a");
    vhost
        .declare_queue("b", QueueConfig::default(), connection)
        .await
        .expect("declare b");

    vhost
        .consume(
            connection,
            channel_a,
            consumer_a,
            "a",
            ConsumerOptions::default(),
        )
        .await
        .expect("consume a");
    vhost
        .consume(
            connection,
            channel_b,
            consumer_b,
            "b",
            ConsumerOptions::default(),
        )
        .await
        .expect("consume b");

    publish(&vhost, connection, "a", b"message-a").await;
    let a_delivered_rx = dispatch(&vhost, "a").await;
    let delivered_a = timeout(SAFETY, a_delivered_rx)
        .await
        .expect("A is delivered within the safety timeout")
        .expect("A delivery signal");
    assert_eq!(delivered_a.consumer, consumer_a);
    assert_eq!(delivered_a.message.body().as_ref(), b"message-a");
    let tag = delivered_a.delivery_tag;

    let ack_vhost = Arc::clone(&vhost);
    let ack = tokio::spawn(async move { ack_vhost.ack(channel_a, tag, false).await });

    assert!(
        timeout(SAFETY, handle.wait_entered())
            .await
            .expect("ack reaches the pause hook"),
        "pause hook reported entry"
    );

    publish(&vhost, connection, "b", b"message-b").await;
    let b_delivered_rx = dispatch(&vhost, "b").await;
    let delivered_b = timeout(SAFETY, b_delivered_rx)
        .await
        .expect("B is delivered while A's ack is paused")
        .expect("B delivery signal");
    assert_eq!(delivered_b.consumer, consumer_b);
    assert_eq!(delivered_b.message.body().as_ref(), b"message-b");
    assert!(!ack.is_finished(), "A's ack must still be paused");

    assert!(handle.release(), "a paused ack is released");

    let outcome = timeout(SAFETY, ack)
        .await
        .expect("ack completes after release")
        .expect("ack task joins")
        .expect("ack succeeds");
    assert_eq!(outcome.settled, 1);
}
