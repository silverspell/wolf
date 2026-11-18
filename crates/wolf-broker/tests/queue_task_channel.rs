use std::future::poll_fn;
use std::sync::{Arc, Barrier, Mutex};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
use tokio::time::timeout;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, Delivery, DeliverySink, Message,
    PublishRequest, QueueConfig, Vhost, VhostConfig, VhostId, VhostName,
};

const SAFETY: Duration = Duration::from_secs(10);

struct BlockingSink {
    barrier: Arc<Barrier>,
    entered: Option<oneshot::Sender<()>>,
    delivered: Option<oneshot::Sender<Bytes>>,
    record: Arc<Mutex<Vec<Bytes>>>,
}

impl DeliverySink for BlockingSink {
    fn deliver(&mut self, delivery: Delivery) {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(());
        }
        self.barrier.wait();
        let body = delivery.message.body().clone();
        self.record
            .lock()
            .expect("delivery record lock is not poisoned")
            .push(body.clone());
        if let Some(delivered) = self.delivered.take() {
            let _ = delivered.send(body);
        }
    }

    fn cancel(&mut self, _cancel: ConsumerCancel) {}
}

struct SignalSink {
    delivered: Option<oneshot::Sender<Bytes>>,
    record: Arc<Mutex<Vec<Bytes>>>,
}

impl DeliverySink for SignalSink {
    fn deliver(&mut self, delivery: Delivery) {
        let body = delivery.message.body().clone();
        self.record
            .lock()
            .expect("delivery record lock is not poisoned")
            .push(body.clone());
        if let Some(delivered) = self.delivered.take() {
            let _ = delivered.send(body);
        }
    }

    fn cancel(&mut self, _cancel: ConsumerCancel) {}
}

fn message(body: &'static [u8]) -> Message {
    Message::new(Bytes::from_static(body))
}

async fn publish(vhost: &Vhost, connection: ConnectionId, body: &'static [u8]) {
    vhost
        .publish(
            connection,
            PublishRequest {
                exchange: "",
                routing_key: "jobs",
                mandatory: false,
                message: message(body),
                properties: (),
            },
        )
        .await
        .expect("publish succeeds");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_task_channel_backpressure() {
    let vhost = Arc::new(Vhost::with_config(
        VhostId::new(1),
        VhostName::new("/").expect("valid vhost"),
        VhostConfig::new(1),
    ));
    let connection = ConnectionId::new(1);

    vhost
        .declare_queue("jobs", QueueConfig::default(), connection)
        .await
        .expect("declare jobs");
    vhost
        .consume(
            connection,
            ChannelId::new(1),
            ConsumerId::new(1),
            "jobs",
            ConsumerOptions::default(),
        )
        .await
        .expect("consume jobs");

    publish(&vhost, connection, b"first").await;

    let record = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(2));
    let (entered_tx, entered_rx) = oneshot::channel();
    let (first_delivered_tx, mut first_delivered_rx) = oneshot::channel();
    let blocking_sink = BlockingSink {
        barrier: Arc::clone(&barrier),
        entered: Some(entered_tx),
        delivered: Some(first_delivered_tx),
        record: Arc::clone(&record),
    };

    let vhost_for_dispatch = Arc::clone(&vhost);
    let dispatch_task =
        tokio::spawn(async move { vhost_for_dispatch.dispatch("jobs", blocking_sink).await });

    timeout(SAFETY, entered_rx)
        .await
        .expect("the queue task reaches the barrier within the safety timeout")
        .expect("the queue task entered delivery");

    let mut second = Box::pin(publish(&vhost, connection, b"second"));
    let first_poll = poll_fn(|cx| Poll::Ready(second.as_mut().poll(cx))).await;
    assert!(
        matches!(first_poll, Poll::Pending),
        "the second publish must not complete while the queue task is blocked"
    );
    assert!(
        !dispatch_task.is_finished(),
        "the queue task is still blocked at the barrier"
    );
    assert!(
        first_delivered_rx.try_recv().is_err(),
        "the first delivery has not completed while the queue task is blocked"
    );
    assert!(
        record
            .lock()
            .expect("delivery record lock is not poisoned")
            .is_empty(),
        "no delivery is recorded before the barrier is released"
    );

    barrier.wait();

    let first_body = timeout(SAFETY, first_delivered_rx)
        .await
        .expect("the first delivery completes after the barrier is released")
        .expect("the first delivery signal");
    assert_eq!(first_body.as_ref(), b"first");

    let dispatched = timeout(SAFETY, dispatch_task)
        .await
        .expect("the dispatch task finishes within the safety timeout")
        .expect("the dispatch task did not panic")
        .expect("the dispatch succeeds");
    assert_eq!(dispatched, 1);

    timeout(SAFETY, second.as_mut())
        .await
        .expect("the second publish completes once the queue task frees space");

    let (second_delivered_tx, second_delivered_rx) = oneshot::channel();
    let signal_sink = SignalSink {
        delivered: Some(second_delivered_tx),
        record: Arc::clone(&record),
    };
    vhost
        .dispatch("jobs", signal_sink)
        .await
        .expect("dispatch the second message");

    let second_body = timeout(SAFETY, second_delivered_rx)
        .await
        .expect("the second delivery arrives within the safety timeout")
        .expect("the second delivery signal");
    assert_eq!(second_body.as_ref(), b"second");

    let delivered = record
        .lock()
        .expect("delivery record lock is not poisoned")
        .clone();
    assert_eq!(
        delivered,
        vec![Bytes::from_static(b"first"), Bytes::from_static(b"second")],
        "messages are delivered in publish order"
    );
}
