use std::future::Future;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use wolf_broker::queue::ConnectionId;
use wolf_broker::{
    ChannelId, ConsumerCancel, ConsumerId, ConsumerOptions, CreditSignal, DeliverOutcome, Delivery,
    DeliverySink, Message, PublishRequest, QueueConfig, Vhost, VhostId, VhostName,
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
async fn per_consumer_prefetch_round_robins_across_two_consumers() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);

    declare(&vhost, connection, "jobs").await;
    consume(&vhost, connection, channel, first, "jobs").await;
    consume(&vhost, connection, channel, second, "jobs").await;
    let signals = vhost
        .set_prefetch(channel, 1, false)
        .await
        .expect("set consumer prefetch");
    assert_eq!(
        signals,
        vec![
            CreditSignal::Consumer {
                consumer: first,
                credit: 1,
            },
            CreditSignal::Consumer {
                consumer: second,
                credit: 1,
            },
        ]
    );

    for body in [b"m1".as_slice(), b"m2", b"m3", b"m4"] {
        publish(&vhost, connection, "jobs", body).await;
    }

    let deliveries = dispatch(&vhost, "jobs").await;
    assert_eq!(consumers(&deliveries), vec![first, second]);
    assert_eq!(
        bodies(&deliveries),
        vec![Bytes::from_static(b"m1"), Bytes::from_static(b"m2")]
    );
    let first_tag = deliveries[0].delivery_tag;
    let second_tag = deliveries[1].delivery_tag;

    vhost
        .ack(channel, first_tag, false)
        .await
        .expect("ack first delivery");
    let deliveries = dispatch(&vhost, "jobs").await;
    assert_eq!(consumers(&deliveries), vec![first]);
    assert_eq!(deliveries[0].message.body().as_ref(), b"m3");
    let third_tag = deliveries[0].delivery_tag;

    vhost
        .ack(channel, third_tag, false)
        .await
        .expect("ack third delivery");
    vhost
        .ack(channel, second_tag, false)
        .await
        .expect("ack second delivery");

    let deliveries = dispatch(&vhost, "jobs").await;
    assert_eq!(consumers(&deliveries), vec![second]);
    assert_eq!(deliveries[0].message.body().as_ref(), b"m4");
    assert_eq!(vhost.queue_message_count("jobs"), 0);
}

#[tokio::test]
async fn per_consumer_prefetch_is_independent_across_two_queues() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, first, "first").await;
    consume(&vhost, connection, channel, second, "second").await;
    vhost
        .set_prefetch(channel, 1, false)
        .await
        .expect("set consumer prefetch");

    publish(&vhost, connection, "first", b"a1").await;
    publish(&vhost, connection, "first", b"a2").await;
    publish(&vhost, connection, "second", b"b1").await;
    publish(&vhost, connection, "second", b"b2").await;

    let first_deliveries = dispatch(&vhost, "first").await;
    assert_eq!(first_deliveries.len(), 1);
    let second_deliveries = dispatch(&vhost, "second").await;
    assert_eq!(second_deliveries.len(), 1);
    assert!(dispatch(&vhost, "first").await.is_empty());
    assert!(dispatch(&vhost, "second").await.is_empty());

    vhost
        .ack(channel, first_deliveries[0].delivery_tag, false)
        .await
        .expect("ack first queue delivery");
    let first_deliveries = dispatch(&vhost, "first").await;
    assert_eq!(first_deliveries.len(), 1);
    assert_eq!(first_deliveries[0].message.body().as_ref(), b"a2");

    vhost
        .ack(channel, second_deliveries[0].delivery_tag, false)
        .await
        .expect("ack second queue delivery");
    let second_deliveries = dispatch(&vhost, "second").await;
    assert_eq!(second_deliveries.len(), 1);
    assert_eq!(second_deliveries[0].message.body().as_ref(), b"b2");
}

#[tokio::test]
async fn global_prefetch_caps_total_inflight_across_queues_and_consumers() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "first").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "second").await;
    vhost
        .set_prefetch(channel, 2, true)
        .await
        .expect("set channel prefetch");

    publish(&vhost, connection, "first", b"a1").await;
    publish(&vhost, connection, "first", b"a2").await;
    publish(&vhost, connection, "first", b"a3").await;
    publish(&vhost, connection, "second", b"b1").await;
    publish(&vhost, connection, "second", b"b2").await;

    let first = dispatch(&vhost, "first").await;
    assert_eq!(first.len(), 2);
    assert!(dispatch(&vhost, "second").await.is_empty());
    assert!(dispatch(&vhost, "first").await.is_empty());

    vhost
        .ack(channel, first[0].delivery_tag, false)
        .await
        .expect("ack first queue delivery");
    let second = dispatch(&vhost, "second").await;
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].message.body().as_ref(), b"b1");
    assert!(dispatch(&vhost, "first").await.is_empty());

    vhost
        .ack(channel, first[1].delivery_tag, false)
        .await
        .expect("ack second first-queue delivery");
    let first = dispatch(&vhost, "first").await;
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].message.body().as_ref(), b"a3");
}

#[tokio::test]
async fn global_prefetch_credit_signal_is_channel_scoped() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);

    declare(&vhost, connection, "jobs").await;
    consume(&vhost, connection, channel, ConsumerId::new(1), "jobs").await;
    consume(&vhost, connection, channel, ConsumerId::new(2), "jobs").await;

    let signals = vhost
        .set_prefetch(channel, 4, true)
        .await
        .expect("set channel prefetch");
    assert_eq!(signals, vec![CreditSignal::Channel { channel, credit: 4 }]);

    publish(&vhost, connection, "jobs", b"m1").await;
    publish(&vhost, connection, "jobs", b"m2").await;
    assert_eq!(dispatch(&vhost, "jobs").await.len(), 2);

    let signals = vhost
        .set_prefetch(channel, 4, true)
        .await
        .expect("recompute channel credit");
    assert_eq!(signals, vec![CreditSignal::Channel { channel, credit: 2 }]);
}

#[tokio::test]
async fn per_queue_consumers_and_channel_prefetch_coexist() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let first = ConsumerId::new(1);
    let second = ConsumerId::new(2);
    let third = ConsumerId::new(3);

    declare(&vhost, connection, "first").await;
    declare(&vhost, connection, "second").await;
    consume(&vhost, connection, channel, first, "first").await;
    consume(&vhost, connection, channel, second, "first").await;
    consume(&vhost, connection, channel, third, "second").await;
    vhost
        .set_prefetch(channel, 2, true)
        .await
        .expect("set channel prefetch");

    for body in [b"a1".as_slice(), b"a2", b"a3"] {
        publish(&vhost, connection, "first", body).await;
    }
    publish(&vhost, connection, "second", b"b1").await;

    let deliveries = dispatch(&vhost, "first").await;
    assert_eq!(consumers(&deliveries), vec![first, second]);
    assert_eq!(
        bodies(&deliveries),
        vec![Bytes::from_static(b"a1"), Bytes::from_static(b"a2")]
    );
    assert!(dispatch(&vhost, "second").await.is_empty());

    vhost
        .ack(channel, deliveries[1].delivery_tag, false)
        .await
        .expect("ack second consumer delivery");

    let deliveries = dispatch(&vhost, "second").await;
    assert_eq!(consumers(&deliveries), vec![third]);
    assert_eq!(deliveries[0].message.body().as_ref(), b"b1");
}
