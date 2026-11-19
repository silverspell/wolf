//! Consumer outbox capture through the AMQP `basic.consume` adapter.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;
use wolf_amqp_codec::{BasicCancel, BasicConsume, FieldTable, ShortString};
use wolf_broker::{
    ChannelId, ConnectionId, ConsumerId, Message, PublishRequest, QueueConfig, Vhost, VhostId,
    VhostName,
};
use wolf_server::amqp::basic_consume;
use wolf_server::amqp::consumer::ConsumerDirectory;
use wolf_server::delivery::{DeliveryHub, ServerEvent};

const SAFETY: Duration = Duration::from_secs(10);
const SETTLE: Duration = Duration::from_millis(200);

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

#[allow(clippy::too_many_arguments)]
async fn consume(
    vhost: &Vhost,
    directory: &mut ConsumerDirectory,
    hub: &DeliveryHub,
    outbox: &mpsc::Sender<ServerEvent>,
    channel: u16,
    connection: ConnectionId,
    queue: &str,
    tag: &str,
) -> ConsumerId {
    basic_consume::consume(
        vhost,
        directory,
        hub,
        outbox,
        channel,
        ChannelId::new(u64::from(channel)),
        connection,
        BasicConsume {
            ticket: 0,
            queue: ShortString::new(queue),
            consumer_tag: ShortString::new(tag),
            no_local: false,
            no_ack: false,
            exclusive: false,
            nowait: false,
            arguments: FieldTable::new(),
        },
    )
    .await
    .expect("basic.consume succeeds");

    directory
        .find_by_tag(channel, &ShortString::new(tag))
        .expect("the consumer is registered")
}

#[tokio::test]
async fn consume_hands_outbox_to_queue_so_delivery_survives_route_unregister() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = 1;
    vhost
        .declare_queue("jobs", QueueConfig::default(), connection)
        .await
        .expect("declare queue");

    let hub = DeliveryHub::new();
    let (outbox, mut receiver) = hub.subscribe();
    let mut directory = ConsumerDirectory::new();
    let consumer = consume(
        &vhost,
        &mut directory,
        &hub,
        &outbox,
        channel,
        connection,
        "jobs",
        "ctag",
    )
    .await;

    hub.unregister(consumer);
    assert_eq!(hub.route_count(), 0);

    publish(&vhost, connection, "jobs", b"survives").await;
    vhost
        .dispatch_detached("jobs", hub.sink())
        .expect("dispatch detached");

    let event = timeout(SAFETY, receiver.recv())
        .await
        .expect("the delivery arrives within the safety timeout")
        .expect("the outbox stays open");
    let ServerEvent::Delivery(delivery) = event else {
        panic!("expected a delivery event, got {event:?}");
    };
    assert_eq!(delivery.consumer, consumer);
    assert_eq!(delivery.message.body().as_ref(), b"survives");
}

#[tokio::test]
async fn cancel_unregisters_route_and_drops_captured_outbox() {
    let vhost = vhost();
    let connection = ConnectionId::new(1);
    let channel = 1;
    vhost
        .declare_queue("jobs", QueueConfig::default(), connection)
        .await
        .expect("declare queue");

    let hub = DeliveryHub::new();
    let (outbox, mut receiver) = hub.subscribe();
    let mut directory = ConsumerDirectory::new();
    let consumer = consume(
        &vhost,
        &mut directory,
        &hub,
        &outbox,
        channel,
        connection,
        "jobs",
        "ctag",
    )
    .await;
    assert_eq!(hub.route_count(), 1);

    publish(&vhost, connection, "jobs", b"first").await;
    vhost
        .dispatch_detached("jobs", hub.sink())
        .expect("dispatch detached");
    let event = timeout(SAFETY, receiver.recv())
        .await
        .expect("the first delivery arrives within the safety timeout")
        .expect("the outbox stays open");
    let ServerEvent::Delivery(delivery) = event else {
        panic!("expected a delivery event, got {event:?}");
    };
    assert_eq!(delivery.consumer, consumer);

    basic_consume::cancel(
        &vhost,
        &mut directory,
        &hub,
        channel,
        BasicCancel {
            consumer_tag: ShortString::new("ctag"),
            nowait: false,
        },
    )
    .await
    .expect("basic.cancel succeeds");
    assert_eq!(hub.route_count(), 0);

    publish(&vhost, connection, "jobs", b"second").await;
    vhost
        .dispatch_detached("jobs", hub.sink())
        .expect("dispatch detached");

    match timeout(SETTLE, receiver.recv()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(event)) => panic!("unexpected event after cancel: {event:?}"),
    }
    assert_eq!(vhost.queue_message_count("jobs"), 1);
}
