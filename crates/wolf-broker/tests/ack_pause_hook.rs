#![cfg(feature = "test-hooks")]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::time::timeout;
use wolf_broker::queue::{ConnectionId, QueueConfig, QueueRegistry};
use wolf_broker::router::Router;
use wolf_broker::{
    AckPauseHook, ChannelId, DeliveryTag, GetOutcome, Message, PublishOutcome, PublishRequest,
    Vhost, VhostId, VhostName,
};

const SAFETY: Duration = Duration::from_secs(10);

fn name() -> VhostName {
    VhostName::new("/").expect("valid vhost name")
}

fn message(body: &'static [u8]) -> Message {
    Message::new(Bytes::from_static(body))
}

fn queues(connection: ConnectionId) -> QueueRegistry {
    let mut queues = QueueRegistry::new();
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");
    queues
}

async fn publish_job(vhost: &Vhost, connection: ConnectionId) {
    let outcome = vhost
        .publish(
            connection,
            PublishRequest {
                exchange: "",
                routing_key: "jobs",
                mandatory: false,
                message: message(b"payload"),
                properties: (),
            },
        )
        .await
        .expect("publish is routed");
    assert_eq!(outcome, PublishOutcome::Routed { queues: 1 });
}

async fn fetch_delivery_tag(
    vhost: &Vhost,
    connection: ConnectionId,
    channel: ChannelId,
) -> DeliveryTag {
    let outcome = vhost
        .get(connection, channel, "jobs", false)
        .await
        .expect("get message");
    let GetOutcome::Message(got) = outcome else {
        panic!("expected a fetched message, got {outcome:?}");
    };
    got.delivery_tag
        .expect("acknowledging get has a delivery tag")
}

#[tokio::test]
async fn ack_pauses_until_the_hook_is_released() {
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let (hook, mut handle) = AckPauseHook::pair();
    let vhost = Arc::new(
        Vhost::with_routing(
            VhostId::new(1),
            name(),
            Router::default(),
            queues(connection),
        )
        .with_ack_pause_hook(hook),
    );

    publish_job(&vhost, connection).await;
    let tag = fetch_delivery_tag(&vhost, connection, channel).await;

    let ack_vhost = Arc::clone(&vhost);
    let ack = tokio::spawn(async move { ack_vhost.ack(channel, tag, false).await });

    assert!(
        timeout(SAFETY, handle.wait_entered())
            .await
            .expect("ack reaches the pause hook"),
        "pause hook reported entry"
    );
    assert!(!ack.is_finished(), "ack must stay paused until released");

    assert!(handle.release(), "a paused ack is released");

    let outcome = timeout(SAFETY, ack)
        .await
        .expect("ack completes after release")
        .expect("ack task joins")
        .expect("ack succeeds");
    assert_eq!(outcome.settled, 1);
}

#[tokio::test]
async fn ack_without_an_installed_hook_settles_directly() {
    let connection = ConnectionId::new(1);
    let channel = ChannelId::new(1);
    let vhost = Vhost::with_routing(
        VhostId::new(1),
        name(),
        Router::default(),
        queues(connection),
    );

    publish_job(&vhost, connection).await;
    let tag = fetch_delivery_tag(&vhost, connection, channel).await;

    let outcome = vhost.ack(channel, tag, false).await.expect("ack succeeds");
    assert_eq!(outcome.settled, 1);
}
