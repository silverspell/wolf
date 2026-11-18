//! Memory-alarm behavior: connection.blocked/unblocked and publish gating.

mod common;
mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::{Stream, StreamExt};
use lapin::{Event, options::BasicPublishOptions};
use tokio::time::timeout;
use wolf_amqp_codec::{
    BasicMethod, BasicProperties, ConnectionMethod, FieldTable, FieldValue, Frame, Method,
};
use wolf_server::{
    ManualMemoryProbe, MemoryHighWatermark, ServerConfig, ShutdownHandle, start_with_memory_probe,
};

use support::TestClient;

const TOTAL: u64 = 1_000_000;
const LOW: u64 = 100_000;
const HIGH: u64 = 900_000;
const THRESHOLD: u64 = 500_000;
const SETTLE: Duration = Duration::from_millis(250);

fn alarm_config() -> ServerConfig {
    let mut config = support::config();
    config.memory_high_watermark = MemoryHighWatermark::Absolute(THRESHOLD);
    config.memory_check_interval = Duration::from_millis(10);
    config
}

async fn start_alarm(
    config: ServerConfig,
    used: u64,
) -> (SocketAddr, ShutdownHandle, Arc<ManualMemoryProbe>) {
    let probe = Arc::new(ManualMemoryProbe::new(TOTAL, used));
    let (addr, handle) = start_with_memory_probe(config, probe.clone())
        .await
        .expect("server starts");
    (addr, handle, probe)
}

fn blocked_capabilities() -> FieldTable {
    let mut capabilities = FieldTable::new();
    capabilities.insert("connection.blocked", FieldValue::Bool(true));
    let mut properties = FieldTable::new();
    properties.insert("capabilities", FieldValue::FieldTable(capabilities));
    properties
}

fn expect_blocked(method: ConnectionMethod) {
    match method {
        ConnectionMethod::Blocked(blocked) => {
            assert_eq!(blocked.reason.as_str(), "low on memory");
        }
        other => panic!("expected connection.blocked, got {other:?}"),
    }
}

fn expect_unblocked(method: ConnectionMethod) {
    match method {
        ConnectionMethod::Unblocked(_) => {}
        other => panic!("expected connection.unblocked, got {other:?}"),
    }
}

async fn select_confirm(client: &mut TestClient, channel: u16) {
    client.select_confirm(channel, false).await;
    match client.read_method_on(channel).await {
        Method::Confirm(wolf_amqp_codec::ConfirmMethod::SelectOk(_)) => {}
        other => panic!("expected confirm.select-ok, got {other:?}"),
    }
}

async fn expect_delivery(client: &mut TestClient, expected: &[u8]) {
    let delivery_tag = match client.read_method_on(1).await {
        Method::Basic(BasicMethod::Deliver(delivery)) => delivery.delivery_tag,
        other => panic!("expected basic.deliver, got {other:?}"),
    };
    let (_, body) = client.read_content(1).await;
    assert_eq!(body.as_ref(), expected);
    client.send_ack(1, delivery_tag, false).await;
}

async fn wait_for_blocked<S>(events: &mut S) -> String
where
    S: Stream<Item = Event> + Unpin,
{
    timeout(common::OP_TIMEOUT, async {
        loop {
            match events.next().await.expect("connection events stay open") {
                Event::ConnectionBlocked(reason) => {
                    println!("lapin client received connection.blocked: {reason}");
                    break reason;
                }
                Event::Error(error) => panic!("unexpected connection error: {error}"),
                _ => {}
            }
        }
    })
    .await
    .expect("connection.blocked arrives within the timeout")
}

async fn wait_for_unblocked<S>(events: &mut S)
where
    S: Stream<Item = Event> + Unpin,
{
    timeout(common::OP_TIMEOUT, async {
        loop {
            match events.next().await.expect("connection events stay open") {
                Event::ConnectionUnblocked => {
                    println!("lapin client received connection.unblocked");
                    break;
                }
                Event::Error(error) => panic!("unexpected connection error: {error}"),
                _ => {}
            }
        }
    })
    .await
    .expect("connection.unblocked arrives within the timeout");
}

#[tokio::test]
async fn memory_alarm_blocks_publish() {
    let (addr, handle, probe) = start_alarm(alarm_config(), LOW).await;

    let lapin_connection = common::connect(addr).await;
    let mut events = Box::pin(lapin_connection.events_listener());
    let lapin_channel = common::create_channel(&lapin_connection).await;
    common::declare_queue(&lapin_channel, "during-alarm").await;
    let preload = common::publish(
        &lapin_channel,
        "",
        "during-alarm",
        b"preloaded",
        lapin::BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;
    preload.await.expect("preloaded publish is confirmed");

    let mut publisher = TestClient::connect(addr).await;
    publisher
        .handshake_with_client_properties(blocked_capabilities())
        .await;
    let _ = publisher.open_channel(1).await;
    select_confirm(&mut publisher, 1).await;

    probe.set_used_bytes(HIGH);
    let reason = wait_for_blocked(&mut events).await;
    assert_eq!(reason, "low on memory");
    assert!(
        lapin_connection.status().blocked(),
        "status must report blocked"
    );
    expect_blocked(publisher.read_connection_method().await);

    let body = b"blocked-body";
    publisher.send_publish(1, "", "missing", true, false).await;
    publisher
        .send_content(1, &BasicProperties::default(), body)
        .await;
    assert!(
        publisher.read_frame_within(SETTLE).await.is_none(),
        "a blocked publish must not be returned or confirmed"
    );

    let mut consumer = TestClient::connect(addr).await;
    consumer
        .handshake_with_client_properties(FieldTable::new())
        .await;
    let _ = consumer.open_channel(1).await;
    consumer
        .send_consume(1, "during-alarm", "during-alarm-consumer", false, false)
        .await;
    match consumer.read_method_on(1).await {
        Method::Basic(BasicMethod::ConsumeOk(ok)) => {
            assert_eq!(ok.consumer_tag.as_str(), "during-alarm-consumer");
        }
        other => panic!("expected basic.consume-ok, got {other:?}"),
    }
    expect_delivery(&mut consumer, b"preloaded").await;
    assert!(
        lapin_connection.status().blocked(),
        "consume and ack must progress while the alarm is still active"
    );

    probe.set_used_bytes(LOW);
    wait_for_unblocked(&mut events).await;
    assert!(
        !lapin_connection.status().blocked(),
        "status must report unblocked"
    );
    expect_unblocked(publisher.read_connection_method().await);

    match publisher.read_method_on(1).await {
        Method::Basic(BasicMethod::Return(returned)) => {
            assert_eq!(returned.reply_code, 312);
            assert_eq!(returned.routing_key.as_str(), "missing");
        }
        other => panic!("expected basic.return, got {other:?}"),
    }
    match publisher.read_frame().await {
        Frame::ContentHeader { channel, header } => {
            assert_eq!(channel, 1);
            assert_eq!(header.body_size, body.len() as u64);
        }
        other => panic!("expected returned content header, got {other:?}"),
    }
    match publisher.read_frame().await {
        Frame::ContentBody {
            channel,
            body: chunk,
        } => {
            assert_eq!(channel, 1);
            assert_eq!(chunk.as_ref(), body);
        }
        other => panic!("expected returned content body, got {other:?}"),
    }
    match publisher.read_method_on(1).await {
        Method::Basic(BasicMethod::Ack(ack)) => assert_eq!(ack.delivery_tag, 1),
        other => panic!("expected basic.ack, got {other:?}"),
    }

    drop(consumer);
    drop(publisher);
    common::close_channel(&lapin_channel).await;
    common::finish(&lapin_connection, handle).await;
}

#[tokio::test]
async fn memory_alarm_at_startup_blocks_capable_client() {
    let (addr, handle, _probe) = start_alarm(alarm_config(), HIGH).await;
    let mut client = TestClient::connect(addr).await;
    client
        .handshake_with_client_properties(blocked_capabilities())
        .await;

    expect_blocked(client.read_connection_method().await);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn second_publish_is_retained_until_unblock() {
    let (addr, handle, probe) = start_alarm(alarm_config(), LOW).await;
    let mut publisher = TestClient::connect(addr).await;
    publisher
        .handshake_with_client_properties(blocked_capabilities())
        .await;
    let _ = publisher.open_channel(1).await;
    select_confirm(&mut publisher, 1).await;

    probe.set_used_bytes(HIGH);
    expect_blocked(publisher.read_connection_method().await);

    for body in [b"first".as_slice(), b"second"] {
        publisher.send_publish(1, "", "missing", false, false).await;
        publisher
            .send_content(1, &BasicProperties::default(), body)
            .await;
    }
    assert!(
        publisher.read_frame_within(SETTLE).await.is_none(),
        "deferred publishes must not be confirmed while blocked"
    );

    probe.set_used_bytes(LOW);
    expect_unblocked(publisher.read_connection_method().await);
    for expected in [1, 2] {
        match publisher.read_method_on(1).await {
            Method::Basic(BasicMethod::Ack(ack)) => assert_eq!(ack.delivery_tag, expected),
            other => panic!("expected basic.ack {expected}, got {other:?}"),
        }
    }
    assert!(
        publisher.read_frame_within(SETTLE).await.is_none(),
        "retained publishes must be confirmed exactly once"
    );

    drop(publisher);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn memory_alarm_unblocks_publish() {
    let (addr, handle, probe) = start_alarm(alarm_config(), LOW).await;
    let connection = common::connect(addr).await;
    let mut events = Box::pin(connection.events_listener());
    let channel = common::create_channel(&connection).await;

    probe.set_used_bytes(HIGH);
    let reason = wait_for_blocked(&mut events).await;
    assert_eq!(reason, "low on memory");
    assert!(connection.status().blocked(), "status must report blocked");

    probe.set_used_bytes(LOW);
    wait_for_unblocked(&mut events).await;
    assert!(
        !connection.status().blocked(),
        "status must report unblocked"
    );

    common::declare_queue(&channel, "resumed").await;
    common::publish_to_queue(&channel, "resumed", b"after-unblock").await;
    let (body, _) = common::get_and_ack(&channel, "resumed")
        .await
        .expect("publish after unblock is accepted");
    assert_eq!(body.as_slice(), b"after-unblock");

    common::close_channel(&channel).await;
    common::finish(&connection, handle).await;
}

#[tokio::test]
async fn client_without_capability_gets_no_blocked_method() {
    let (addr, handle, probe) = start_alarm(alarm_config(), LOW).await;
    let mut publisher = TestClient::connect(addr).await;
    publisher
        .handshake_with_client_properties(FieldTable::new())
        .await;
    let _ = publisher.open_channel(1).await;
    select_confirm(&mut publisher, 1).await;

    probe.set_used_bytes(HIGH);
    assert!(
        publisher.read_frame_within(SETTLE).await.is_none(),
        "non-capable clients must not receive connection.blocked"
    );

    let body = b"non-capable-body";
    publisher.send_publish(1, "", "missing", true, false).await;
    publisher
        .send_content(1, &BasicProperties::default(), body)
        .await;
    assert!(
        publisher.read_frame_within(SETTLE).await.is_none(),
        "non-capable publishes must still be deferred while blocked"
    );

    probe.set_used_bytes(LOW);
    match publisher.read_frame().await {
        Frame::Method {
            channel: 1,
            method: Method::Basic(BasicMethod::Return(returned)),
        } => {
            assert_eq!(returned.reply_code, 312);
        }
        other => panic!("expected basic.return without connection.unblocked, got {other:?}"),
    }
    match publisher.read_frame().await {
        Frame::ContentHeader { channel, .. } => assert_eq!(channel, 1),
        other => panic!("expected returned content header, got {other:?}"),
    }
    match publisher.read_frame().await {
        Frame::ContentBody {
            channel,
            body: chunk,
        } => {
            assert_eq!(channel, 1);
            assert_eq!(chunk.as_ref(), body);
        }
        other => panic!("expected returned content body, got {other:?}"),
    }
    match publisher.read_method_on(1).await {
        Method::Basic(BasicMethod::Ack(ack)) => assert_eq!(ack.delivery_tag, 1),
        other => panic!("expected basic.ack, got {other:?}"),
    }
    assert!(
        publisher.read_frame_within(SETTLE).await.is_none(),
        "non-capable clients must not receive connection.unblocked"
    );

    drop(publisher);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn memory_alarm_and_bounded_outbox_together() {
    let mut config = alarm_config();
    config.outbox_capacity = 1;
    config.queue_channel_capacity = 1;
    let (addr, handle, probe) = start_alarm(config, LOW).await;

    let setup = common::connect(addr).await;
    let setup_channel = common::create_channel(&setup).await;
    common::declare_queue(&setup_channel, "bounded").await;
    for body in [b"one".as_slice(), b"two", b"three"] {
        let confirm = common::publish(
            &setup_channel,
            "",
            "bounded",
            body,
            lapin::BasicProperties::default(),
            BasicPublishOptions::default(),
        )
        .await;
        confirm.await.expect("preloaded publish is confirmed");
    }
    let _ = setup.close(200, "setup done").await;

    let mut publisher = TestClient::connect(addr).await;
    publisher
        .handshake_with_client_properties(blocked_capabilities())
        .await;
    let _ = publisher.open_channel(1).await;
    select_confirm(&mut publisher, 1).await;

    probe.set_used_bytes(HIGH);
    expect_blocked(publisher.read_connection_method().await);

    let mut consumer = TestClient::connect(addr).await;
    consumer
        .handshake_with_client_properties(FieldTable::new())
        .await;
    let _ = consumer.open_channel(1).await;
    consumer
        .send_consume(1, "bounded", "ctag", false, false)
        .await;
    match consumer.read_method_on(1).await {
        Method::Basic(BasicMethod::ConsumeOk(ok)) => {
            assert_eq!(ok.consumer_tag.as_str(), "ctag");
        }
        other => panic!("expected basic.consume-ok, got {other:?}"),
    }

    for expected in [b"one".as_slice(), b"two", b"three"] {
        expect_delivery(&mut consumer, expected).await;
    }

    publisher.send_publish(1, "", "bounded", false, false).await;
    publisher
        .send_content(1, &BasicProperties::default(), b"during-block")
        .await;
    assert!(
        publisher.read_frame_within(SETTLE).await.is_none(),
        "the blocked publish must not be confirmed yet"
    );
    assert!(
        consumer.read_frame_within(SETTLE).await.is_none(),
        "the deferred publish must not reach the consumer yet"
    );

    probe.set_used_bytes(LOW);
    expect_unblocked(publisher.read_connection_method().await);
    match publisher.read_method_on(1).await {
        Method::Basic(BasicMethod::Ack(ack)) => assert_eq!(ack.delivery_tag, 1),
        other => panic!("expected basic.ack after unblock, got {other:?}"),
    }

    expect_delivery(&mut consumer, b"during-block").await;
    assert!(
        consumer.read_frame_within(SETTLE).await.is_none(),
        "no message may be lost or duplicated"
    );

    drop(consumer);
    drop(publisher);
    handle.shutdown().await.expect("graceful shutdown");
}
