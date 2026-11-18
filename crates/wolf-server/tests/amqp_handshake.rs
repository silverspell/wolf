//! Raw AMQP protocol tests for the M1 connection, auth, tune and channel flow.

mod support;

use wolf_amqp_codec::{ChannelFlow, ChannelMethod, FieldValue, Frame, Method, ShortString};
use wolf_server::amqp::connection::DEFAULT_HEARTBEAT;

use support::{TestClient, start_server};

fn capabilities(start: &wolf_amqp_codec::Start) -> &wolf_amqp_codec::FieldTable {
    match start.server_properties.get("capabilities") {
        Some(FieldValue::FieldTable(capabilities)) => capabilities,
        other => panic!("expected capabilities table, got {other:?}"),
    }
}

#[tokio::test]
async fn connection_start_advertises_exactly_the_m1_capabilities() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    let start = client.begin().await;

    let capabilities = capabilities(&start);
    assert_eq!(capabilities.len(), 5, "capabilities: {capabilities:?}");
    for required in [
        "publisher_confirms",
        "basic.nack",
        "consumer_cancel_notify",
        "per_consumer_qos",
        "authentication_failure_close",
    ] {
        assert_eq!(
            capabilities.get(required),
            Some(&FieldValue::Bool(true)),
            "missing advertised capability {required}"
        );
    }
    for forbidden in ["connection.blocked", "connection.unblocked", "channel.flow"] {
        assert!(
            !capabilities.contains_key(forbidden),
            "capability {forbidden} must not be advertised"
        );
    }

    assert_eq!(start.mechanisms.as_bytes(), b"PLAIN");
    assert_eq!(start.locales.as_bytes(), b"en_US");
    assert!(start.server_properties.contains_key("product"));
    assert!(start.server_properties.contains_key("version"));
    assert!(start.server_properties.contains_key("platform"));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn guest_loopback_handshake_opens_and_closes_a_channel() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    let tune = client.handshake().await;

    assert_eq!(tune.channel_max, 2047);
    assert_eq!(tune.frame_max, 131_072);
    assert_eq!(tune.heartbeat, DEFAULT_HEARTBEAT);

    let open_ok = client.open_channel(1).await;
    assert!(open_ok.reserved.is_empty());
    client.close_channel(1).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn tune_ok_negotiates_lower_limits_and_heartbeat() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    let _ = client.begin().await;
    client.send_start_ok("guest", "guest").await;
    let tune = client.read_tune().await;
    assert_eq!(
        (tune.channel_max, tune.frame_max, tune.heartbeat),
        (2047, 131_072, 60)
    );

    client.send_tune_ok(10, 65_536, 30).await;
    client.send_open("/").await;
    client.read_open_ok().await;

    let _ = client.open_channel(10).await;
    client
        .send_method(
            11,
            Method::Channel(ChannelMethod::Open(wolf_amqp_codec::ChannelOpen {
                out_of_band: ShortString::new(""),
            })),
        )
        .await;
    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 503);
    assert_eq!((close.class_id, close.method_id), (20, 10));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn heartbeats_are_exchanged_without_closing_the_connection() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake_with(0, 0, 1).await;

    client.send_heartbeat().await;
    let server_heartbeat = client.read_frame().await;
    assert!(
        matches!(server_heartbeat, Frame::Heartbeat { channel: 0 }),
        "expected a server heartbeat, got {server_heartbeat:?}"
    );

    let _ = client.open_channel(1).await;
    client.close_channel(1).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn unknown_vhost_is_rejected_at_connection_level_with_530() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    let _ = client.begin().await;
    client.send_start_ok("guest", "guest").await;
    let _ = client.read_tune().await;
    client.send_tune_ok(0, 0, 0).await;
    client.send_open("/missing").await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 530);
    assert_eq!((close.class_id, close.method_id), (10, 40));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn channel_flow_true_replies_and_false_closes_the_connection() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client
        .send_method(
            1,
            Method::Channel(ChannelMethod::Flow(ChannelFlow { active: true })),
        )
        .await;
    match client.read_method_on(1).await {
        Method::Channel(ChannelMethod::FlowOk(flow_ok)) => assert!(flow_ok.active),
        other => panic!("expected channel.flow-ok, got {other:?}"),
    }

    client
        .send_method(
            1,
            Method::Channel(ChannelMethod::Flow(ChannelFlow { active: false })),
        )
        .await;
    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 540);
    assert_eq!((close.class_id, close.method_id), (20, 20));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}
