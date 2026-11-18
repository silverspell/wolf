//! Raw-wire tests for `basic.get`, ack/nack/reject and recover.

mod support;

use bytes::Bytes;
use wolf_amqp_codec::{
    BasicMethod, BasicProperties, FieldTable, Method, QueueDeclare, QueueMethod, ShortString,
};

use support::{TestClient, start_server};

async fn declare_queue(client: &mut TestClient, channel: u16, name: &str) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Declare(QueueDeclare {
                ticket: 0,
                queue: ShortString::new(name),
                passive: false,
                durable: false,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: FieldTable::new(),
            })),
        )
        .await;
    match client.read_method_on(channel).await {
        Method::Queue(QueueMethod::DeclareOk(_)) => {}
        other => panic!("expected queue.declare-ok, got {other:?}"),
    }
}

async fn publish(
    client: &mut TestClient,
    channel: u16,
    queue: &str,
    properties: &BasicProperties,
    body: &[u8],
) {
    client.send_publish(channel, "", queue, false, false).await;
    client.send_content(channel, properties, body).await;
}

async fn get_message(client: &mut TestClient, channel: u16, queue: &str) -> (u64, bool, Bytes) {
    client.send_get(channel, queue, false).await;
    match client.read_method_on(channel).await {
        Method::Basic(BasicMethod::GetOk(ok)) => {
            let (_, body) = client.read_content(channel).await;
            (ok.delivery_tag, ok.redelivered, body)
        }
        other => panic!("expected basic.get-ok, got {other:?}"),
    }
}

fn expect_get_empty(method: Method) {
    match method {
        Method::Basic(BasicMethod::GetEmpty(empty)) => {
            assert_eq!(empty.cluster_id.as_str(), "");
        }
        other => panic!("expected basic.get-empty, got {other:?}"),
    }
}

#[tokio::test]
async fn get_empty_and_get_ok_preserve_metadata() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;

    client.send_get(1, "jobs", false).await;
    expect_get_empty(client.read_method_on(1).await);

    let properties = BasicProperties {
        content_type: Some(ShortString::new("text/plain")),
        message_id: Some(ShortString::new("m-1")),
        ..BasicProperties::default()
    };
    publish(&mut client, 1, "jobs", &properties, b"first").await;

    client.send_get(1, "jobs", false).await;
    let (tag, redelivered, body) = match client.read_method_on(1).await {
        Method::Basic(BasicMethod::GetOk(ok)) => {
            assert_eq!(ok.exchange.as_str(), "");
            assert_eq!(ok.routing_key.as_str(), "jobs");
            assert_eq!(ok.message_count, 0);
            let (header, body) = client.read_content(1).await;
            assert_eq!(header.basic_properties().expect("properties"), properties);
            (ok.delivery_tag, ok.redelivered, body)
        }
        other => panic!("expected basic.get-ok, got {other:?}"),
    };
    assert!(!redelivered);
    assert_eq!(body.as_ref(), b"first");

    client.send_ack(1, tag, false).await;
    client.send_get(1, "jobs", false).await;
    expect_get_empty(client.read_method_on(1).await);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn ack_multiple_nack_requeue_and_reject_discard() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    for body in [b"m1".as_slice(), b"m2", b"m3"] {
        publish(&mut client, 1, "jobs", &BasicProperties::default(), body).await;
    }

    let (_tag1, _, body1) = get_message(&mut client, 1, "jobs").await;
    let (tag2, _, body2) = get_message(&mut client, 1, "jobs").await;
    let (tag3, _, body3) = get_message(&mut client, 1, "jobs").await;
    assert_eq!(
        (body1.as_ref(), body2.as_ref(), body3.as_ref()),
        (b"m1".as_slice(), b"m2".as_slice(), b"m3".as_slice())
    );

    client.send_ack(1, tag2, true).await;
    client.send_nack(1, tag3, false, true).await;

    let (tag4, redelivered, body) = get_message(&mut client, 1, "jobs").await;
    assert!(redelivered);
    assert_eq!(body.as_ref(), b"m3");

    client.send_reject(1, tag4, false).await;
    client.send_get(1, "jobs", true).await;
    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::GetEmpty(_)) => {}
        other => panic!("expected empty queue after rejection, got {other:?}"),
    }

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn reject_with_requeue_sets_redelivered() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    publish(
        &mut client,
        1,
        "jobs",
        &BasicProperties::default(),
        b"again",
    )
    .await;

    let (tag, _, _) = get_message(&mut client, 1, "jobs").await;
    client.send_reject(1, tag, true).await;

    let (_, redelivered, body) = get_message(&mut client, 1, "jobs").await;
    assert!(redelivered);
    assert_eq!(body.as_ref(), b"again");

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn unknown_delivery_tag_closes_only_the_channel_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_ack(1, 99, false).await;

    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);
    assert_eq!((close.class_id, close.method_id), (60, 80));

    client.send_channel_close_ok(1).await;
    let _ = client.open_channel(2).await;
    client.close_channel(2).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn connection_close_requeues_unacked_basic_get() {
    let (addr, handle) = start_server().await;
    let mut first = TestClient::connect(addr).await;
    first.handshake().await;
    let _ = first.open_channel(1).await;
    declare_queue(&mut first, 1, "jobs").await;
    publish(
        &mut first,
        1,
        "jobs",
        &BasicProperties::default(),
        b"pending",
    )
    .await;
    let _ = get_message(&mut first, 1, "jobs").await;
    first.close().await;

    let mut second = TestClient::connect(addr).await;
    second.handshake().await;
    let _ = second.open_channel(1).await;
    let (_, redelivered, body) = get_message(&mut second, 1, "jobs").await;
    assert!(redelivered);
    assert_eq!(body.as_ref(), b"pending");
    second.send_ack(1, 1, false).await;

    drop(second);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn connection_close_requeues_unacked_consumer_deliveries() {
    let (addr, handle) = start_server().await;
    let mut first = TestClient::connect(addr).await;
    first.handshake().await;
    let _ = first.open_channel(1).await;
    declare_queue(&mut first, 1, "jobs").await;
    publish(
        &mut first,
        1,
        "jobs",
        &BasicProperties::default(),
        b"delivered",
    )
    .await;
    first.send_consume(1, "jobs", "ctag", false, false).await;
    match first.read_method_on(1).await {
        Method::Basic(BasicMethod::ConsumeOk(_)) => {}
        other => panic!("expected basic.consume-ok, got {other:?}"),
    }
    match first.read_method_on(1).await {
        Method::Basic(BasicMethod::Deliver(_)) => {
            let _ = first.read_content(1).await;
        }
        other => panic!("expected basic.deliver, got {other:?}"),
    }
    first.close().await;

    let mut second = TestClient::connect(addr).await;
    second.handshake().await;
    let _ = second.open_channel(1).await;
    let (_, redelivered, body) = get_message(&mut second, 1, "jobs").await;
    assert!(redelivered);
    assert_eq!(body.as_ref(), b"delivered");

    drop(second);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn recover_requeues_unacked_and_returns_recover_ok() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    publish(&mut client, 1, "jobs", &BasicProperties::default(), b"m1").await;
    publish(&mut client, 1, "jobs", &BasicProperties::default(), b"m2").await;
    let _ = get_message(&mut client, 1, "jobs").await;
    let _ = get_message(&mut client, 1, "jobs").await;

    client.send_recover(1, true).await;
    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::RecoverOk(_)) => {}
        other => panic!("expected basic.recover-ok, got {other:?}"),
    }

    let (_, first_redelivered, first_body) = get_message(&mut client, 1, "jobs").await;
    let (_, second_redelivered, second_body) = get_message(&mut client, 1, "jobs").await;
    assert!(first_redelivered);
    assert!(second_redelivered);
    assert_eq!(first_body.as_ref(), b"m1");
    assert_eq!(second_body.as_ref(), b"m2");

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn recover_async_requeues_without_a_reply() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    publish(
        &mut client,
        1,
        "jobs",
        &BasicProperties::default(),
        b"again",
    )
    .await;
    let _ = get_message(&mut client, 1, "jobs").await;

    client.send_recover_async(1, true).await;
    client.send_get(1, "jobs", true).await;
    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::GetOk(ok)) => {
            assert!(ok.redelivered);
            let (_, body) = client.read_content(1).await;
            assert_eq!(body.as_ref(), b"again");
        }
        other => panic!("expected get-ok without recover-ok, got {other:?}"),
    }

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn recover_with_requeue_false_is_rejected_with_540() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_recover(1, false).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 540);
    assert_eq!((close.class_id, close.method_id), (60, 110));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn recover_async_with_requeue_false_is_rejected_with_540() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_recover_async(1, false).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 540);
    assert_eq!((close.class_id, close.method_id), (60, 100));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}
