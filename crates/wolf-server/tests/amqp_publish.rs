//! Raw-wire tests for `basic.publish`, mandatory returns and publisher confirms.

mod support;

use wolf_amqp_codec::{
    BasicMethod, BasicProperties, ContentHeader, FieldTable, FieldValue, Frame, Method, ShortString,
};

use support::{TestClient, start_server};

fn properties() -> BasicProperties {
    let mut headers = FieldTable::new();
    headers.insert(
        "source",
        FieldValue::LongString(wolf_amqp_codec::LongString::new("test")),
    );
    BasicProperties {
        content_type: Some(ShortString::new("text/plain")),
        headers: Some(headers),
        delivery_mode: Some(2),
        message_id: Some(ShortString::new("message-1")),
        ..BasicProperties::default()
    }
}

fn expect_ack(method: Method) -> u64 {
    let Method::Basic(BasicMethod::Ack(ack)) = method else {
        panic!("expected basic.ack, got {method:?}");
    };
    assert!(!ack.multiple, "confirm acks must be single-message");
    ack.delivery_tag
}

#[tokio::test]
async fn mandatory_unroutable_publish_returns_body_and_properties_before_confirm() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    client.select_confirm(1, false).await;
    match client.read_method_on(1).await {
        Method::Confirm(wolf_amqp_codec::ConfirmMethod::SelectOk(_)) => {}
        other => panic!("expected confirm.select-ok, got {other:?}"),
    }

    let properties = properties();
    let body = b"return-me";
    client.send_publish(1, "", "missing", true, false).await;
    client.send_content(1, &properties, body).await;

    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::Return(returned)) => {
            assert_eq!(returned.reply_code, 312);
            assert_eq!(returned.reply_text.as_str(), "NO_ROUTE");
            assert_eq!(returned.exchange.as_str(), "");
            assert_eq!(returned.routing_key.as_str(), "missing");
        }
        other => panic!("expected basic.return, got {other:?}"),
    }

    let Frame::ContentHeader { channel, header } = client.read_frame().await else {
        panic!("expected returned content header");
    };
    assert_eq!(channel, 1);
    assert_eq!(header.body_size, body.len() as u64);
    assert_eq!(header.class_id, 60);
    assert_eq!(header.weight, 0);
    assert_eq!(header.basic_properties().expect("properties"), properties);

    let Frame::ContentBody {
        channel,
        body: returned,
    } = client.read_frame().await
    else {
        panic!("expected returned content body");
    };
    assert_eq!(channel, 1);
    assert_eq!(returned.as_ref(), body);

    assert_eq!(expect_ack(client.read_method_on(1).await), 1);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn confirms_are_emitted_in_publish_order_exactly_once() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    client.select_confirm(1, false).await;
    match client.read_method_on(1).await {
        Method::Confirm(wolf_amqp_codec::ConfirmMethod::SelectOk(_)) => {}
        other => panic!("expected confirm.select-ok, got {other:?}"),
    }

    for (index, body) in [b"first".as_slice(), b"second", b"third"]
        .into_iter()
        .enumerate()
    {
        client.send_publish(1, "", "missing", false, false).await;
        client
            .send_content(1, &BasicProperties::default(), body)
            .await;
        assert_eq!(
            expect_ack(client.read_method_on(1).await),
            index as u64 + 1,
            "confirm order must match publish order"
        );
    }

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn nowait_confirm_select_suppresses_select_ok() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.select_confirm(1, true).await;
    client.send_publish(1, "", "missing", false, false).await;
    client
        .send_content(1, &BasicProperties::default(), b"payload")
        .await;

    assert_eq!(expect_ack(client.read_method_on(1).await), 1);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn immediate_publish_closes_the_connection_with_540() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_publish(1, "", "jobs", false, true).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 540);
    assert_eq!((close.class_id, close.method_id), (60, 40));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn expiration_closes_only_the_channel_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    let properties = BasicProperties {
        expiration: Some(ShortString::new("1000")),
        ..BasicProperties::default()
    };
    client.send_publish(1, "", "jobs", false, false).await;
    let header = ContentHeader::from_basic_properties(0, &properties).expect("header");
    client.send_content_header(1, header).await;

    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);
    assert_eq!((close.class_id, close.method_id), (60, 40));

    client.send_channel_close_ok(1).await;
    let _ = client.open_channel(2).await;
    client.close_channel(2).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn content_header_without_publish_closes_the_connection_with_505() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    let header =
        ContentHeader::from_basic_properties(0, &BasicProperties::default()).expect("header");
    client.send_content_header(1, header).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 505);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn content_body_without_header_closes_the_connection_with_505() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_publish(1, "", "missing", false, false).await;
    client.send_content_body(1, &b"orphan"[..]).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 505);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn publish_to_an_unknown_exchange_closes_only_the_channel_with_404() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_publish(1, "missing", "rk", false, false).await;
    client
        .send_content(1, &BasicProperties::default(), b"payload")
        .await;

    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 404);
    assert_eq!((close.class_id, close.method_id), (60, 40));

    client.send_channel_close_ok(1).await;
    let _ = client.open_channel(2).await;
    client.close_channel(2).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}
