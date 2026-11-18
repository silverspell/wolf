//! Raw-wire tests for `exchange.declare` and `exchange.delete`.

mod support;

use wolf_amqp_codec::exchange;
use wolf_amqp_codec::{
    ExchangeDeclare, ExchangeDelete, ExchangeMethod, FieldTable, Method, ShortString,
};

use support::{TestClient, start_server};

async fn send_declare(
    client: &mut TestClient,
    channel: u16,
    name: &str,
    exchange_type: &str,
    passive: bool,
    nowait: bool,
) {
    client
        .send_method(
            channel,
            Method::Exchange(ExchangeMethod::Declare(ExchangeDeclare {
                ticket: 0,
                exchange: ShortString::new(name),
                exchange_type: ShortString::new(exchange_type),
                passive,
                durable: false,
                auto_delete: false,
                internal: false,
                nowait,
                arguments: FieldTable::new(),
            })),
        )
        .await;
}

async fn send_delete(
    client: &mut TestClient,
    channel: u16,
    name: &str,
    if_unused: bool,
    nowait: bool,
) {
    client
        .send_method(
            channel,
            Method::Exchange(ExchangeMethod::Delete(ExchangeDelete {
                ticket: 0,
                exchange: ShortString::new(name),
                if_unused,
                nowait,
            })),
        )
        .await;
}

fn expect_declare_ok(method: Method) {
    match method {
        Method::Exchange(ExchangeMethod::DeclareOk(_)) => {}
        other => panic!("expected exchange.declare-ok, got {other:?}"),
    }
}

fn expect_delete_ok(method: Method) {
    match method {
        Method::Exchange(ExchangeMethod::DeleteOk(_)) => {}
        other => panic!("expected exchange.delete-ok, got {other:?}"),
    }
}

#[tokio::test]
async fn passive_declare_of_missing_exchange_closes_only_the_channel() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(&mut client, 1, "missing", "direct", true, false).await;

    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 404);
    assert_eq!(
        (close.class_id, close.method_id),
        (exchange::CLASS_ID, exchange::DECLARE),
        "offending method must be exchange.declare"
    );

    client.send_channel_close_ok(1).await;
    let _ = client.open_channel(2).await;
    send_declare(&mut client, 2, "amq.direct", "direct", true, false).await;
    expect_declare_ok(client.read_method_on(2).await);
    client.close_channel(2).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn redeclaring_with_a_different_kind_closes_the_channel_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(&mut client, 1, "events", "fanout", false, false).await;
    expect_declare_ok(client.read_method_on(1).await);

    send_declare(&mut client, 1, "events", "direct", false, false).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);
    assert_eq!(
        (close.class_id, close.method_id),
        (exchange::CLASS_ID, exchange::DECLARE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn headers_exchange_closes_the_connection_with_503() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(&mut client, 1, "headers-x", "headers", false, false).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 503);
    assert_eq!(
        (close.class_id, close.method_id),
        (exchange::CLASS_ID, exchange::DECLARE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn reserved_exchange_names_are_access_refused() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(&mut client, 1, "amq.custom", "direct", false, false).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 403);
    assert_eq!(
        (close.class_id, close.method_id),
        (exchange::CLASS_ID, exchange::DECLARE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn deleting_reserved_and_default_exchanges_is_access_refused() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_delete(&mut client, 1, "amq.direct", false, false).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 403);
    assert_eq!(
        (close.class_id, close.method_id),
        (exchange::CLASS_ID, exchange::DELETE)
    );
    client.send_channel_close_ok(1).await;

    let _ = client.open_channel(2).await;
    send_delete(&mut client, 2, "", false, false).await;
    let close = client.expect_channel_close(2).await;
    assert_eq!(close.reply_code, 403);
    assert_eq!(
        (close.class_id, close.method_id),
        (exchange::CLASS_ID, exchange::DELETE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn nowait_declare_sends_no_reply_but_still_declares_the_exchange() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(&mut client, 1, "nowait-x", "direct", false, true).await;
    client.close_channel(1).await;

    let _ = client.open_channel(2).await;
    send_declare(&mut client, 2, "nowait-x", "direct", true, false).await;
    expect_declare_ok(client.read_method_on(2).await);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn nowait_declare_of_a_reserved_name_still_closes_the_channel_with_403() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(&mut client, 1, "amq.custom", "direct", false, true).await;

    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 403);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn deleting_an_exchange_with_bindings_and_if_unused_closes_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(&mut client, 1, "bound", "direct", false, false).await;
    expect_declare_ok(client.read_method_on(1).await);

    client
        .send_method(
            1,
            Method::Queue(wolf_amqp_codec::QueueMethod::Declare(
                wolf_amqp_codec::QueueDeclare {
                    ticket: 0,
                    queue: ShortString::new("bound-q"),
                    passive: false,
                    durable: false,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                    arguments: FieldTable::new(),
                },
            )),
        )
        .await;
    match client.read_method_on(1).await {
        Method::Queue(wolf_amqp_codec::QueueMethod::DeclareOk(_)) => {}
        other => panic!("expected queue.declare-ok, got {other:?}"),
    }

    client
        .send_method(
            1,
            Method::Queue(wolf_amqp_codec::QueueMethod::Bind(
                wolf_amqp_codec::QueueBind {
                    ticket: 0,
                    queue: ShortString::new("bound-q"),
                    exchange: ShortString::new("bound"),
                    routing_key: ShortString::new("rk"),
                    nowait: false,
                    arguments: FieldTable::new(),
                },
            )),
        )
        .await;
    match client.read_method_on(1).await {
        Method::Queue(wolf_amqp_codec::QueueMethod::BindOk(_)) => {}
        other => panic!("expected queue.bind-ok, got {other:?}"),
    }

    send_delete(&mut client, 1, "bound", true, false).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);
    assert_eq!(
        (close.class_id, close.method_id),
        (exchange::CLASS_ID, exchange::DELETE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn deleting_a_missing_exchange_is_idempotent() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_delete(&mut client, 1, "missing", true, false).await;
    expect_delete_ok(client.read_method_on(1).await);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}
