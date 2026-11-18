//! Raw-wire tests for the AMQP queue class methods.

mod support;

use wolf_amqp_codec::{
    BasicProperties, FieldTable, FieldValue, LongString, Method, QueueBind, QueueDeclare,
    QueueDelete, QueueMethod, QueuePurge, QueueUnbind, ShortString, queue,
};

use support::{TestClient, start_server};

#[allow(clippy::too_many_arguments)]
async fn send_declare(
    client: &mut TestClient,
    channel: u16,
    name: &str,
    passive: bool,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    nowait: bool,
    arguments: FieldTable,
) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Declare(QueueDeclare {
                ticket: 0,
                queue: ShortString::new(name),
                passive,
                durable,
                exclusive,
                auto_delete,
                nowait,
                arguments,
            })),
        )
        .await;
}

async fn send_bind(
    client: &mut TestClient,
    channel: u16,
    queue_name: &str,
    exchange: &str,
    routing_key: &str,
    nowait: bool,
) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Bind(QueueBind {
                ticket: 0,
                queue: ShortString::new(queue_name),
                exchange: ShortString::new(exchange),
                routing_key: ShortString::new(routing_key),
                nowait,
                arguments: FieldTable::new(),
            })),
        )
        .await;
}

async fn send_unbind(
    client: &mut TestClient,
    channel: u16,
    queue_name: &str,
    exchange: &str,
    routing_key: &str,
) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Unbind(QueueUnbind {
                ticket: 0,
                queue: ShortString::new(queue_name),
                exchange: ShortString::new(exchange),
                routing_key: ShortString::new(routing_key),
                arguments: FieldTable::new(),
            })),
        )
        .await;
}

async fn send_purge(client: &mut TestClient, channel: u16, queue_name: &str) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Purge(QueuePurge {
                ticket: 0,
                queue: ShortString::new(queue_name),
                nowait: false,
            })),
        )
        .await;
}

async fn send_delete(
    client: &mut TestClient,
    channel: u16,
    queue_name: &str,
    if_unused: bool,
    if_empty: bool,
    nowait: bool,
) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Delete(QueueDelete {
                ticket: 0,
                queue: ShortString::new(queue_name),
                if_unused,
                if_empty,
                nowait,
            })),
        )
        .await;
}

async fn declare_ok(client: &mut TestClient, channel: u16) -> wolf_amqp_codec::QueueDeclareOk {
    match client.read_method_on(channel).await {
        Method::Queue(QueueMethod::DeclareOk(ok)) => ok,
        other => panic!("expected queue.declare-ok, got {other:?}"),
    }
}

fn expect_bind_ok(method: Method) {
    match method {
        Method::Queue(QueueMethod::BindOk(_)) => {}
        other => panic!("expected queue.bind-ok, got {other:?}"),
    }
}

fn expect_unbind_ok(method: Method) {
    match method {
        Method::Queue(QueueMethod::UnbindOk(_)) => {}
        other => panic!("expected queue.unbind-ok, got {other:?}"),
    }
}

async fn publish(
    client: &mut TestClient,
    channel: u16,
    exchange: &str,
    routing_key: &str,
    body: &[u8],
) {
    client
        .send_publish(channel, exchange, routing_key, false, false)
        .await;
    client
        .send_content(channel, &BasicProperties::default(), body)
        .await;
}

#[tokio::test]
async fn passive_declare_of_missing_queue_closes_only_the_channel() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "missing",
        true,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;

    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 404);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::DECLARE)
    );

    client.send_channel_close_ok(1).await;
    let _ = client.open_channel(2).await;
    client.close_channel(2).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn passive_declare_reports_ready_messages_and_consumers() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "counted",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let ok = declare_ok(&mut client, 1).await;
    assert_eq!(ok.queue.as_str(), "counted");
    assert_eq!((ok.message_count, ok.consumer_count), (0, 0));

    publish(&mut client, 1, "", "counted", b"payload").await;

    send_declare(
        &mut client,
        1,
        "counted",
        true,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let ok = declare_ok(&mut client, 1).await;
    assert_eq!((ok.message_count, ok.consumer_count), (1, 0));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn redeclaring_with_different_arguments_closes_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "stable",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut client, 1).await;

    send_declare(
        &mut client,
        1,
        "stable",
        false,
        true,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::DECLARE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn stream_queue_type_closes_the_channel_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    let mut arguments = FieldTable::new();
    arguments.insert(
        "x-queue-type",
        FieldValue::LongString(LongString::new("stream")),
    );
    send_declare(
        &mut client,
        1,
        "streamed",
        false,
        false,
        false,
        false,
        false,
        arguments,
    )
    .await;

    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::DECLARE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn exclusive_conflict_from_another_connection_closes_with_405() {
    let (addr, handle) = start_server().await;
    let mut owner = TestClient::connect(addr).await;
    owner.handshake().await;
    let _ = owner.open_channel(1).await;

    send_declare(
        &mut owner,
        1,
        "private",
        false,
        false,
        true,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut owner, 1).await;

    let mut intruder = TestClient::connect(addr).await;
    intruder.handshake().await;
    let _ = intruder.open_channel(1).await;

    send_declare(
        &mut intruder,
        1,
        "private",
        false,
        false,
        true,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let close = intruder.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 405);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::DECLARE)
    );

    drop(intruder);
    drop(owner);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn empty_declaration_returns_a_server_named_queue() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let ok = declare_ok(&mut client, 1).await;
    assert!(ok.queue.as_str().starts_with("amq.gen-"));

    let generated = ok.queue.as_str().to_string();
    send_declare(
        &mut client,
        1,
        &generated,
        true,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let ok = declare_ok(&mut client, 1).await;
    assert_eq!(ok.queue.as_str(), generated);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn bind_and_unbind_on_the_default_exchange_close_with_403() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "bound-q",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut client, 1).await;

    send_bind(&mut client, 1, "bound-q", "", "bound-q", false).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 403);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::BIND)
    );
    client.send_channel_close_ok(1).await;

    let _ = client.open_channel(2).await;
    send_unbind(&mut client, 2, "bound-q", "", "bound-q").await;
    let close = client.expect_channel_close(2).await;
    assert_eq!(close.reply_code, 403);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::UNBIND)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn bind_routes_messages_and_purge_reports_the_removed_count() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client
        .send_method(
            1,
            Method::Exchange(wolf_amqp_codec::ExchangeMethod::Declare(
                wolf_amqp_codec::ExchangeDeclare {
                    ticket: 0,
                    exchange: ShortString::new("jobs-x"),
                    exchange_type: ShortString::new("direct"),
                    passive: false,
                    durable: false,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    arguments: FieldTable::new(),
                },
            )),
        )
        .await;
    match client.read_method_on(1).await {
        Method::Exchange(wolf_amqp_codec::ExchangeMethod::DeclareOk(_)) => {}
        other => panic!("expected exchange.declare-ok, got {other:?}"),
    }
    send_declare(
        &mut client,
        1,
        "jobs-q",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut client, 1).await;

    send_bind(&mut client, 1, "jobs-q", "jobs-x", "rk", false).await;
    expect_bind_ok(client.read_method_on(1).await);

    publish(&mut client, 1, "jobs-x", "rk", b"first").await;
    publish(&mut client, 1, "jobs-x", "rk", b"second").await;

    send_purge(&mut client, 1, "jobs-q").await;
    match client.read_method_on(1).await {
        Method::Queue(QueueMethod::PurgeOk(ok)) => assert_eq!(ok.message_count, 2),
        other => panic!("expected queue.purge-ok, got {other:?}"),
    }

    send_purge(&mut client, 1, "jobs-q").await;
    match client.read_method_on(1).await {
        Method::Queue(QueueMethod::PurgeOk(ok)) => assert_eq!(ok.message_count, 0),
        other => panic!("expected queue.purge-ok, got {other:?}"),
    }

    send_unbind(&mut client, 1, "jobs-q", "jobs-x", "rk").await;
    expect_unbind_ok(client.read_method_on(1).await);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn binding_a_missing_queue_closes_with_404() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client
        .send_method(
            1,
            Method::Exchange(wolf_amqp_codec::ExchangeMethod::Declare(
                wolf_amqp_codec::ExchangeDeclare {
                    ticket: 0,
                    exchange: ShortString::new("jobs-x"),
                    exchange_type: ShortString::new("direct"),
                    passive: false,
                    durable: false,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    arguments: FieldTable::new(),
                },
            )),
        )
        .await;
    match client.read_method_on(1).await {
        Method::Exchange(wolf_amqp_codec::ExchangeMethod::DeclareOk(_)) => {}
        other => panic!("expected exchange.declare-ok, got {other:?}"),
    }

    send_bind(&mut client, 1, "missing", "jobs-x", "rk", false).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 404);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::BIND)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn delete_reports_the_discarded_message_count_and_removes_the_queue() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "doomed",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut client, 1).await;
    publish(&mut client, 1, "", "doomed", b"one").await;
    publish(&mut client, 1, "", "doomed", b"two").await;

    send_delete(&mut client, 1, "doomed", false, false, false).await;
    match client.read_method_on(1).await {
        Method::Queue(QueueMethod::DeleteOk(ok)) => assert_eq!(ok.message_count, 2),
        other => panic!("expected queue.delete-ok, got {other:?}"),
    }

    send_declare(
        &mut client,
        1,
        "doomed",
        true,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 404);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn deleting_a_missing_queue_is_idempotent() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_delete(&mut client, 1, "missing", true, true, false).await;
    match client.read_method_on(1).await {
        Method::Queue(QueueMethod::DeleteOk(ok)) => assert_eq!(ok.message_count, 0),
        other => panic!("expected queue.delete-ok, got {other:?}"),
    }

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn deleting_a_non_empty_queue_with_if_empty_closes_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "nonempty",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut client, 1).await;
    publish(&mut client, 1, "", "nonempty", b"body").await;

    send_delete(&mut client, 1, "nonempty", false, true, false).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);
    assert_eq!(
        (close.class_id, close.method_id),
        (queue::CLASS_ID, queue::DELETE)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn nowait_declare_and_delete_send_no_reply() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "nowait-q",
        false,
        false,
        false,
        false,
        true,
        FieldTable::new(),
    )
    .await;
    client.close_channel(1).await;

    let _ = client.open_channel(2).await;
    send_declare(
        &mut client,
        2,
        "nowait-q",
        true,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut client, 2).await;

    send_delete(&mut client, 2, "nowait-q", false, false, true).await;
    client.close_channel(2).await;

    let _ = client.open_channel(3).await;
    send_declare(
        &mut client,
        3,
        "nowait-q",
        true,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let close = client.expect_channel_close(3).await;
    assert_eq!(close.reply_code, 404);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn nowait_delete_of_a_non_empty_queue_still_closes_with_406() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    send_declare(
        &mut client,
        1,
        "nonempty",
        false,
        false,
        false,
        false,
        false,
        FieldTable::new(),
    )
    .await;
    let _ = declare_ok(&mut client, 1).await;
    publish(&mut client, 1, "", "nonempty", b"body").await;

    send_delete(&mut client, 1, "nonempty", false, true, true).await;
    let close = client.expect_channel_close(1).await;
    assert_eq!(close.reply_code, 406);

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}
