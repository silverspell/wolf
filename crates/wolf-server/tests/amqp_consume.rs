//! Raw-wire tests for `basic.qos`, consume/cancel, deliveries and cancels.

mod support;

use bytes::Bytes;
use wolf_amqp_codec::{
    BasicDeliver, BasicMethod, BasicProperties, ExchangeDeclare, ExchangeMethod, FieldTable,
    Method, QueueBind, QueueDeclare, QueueDelete, QueueMethod, ShortString,
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

async fn declare_direct_exchange(client: &mut TestClient, channel: u16, name: &str) {
    client
        .send_method(
            channel,
            Method::Exchange(ExchangeMethod::Declare(ExchangeDeclare {
                ticket: 0,
                exchange: ShortString::new(name),
                exchange_type: ShortString::new("direct"),
                passive: false,
                durable: false,
                auto_delete: false,
                internal: false,
                nowait: false,
                arguments: FieldTable::new(),
            })),
        )
        .await;
    match client.read_method_on(channel).await {
        Method::Exchange(ExchangeMethod::DeclareOk(_)) => {}
        other => panic!("expected exchange.declare-ok, got {other:?}"),
    }
}

async fn bind_queue(
    client: &mut TestClient,
    channel: u16,
    queue: &str,
    exchange: &str,
    routing_key: &str,
) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Bind(QueueBind {
                ticket: 0,
                queue: ShortString::new(queue),
                exchange: ShortString::new(exchange),
                routing_key: ShortString::new(routing_key),
                nowait: false,
                arguments: FieldTable::new(),
            })),
        )
        .await;
    match client.read_method_on(channel).await {
        Method::Queue(QueueMethod::BindOk(_)) => {}
        other => panic!("expected queue.bind-ok, got {other:?}"),
    }
}

async fn publish(
    client: &mut TestClient,
    channel: u16,
    exchange: &str,
    routing_key: &str,
    properties: &BasicProperties,
    body: &[u8],
) {
    client
        .send_publish(channel, exchange, routing_key, false, false)
        .await;
    client.send_content(channel, properties, body).await;
}

async fn delete_queue(client: &mut TestClient, channel: u16, name: &str) {
    client
        .send_method(
            channel,
            Method::Queue(QueueMethod::Delete(QueueDelete {
                ticket: 0,
                queue: ShortString::new(name),
                if_unused: false,
                if_empty: false,
                nowait: false,
            })),
        )
        .await;
}

async fn read_delivery(client: &mut TestClient, channel: u16) -> (BasicDeliver, Bytes) {
    let (frame_channel, method) = client.read_method().await;
    assert_eq!(frame_channel, channel);
    let Method::Basic(BasicMethod::Deliver(deliver)) = method else {
        panic!("expected basic.deliver, got {method:?}");
    };
    let (_, body) = client.read_content(channel).await;
    (deliver, body)
}

fn expect_consume_ok(method: Method, expected_tag: &str) {
    let Method::Basic(BasicMethod::ConsumeOk(ok)) = method else {
        panic!("expected basic.consume-ok, got {method:?}");
    };
    assert_eq!(ok.consumer_tag.as_str(), expected_tag);
}

#[tokio::test]
async fn consume_ok_precedes_delivery_with_publish_metadata() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_direct_exchange(&mut client, 1, "events").await;
    declare_queue(&mut client, 1, "jobs").await;
    bind_queue(&mut client, 1, "jobs", "events", "rk").await;

    let properties = BasicProperties {
        content_type: Some(ShortString::new("text/plain")),
        message_id: Some(ShortString::new("m-1")),
        ..BasicProperties::default()
    };
    publish(&mut client, 1, "events", "rk", &properties, b"hello").await;

    client.send_consume(1, "jobs", "ctag", false, false).await;
    expect_consume_ok(client.read_method_on(1).await, "ctag");

    let (delivery, body) = read_delivery(&mut client, 1).await;
    assert_eq!(delivery.consumer_tag.as_str(), "ctag");
    assert_eq!(delivery.delivery_tag, 1);
    assert!(!delivery.redelivered);
    assert_eq!(delivery.exchange.as_str(), "events");
    assert_eq!(delivery.routing_key.as_str(), "rk");
    assert_eq!(body.as_ref(), b"hello");

    client.send_ack(1, delivery.delivery_tag, false).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn cancel_suppresses_delivery_to_the_cancelled_consumer() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    publish(
        &mut client,
        1,
        "",
        "jobs",
        &BasicProperties::default(),
        b"first",
    )
    .await;

    client.send_consume(1, "jobs", "ctag", false, false).await;
    expect_consume_ok(client.read_method_on(1).await, "ctag");
    let (delivery, _) = read_delivery(&mut client, 1).await;
    client.send_ack(1, delivery.delivery_tag, false).await;

    client.send_cancel(1, "ctag", false).await;
    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::CancelOk(ok)) => {
            assert_eq!(ok.consumer_tag.as_str(), "ctag");
        }
        other => panic!("expected basic.cancel-ok, got {other:?}"),
    }

    publish(
        &mut client,
        1,
        "",
        "jobs",
        &BasicProperties::default(),
        b"second",
    )
    .await;
    client.send_get(1, "jobs", true).await;

    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::GetOk(ok)) => {
            let (_, body) = client.read_content(1).await;
            assert_eq!(body.as_ref(), b"second");
            assert!(!ok.redelivered);
        }
        other => panic!("expected basic.get-ok, got {other:?}"),
    }

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn qos_prefetch_size_is_rejected_with_540() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_qos(1, 1024, 10, false).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 540);
    assert_eq!((close.class_id, close.method_id), (60, 10));

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn qos_prefetch_count_and_global_flag_are_applied() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    let _ = client.open_channel(2).await;
    declare_queue(&mut client, 1, "jobs").await;

    client.send_qos(1, 0, 1, true).await;
    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::QosOk(_)) => {}
        other => panic!("expected basic.qos-ok, got {other:?}"),
    }
    client.send_consume(1, "jobs", "ctag", false, false).await;
    expect_consume_ok(client.read_method_on(1).await, "ctag");

    publish(
        &mut client,
        1,
        "",
        "jobs",
        &BasicProperties::default(),
        b"first",
    )
    .await;
    publish(
        &mut client,
        1,
        "",
        "jobs",
        &BasicProperties::default(),
        b"second",
    )
    .await;

    let (first, body) = read_delivery(&mut client, 1).await;
    assert_eq!(body.as_ref(), b"first");

    client.send_get(2, "jobs", true).await;
    match client.read_method_on(2).await {
        Method::Basic(BasicMethod::GetOk(_)) => {
            let (_, body) = client.read_content(2).await;
            assert_eq!(body.as_ref(), b"second");
        }
        other => panic!("expected prefetch to retain the second message, got {other:?}"),
    }

    client.send_ack(1, first.delivery_tag, false).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn cross_connection_consume_receives_another_connections_publish() {
    let (addr, handle) = start_server().await;
    let mut producer = TestClient::connect(addr).await;
    producer.handshake().await;
    let _ = producer.open_channel(1).await;
    declare_queue(&mut producer, 1, "jobs").await;

    let mut consumer = TestClient::connect(addr).await;
    consumer.handshake().await;
    let _ = consumer.open_channel(1).await;
    consumer
        .send_consume(1, "jobs", "remote", false, false)
        .await;
    expect_consume_ok(consumer.read_method_on(1).await, "remote");

    publish(
        &mut producer,
        1,
        "",
        "jobs",
        &BasicProperties::default(),
        b"cross",
    )
    .await;

    let (delivery, body) = read_delivery(&mut consumer, 1).await;
    assert_eq!(delivery.consumer_tag.as_str(), "remote");
    assert_eq!(body.as_ref(), b"cross");

    drop(consumer);
    drop(producer);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn queue_delete_cancels_local_consumer_before_delete_ok() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    client.send_consume(1, "jobs", "local", false, false).await;
    expect_consume_ok(client.read_method_on(1).await, "local");

    delete_queue(&mut client, 1, "jobs").await;

    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::Cancel(cancel)) => {
            assert_eq!(cancel.consumer_tag.as_str(), "local");
            assert!(cancel.nowait);
        }
        other => panic!("expected basic.cancel first, got {other:?}"),
    }
    match client.read_method_on(1).await {
        Method::Queue(QueueMethod::DeleteOk(_)) => {}
        other => panic!("expected queue.delete-ok second, got {other:?}"),
    }

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn queue_delete_cancels_cross_connection_consumers() {
    let (addr, handle) = start_server().await;
    let mut owner = TestClient::connect(addr).await;
    owner.handshake().await;
    let _ = owner.open_channel(1).await;
    declare_queue(&mut owner, 1, "jobs").await;

    let mut consumer = TestClient::connect(addr).await;
    consumer.handshake().await;
    let _ = consumer.open_channel(1).await;
    consumer
        .send_consume(1, "jobs", "remote", false, false)
        .await;
    expect_consume_ok(consumer.read_method_on(1).await, "remote");

    delete_queue(&mut owner, 1, "jobs").await;

    match consumer.read_method_on(1).await {
        Method::Basic(BasicMethod::Cancel(cancel)) => {
            assert_eq!(cancel.consumer_tag.as_str(), "remote");
        }
        other => panic!("expected basic.cancel for the remote consumer, got {other:?}"),
    }
    match owner.read_method_on(1).await {
        Method::Queue(QueueMethod::DeleteOk(_)) => {}
        other => panic!("expected queue.delete-ok, got {other:?}"),
    }

    drop(consumer);
    drop(owner);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn nack_requeue_redelivers_to_the_same_consumer() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    client.send_consume(1, "jobs", "ctag", false, false).await;
    expect_consume_ok(client.read_method_on(1).await, "ctag");

    publish(
        &mut client,
        1,
        "",
        "jobs",
        &BasicProperties::default(),
        b"redeliver",
    )
    .await;
    let (first, body) = read_delivery(&mut client, 1).await;
    assert_eq!(body.as_ref(), b"redeliver");
    assert!(!first.redelivered);

    client.send_nack(1, first.delivery_tag, false, true).await;

    let (second, body) = read_delivery(&mut client, 1).await;
    assert!(second.redelivered);
    assert_eq!(body.as_ref(), b"redeliver");
    client.send_ack(1, second.delivery_tag, false).await;

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn recover_ok_precedes_redelivery_to_the_same_consumer() {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;
    declare_queue(&mut client, 1, "jobs").await;
    client.send_consume(1, "jobs", "ctag", false, false).await;
    expect_consume_ok(client.read_method_on(1).await, "ctag");

    publish(
        &mut client,
        1,
        "",
        "jobs",
        &BasicProperties::default(),
        b"recovered",
    )
    .await;
    let _ = read_delivery(&mut client, 1).await;

    client.send_recover(1, true).await;
    match client.read_method_on(1).await {
        Method::Basic(BasicMethod::RecoverOk(_)) => {}
        other => panic!("expected basic.recover-ok first, got {other:?}"),
    }
    let (delivery, body) = read_delivery(&mut client, 1).await;
    assert!(delivery.redelivered);
    assert_eq!(body.as_ref(), b"recovered");

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}
