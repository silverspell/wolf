#![allow(dead_code)]

//! Lapin-based end-to-end helpers shared by the AMQP e2e suite.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_lite::{Stream, StreamExt};
use lapin::{
    BasicProperties, Channel, ChannelState, Connection, ConnectionProperties, Consumer,
    ConsumerState, ExchangeKind,
    message::{BasicGetMessage, Delivery},
    options::{
        BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicGetOptions,
        BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
    },
    publisher_confirm::PublisherConfirm,
    types::{FieldTable, ShortString},
};
use tokio::time::{sleep, timeout};
use wolf_server::{ServerConfig, ShutdownHandle, start};

/// Timeout for individual client operations.
pub const OP_TIMEOUT: Duration = Duration::from_secs(10);

/// Short window used to prove that no further delivery arrives.
pub const SETTLE: Duration = Duration::from_millis(200);

static NEXT_DATA_DIR: AtomicU64 = AtomicU64::new(0);

fn unique_data_dir() -> PathBuf {
    let id = NEXT_DATA_DIR.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("wolf-e2e-{}-{id}", std::process::id()))
}

/// Builds a loopback-only configuration with an isolated data directory.
pub fn config() -> ServerConfig {
    let mut config = crate::support::config();
    config.data_dir = unique_data_dir();
    std::fs::create_dir_all(&config.data_dir).expect("create test data dir");
    config
}

/// Starts a test server on an ephemeral loopback port.
pub async fn start_server() -> (SocketAddr, ShutdownHandle) {
    start(config()).await.expect("server starts")
}

/// Builds the AMQP URI for a test server.
pub fn uri(addr: SocketAddr) -> String {
    format!(
        "amqp://guest:guest@127.0.0.1:{}/%2f?connection_timeout=5000",
        addr.port()
    )
}

/// Connects a lapin client to the test server.
pub async fn connect(addr: SocketAddr) -> Connection {
    timeout(
        OP_TIMEOUT,
        Connection::connect(&uri(addr), ConnectionProperties::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connection succeeds")
}

/// Opens a new channel.
pub async fn create_channel(connection: &Connection) -> Channel {
    timeout(OP_TIMEOUT, connection.create_channel())
        .await
        .expect("channel creation did not time out")
        .expect("channel opens")
}

/// Declares an exchange with default options.
pub async fn declare_exchange(channel: &Channel, name: &str, kind: ExchangeKind) {
    timeout(
        OP_TIMEOUT,
        channel.exchange_declare(
            name,
            kind,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        ),
    )
    .await
    .expect("exchange.declare did not time out")
    .expect("exchange declares");
}

/// Declares a queue with default options.
pub async fn declare_queue(channel: &Channel, name: &str) {
    timeout(
        OP_TIMEOUT,
        channel.queue_declare(name, QueueDeclareOptions::default(), FieldTable::default()),
    )
    .await
    .expect("queue.declare did not time out")
    .expect("queue declares");
}

/// Binds a queue to an exchange with a routing key.
pub async fn bind(channel: &Channel, queue: &str, exchange: &str, routing_key: &str) {
    timeout(
        OP_TIMEOUT,
        channel.queue_bind(
            queue,
            exchange,
            routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        ),
    )
    .await
    .expect("queue.bind did not time out")
    .expect("queue binds");
}

/// Unbinds a queue from an exchange with a routing key.
pub async fn unbind(channel: &Channel, queue: &str, exchange: &str, routing_key: &str) {
    timeout(
        OP_TIMEOUT,
        channel.queue_unbind(queue, exchange, routing_key, FieldTable::default()),
    )
    .await
    .expect("queue.unbind did not time out")
    .expect("queue unbinds");
}

/// Publishes a message and returns its publisher confirm handle.
pub async fn publish(
    channel: &Channel,
    exchange: &str,
    routing_key: &str,
    body: &[u8],
    properties: BasicProperties,
    options: BasicPublishOptions,
) -> PublisherConfirm {
    timeout(
        OP_TIMEOUT,
        channel.basic_publish(exchange, routing_key, options, body, properties),
    )
    .await
    .expect("basic.publish did not time out")
    .expect("basic.publish sends")
}

/// Publishes a message to the default exchange.
pub async fn publish_to_queue(channel: &Channel, queue: &str, body: &[u8]) {
    let _confirm = publish(
        channel,
        "",
        queue,
        body,
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;
}

/// Starts consuming from a queue.
pub async fn consume(channel: &Channel, queue: &str, tag: &str) -> Consumer {
    timeout(
        OP_TIMEOUT,
        channel.basic_consume(
            queue,
            tag,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ),
    )
    .await
    .expect("basic.consume did not time out")
    .expect("consumer registers")
}

/// Waits for the next delivery from a consumer.
pub async fn next_delivery(consumer: &mut Consumer) -> Delivery {
    timeout(OP_TIMEOUT, consumer.next())
        .await
        .expect("timed out waiting for a delivery")
        .expect("consumer ended before a delivery arrived")
        .expect("delivery received")
}

/// Asserts that no delivery arrives within [`SETTLE`].
pub async fn expect_no_delivery(consumer: &mut Consumer) {
    match timeout(SETTLE, consumer.next()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(Ok(delivery))) => panic!("unexpected delivery: {:?}", delivery.data),
        Ok(Some(Err(error))) => panic!("unexpected consumer error: {error}"),
    }
}

/// Fetches one message with `basic.get`.
pub async fn get(channel: &Channel, queue: &str) -> Option<BasicGetMessage> {
    timeout(
        OP_TIMEOUT,
        channel.basic_get(queue, BasicGetOptions::default()),
    )
    .await
    .expect("basic.get did not time out")
    .expect("basic.get succeeds")
}

/// Fetches one message and acknowledges it.
pub async fn get_and_ack(channel: &Channel, queue: &str) -> Option<(Vec<u8>, bool)> {
    let message = get(channel, queue).await?;
    let body = message.delivery.data.clone();
    let redelivered = message.delivery.redelivered;
    ack_message(&message).await;
    Some((body, redelivered))
}

/// Acknowledges a fetched message.
pub async fn ack_message(message: &BasicGetMessage) {
    timeout(
        OP_TIMEOUT,
        message.delivery.acker.ack(BasicAckOptions::default()),
    )
    .await
    .expect("basic.ack did not time out")
    .expect("basic.ack sends");
}

/// Cancels a consumer by tag.
pub async fn cancel_consumer(channel: &Channel, tag: &str) {
    let _ = channel
        .basic_cancel(tag, BasicCancelOptions::default())
        .await;
}

/// Closes a channel, ignoring an already-closed channel.
pub async fn close_channel(channel: &Channel) {
    let _ = channel.close(200, "test done").await;
}

/// Closes the connection and shuts the server down.
pub async fn finish(connection: &Connection, handle: ShutdownHandle) {
    let _ = connection.close(200, "test done").await;
    handle.shutdown().await.expect("graceful shutdown");
}

/// Polls a condition until it holds or [`OP_TIMEOUT`] elapses.
pub async fn wait_until(mut condition: impl FnMut() -> bool, description: &str) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < OP_TIMEOUT,
            "timed out waiting for {description}"
        );
        sleep(Duration::from_millis(5)).await;
    }
}

/// Waits for a channel to reach its closed state.
pub async fn wait_for_channel_closed(channel: &Channel) {
    wait_until(
        || {
            matches!(
                channel.status().state(),
                ChannelState::Closed | ChannelState::Error
            )
        },
        "channel to close",
    )
    .await;
}

/// Waits for a consumer to be canceled by the server.
pub async fn wait_for_consumer_canceled(consumer: &Consumer) {
    wait_until(
        || consumer.state() == ConsumerState::Canceled,
        "consumer to be canceled",
    )
    .await;
}

/// Asserts that an error is an AMQP protocol error with the expected reply code
/// and close level.
pub fn assert_amqp_error(error: &lapin::Error, expected_code: u16, soft: bool) {
    match error.kind() {
        lapin::ErrorKind::ProtocolError(protocol) => {
            assert_eq!(
                protocol.get_id(),
                expected_code,
                "unexpected reply code: {error}"
            );
            assert_eq!(
                error.is_amqp_soft_error(),
                soft,
                "unexpected close level: {error}"
            );
        }
        other => panic!("expected AMQP protocol error {expected_code}, got {other:?}"),
    }
}

/// Waits for the next AMQP error event and asserts its reply code and level.
pub async fn expect_error_event<S>(mut events: S, expected_code: u16, soft: bool)
where
    S: Stream<Item = lapin::Event> + Unpin,
{
    loop {
        let event = timeout(OP_TIMEOUT, events.next())
            .await
            .expect("timed out waiting for an error event")
            .expect("connection event stream ended");
        if let lapin::Event::Error(error) = event {
            assert_amqp_error(&error, expected_code, soft);
            return;
        }
    }
}

/// Waits for a connection-level AMQP error event and asserts a hard error.
pub async fn expect_connection_error<S>(events: S, expected_code: u16)
where
    S: Stream<Item = lapin::Event> + Unpin,
{
    expect_error_event(events, expected_code, false).await;
}

/// A short-string helper for basic properties.
pub fn short(value: &str) -> ShortString {
    ShortString::from(value)
}
