//! Lapin client integration tests for the M1 connection surface.

mod support;

use std::time::Duration;

use lapin::{Connection, ConnectionProperties};
use tokio::time::{sleep, timeout};

use support::start_server;

const LAPIN_TIMEOUT: Duration = Duration::from_secs(10);

fn uri(port: u16, vhost: &str, extra: &str) -> String {
    format!("amqp://guest:guest@127.0.0.1:{port}/{vhost}?{extra}")
}

#[tokio::test]
async fn lapin_connects_opens_channels_and_survives_heartbeats() {
    let (addr, handle) = start_server().await;
    let uri = uri(addr.port(), "%2f", "heartbeat=1&connection_timeout=5000");

    let connection = timeout(
        LAPIN_TIMEOUT,
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("lapin connect timed out")
    .expect("lapin connects");

    let first = timeout(LAPIN_TIMEOUT, connection.create_channel())
        .await
        .expect("channel creation timed out")
        .expect("first channel opens");

    sleep(Duration::from_secs(2)).await;

    let second = timeout(LAPIN_TIMEOUT, connection.create_channel())
        .await
        .expect("second channel creation timed out")
        .expect("connection stays open across heartbeats");

    first.close(200, "OK").await.expect("first channel closes");
    second
        .close(200, "OK")
        .await
        .expect("second channel closes");
    connection
        .close(200, "OK")
        .await
        .expect("connection closes");

    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn lapin_unknown_vhost_is_rejected() {
    let (addr, handle) = start_server().await;
    let uri = uri(addr.port(), "%2fmissing", "connection_timeout=3000");

    let result = timeout(
        LAPIN_TIMEOUT,
        Connection::connect(&uri, ConnectionProperties::default()),
    )
    .await
    .expect("lapin connect attempt timed out");

    assert!(result.is_err(), "unknown vhost must be rejected");

    handle.shutdown().await.expect("graceful shutdown");
}
