//! Lifecycle contract for the AMQP listener and its sessions.

use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::time::sleep;
use wolf_server::{ServerConfig, ShutdownHandle, start};

const WAIT: Duration = Duration::from_secs(2);

fn config() -> ServerConfig {
    ServerConfig {
        amqp_addr: "127.0.0.1:0".parse().expect("valid loopback address"),
        shutdown_timeout: Duration::from_secs(5),
        ..ServerConfig::default()
    }
}

async fn wait_for_sessions(handle: &ShutdownHandle, expected: usize) {
    let start = Instant::now();
    while handle.active_sessions() != expected {
        assert!(
            start.elapsed() < WAIT,
            "expected {expected} active sessions, found {}",
            handle.active_sessions()
        );
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn start_reports_the_real_loopback_address() {
    let (addr, handle) = start(config()).await.expect("server starts");

    assert!(addr.ip().is_loopback());
    assert_ne!(addr.port(), 0);

    let _connection = TcpStream::connect(addr).await.expect("connect to listener");
    wait_for_sessions(&handle, 1).await;

    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn accepted_connection_creates_a_session() {
    let (addr, handle) = start(config()).await.expect("server starts");

    let _connection = TcpStream::connect(addr).await.expect("connect to listener");
    wait_for_sessions(&handle, 1).await;

    handle.shutdown().await.expect("graceful shutdown");
    wait_for_sessions(&handle, 0).await;

    assert!(
        TcpStream::connect(addr).await.is_err(),
        "listener must be closed after shutdown"
    );
}
