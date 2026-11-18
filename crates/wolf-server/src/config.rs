//! Server configuration.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

/// Configuration used to start a Wolf server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Address the AMQP listener binds to.
    pub amqp_addr: SocketAddr,
    /// Root directory for persistent broker state.
    pub data_dir: PathBuf,
    /// Maximum time to wait for sessions to finish during shutdown.
    pub shutdown_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            amqp_addr: SocketAddr::from(([127, 0, 0, 1], 5672)),
            data_dir: PathBuf::from("data"),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}
