//! Wolf broker binary.

use std::error::Error;

use tokio::signal;
use wolf_server::{ServerConfig, start};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (addr, shutdown) = start(ServerConfig::default()).await?;
    println!("wolf listening on {addr}");

    signal::ctrl_c().await?;
    println!("shutting down");
    shutdown.shutdown().await?;

    Ok(())
}
