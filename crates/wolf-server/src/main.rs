//! Wolf broker binary.

use std::error::Error;

use tokio::signal;
use wolf_server::{ServerConfig, start};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = match ServerConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("wolf configuration error: {error}");
            std::process::exit(1);
        }
    };
    let (addr, shutdown) = start(config).await?;
    println!("wolf listening on {addr}");

    signal::ctrl_c().await?;
    println!("shutting down");
    shutdown.shutdown().await?;

    Ok(())
}
