//! Wolf broker server: configuration, listeners, sessions and protocol error
//! mapping.

use wolf_amqp_codec as _;
use wolf_storage as _;
use wolf_stream_codec as _;

pub mod amqp;
pub mod config;
pub mod delivery;
pub mod listener;
pub mod memory;
pub mod session;
pub mod writer;

pub use amqp::auth::Account;
pub use config::{
    ConfigError, DEFAULT_MEMORY_CHECK_INTERVAL_MS, DEFAULT_MEMORY_HIGH_WATERMARK_RELATIVE,
    DEFAULT_OUTBOX_CAPACITY, DEFAULT_QUEUE_CHANNEL_CAPACITY, MEMORY_CHECK_INTERVAL_MS_ENV,
    MEMORY_HIGH_WATERMARK_BYTES_ENV, MEMORY_HIGH_WATERMARK_RELATIVE_ENV, OUTBOX_CAPACITY_ENV,
    ServerConfig,
};
pub use listener::{ShutdownError, ShutdownHandle, start, start_with_memory_probe};
pub use memory::{
    ManualMemoryProbe, MemoryHighWatermark, MemoryLimit, MemoryProbe, MemorySource, MemoryTotal,
    SystemMemoryProbe, total_memory, used_memory,
};
