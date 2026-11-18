//! AMQP 0-9-1 frame and method encoding/decoding. Pure, no I/O.
//!
//! The codec covers the protocol header, frames, field tables, basic
//! content-header properties and the connection, channel, exchange, queue,
//! confirm and tx method sets. Remaining higher-level classes are represented
//! by class-id stubs until their tickets land.

pub mod error;
pub mod field_table;
pub mod frame;
pub mod methods;
pub mod properties;
pub mod types;

pub use error::{Error, Result};
pub use field_table::{FieldTable, FieldValue, MAX_TABLE_DEPTH};
pub use frame::{
    ContentHeader, FRAME_END, Frame, FrameType, MAX_FRAME_SIZE, ProtocolHeader, decode_frame,
};
pub use methods::channel::{
    ChannelClose, ChannelCloseOk, ChannelMethod, ChannelOpen, ChannelOpenOk,
};
pub use methods::confirm::{ConfirmMethod, ConfirmSelect, ConfirmSelectOk};
pub use methods::connection::{
    Close, CloseOk, ConnectionMethod, Open, OpenOk, Start, StartOk, Tune, TuneOk,
};
pub use methods::exchange::{
    ExchangeDeclare, ExchangeDeclareOk, ExchangeDelete, ExchangeDeleteOk, ExchangeMethod,
};
pub use methods::queue::{
    QueueBind, QueueBindOk, QueueDeclare, QueueDeclareOk, QueueDelete, QueueDeleteOk, QueueMethod,
    QueuePurge, QueuePurgeOk, QueueUnbind, QueueUnbindOk,
};
pub use methods::tx::{
    TxCommit, TxCommitOk, TxMethod, TxRollback, TxRollbackOk, TxSelect, TxSelectOk,
};
pub use methods::{Method, channel, confirm, connection, exchange, queue, tx};
pub use properties::BasicProperties;
pub use types::{LongString, ShortString};
