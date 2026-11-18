//! AMQP Queue class (50) methods.

use bytes::BufMut;

use crate::error::{Error, Result};
use crate::methods::{bit_set, pack_bits};
use crate::types::{FieldTable, Reader, ShortString, Writer};

/// AMQP Queue class id.
pub const CLASS_ID: u16 = 50;

/// `queue.declare` method id.
pub const DECLARE: u16 = 10;
/// `queue.declare-ok` method id.
pub const DECLARE_OK: u16 = 11;
/// `queue.bind` method id.
pub const BIND: u16 = 20;
/// `queue.bind-ok` method id.
pub const BIND_OK: u16 = 21;
/// `queue.purge` method id.
pub const PURGE: u16 = 30;
/// `queue.purge-ok` method id.
pub const PURGE_OK: u16 = 31;
/// `queue.delete` method id.
pub const DELETE: u16 = 40;
/// `queue.delete-ok` method id.
pub const DELETE_OK: u16 = 41;
/// `queue.unbind` method id.
pub const UNBIND: u16 = 50;
/// `queue.unbind-ok` method id.
pub const UNBIND_OK: u16 = 51;

/// `queue.declare` — request to declare a queue.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclare {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Queue name, or empty for a server-generated name.
    pub queue: ShortString,
    /// Whether the server should only check for an existing queue.
    pub passive: bool,
    /// Whether the queue survives broker restarts.
    pub durable: bool,
    /// Whether the queue is used by only one connection.
    pub exclusive: bool,
    /// Whether the queue is deleted when the last consumer leaves.
    pub auto_delete: bool,
    /// Whether the client expects no `queue.declare-ok` reply.
    pub nowait: bool,
    /// Queue-specific arguments.
    pub arguments: FieldTable,
}

/// `queue.declare-ok` — successful queue declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDeclareOk {
    /// Queue name.
    pub queue: ShortString,
    /// Number of messages ready in the queue.
    pub message_count: u32,
    /// Number of active consumers.
    pub consumer_count: u32,
}

/// `queue.bind` — request to bind a queue to an exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueBind {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Queue name.
    pub queue: ShortString,
    /// Exchange name.
    pub exchange: ShortString,
    /// Routing key.
    pub routing_key: ShortString,
    /// Whether the client expects no `queue.bind-ok` reply.
    pub nowait: bool,
    /// Binding arguments.
    pub arguments: FieldTable,
}

/// `queue.bind-ok` — acknowledgement of `queue.bind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueBindOk;

/// `queue.purge` — request to remove all messages from a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePurge {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Queue name.
    pub queue: ShortString,
    /// Whether the client expects no `queue.purge-ok` reply.
    pub nowait: bool,
}

/// `queue.purge-ok` — acknowledgement of `queue.purge`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePurgeOk {
    /// Number of messages removed.
    pub message_count: u32,
}

/// `queue.delete` — request to delete a queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDelete {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Queue name.
    pub queue: ShortString,
    /// Whether the server should only delete an unused queue.
    pub if_unused: bool,
    /// Whether the server should only delete an empty queue.
    pub if_empty: bool,
    /// Whether the client expects no `queue.delete-ok` reply.
    pub nowait: bool,
}

/// `queue.delete-ok` — acknowledgement of `queue.delete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDeleteOk {
    /// Number of messages deleted with the queue.
    pub message_count: u32,
}

/// `queue.unbind` — request to unbind a queue from an exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueUnbind {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Queue name.
    pub queue: ShortString,
    /// Exchange name.
    pub exchange: ShortString,
    /// Routing key.
    pub routing_key: ShortString,
    /// Binding arguments.
    pub arguments: FieldTable,
}

/// `queue.unbind-ok` — acknowledgement of `queue.unbind`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueUnbindOk;

/// A method from the AMQP Queue class.
#[derive(Debug, Clone, PartialEq)]
pub enum QueueMethod {
    /// `queue.declare`
    Declare(QueueDeclare),
    /// `queue.declare-ok`
    DeclareOk(QueueDeclareOk),
    /// `queue.bind`
    Bind(QueueBind),
    /// `queue.bind-ok`
    BindOk(QueueBindOk),
    /// `queue.purge`
    Purge(QueuePurge),
    /// `queue.purge-ok`
    PurgeOk(QueuePurgeOk),
    /// `queue.delete`
    Delete(QueueDelete),
    /// `queue.delete-ok`
    DeleteOk(QueueDeleteOk),
    /// `queue.unbind`
    Unbind(QueueUnbind),
    /// `queue.unbind-ok`
    UnbindOk(QueueUnbindOk),
}

impl QueueMethod {
    /// Returns the AMQP class id (always 50).
    pub fn class_id(&self) -> u16 {
        CLASS_ID
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Declare(_) => DECLARE,
            Self::DeclareOk(_) => DECLARE_OK,
            Self::Bind(_) => BIND,
            Self::BindOk(_) => BIND_OK,
            Self::Purge(_) => PURGE,
            Self::PurgeOk(_) => PURGE_OK,
            Self::Delete(_) => DELETE,
            Self::DeleteOk(_) => DELETE_OK,
            Self::Unbind(_) => UNBIND,
            Self::UnbindOk(_) => UNBIND_OK,
        }
    }

    pub(crate) fn encode_body_into<B: BufMut>(&self, writer: &mut Writer<B>) -> Result<()> {
        match self {
            Self::Declare(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.queue.as_str())?;
                writer.write_u8(pack_bits(&[
                    method.passive,
                    method.durable,
                    method.exclusive,
                    method.auto_delete,
                    method.nowait,
                ]));
                writer.write_field_table(&method.arguments, 0)?;
            }
            Self::DeclareOk(method) => {
                writer.write_short_string(method.queue.as_str())?;
                writer.write_u32(method.message_count);
                writer.write_u32(method.consumer_count);
            }
            Self::Bind(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.queue.as_str())?;
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_short_string(method.routing_key.as_str())?;
                writer.write_u8(pack_bits(&[method.nowait]));
                writer.write_field_table(&method.arguments, 0)?;
            }
            Self::BindOk(_) => {}
            Self::Purge(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.queue.as_str())?;
                writer.write_u8(pack_bits(&[method.nowait]));
            }
            Self::PurgeOk(method) => {
                writer.write_u32(method.message_count);
            }
            Self::Delete(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.queue.as_str())?;
                writer.write_u8(pack_bits(&[
                    method.if_unused,
                    method.if_empty,
                    method.nowait,
                ]));
            }
            Self::DeleteOk(method) => {
                writer.write_u32(method.message_count);
            }
            Self::Unbind(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.queue.as_str())?;
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_short_string(method.routing_key.as_str())?;
                writer.write_field_table(&method.arguments, 0)?;
            }
            Self::UnbindOk(_) => {}
        }
        Ok(())
    }

    pub(crate) fn decode(method_id: u16, reader: &mut Reader<'_>) -> Result<Self> {
        let method = match method_id {
            DECLARE => {
                let ticket = reader.read_u16()?;
                let queue = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Declare(QueueDeclare {
                    ticket,
                    queue,
                    passive: bit_set(bits, 0),
                    durable: bit_set(bits, 1),
                    exclusive: bit_set(bits, 2),
                    auto_delete: bit_set(bits, 3),
                    nowait: bit_set(bits, 4),
                    arguments: reader.read_field_table(0)?,
                })
            }
            DECLARE_OK => Self::DeclareOk(QueueDeclareOk {
                queue: reader.read_short_string()?,
                message_count: reader.read_u32()?,
                consumer_count: reader.read_u32()?,
            }),
            BIND => {
                let ticket = reader.read_u16()?;
                let queue = reader.read_short_string()?;
                let exchange = reader.read_short_string()?;
                let routing_key = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Bind(QueueBind {
                    ticket,
                    queue,
                    exchange,
                    routing_key,
                    nowait: bit_set(bits, 0),
                    arguments: reader.read_field_table(0)?,
                })
            }
            BIND_OK => Self::BindOk(QueueBindOk),
            PURGE => {
                let ticket = reader.read_u16()?;
                let queue = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Purge(QueuePurge {
                    ticket,
                    queue,
                    nowait: bit_set(bits, 0),
                })
            }
            PURGE_OK => Self::PurgeOk(QueuePurgeOk {
                message_count: reader.read_u32()?,
            }),
            DELETE => {
                let ticket = reader.read_u16()?;
                let queue = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Delete(QueueDelete {
                    ticket,
                    queue,
                    if_unused: bit_set(bits, 0),
                    if_empty: bit_set(bits, 1),
                    nowait: bit_set(bits, 2),
                })
            }
            DELETE_OK => Self::DeleteOk(QueueDeleteOk {
                message_count: reader.read_u32()?,
            }),
            UNBIND => Self::Unbind(QueueUnbind {
                ticket: reader.read_u16()?,
                queue: reader.read_short_string()?,
                exchange: reader.read_short_string()?,
                routing_key: reader.read_short_string()?,
                arguments: reader.read_field_table(0)?,
            }),
            UNBIND_OK => Self::UnbindOk(QueueUnbindOk),
            other => {
                return Err(Error::UnknownMethod {
                    class_id: CLASS_ID,
                    method_id: other,
                });
            }
        };
        Ok(method)
    }
}
