//! AMQP Exchange class (40) methods.

use bytes::BufMut;

use crate::error::{Error, Result};
use crate::methods::{bit_set, pack_bits};
use crate::types::{FieldTable, Reader, ShortString, Writer};

/// AMQP Exchange class id.
pub const CLASS_ID: u16 = 40;

/// `exchange.declare` method id.
pub const DECLARE: u16 = 10;
/// `exchange.declare-ok` method id.
pub const DECLARE_OK: u16 = 11;
/// `exchange.delete` method id.
pub const DELETE: u16 = 20;
/// `exchange.delete-ok` method id.
pub const DELETE_OK: u16 = 21;

/// `exchange.declare` — request to declare an exchange.
#[derive(Debug, Clone, PartialEq)]
pub struct ExchangeDeclare {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Exchange name.
    pub exchange: ShortString,
    /// Exchange type, such as `direct`, `fanout` or `topic`.
    pub exchange_type: ShortString,
    /// Whether the server should only check for an existing exchange.
    pub passive: bool,
    /// Whether the exchange survives broker restarts.
    pub durable: bool,
    /// Whether the exchange is deleted when the last queue unbinds.
    pub auto_delete: bool,
    /// Whether the exchange is internal to the broker.
    pub internal: bool,
    /// Whether the client expects no `exchange.declare-ok` reply.
    pub nowait: bool,
    /// Exchange-specific arguments.
    pub arguments: FieldTable,
}

/// `exchange.declare-ok` — successful exchange declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeDeclareOk;

/// `exchange.delete` — request to delete an exchange.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeDelete {
    /// Reserved ticket field.
    pub ticket: u16,
    /// Exchange name.
    pub exchange: ShortString,
    /// Whether the server should only delete an unused exchange.
    pub if_unused: bool,
    /// Whether the client expects no `exchange.delete-ok` reply.
    pub nowait: bool,
}

/// `exchange.delete-ok` — acknowledgement of `exchange.delete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeDeleteOk;

/// A method from the AMQP Exchange class.
#[derive(Debug, Clone, PartialEq)]
pub enum ExchangeMethod {
    /// `exchange.declare`
    Declare(ExchangeDeclare),
    /// `exchange.declare-ok`
    DeclareOk(ExchangeDeclareOk),
    /// `exchange.delete`
    Delete(ExchangeDelete),
    /// `exchange.delete-ok`
    DeleteOk(ExchangeDeleteOk),
}

impl ExchangeMethod {
    /// Returns the AMQP class id (always 40).
    pub fn class_id(&self) -> u16 {
        CLASS_ID
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Declare(_) => DECLARE,
            Self::DeclareOk(_) => DECLARE_OK,
            Self::Delete(_) => DELETE,
            Self::DeleteOk(_) => DELETE_OK,
        }
    }

    pub(crate) fn encode_body_into<B: BufMut>(&self, writer: &mut Writer<B>) -> Result<()> {
        match self {
            Self::Declare(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_short_string(method.exchange_type.as_str())?;
                writer.write_u8(pack_bits(&[
                    method.passive,
                    method.durable,
                    method.auto_delete,
                    method.internal,
                    method.nowait,
                ]));
                writer.write_field_table(&method.arguments, 0)?;
            }
            Self::DeclareOk(_) => {}
            Self::Delete(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_u8(pack_bits(&[method.if_unused, method.nowait]));
            }
            Self::DeleteOk(_) => {}
        }
        Ok(())
    }

    pub(crate) fn decode(method_id: u16, reader: &mut Reader<'_>) -> Result<Self> {
        let method = match method_id {
            DECLARE => {
                let ticket = reader.read_u16()?;
                let exchange = reader.read_short_string()?;
                let exchange_type = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Declare(ExchangeDeclare {
                    ticket,
                    exchange,
                    exchange_type,
                    passive: bit_set(bits, 0),
                    durable: bit_set(bits, 1),
                    auto_delete: bit_set(bits, 2),
                    internal: bit_set(bits, 3),
                    nowait: bit_set(bits, 4),
                    arguments: reader.read_field_table(0)?,
                })
            }
            DECLARE_OK => Self::DeclareOk(ExchangeDeclareOk),
            DELETE => {
                let ticket = reader.read_u16()?;
                let exchange = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Delete(ExchangeDelete {
                    ticket,
                    exchange,
                    if_unused: bit_set(bits, 0),
                    nowait: bit_set(bits, 1),
                })
            }
            DELETE_OK => Self::DeleteOk(ExchangeDeleteOk),
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
