//! AMQP Confirm class (85) methods.

use bytes::BufMut;

use crate::error::{Error, Result};
use crate::types::{Reader, Writer};

/// AMQP Confirm class id.
pub const CLASS_ID: u16 = 85;

/// `confirm.select` method id.
pub const SELECT: u16 = 10;
/// `confirm.select-ok` method id.
pub const SELECT_OK: u16 = 11;

/// `confirm.select` — request to put a channel in confirm mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmSelect {
    /// Whether the client expects no `confirm.select-ok` reply.
    pub nowait: bool,
}

/// `confirm.select-ok` — acknowledgement of `confirm.select`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmSelectOk;

/// A method from the AMQP Confirm class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfirmMethod {
    /// `confirm.select`
    Select(ConfirmSelect),
    /// `confirm.select-ok`
    SelectOk(ConfirmSelectOk),
}

impl ConfirmMethod {
    /// Returns the AMQP class id (always 85).
    pub fn class_id(&self) -> u16 {
        CLASS_ID
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Select(_) => SELECT,
            Self::SelectOk(_) => SELECT_OK,
        }
    }

    pub(crate) fn encode_body_into<B: BufMut>(&self, writer: &mut Writer<B>) -> Result<()> {
        match self {
            Self::Select(method) => writer.write_u8(u8::from(method.nowait)),
            Self::SelectOk(_) => {}
        }
        Ok(())
    }

    pub(crate) fn decode(method_id: u16, reader: &mut Reader<'_>) -> Result<Self> {
        let method = match method_id {
            SELECT => Self::Select(ConfirmSelect {
                nowait: reader.read_u8()? != 0,
            }),
            SELECT_OK => Self::SelectOk(ConfirmSelectOk),
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
