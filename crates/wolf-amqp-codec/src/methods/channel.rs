//! AMQP Channel class (20) methods.

use bytes::BufMut;

use crate::error::{Error, Result};
use crate::methods::{bit_set, pack_bits};
use crate::types::{LongString, Reader, ShortString, Writer};

/// AMQP Channel class id.
pub const CLASS_ID: u16 = 20;

/// `channel.open` method id.
pub const OPEN: u16 = 10;
/// `channel.open-ok` method id.
pub const OPEN_OK: u16 = 11;
/// `channel.flow` method id.
pub const FLOW: u16 = 20;
/// `channel.flow-ok` method id.
pub const FLOW_OK: u16 = 21;
/// `channel.close` method id.
pub const CLOSE: u16 = 40;
/// `channel.close-ok` method id.
pub const CLOSE_OK: u16 = 41;

/// `channel.open` — request to open a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelOpen {
    /// Reserved out-of-band field.
    pub out_of_band: ShortString,
}

/// `channel.open-ok` — successful channel open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelOpenOk {
    /// Reserved long string field.
    pub reserved: LongString,
}

/// `channel.flow` — request to pause or resume content flow on a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelFlow {
    /// Whether content flow should be active.
    pub active: bool,
}

/// `channel.flow-ok` — response to `channel.flow`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelFlowOk {
    /// Whether content flow is active.
    pub active: bool,
}

/// `channel.close` — request or notification to close a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelClose {
    /// AMQP reply code.
    pub reply_code: u16,
    /// Human-readable reply text.
    pub reply_text: ShortString,
    /// Class id of the failing method, or zero.
    pub class_id: u16,
    /// Method id of the failing method, or zero.
    pub method_id: u16,
}

/// `channel.close-ok` — acknowledgement of `channel.close`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelCloseOk;

/// A method from the AMQP Channel class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelMethod {
    /// `channel.open`
    Open(ChannelOpen),
    /// `channel.open-ok`
    OpenOk(ChannelOpenOk),
    /// `channel.flow`
    Flow(ChannelFlow),
    /// `channel.flow-ok`
    FlowOk(ChannelFlowOk),
    /// `channel.close`
    Close(ChannelClose),
    /// `channel.close-ok`
    CloseOk(ChannelCloseOk),
}

impl ChannelMethod {
    /// Returns the AMQP class id (always 20).
    pub fn class_id(&self) -> u16 {
        CLASS_ID
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Open(_) => OPEN,
            Self::OpenOk(_) => OPEN_OK,
            Self::Flow(_) => FLOW,
            Self::FlowOk(_) => FLOW_OK,
            Self::Close(_) => CLOSE,
            Self::CloseOk(_) => CLOSE_OK,
        }
    }

    pub(crate) fn encode_body_into<B: BufMut>(&self, writer: &mut Writer<B>) -> Result<()> {
        match self {
            Self::Open(method) => {
                writer.write_short_string(method.out_of_band.as_str())?;
            }
            Self::OpenOk(method) => {
                writer.write_long_string(method.reserved.as_bytes())?;
            }
            Self::Flow(method) => {
                writer.write_u8(pack_bits(&[method.active]));
            }
            Self::FlowOk(method) => {
                writer.write_u8(pack_bits(&[method.active]));
            }
            Self::Close(method) => {
                writer.write_u16(method.reply_code);
                writer.write_short_string(method.reply_text.as_str())?;
                writer.write_u16(method.class_id);
                writer.write_u16(method.method_id);
            }
            Self::CloseOk(_) => {}
        }
        Ok(())
    }

    pub(crate) fn decode(method_id: u16, reader: &mut Reader<'_>) -> Result<Self> {
        let method = match method_id {
            OPEN => Self::Open(ChannelOpen {
                out_of_band: reader.read_short_string()?,
            }),
            OPEN_OK => Self::OpenOk(ChannelOpenOk {
                reserved: reader.read_long_string()?,
            }),
            FLOW => Self::Flow(ChannelFlow {
                active: bit_set(reader.read_u8()?, 0),
            }),
            FLOW_OK => Self::FlowOk(ChannelFlowOk {
                active: bit_set(reader.read_u8()?, 0),
            }),
            CLOSE => Self::Close(ChannelClose {
                reply_code: reader.read_u16()?,
                reply_text: reader.read_short_string()?,
                class_id: reader.read_u16()?,
                method_id: reader.read_u16()?,
            }),
            CLOSE_OK => Self::CloseOk(ChannelCloseOk),
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
