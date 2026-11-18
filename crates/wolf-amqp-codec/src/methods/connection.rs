//! AMQP connection class (class id 10) methods.

use bytes::BufMut;

use crate::error::{Error, Result};
use crate::types::{FieldTable, LongString, Reader, ShortString, Writer};

/// AMQP connection class id.
pub const CLASS_ID: u16 = 10;

/// `connection.start` method id.
pub const START: u16 = 10;
/// `connection.start-ok` method id.
pub const START_OK: u16 = 11;
/// `connection.tune` method id.
pub const TUNE: u16 = 30;
/// `connection.tune-ok` method id.
pub const TUNE_OK: u16 = 31;
/// `connection.open` method id.
pub const OPEN: u16 = 40;
/// `connection.open-ok` method id.
pub const OPEN_OK: u16 = 41;
/// `connection.close` method id.
pub const CLOSE: u16 = 50;
/// `connection.close-ok` method id.
pub const CLOSE_OK: u16 = 51;
/// `connection.blocked` method id.
pub const BLOCKED: u16 = 60;
/// `connection.unblocked` method id.
pub const UNBLOCKED: u16 = 61;

/// `connection.start` — server greeting.
#[derive(Debug, Clone, PartialEq)]
pub struct Start {
    /// Negotiated AMQP major version.
    pub version_major: u8,
    /// Negotiated AMQP minor version.
    pub version_minor: u8,
    /// Server capabilities and implementation properties.
    pub server_properties: FieldTable,
    /// Supported SASL mechanisms.
    pub mechanisms: LongString,
    /// Supported locales.
    pub locales: LongString,
}

/// `connection.start-ok` — client response to `connection.start`.
#[derive(Debug, Clone, PartialEq)]
pub struct StartOk {
    /// Client implementation properties.
    pub client_properties: FieldTable,
    /// Selected SASL mechanism.
    pub mechanism: ShortString,
    /// SASL response bytes.
    pub response: LongString,
    /// Selected locale.
    pub locale: ShortString,
}

/// `connection.tune` — server connection tuning parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tune {
    /// Maximum number of channels.
    pub channel_max: u16,
    /// Maximum frame size in bytes.
    pub frame_max: u32,
    /// Heartbeat interval in seconds.
    pub heartbeat: u16,
}

/// `connection.tune-ok` — client-selected tuning parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TuneOk {
    /// Maximum number of channels.
    pub channel_max: u16,
    /// Maximum frame size in bytes.
    pub frame_max: u32,
    /// Heartbeat interval in seconds.
    pub heartbeat: u16,
}

/// `connection.open` — request to open a virtual host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Open {
    /// Virtual host name.
    pub virtual_host: ShortString,
    /// Deprecated capabilities field, kept for compatibility.
    pub capabilities: ShortString,
    /// Whether the client insists on a dedicated connection.
    pub insist: bool,
}

/// `connection.open-ok` — successful virtual host open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOk {
    /// Reserved known-hosts field.
    pub known_hosts: ShortString,
}

/// `connection.close` — request or notification to close the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Close {
    /// AMQP reply code.
    pub reply_code: u16,
    /// Human-readable reply text.
    pub reply_text: ShortString,
    /// Class id of the failing method, or zero.
    pub class_id: u16,
    /// Method id of the failing method, or zero.
    pub method_id: u16,
}

/// `connection.close-ok` — acknowledgement of `connection.close`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseOk;

/// `connection.blocked` — the server has raised a resource alarm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blocked {
    /// Human-readable reason for the block, such as `low on memory`.
    pub reason: ShortString,
}

/// `connection.unblocked` — the server's resource alarm has cleared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unblocked;

/// A method from the AMQP connection class.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionMethod {
    /// `connection.start`
    Start(Start),
    /// `connection.start-ok`
    StartOk(StartOk),
    /// `connection.tune`
    Tune(Tune),
    /// `connection.tune-ok`
    TuneOk(TuneOk),
    /// `connection.open`
    Open(Open),
    /// `connection.open-ok`
    OpenOk(OpenOk),
    /// `connection.close`
    Close(Close),
    /// `connection.close-ok`
    CloseOk(CloseOk),
    /// `connection.blocked`
    Blocked(Blocked),
    /// `connection.unblocked`
    Unblocked(Unblocked),
}

impl ConnectionMethod {
    /// Returns the AMQP class id (always 10).
    pub fn class_id(&self) -> u16 {
        CLASS_ID
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Start(_) => START,
            Self::StartOk(_) => START_OK,
            Self::Tune(_) => TUNE,
            Self::TuneOk(_) => TUNE_OK,
            Self::Open(_) => OPEN,
            Self::OpenOk(_) => OPEN_OK,
            Self::Close(_) => CLOSE,
            Self::CloseOk(_) => CLOSE_OK,
            Self::Blocked(_) => BLOCKED,
            Self::Unblocked(_) => UNBLOCKED,
        }
    }

    pub(crate) fn encode_body_into<B: BufMut>(&self, writer: &mut Writer<B>) -> Result<()> {
        match self {
            Self::Start(method) => {
                writer.write_u8(method.version_major);
                writer.write_u8(method.version_minor);
                writer.write_field_table(&method.server_properties, 0)?;
                writer.write_long_string(method.mechanisms.as_bytes())?;
                writer.write_long_string(method.locales.as_bytes())?;
            }
            Self::StartOk(method) => {
                writer.write_field_table(&method.client_properties, 0)?;
                writer.write_short_string(method.mechanism.as_str())?;
                writer.write_long_string(method.response.as_bytes())?;
                writer.write_short_string(method.locale.as_str())?;
            }
            Self::Tune(method) => encode_tune(
                writer,
                method.channel_max,
                method.frame_max,
                method.heartbeat,
            ),
            Self::TuneOk(method) => encode_tune(
                writer,
                method.channel_max,
                method.frame_max,
                method.heartbeat,
            ),
            Self::Open(method) => {
                writer.write_short_string(method.virtual_host.as_str())?;
                writer.write_short_string(method.capabilities.as_str())?;
                writer.write_u8(u8::from(method.insist));
            }
            Self::OpenOk(method) => {
                writer.write_short_string(method.known_hosts.as_str())?;
            }
            Self::Close(method) => {
                writer.write_u16(method.reply_code);
                writer.write_short_string(method.reply_text.as_str())?;
                writer.write_u16(method.class_id);
                writer.write_u16(method.method_id);
            }
            Self::CloseOk(_) => {}
            Self::Blocked(method) => {
                writer.write_short_string(method.reason.as_str())?;
            }
            Self::Unblocked(_) => {}
        }
        Ok(())
    }

    pub(crate) fn decode(method_id: u16, reader: &mut Reader<'_>) -> Result<Self> {
        let method = match method_id {
            START => Self::Start(Start {
                version_major: reader.read_u8()?,
                version_minor: reader.read_u8()?,
                server_properties: reader.read_field_table(0)?,
                mechanisms: reader.read_long_string()?,
                locales: reader.read_long_string()?,
            }),
            START_OK => Self::StartOk(StartOk {
                client_properties: reader.read_field_table(0)?,
                mechanism: reader.read_short_string()?,
                response: reader.read_long_string()?,
                locale: reader.read_short_string()?,
            }),
            TUNE => {
                let (channel_max, frame_max, heartbeat) = decode_tune(reader)?;
                Self::Tune(Tune {
                    channel_max,
                    frame_max,
                    heartbeat,
                })
            }
            TUNE_OK => {
                let (channel_max, frame_max, heartbeat) = decode_tune(reader)?;
                Self::TuneOk(TuneOk {
                    channel_max,
                    frame_max,
                    heartbeat,
                })
            }
            OPEN => Self::Open(Open {
                virtual_host: reader.read_short_string()?,
                capabilities: reader.read_short_string()?,
                insist: reader.read_u8()? != 0,
            }),
            OPEN_OK => Self::OpenOk(OpenOk {
                known_hosts: reader.read_short_string()?,
            }),
            CLOSE => Self::Close(Close {
                reply_code: reader.read_u16()?,
                reply_text: reader.read_short_string()?,
                class_id: reader.read_u16()?,
                method_id: reader.read_u16()?,
            }),
            CLOSE_OK => Self::CloseOk(CloseOk),
            BLOCKED => Self::Blocked(Blocked {
                reason: reader.read_short_string()?,
            }),
            UNBLOCKED => Self::Unblocked(Unblocked),
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

fn encode_tune<B: BufMut>(
    writer: &mut Writer<B>,
    channel_max: u16,
    frame_max: u32,
    heartbeat: u16,
) {
    writer.write_u16(channel_max);
    writer.write_u32(frame_max);
    writer.write_u16(heartbeat);
}

fn decode_tune(reader: &mut Reader<'_>) -> Result<(u16, u32, u16)> {
    Ok((reader.read_u16()?, reader.read_u32()?, reader.read_u16()?))
}
