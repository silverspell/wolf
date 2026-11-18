//! AMQP basic-class content-header properties.
//!
//! The content header frame carries a 16-bit property-flag word followed by the
//! values of every property whose bit is set. The flag bits are fixed by the
//! AMQP 0-9-1 basic class; the values appear in the same canonical order
//! regardless of the order in which they were assigned.

use crate::error::{Error, Result};
use crate::field_table::FieldTable;
use crate::types::{Reader, ShortString, Writer};

/// Present-flag bit for the `content-type` property.
pub const CONTENT_TYPE: u16 = 0x8000;
/// Present-flag bit for the `content-encoding` property.
pub const CONTENT_ENCODING: u16 = 0x4000;
/// Present-flag bit for the `headers` property.
pub const HEADERS: u16 = 0x2000;
/// Present-flag bit for the `delivery-mode` property.
pub const DELIVERY_MODE: u16 = 0x1000;
/// Present-flag bit for the `priority` property.
pub const PRIORITY: u16 = 0x0800;
/// Present-flag bit for the `correlation-id` property.
pub const CORRELATION_ID: u16 = 0x0400;
/// Present-flag bit for the `reply-to` property.
pub const REPLY_TO: u16 = 0x0200;
/// Present-flag bit for the `expiration` property.
pub const EXPIRATION: u16 = 0x0100;
/// Present-flag bit for the `message-id` property.
pub const MESSAGE_ID: u16 = 0x0080;
/// Present-flag bit for the `timestamp` property.
pub const TIMESTAMP: u16 = 0x0040;
/// Present-flag bit for the `type` property.
pub const TYPE: u16 = 0x0020;
/// Present-flag bit for the `user-id` property.
pub const USER_ID: u16 = 0x0010;
/// Present-flag bit for the `app-id` property.
pub const APP_ID: u16 = 0x0008;
/// Present-flag bit for the `cluster-id` property.
pub const CLUSTER_ID: u16 = 0x0004;
/// Mask covering the reserved low flag bits, which must never be set.
pub const RESERVED_FLAGS: u16 = 0x0003;

/// The basic-class properties carried by an AMQP content header.
///
/// Each field mirrors one AMQP basic property. `None` means the property is
/// absent (its flag bit is clear), while `Some` holds the value on the wire;
/// in particular `Some` of an empty string or empty table remains distinct
/// from `None`.
///
/// This type deliberately does not implement `Eq` because [`FieldTable`] may
/// contain floating-point values.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct BasicProperties {
    /// MIME content type (`content-type`).
    pub content_type: Option<ShortString>,
    /// MIME content encoding (`content-encoding`).
    pub content_encoding: Option<ShortString>,
    /// Application-defined message headers.
    pub headers: Option<FieldTable>,
    /// Delivery mode: 1 for transient, 2 for persistent.
    pub delivery_mode: Option<u8>,
    /// Message priority, 0 to 9.
    pub priority: Option<u8>,
    /// Correlation identifier for request/reply patterns.
    pub correlation_id: Option<ShortString>,
    /// Address to reply to.
    pub reply_to: Option<ShortString>,
    /// Per-message expiration, as a decimal string in milliseconds.
    pub expiration: Option<ShortString>,
    /// Application message identifier.
    pub message_id: Option<ShortString>,
    /// Message timestamp in seconds since the POSIX epoch.
    pub timestamp: Option<u64>,
    /// Application-defined message type name.
    pub r#type: Option<ShortString>,
    /// Creating user identifier.
    pub user_id: Option<ShortString>,
    /// Creating application identifier.
    pub app_id: Option<ShortString>,
    /// Reserved cluster identifier.
    pub cluster_id: Option<ShortString>,
}

impl BasicProperties {
    /// Returns the property-flag word for the currently present properties.
    ///
    /// Reserved bits are never set.
    pub fn flags(&self) -> u16 {
        let mut flags = 0;
        if self.content_type.is_some() {
            flags |= CONTENT_TYPE;
        }
        if self.content_encoding.is_some() {
            flags |= CONTENT_ENCODING;
        }
        if self.headers.is_some() {
            flags |= HEADERS;
        }
        if self.delivery_mode.is_some() {
            flags |= DELIVERY_MODE;
        }
        if self.priority.is_some() {
            flags |= PRIORITY;
        }
        if self.correlation_id.is_some() {
            flags |= CORRELATION_ID;
        }
        if self.reply_to.is_some() {
            flags |= REPLY_TO;
        }
        if self.expiration.is_some() {
            flags |= EXPIRATION;
        }
        if self.message_id.is_some() {
            flags |= MESSAGE_ID;
        }
        if self.timestamp.is_some() {
            flags |= TIMESTAMP;
        }
        if self.r#type.is_some() {
            flags |= TYPE;
        }
        if self.user_id.is_some() {
            flags |= USER_ID;
        }
        if self.app_id.is_some() {
            flags |= APP_ID;
        }
        if self.cluster_id.is_some() {
            flags |= CLUSTER_ID;
        }
        flags
    }

    /// Encodes the property flags and every present value in spec order.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.write_u16(self.flags());

        if let Some(value) = &self.content_type {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.content_encoding {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.headers {
            writer.write_field_table(value, 0)?;
        }
        if let Some(value) = self.delivery_mode {
            writer.write_u8(value);
        }
        if let Some(value) = self.priority {
            writer.write_u8(value);
        }
        if let Some(value) = &self.correlation_id {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.reply_to {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.expiration {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.message_id {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = self.timestamp {
            writer.write_u64(value);
        }
        if let Some(value) = &self.r#type {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.user_id {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.app_id {
            writer.write_short_string(value.as_str())?;
        }
        if let Some(value) = &self.cluster_id {
            writer.write_short_string(value.as_str())?;
        }

        Ok(writer.into_inner())
    }

    /// Decodes a property-flag word and the present values that follow it.
    ///
    /// The input must contain exactly one encoded property block; extra bytes
    /// are reported as [`Error::TrailingBytes`].
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let flags = reader.read_u16()?;
        if flags & RESERVED_FLAGS != 0 {
            return Err(Error::MalformedFrame {
                reason: "reserved basic property flag bits set",
            });
        }

        let mut properties = Self::default();

        if flags & CONTENT_TYPE != 0 {
            properties.content_type = Some(reader.read_short_string()?);
        }
        if flags & CONTENT_ENCODING != 0 {
            properties.content_encoding = Some(reader.read_short_string()?);
        }
        if flags & HEADERS != 0 {
            properties.headers = Some(reader.read_field_table(0)?);
        }
        if flags & DELIVERY_MODE != 0 {
            properties.delivery_mode = Some(reader.read_u8()?);
        }
        if flags & PRIORITY != 0 {
            properties.priority = Some(reader.read_u8()?);
        }
        if flags & CORRELATION_ID != 0 {
            properties.correlation_id = Some(reader.read_short_string()?);
        }
        if flags & REPLY_TO != 0 {
            properties.reply_to = Some(reader.read_short_string()?);
        }
        if flags & EXPIRATION != 0 {
            properties.expiration = Some(reader.read_short_string()?);
        }
        if flags & MESSAGE_ID != 0 {
            properties.message_id = Some(reader.read_short_string()?);
        }
        if flags & TIMESTAMP != 0 {
            properties.timestamp = Some(u64::from_be_bytes(reader.read_array::<8>()?));
        }
        if flags & TYPE != 0 {
            properties.r#type = Some(reader.read_short_string()?);
        }
        if flags & USER_ID != 0 {
            properties.user_id = Some(reader.read_short_string()?);
        }
        if flags & APP_ID != 0 {
            properties.app_id = Some(reader.read_short_string()?);
        }
        if flags & CLUSTER_ID != 0 {
            properties.cluster_id = Some(reader.read_short_string()?);
        }

        let remaining = reader.remaining();
        if remaining != 0 {
            return Err(Error::TrailingBytes { remaining });
        }

        Ok(properties)
    }
}
