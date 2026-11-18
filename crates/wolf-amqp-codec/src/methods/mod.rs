//! AMQP method encoding and decoding.

use bytes::Bytes;

use crate::error::{Error, Result};
use crate::types::{Reader, Writer};

pub mod basic;
pub mod channel;
pub mod confirm;
pub mod connection;
pub mod exchange;
pub mod queue;
pub mod tx;

use connection::ConnectionMethod;

/// A decoded AMQP method.
#[derive(Debug, Clone, PartialEq)]
pub enum Method {
    /// A method from the connection class.
    Connection(ConnectionMethod),
}

impl Method {
    /// Returns the AMQP class id.
    pub fn class_id(&self) -> u16 {
        match self {
            Self::Connection(method) => method.class_id(),
        }
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Connection(method) => method.method_id(),
        }
    }

    /// Decodes a method payload, including its class and method ids.
    ///
    /// The entire payload must be consumed; trailing bytes are rejected.
    pub fn decode(payload: &Bytes) -> Result<Self> {
        let mut reader = Reader::new(payload);
        let class_id = reader.read_u16()?;
        let method_id = reader.read_u16()?;
        let method = match class_id {
            connection::CLASS_ID => {
                Self::Connection(ConnectionMethod::decode(method_id, &mut reader)?)
            }
            other => return Err(Error::UnsupportedClass { class_id: other }),
        };
        if reader.remaining() != 0 {
            return Err(Error::TrailingBytes {
                remaining: reader.remaining(),
            });
        }
        Ok(method)
    }

    /// Encodes the method, returning an error for values that exceed AMQP limits.
    pub fn try_encode(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.write_u16(self.class_id());
        writer.write_u16(self.method_id());
        match self {
            Self::Connection(method) => writer.write_bytes(&method.encode_body()?),
        }
        Ok(writer.into_inner())
    }

    /// Encodes the method into its AMQP wire representation.
    ///
    /// # Panics
    ///
    /// Panics if a string or table exceeds AMQP encoding limits. Use
    /// [`Method::try_encode`] when encoding untrusted values.
    pub fn encode(&self) -> Vec<u8> {
        self.try_encode()
            .expect("method exceeds AMQP encoding limits")
    }
}

impl From<ConnectionMethod> for Method {
    fn from(method: ConnectionMethod) -> Self {
        Self::Connection(method)
    }
}
