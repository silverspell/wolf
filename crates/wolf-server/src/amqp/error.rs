//! Mapping broker-domain and dispatch failures onto AMQP close frames.

use std::fmt;

use wolf_broker::DomainError;

use crate::session::DispatchError;

/// Whether an error closes a single channel or the whole connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseLevel {
    /// Close only the channel that produced the error.
    Channel,
    /// Close the entire connection.
    Connection,
}

impl CloseLevel {
    /// AMQP class id of the close method for this level.
    pub const fn class_id(self) -> u16 {
        match self {
            Self::Channel => 20,
            Self::Connection => 10,
        }
    }

    /// AMQP method id of the close method for this level.
    pub const fn method_id(self) -> u16 {
        match self {
            Self::Channel => 40,
            Self::Connection => 50,
        }
    }
}

/// AMQP reply code carried by a close frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ReplyCode {
    /// The client is not allowed to access the resource.
    AccessRefused = 403,
    /// The requested resource does not exist.
    NotFound = 404,
    /// The resource is locked by another operation.
    ResourceLocked = 405,
    /// A precondition required by the operation is not met.
    PreconditionFailed = 406,
    /// The command is invalid or unknown.
    CommandInvalid = 503,
    /// The operation is not allowed.
    NotAllowed = 530,
    /// The operation is not implemented.
    NotImplemented = 540,
}

impl ReplyCode {
    /// Returns the numeric AMQP reply code.
    pub const fn code(self) -> u16 {
        self as u16
    }

    /// Returns whether this reply code closes a channel or a connection.
    pub const fn close_level(self) -> CloseLevel {
        match self {
            Self::AccessRefused
            | Self::NotFound
            | Self::ResourceLocked
            | Self::PreconditionFailed => CloseLevel::Channel,
            Self::CommandInvalid | Self::NotAllowed | Self::NotImplemented => {
                CloseLevel::Connection
            }
        }
    }
}

impl From<ReplyCode> for u16 {
    fn from(code: ReplyCode) -> Self {
        code.code()
    }
}

impl fmt::Display for ReplyCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({self:?})", self.code())
    }
}

/// An AMQP error ready to be emitted as a channel or connection close.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmqpError {
    /// Reply code reported to the peer.
    pub reply_code: ReplyCode,
    /// Whether the close applies to a channel or a connection.
    pub close_level: CloseLevel,
    /// AMQP class id of the resulting close method.
    pub class_id: u16,
    /// AMQP method id of the resulting close method.
    pub method_id: u16,
    /// Human-readable explanation sent to the peer.
    pub text: String,
}

impl AmqpError {
    /// Builds an error from a reply code and explanation.
    pub fn new(reply_code: ReplyCode, text: impl Into<String>) -> Self {
        let close_level = reply_code.close_level();
        Self {
            reply_code,
            close_level,
            class_id: close_level.class_id(),
            method_id: close_level.method_id(),
            text: text.into(),
        }
    }

    /// Maps a broker-domain error onto its AMQP reply code.
    pub fn from_domain(error: &DomainError) -> Self {
        let reply_code = match error {
            DomainError::VhostNotFound { .. } => ReplyCode::NotAllowed,
            DomainError::ExchangeNotFound { .. } | DomainError::QueueNotFound { .. } => {
                ReplyCode::NotFound
            }
            DomainError::AlreadyExists { .. } | DomainError::InvalidArgument { .. } => {
                ReplyCode::PreconditionFailed
            }
            DomainError::BrokerClosed => ReplyCode::CommandInvalid,
        };
        Self::new(reply_code, error.to_string())
    }

    /// Maps a session dispatch failure onto its AMQP reply code.
    pub fn from_dispatch(error: &DispatchError) -> Self {
        match error {
            DispatchError::UnknownMethod(method) => Self::new(
                ReplyCode::CommandInvalid,
                format!("unknown method {}.{}", method.class_id, method.method_id),
            ),
        }
    }
}

impl fmt::Display for AmqpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.reply_code, self.text)
    }
}

impl std::error::Error for AmqpError {}

#[cfg(test)]
mod tests {
    use wolf_broker::ResourceKind;

    use super::*;
    use crate::amqp::MethodKey;

    #[test]
    fn reply_codes_use_the_expected_numeric_values() {
        let cases = [
            (ReplyCode::AccessRefused, 403),
            (ReplyCode::NotFound, 404),
            (ReplyCode::ResourceLocked, 405),
            (ReplyCode::PreconditionFailed, 406),
            (ReplyCode::CommandInvalid, 503),
            (ReplyCode::NotAllowed, 530),
            (ReplyCode::NotImplemented, 540),
        ];

        for (code, expected) in cases {
            assert_eq!(code.code(), expected);
        }
    }

    #[test]
    fn reply_codes_map_to_the_expected_close_level() {
        let channel = [
            ReplyCode::AccessRefused,
            ReplyCode::NotFound,
            ReplyCode::ResourceLocked,
            ReplyCode::PreconditionFailed,
        ];
        let connection = [
            ReplyCode::CommandInvalid,
            ReplyCode::NotAllowed,
            ReplyCode::NotImplemented,
        ];

        for code in channel {
            assert_eq!(code.close_level(), CloseLevel::Channel);
        }
        for code in connection {
            assert_eq!(code.close_level(), CloseLevel::Connection);
        }
    }

    #[test]
    fn close_levels_use_channel_and_connection_close_method_ids() {
        assert_eq!(CloseLevel::Channel.class_id(), 20);
        assert_eq!(CloseLevel::Channel.method_id(), 40);
        assert_eq!(CloseLevel::Connection.class_id(), 10);
        assert_eq!(CloseLevel::Connection.method_id(), 50);
    }

    #[test]
    fn domain_errors_map_to_the_expected_reply_codes() {
        let cases = [
            (
                DomainError::VhostNotFound { vhost: "/".into() },
                ReplyCode::NotAllowed,
            ),
            (
                DomainError::ExchangeNotFound {
                    vhost: "/".into(),
                    exchange: "missing".into(),
                },
                ReplyCode::NotFound,
            ),
            (
                DomainError::QueueNotFound {
                    vhost: "/".into(),
                    queue: "missing".into(),
                },
                ReplyCode::NotFound,
            ),
            (
                DomainError::AlreadyExists {
                    kind: ResourceKind::Queue,
                    name: "duplicate".into(),
                },
                ReplyCode::PreconditionFailed,
            ),
            (
                DomainError::InvalidArgument {
                    message: "bad".into(),
                },
                ReplyCode::PreconditionFailed,
            ),
            (DomainError::BrokerClosed, ReplyCode::CommandInvalid),
        ];

        for (domain, expected) in cases {
            let error = AmqpError::from_domain(&domain);
            assert_eq!(error.reply_code, expected, "mapping for {domain:?}");
            assert_eq!(error.close_level, expected.close_level());
            assert_eq!(error.class_id, expected.close_level().class_id());
            assert_eq!(error.method_id, expected.close_level().method_id());
        }
    }

    #[test]
    fn unknown_methods_close_the_connection() {
        let error = AmqpError::from_dispatch(&DispatchError::UnknownMethod(MethodKey::new(60, 40)));

        assert_eq!(error.reply_code, ReplyCode::CommandInvalid);
        assert_eq!(error.close_level, CloseLevel::Connection);
        assert_eq!(error.class_id, 10);
        assert_eq!(error.method_id, 50);
    }
}
