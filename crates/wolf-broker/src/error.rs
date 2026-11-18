//! Protocol-neutral error types returned by the broker domain.

use std::fmt;

/// The kind of broker resource an error refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    Vhost,
    Exchange,
    Queue,
    Binding,
}

/// A broker-domain error carrying no wire-protocol details.
///
/// Mapping onto AMQP reply codes and class/method identifiers belongs to
/// `wolf-server`, keeping this crate protocol-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainError {
    VhostNotFound { vhost: String },
    ExchangeNotFound { vhost: String, exchange: String },
    QueueNotFound { vhost: String, queue: String },
    AlreadyExists { kind: ResourceKind, name: String },
    InvalidArgument { message: String },
    BrokerClosed,
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VhostNotFound { vhost } => write!(f, "vhost '{vhost}' not found"),
            Self::ExchangeNotFound { vhost, exchange } => {
                write!(f, "exchange '{exchange}' not found in vhost '{vhost}'")
            }
            Self::QueueNotFound { vhost, queue } => {
                write!(f, "queue '{queue}' not found in vhost '{vhost}'")
            }
            Self::AlreadyExists { kind, name } => {
                write!(f, "{kind:?} '{name}' already exists")
            }
            Self::InvalidArgument { message } => write!(f, "invalid argument: {message}"),
            Self::BrokerClosed => write!(f, "broker is closed"),
        }
    }
}

impl std::error::Error for DomainError {}
