//! AMQP Tx class (90) methods.

use crate::error::{Error, Result};
use crate::types::{Reader, Writer};

/// AMQP Tx class id.
pub const CLASS_ID: u16 = 90;

/// `tx.select` method id.
pub const SELECT: u16 = 10;
/// `tx.select-ok` method id.
pub const SELECT_OK: u16 = 11;
/// `tx.commit` method id.
pub const COMMIT: u16 = 20;
/// `tx.commit-ok` method id.
pub const COMMIT_OK: u16 = 21;
/// `tx.rollback` method id.
pub const ROLLBACK: u16 = 30;
/// `tx.rollback-ok` method id.
pub const ROLLBACK_OK: u16 = 31;

/// `tx.select` — request to put a channel in transaction mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxSelect;

/// `tx.select-ok` — acknowledgement of `tx.select`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxSelectOk;

/// `tx.commit` — request to commit the current transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxCommit;

/// `tx.commit-ok` — acknowledgement of `tx.commit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxCommitOk;

/// `tx.rollback` — request to roll back the current transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxRollback;

/// `tx.rollback-ok` — acknowledgement of `tx.rollback`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxRollbackOk;

/// A method from the AMQP Tx class.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxMethod {
    /// `tx.select`
    Select(TxSelect),
    /// `tx.select-ok`
    SelectOk(TxSelectOk),
    /// `tx.commit`
    Commit(TxCommit),
    /// `tx.commit-ok`
    CommitOk(TxCommitOk),
    /// `tx.rollback`
    Rollback(TxRollback),
    /// `tx.rollback-ok`
    RollbackOk(TxRollbackOk),
}

impl TxMethod {
    /// Returns the AMQP class id (always 90).
    pub fn class_id(&self) -> u16 {
        CLASS_ID
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Select(_) => SELECT,
            Self::SelectOk(_) => SELECT_OK,
            Self::Commit(_) => COMMIT,
            Self::CommitOk(_) => COMMIT_OK,
            Self::Rollback(_) => ROLLBACK,
            Self::RollbackOk(_) => ROLLBACK_OK,
        }
    }

    pub(crate) fn encode_body(&self) -> Result<Vec<u8>> {
        let writer = Writer::new();
        Ok(writer.into_inner())
    }

    pub(crate) fn decode(method_id: u16, _reader: &mut Reader<'_>) -> Result<Self> {
        let method = match method_id {
            SELECT => Self::Select(TxSelect),
            SELECT_OK => Self::SelectOk(TxSelectOk),
            COMMIT => Self::Commit(TxCommit),
            COMMIT_OK => Self::CommitOk(TxCommitOk),
            ROLLBACK => Self::Rollback(TxRollback),
            ROLLBACK_OK => Self::RollbackOk(TxRollbackOk),
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
