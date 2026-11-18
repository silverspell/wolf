//! Handlers for AMQP methods the broker explicitly rejects as unsupported.

use wolf_amqp_codec::{Frame, TxMethod, tx};

use crate::amqp::error::{AmqpError, ReplyCode};

const TX_UNSUPPORTED_TEXT: &str = "transactions are not supported";

/// Rejects every method in the `tx` class.
///
/// Transactional channels are not implemented. Request methods fail with
/// `540 NOT_IMPLEMENTED` at connection level so a client never sees a silent
/// acceptance, while client-sent server-only acknowledgement methods fail with
/// `503 COMMAND_INVALID`.
pub fn handle_tx(_channel: u16, method: TxMethod) -> Result<Vec<Frame>, AmqpError> {
    let (reply_code, text) = match &method {
        TxMethod::Select(_) | TxMethod::Commit(_) | TxMethod::Rollback(_) => {
            (ReplyCode::NotImplemented, TX_UNSUPPORTED_TEXT)
        }
        TxMethod::SelectOk(_) | TxMethod::CommitOk(_) | TxMethod::RollbackOk(_) => (
            ReplyCode::CommandInvalid,
            "client sent a server-only tx method",
        ),
    };

    Err(AmqpError::connection(reply_code, text).with_offending(tx::CLASS_ID, method.method_id()))
}

#[cfg(test)]
mod tests {
    use wolf_amqp_codec::{TxCommit, TxCommitOk, TxRollback, TxRollbackOk, TxSelect, TxSelectOk};

    use super::*;
    use crate::amqp::error::CloseLevel;

    #[test]
    fn tx_request_methods_are_rejected_with_540_at_connection_level() {
        let cases = [
            (TxMethod::Select(TxSelect), tx::SELECT),
            (TxMethod::Commit(TxCommit), tx::COMMIT),
            (TxMethod::Rollback(TxRollback), tx::ROLLBACK),
        ];

        for (method, expected_method_id) in cases {
            let error = handle_tx(1, method).expect_err("tx request must be rejected");

            assert_eq!(error.reply_code, ReplyCode::NotImplemented);
            assert_eq!(error.reply_code.code(), 540);
            assert_eq!(error.close_level, CloseLevel::Connection);
            assert_eq!(error.class_id, 10);
            assert_eq!(error.method_id, 50);
            assert_eq!(error.offending_class_id, tx::CLASS_ID);
            assert_eq!(error.offending_method_id, expected_method_id);
        }
    }

    #[test]
    fn client_sent_tx_ok_methods_are_rejected_with_503_at_connection_level() {
        let cases = [
            (TxMethod::SelectOk(TxSelectOk), tx::SELECT_OK),
            (TxMethod::CommitOk(TxCommitOk), tx::COMMIT_OK),
            (TxMethod::RollbackOk(TxRollbackOk), tx::ROLLBACK_OK),
        ];

        for (method, expected_method_id) in cases {
            let error = handle_tx(1, method).expect_err("tx ok method must be rejected");

            assert_eq!(error.reply_code, ReplyCode::CommandInvalid);
            assert_eq!(error.reply_code.code(), 503);
            assert_eq!(error.close_level, CloseLevel::Connection);
            assert_eq!(error.class_id, 10);
            assert_eq!(error.method_id, 50);
            assert_eq!(error.offending_class_id, tx::CLASS_ID);
            assert_eq!(error.offending_method_id, expected_method_id);
        }
    }
}
