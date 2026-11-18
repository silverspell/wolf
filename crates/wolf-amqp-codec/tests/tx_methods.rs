use bytes::Bytes;
use wolf_amqp_codec::{
    Error, Method, TxCommit, TxCommitOk, TxMethod, TxRollback, TxRollbackOk, TxSelect, TxSelectOk,
    tx,
};

fn sample_methods() -> Vec<(TxMethod, u16)> {
    vec![
        (TxMethod::Select(TxSelect), tx::SELECT),
        (TxMethod::SelectOk(TxSelectOk), tx::SELECT_OK),
        (TxMethod::Commit(TxCommit), tx::COMMIT),
        (TxMethod::CommitOk(TxCommitOk), tx::COMMIT_OK),
        (TxMethod::Rollback(TxRollback), tx::ROLLBACK),
        (TxMethod::RollbackOk(TxRollbackOk), tx::ROLLBACK_OK),
    ]
}

#[test]
fn method_ids_match_amqp_spec() {
    assert_eq!(
        (
            tx::CLASS_ID,
            tx::SELECT,
            tx::SELECT_OK,
            tx::COMMIT,
            tx::COMMIT_OK,
            tx::ROLLBACK,
            tx::ROLLBACK_OK,
        ),
        (90, 10, 11, 20, 21, 30, 31)
    );
}

#[test]
fn methods_encode_exact_wire_bytes() {
    let cases = [
        (
            Method::Tx(TxMethod::Select(TxSelect)),
            [0x00, 0x5A, 0x00, 0x0A],
        ),
        (
            Method::Tx(TxMethod::SelectOk(TxSelectOk)),
            [0x00, 0x5A, 0x00, 0x0B],
        ),
        (
            Method::Tx(TxMethod::Commit(TxCommit)),
            [0x00, 0x5A, 0x00, 0x14],
        ),
        (
            Method::Tx(TxMethod::CommitOk(TxCommitOk)),
            [0x00, 0x5A, 0x00, 0x15],
        ),
        (
            Method::Tx(TxMethod::Rollback(TxRollback)),
            [0x00, 0x5A, 0x00, 0x1E],
        ),
        (
            Method::Tx(TxMethod::RollbackOk(TxRollbackOk)),
            [0x00, 0x5A, 0x00, 0x1F],
        ),
    ];
    for (method, expected) in cases {
        assert_eq!(method.encode(), expected);
    }
}

#[test]
fn all_tx_methods_round_trip() {
    for (method, method_id) in sample_methods() {
        let expected = Method::from(method);
        assert_eq!(expected.class_id(), tx::CLASS_ID);
        assert_eq!(expected.method_id(), method_id);

        let encoded = expected.encode();
        let decoded = Method::decode(&Bytes::from(encoded.clone())).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(decoded.try_encode().unwrap(), encoded);
    }
}

#[test]
fn unknown_method_id_is_an_error() {
    let error = Method::decode(&Bytes::from_static(&[0x00, 0x5A, 0x00, 0x63])).unwrap_err();
    assert_eq!(
        error,
        Error::UnknownMethod {
            class_id: 90,
            method_id: 99,
        }
    );
}
