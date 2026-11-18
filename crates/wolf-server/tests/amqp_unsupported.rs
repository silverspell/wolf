//! Raw-wire tests for methods the broker rejects as unsupported.

mod support;

use wolf_amqp_codec::{
    Method, TxCommit, TxCommitOk, TxMethod, TxRollback, TxRollbackOk, TxSelect, TxSelectOk, tx,
};

use support::{TestClient, start_server};

async fn expect_tx_request_rejected(method: TxMethod, expected_method_id: u16) {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_method(1, Method::Tx(method)).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 540);
    assert_eq!(
        (close.class_id, close.method_id),
        (tx::CLASS_ID, expected_method_id)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

async fn expect_tx_client_method_rejected(method: TxMethod, expected_method_id: u16) {
    let (addr, handle) = start_server().await;
    let mut client = TestClient::connect(addr).await;
    client.handshake().await;
    let _ = client.open_channel(1).await;

    client.send_method(1, Method::Tx(method)).await;

    let close = client.expect_connection_close().await;
    assert_eq!(close.reply_code, 503);
    assert_eq!(
        (close.class_id, close.method_id),
        (tx::CLASS_ID, expected_method_id)
    );

    drop(client);
    handle.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn tx_select_closes_the_connection_with_540() {
    expect_tx_request_rejected(TxMethod::Select(TxSelect), tx::SELECT).await;
}

#[tokio::test]
async fn tx_commit_closes_the_connection_with_540() {
    expect_tx_request_rejected(TxMethod::Commit(TxCommit), tx::COMMIT).await;
}

#[tokio::test]
async fn tx_rollback_closes_the_connection_with_540() {
    expect_tx_request_rejected(TxMethod::Rollback(TxRollback), tx::ROLLBACK).await;
}

#[tokio::test]
async fn client_sent_tx_ok_methods_close_the_connection_with_503() {
    expect_tx_client_method_rejected(TxMethod::SelectOk(TxSelectOk), tx::SELECT_OK).await;
    expect_tx_client_method_rejected(TxMethod::CommitOk(TxCommitOk), tx::COMMIT_OK).await;
    expect_tx_client_method_rejected(TxMethod::RollbackOk(TxRollbackOk), tx::ROLLBACK_OK).await;
}
