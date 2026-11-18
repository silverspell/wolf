//! End-to-end AMQP behavior exercised through a real lapin client.

mod common;
mod support;

use common::{
    OP_TIMEOUT, ack_message, assert_amqp_error, bind, cancel_consumer, close_channel, connect,
    consume, create_channel, declare_exchange, declare_queue, expect_connection_error,
    expect_error_event, expect_no_delivery, finish, get, get_and_ack, next_delivery, publish,
    publish_to_queue, short, start_server, wait_for_channel_closed, wait_for_consumer_canceled,
};
use lapin::{
    BasicProperties, ConsumerState, ExchangeKind,
    message::Delivery,
    options::{
        BasicNackOptions, BasicPublishOptions, BasicQosOptions, BasicRecoverAsyncOptions,
        BasicRecoverOptions, BasicRejectOptions, ConfirmSelectOptions, ExchangeDeclareOptions,
        ExchangeDeleteOptions, QueueBindOptions, QueueDeclareOptions, QueueDeleteOptions,
    },
    publisher_confirm::Confirmation,
    types::{AMQPValue, FieldTable},
};
use tokio::time::timeout;

fn body_of(delivery: &Delivery) -> &[u8] {
    delivery.data.as_slice()
}

#[tokio::test]
async fn server_start_reports_ephemeral_port_and_shuts_down_cleanly() {
    let (addr, handle) = start_server().await;

    assert!(addr.ip().is_loopback());
    assert_ne!(addr.port(), 0);

    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "startup").await;
    close_channel(&channel).await;

    finish(&connection, handle).await;

    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "listener must be closed after shutdown"
    );
}

#[tokio::test]
async fn connect_declare_publish_consume_ack_roundtrip() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    declare_exchange(&channel, "events", ExchangeKind::Direct).await;
    declare_queue(&channel, "jobs").await;
    bind(&channel, "jobs", "events", "rk").await;

    let _confirm = publish(
        &channel,
        "events",
        "rk",
        b"hello",
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;

    let mut consumer = consume(&channel, "jobs", "ctag").await;
    let delivery = next_delivery(&mut consumer).await;
    assert_eq!(body_of(&delivery), b"hello");
    assert_eq!(delivery.exchange.as_str(), "events");
    assert_eq!(delivery.routing_key.as_str(), "rk");

    delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("delivery acknowledges");
    expect_no_delivery(&mut consumer).await;

    cancel_consumer(&channel, "ctag").await;
    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn topic_star_and_hash_bindings_route_only_matching_keys() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    declare_exchange(&channel, "market", ExchangeKind::Topic).await;
    declare_queue(&channel, "star").await;
    declare_queue(&channel, "hash").await;
    declare_queue(&channel, "other").await;
    bind(&channel, "star", "market", "stock.*.price").await;
    bind(&channel, "hash", "market", "stock.#").await;
    bind(&channel, "other", "market", "only.this").await;

    let _confirm = publish(
        &channel,
        "market",
        "stock.usd.price",
        b"both",
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;
    let (star_body, _) = get_and_ack(&channel, "star")
        .await
        .expect("star binding matches three segments");
    let (hash_body, _) = get_and_ack(&channel, "hash")
        .await
        .expect("hash binding matches three segments");
    assert_eq!(star_body.as_slice(), b"both");
    assert_eq!(hash_body.as_slice(), b"both");

    let _confirm = publish(
        &channel,
        "market",
        "stock.price",
        b"hash-only",
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;
    assert!(
        get_and_ack(&channel, "star").await.is_none(),
        "star must match exactly one segment"
    );
    let (hash_body, _) = get_and_ack(&channel, "hash")
        .await
        .expect("hash matches zero or more segments");
    assert_eq!(hash_body.as_slice(), b"hash-only");

    let _confirm = publish(
        &channel,
        "market",
        "not.this",
        b"unmatched",
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;
    assert!(get_and_ack(&channel, "star").await.is_none());
    assert!(get_and_ack(&channel, "hash").await.is_none());
    assert!(
        get_and_ack(&channel, "other").await.is_none(),
        "non-matching binding must stay empty"
    );

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn fanout_delivers_exactly_once_to_each_bound_queue() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    declare_exchange(&channel, "broadcast", ExchangeKind::Fanout).await;
    declare_queue(&channel, "fan-one").await;
    declare_queue(&channel, "fan-two").await;
    bind(&channel, "fan-one", "broadcast", "").await;
    bind(&channel, "fan-two", "broadcast", "ignored").await;

    let _confirm = publish(
        &channel,
        "broadcast",
        "ignored",
        b"once",
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;

    let (first, _) = get_and_ack(&channel, "fan-one")
        .await
        .expect("first fanout queue receives");
    let (second, _) = get_and_ack(&channel, "fan-two")
        .await
        .expect("second fanout queue receives");
    assert_eq!(first.as_slice(), b"once");
    assert_eq!(second.as_slice(), b"once");
    assert!(
        get_and_ack(&channel, "fan-one").await.is_none(),
        "fanout must not duplicate"
    );
    assert!(
        get_and_ack(&channel, "fan-two").await.is_none(),
        "fanout must not duplicate"
    );

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn direct_exchange_routes_only_exact_binding_key() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    declare_exchange(&channel, "direct-x", ExchangeKind::Direct).await;
    declare_queue(&channel, "hit").await;
    declare_queue(&channel, "miss").await;
    bind(&channel, "hit", "direct-x", "rk").await;
    bind(&channel, "miss", "direct-x", "other").await;

    let _confirm = publish(
        &channel,
        "direct-x",
        "rk",
        b"matched",
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;
    let (hit, _) = get_and_ack(&channel, "hit")
        .await
        .expect("exact key matches");
    assert_eq!(hit.as_slice(), b"matched");
    assert!(
        get_and_ack(&channel, "miss").await.is_none(),
        "unmatched key must not route"
    );

    let _confirm = publish(
        &channel,
        "direct-x",
        "other",
        b"other-body",
        BasicProperties::default(),
        BasicPublishOptions::default(),
    )
    .await;
    let (miss, _) = get_and_ack(&channel, "miss")
        .await
        .expect("second key matches its binding");
    assert_eq!(miss.as_slice(), b"other-body");
    assert!(get_and_ack(&channel, "hit").await.is_none());

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn prefetch_one_round_robins_two_consumers_without_exceeding_limit() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "jobs").await;

    channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .expect("basic.qos applies");

    let mut first = consume(&channel, "jobs", "first").await;
    let mut second = consume(&channel, "jobs", "second").await;

    for body in [b"m1".as_slice(), b"m2", b"m3", b"m4"] {
        publish_to_queue(&channel, "jobs", body).await;
    }

    let first_delivery = next_delivery(&mut first).await;
    assert_eq!(body_of(&first_delivery), b"m1");
    let second_delivery = next_delivery(&mut second).await;
    assert_eq!(body_of(&second_delivery), b"m2");

    expect_no_delivery(&mut first).await;
    expect_no_delivery(&mut second).await;

    first_delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("first delivery acknowledges");
    second_delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("second delivery acknowledges");

    let third_delivery = next_delivery(&mut first).await;
    assert_eq!(body_of(&third_delivery), b"m3");
    let fourth_delivery = next_delivery(&mut second).await;
    assert_eq!(body_of(&fourth_delivery), b"m4");

    third_delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("third delivery acknowledges");
    fourth_delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("fourth delivery acknowledges");

    expect_no_delivery(&mut first).await;
    expect_no_delivery(&mut second).await;

    cancel_consumer(&channel, "first").await;
    cancel_consumer(&channel, "second").await;
    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn acked_delivery_is_not_redelivered_and_connection_close_requeues_unacked() {
    let (addr, handle) = start_server().await;
    let first_connection = connect(addr).await;
    let first_channel = create_channel(&first_connection).await;
    declare_queue(&first_channel, "jobs").await;

    publish_to_queue(&first_channel, "jobs", b"acked").await;
    publish_to_queue(&first_channel, "jobs", b"pending").await;

    let acked = get(&first_channel, "jobs")
        .await
        .expect("first message is available");
    assert_eq!(acked.delivery.data.as_slice(), b"acked");
    assert!(!acked.delivery.redelivered);
    ack_message(&acked).await;

    let pending = get(&first_channel, "jobs")
        .await
        .expect("second message is available");
    assert_eq!(pending.delivery.data.as_slice(), b"pending");
    assert!(!pending.delivery.redelivered);

    let _ = first_connection.close(200, "simulate disconnect").await;

    let second_connection = connect(addr).await;
    let second_channel = create_channel(&second_connection).await;

    let (body, redelivered) = get_and_ack(&second_channel, "jobs")
        .await
        .expect("unacked message is requeued on connection close");
    assert!(redelivered, "requeued delivery must be marked redelivered");
    assert_eq!(body.as_slice(), b"pending");
    assert!(
        get_and_ack(&second_channel, "jobs").await.is_none(),
        "acked message must not be redelivered"
    );

    close_channel(&second_channel).await;
    finish(&second_connection, handle).await;
}

#[tokio::test]
async fn nack_requeue_redelivers_with_redelivered_flag() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "jobs").await;
    publish_to_queue(&channel, "jobs", b"again").await;

    let delivery = get(&channel, "jobs").await.expect("message is available");
    assert!(!delivery.delivery.redelivered);
    delivery
        .delivery
        .acker
        .nack(BasicNackOptions {
            multiple: false,
            requeue: true,
        })
        .await
        .expect("basic.nack sends");

    let (body, redelivered) = get_and_ack(&channel, "jobs")
        .await
        .expect("nacked message is requeued");
    assert!(redelivered);
    assert_eq!(body.as_slice(), b"again");

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn reject_requeue_redelivers_with_redelivered_flag() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "jobs").await;
    publish_to_queue(&channel, "jobs", b"again").await;

    let delivery = get(&channel, "jobs").await.expect("message is available");
    assert!(!delivery.delivery.redelivered);
    delivery
        .delivery
        .acker
        .reject(BasicRejectOptions { requeue: true })
        .await
        .expect("basic.reject sends");

    let (body, redelivered) = get_and_ack(&channel, "jobs")
        .await
        .expect("rejected message is requeued");
    assert!(redelivered);
    assert_eq!(body.as_slice(), b"again");

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn publisher_confirms_ack_each_publish_in_order() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "confirm-q").await;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select succeeds");

    let mut confirms = Vec::new();
    for body in [b"first".as_slice(), b"second", b"third"] {
        confirms.push(
            publish(
                &channel,
                "",
                "confirm-q",
                body,
                BasicProperties::default(),
                BasicPublishOptions::default(),
            )
            .await,
        );
    }

    for confirm in confirms {
        let confirmation = confirm.await.expect("publisher confirm arrives");
        assert!(
            matches!(&confirmation, Confirmation::Ack(None)),
            "unexpected confirmation: {confirmation:?}"
        );
    }

    for expected in [b"first".as_slice(), b"second", b"third"] {
        let (body, _) = get_and_ack(&channel, "confirm-q")
            .await
            .expect("confirmed message is stored");
        assert_eq!(body.as_slice(), expected);
    }
    assert!(get_and_ack(&channel, "confirm-q").await.is_none());

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn mandatory_unroutable_publish_returns_body_and_properties() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select succeeds");

    let properties = BasicProperties::default()
        .with_content_type(short("text/plain"))
        .with_delivery_mode(2)
        .with_message_id(short("message-1"));
    let body = b"return-me";
    let confirm = publish(
        &channel,
        "",
        "missing",
        body,
        properties.clone(),
        BasicPublishOptions {
            mandatory: true,
            immediate: false,
        },
    )
    .await;

    let confirmation = confirm.await.expect("publisher confirm arrives");
    assert!(confirmation.is_ack(), "returned publish is still confirmed");
    let returned = confirmation
        .take_message()
        .expect("basic.return is attached to the confirm");
    assert_eq!(returned.reply_code, 312);
    assert_eq!(returned.reply_text.as_str(), "NO_ROUTE");
    assert_eq!(returned.delivery.exchange.as_str(), "");
    assert_eq!(returned.delivery.routing_key.as_str(), "missing");
    assert_eq!(returned.delivery.data.as_slice(), body);
    assert_eq!(returned.delivery.properties, properties);

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn basic_get_returns_message_and_ack_removes_it() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "jobs").await;
    publish_to_queue(&channel, "jobs", b"payload").await;

    let message = get(&channel, "jobs")
        .await
        .expect("basic.get returns the queued message");
    assert_eq!(message.delivery.data.as_slice(), b"payload");
    assert!(!message.delivery.redelivered);
    ack_message(&message).await;

    assert!(
        get(&channel, "jobs").await.is_none(),
        "acked message must no longer be available"
    );

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn basic_get_on_empty_queue_returns_none() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "empty").await;

    assert!(get(&channel, "empty").await.is_none());

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn passive_missing_exchange_returns_404_and_closes_channel() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    let error = channel
        .exchange_declare(
            "missing",
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                passive: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("passive declare of a missing exchange fails");
    assert_amqp_error(&error, 404, true);
    wait_for_channel_closed(&channel).await;
    assert!(
        connection.status().connected(),
        "channel close must not close the connection"
    );

    let replacement = create_channel(&connection).await;
    close_channel(&replacement).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn passive_missing_queue_returns_404_and_closes_channel() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    let error = channel
        .queue_declare(
            "missing",
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .expect_err("passive declare of a missing queue fails");
    assert_amqp_error(&error, 404, true);
    wait_for_channel_closed(&channel).await;
    assert!(
        connection.status().connected(),
        "channel close must not close the connection"
    );

    let replacement = create_channel(&connection).await;
    close_channel(&replacement).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn queue_delete_sends_basic_cancel_to_consumer() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "jobs").await;

    let consumer = consume(&channel, "jobs", "doomed").await;
    assert_eq!(consumer.state(), ConsumerState::Active);

    let discarded = channel
        .queue_delete("jobs", QueueDeleteOptions::default())
        .await
        .expect("queue.delete succeeds");
    assert_eq!(discarded, 0);

    wait_for_consumer_canceled(&consumer).await;
    assert_eq!(consumer.state(), ConsumerState::Canceled);

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn default_exchange_bind_unbind_delete_return_403() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;

    let channel = create_channel(&connection).await;
    declare_queue(&channel, "bound").await;
    let error = channel
        .queue_bind(
            "bound",
            "",
            "bound",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("binding on the default exchange is refused");
    assert_amqp_error(&error, 403, true);
    wait_for_channel_closed(&channel).await;

    let channel = create_channel(&connection).await;
    let error = channel
        .queue_unbind("bound", "", "bound", FieldTable::default())
        .await
        .expect_err("unbinding on the default exchange is refused");
    assert_amqp_error(&error, 403, true);
    wait_for_channel_closed(&channel).await;

    let channel = create_channel(&connection).await;
    let error = channel
        .exchange_delete("", ExchangeDeleteOptions::default())
        .await
        .expect_err("deleting the default exchange is refused");
    assert_amqp_error(&error, 403, true);
    wait_for_channel_closed(&channel).await;

    assert!(connection.status().connected());
    finish(&connection, handle).await;
}

#[tokio::test]
async fn amq_prefixed_exchange_declare_returns_403() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    let error = channel
        .exchange_declare(
            "amq.custom",
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("reserved exchange names are refused");
    assert_amqp_error(&error, 403, true);
    wait_for_channel_closed(&channel).await;
    assert!(connection.status().connected());

    finish(&connection, handle).await;
}

#[tokio::test]
async fn recover_async_requeues_unacked_with_redelivered_flag() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "jobs").await;
    publish_to_queue(&channel, "jobs", b"recovered").await;

    let delivery = get(&channel, "jobs").await.expect("message is available");
    assert!(!delivery.delivery.redelivered);

    channel
        .basic_recover_async(BasicRecoverAsyncOptions { requeue: true })
        .await
        .expect("basic.recover-async sends");
    assert!(
        connection.status().connected(),
        "recover-async must not close the connection"
    );

    let (body, redelivered) = get_and_ack(&channel, "jobs")
        .await
        .expect("recover-async requeues the unacked message");
    assert!(redelivered);
    assert_eq!(body.as_slice(), b"recovered");
    assert!(connection.status().connected());

    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn global_qos_limits_total_unacked_across_consumers_on_channel() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    declare_queue(&channel, "jobs").await;

    channel
        .basic_qos(1, BasicQosOptions { global: true })
        .await
        .expect("global basic.qos applies");

    let mut first = consume(&channel, "jobs", "first").await;
    let mut second = consume(&channel, "jobs", "second").await;

    publish_to_queue(&channel, "jobs", b"m1").await;
    publish_to_queue(&channel, "jobs", b"m2").await;

    let first_delivery = next_delivery(&mut first).await;
    assert_eq!(body_of(&first_delivery), b"m1");
    expect_no_delivery(&mut first).await;
    expect_no_delivery(&mut second).await;

    first_delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("first delivery acknowledges");

    let second_delivery = next_delivery(&mut second).await;
    assert_eq!(body_of(&second_delivery), b"m2");
    expect_no_delivery(&mut second).await;
    second_delivery
        .ack(lapin::options::BasicAckOptions::default())
        .await
        .expect("second delivery acknowledges");

    cancel_consumer(&channel, "first").await;
    cancel_consumer(&channel, "second").await;
    close_channel(&channel).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn recover_with_requeue_false_closes_connection_with_540() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    let error = channel
        .basic_recover(BasicRecoverOptions { requeue: false })
        .await
        .expect_err("basic.recover with requeue=false is rejected");
    assert_amqp_error(&error, 540, false);
    common::wait_until(
        || connection.status().closed() || connection.status().errored(),
        "connection to close",
    )
    .await;

    finish(&connection, handle).await;
}

#[tokio::test]
async fn recover_async_with_requeue_false_closes_connection_with_540() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    let events = Box::pin(connection.events_listener());

    let _ = channel
        .basic_recover_async(BasicRecoverAsyncOptions { requeue: false })
        .await;

    expect_connection_error(events, 540).await;
    common::wait_until(
        || connection.status().closed() || connection.status().errored(),
        "connection to close",
    )
    .await;

    finish(&connection, handle).await;
}

#[tokio::test]
async fn tx_select_closes_connection_with_540() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    let error = channel
        .tx_select()
        .await
        .expect_err("tx.select is rejected");
    assert_amqp_error(&error, 540, false);
    common::wait_until(
        || connection.status().closed() || connection.status().errored(),
        "connection to close",
    )
    .await;

    finish(&connection, handle).await;
}

#[tokio::test]
async fn stream_queue_type_closes_channel_with_406() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    let mut arguments = FieldTable::default();
    arguments.insert(
        short("x-queue-type"),
        AMQPValue::LongString("stream".into()),
    );
    let error = channel
        .queue_declare("streamed", QueueDeclareOptions::default(), arguments)
        .await
        .expect_err("stream queues are unsupported");
    assert_amqp_error(&error, 406, true);
    wait_for_channel_closed(&channel).await;
    assert!(
        connection.status().connected(),
        "channel close must not close the connection"
    );

    let replacement = create_channel(&connection).await;
    close_channel(&replacement).await;
    finish(&connection, handle).await;
}

#[tokio::test]
async fn headers_exchange_closes_connection_with_503() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;

    let error = channel
        .exchange_declare(
            "headers-x",
            ExchangeKind::Headers,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect_err("headers exchanges are unsupported");
    assert_amqp_error(&error, 503, false);
    common::wait_until(
        || connection.status().closed() || connection.status().errored(),
        "connection to close",
    )
    .await;

    finish(&connection, handle).await;
}

#[tokio::test]
async fn immediate_publish_closes_connection_with_540() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .expect("confirm.select succeeds");

    let confirm = timeout(
        OP_TIMEOUT,
        channel.basic_publish(
            "",
            "missing",
            BasicPublishOptions {
                mandatory: false,
                immediate: true,
            },
            b"",
            BasicProperties::default(),
        ),
    )
    .await
    .expect("basic.publish did not time out")
    .expect("immediate publish reaches the server");

    let error = confirm.await.expect_err("immediate publishes are rejected");
    assert_amqp_error(&error, 540, false);
    common::wait_until(
        || connection.status().closed() || connection.status().errored(),
        "connection to close",
    )
    .await;

    finish(&connection, handle).await;
}

#[tokio::test]
async fn non_empty_expiration_closes_channel_with_406() {
    let (addr, handle) = start_server().await;
    let connection = connect(addr).await;
    let channel = create_channel(&connection).await;
    let events = Box::pin(connection.events_listener());

    let _ = channel
        .confirm_select(ConfirmSelectOptions::default())
        .await;
    let properties = BasicProperties::default().with_expiration(short("1000"));
    let _ = timeout(
        OP_TIMEOUT,
        channel.basic_publish(
            "",
            "missing",
            BasicPublishOptions::default(),
            b"",
            properties,
        ),
    )
    .await;

    expect_error_event(events, 406, true).await;
    wait_for_channel_closed(&channel).await;
    assert!(
        connection.status().connected(),
        "channel close must not close the connection"
    );

    let replacement = create_channel(&connection).await;
    close_channel(&replacement).await;
    finish(&connection, handle).await;
}
