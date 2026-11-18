use bytes::Bytes;
use wolf_broker::queue::ConnectionId;
use wolf_broker::router::Router;
use wolf_broker::{
    Broker, DomainError, Message, PublishOutcome, PublishRequest, QueueConfig, QueueRegistry,
    Vhost, VhostId, VhostName, publish::Returned,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Properties {
    id: u32,
}

fn valid_name() -> VhostName {
    VhostName::new("/").expect("valid vhost name")
}

fn message(body: &'static [u8]) -> Message {
    Message::new(Bytes::from_static(body))
}

fn vhost(queues: QueueRegistry) -> Vhost {
    Vhost::with_routing(VhostId::new(1), valid_name(), Router::default(), queues)
}

#[test]
fn default_exchange_publish_is_routed_to_the_named_queue() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");
    let vhost = vhost(queues);

    let outcome = vhost
        .publish(
            connection,
            PublishRequest {
                exchange: "",
                routing_key: "jobs",
                mandatory: false,
                message: message(b"payload"),
                properties: (),
            },
        )
        .expect("publish is routed");

    assert_eq!(outcome, PublishOutcome::Routed { queues: 1 });
    assert_eq!(vhost.queue_message_count("jobs"), 1);
    assert_eq!(vhost.queue_message_count("missing"), 0);
}

#[test]
fn mandatory_return_preserves_the_body_handle_and_properties() {
    let connection = ConnectionId::new(1);
    let vhost = vhost(QueueRegistry::new());
    let body = Bytes::from_static(b"return-me");
    let pointer = body.as_ptr();

    let outcome = vhost
        .publish(
            connection,
            PublishRequest {
                exchange: "",
                routing_key: "missing",
                mandatory: true,
                message: Message::new(body),
                properties: Properties { id: 9 },
            },
        )
        .expect("mandatory publish completes");

    let PublishOutcome::Returned(Returned {
        exchange,
        routing_key,
        message,
        properties,
    }) = outcome
    else {
        panic!("expected a returned message, got {outcome:?}");
    };
    assert_eq!(exchange.as_str(), "");
    assert_eq!(routing_key.as_str(), "missing");
    assert_eq!(message.body().as_ptr(), pointer);
    assert_eq!(message.body().as_ref(), b"return-me");
    assert_eq!(properties, Properties { id: 9 });
}

#[test]
fn publish_to_unknown_exchange_is_a_domain_error() {
    let connection = ConnectionId::new(1);
    let vhost = vhost(QueueRegistry::new());

    let error = vhost
        .publish(
            connection,
            PublishRequest {
                exchange: "missing",
                routing_key: "rk",
                mandatory: false,
                message: message(b"payload"),
                properties: (),
            },
        )
        .expect_err("unknown exchange must fail");

    assert!(matches!(
        error,
        DomainError::ExchangeNotFound { exchange, .. } if exchange == "missing"
    ));
}

#[test]
fn publishing_to_an_exclusive_queue_from_another_connection_is_rejected() {
    let owner = ConnectionId::new(1);
    let intruder = ConnectionId::new(2);
    let mut queues = QueueRegistry::new();
    queues
        .declare(
            "private",
            QueueConfig {
                exclusive: true,
                ..QueueConfig::default()
            },
            owner,
        )
        .expect("declare exclusive queue");
    let vhost = vhost(queues);

    let error = vhost
        .publish(
            intruder,
            PublishRequest {
                exchange: "",
                routing_key: "private",
                mandatory: false,
                message: message(b"secret"),
                properties: (),
            },
        )
        .expect_err("exclusive access must be denied");

    assert!(matches!(error, DomainError::ResourceLocked { .. }));
    assert_eq!(vhost.queue_message_count("private"), 0);
}

#[test]
fn broker_publish_resolves_the_vhost_and_delegates() {
    let name = valid_name();
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    queues
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");

    let mut broker = Broker::new();
    broker
        .add_vhost(Vhost::with_routing(
            VhostId::new(1),
            name.clone(),
            Router::default(),
            queues,
        ))
        .expect("add vhost");

    let outcome = broker
        .publish(
            &name,
            connection,
            PublishRequest {
                exchange: "",
                routing_key: "jobs",
                mandatory: false,
                message: message(b"payload"),
                properties: (),
            },
        )
        .expect("broker publish succeeds");

    assert_eq!(outcome, PublishOutcome::Routed { queues: 1 });
    assert_eq!(
        broker
            .vhost(&name)
            .expect("vhost exists")
            .queue_message_count("jobs"),
        1
    );
}

#[test]
fn broker_publish_reports_a_missing_vhost() {
    let broker = Broker::new();
    let name = valid_name();

    let error = broker
        .publish(
            &name,
            ConnectionId::new(1),
            PublishRequest {
                exchange: "",
                routing_key: "jobs",
                mandatory: false,
                message: message(b"payload"),
                properties: (),
            },
        )
        .expect_err("missing vhost must fail");

    assert_eq!(error, DomainError::VhostNotFound { vhost: "/".into() });
}
