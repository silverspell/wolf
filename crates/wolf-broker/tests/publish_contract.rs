use bytes::Bytes;
use wolf_broker::publish::{PublishOutcome, PublishRequest, publish};
use wolf_broker::queue::{ConnectionId, QueueConfig, QueueRegistry};
use wolf_broker::router::Router;
use wolf_broker::{
    DomainError, ExchangeConfig, ExchangeKind, ExchangeName, Message, QueueName, RoutingKey,
};

#[derive(Debug, PartialEq, Eq)]
struct Properties {
    id: u32,
}

fn message(body: &'static [u8]) -> Message {
    Message::new(Bytes::from_static(body))
}

fn declare_queue(queues: &mut QueueRegistry, connection: ConnectionId, name: &str) {
    queues
        .declare(name, QueueConfig::default(), connection)
        .expect("declare queue");
}

fn route_direct(router: &mut Router, exchange: &str, queue: &str, routing_key: &str) {
    router
        .registry_mut()
        .declare(exchange, ExchangeConfig::new(ExchangeKind::Direct))
        .expect("declare direct exchange");
    router
        .registry_mut()
        .bind(exchange, queue, routing_key)
        .expect("bind queue");
}

fn route_fanout(router: &mut Router, exchange: &str, queues: &[&str]) {
    router
        .registry_mut()
        .declare(exchange, ExchangeConfig::new(ExchangeKind::Fanout))
        .expect("declare fanout exchange");
    for queue in queues {
        router
            .registry_mut()
            .bind(exchange, *queue, "")
            .expect("bind queue");
    }
}

#[test]
fn publish_writes_shared_body_handle_to_every_routed_queue() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    for name in ["q1", "q2", "q3"] {
        declare_queue(&mut queues, connection, name);
    }

    let mut router = Router::default();
    route_fanout(&mut router, "broadcast", &["q1", "q2", "q3"]);

    let body = Bytes::from_static(b"shared-payload");
    let pointer = body.as_ptr();
    let request = PublishRequest {
        exchange: "broadcast",
        routing_key: "ignored",
        mandatory: false,
        message: Message::new(body),
        properties: (),
    };

    let mut destinations = Vec::new();
    let outcome =
        publish(&router, &mut queues, &mut destinations, connection, request).expect("publish");

    assert_eq!(outcome, PublishOutcome::Routed { queues: 3 });
    for name in ["q1", "q2", "q3"] {
        let queue = queues.queue(name).expect("routed queue");
        assert_eq!(queue.message_count(), 1);
        let stored = queue.messages().next().expect("stored message");
        assert_eq!(stored.body().as_ptr(), pointer);
        assert_eq!(stored.body().as_ref(), b"shared-payload");
    }
}

#[test]
fn publish_reuses_and_clears_destination_buffer() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    for name in ["q1", "q2"] {
        declare_queue(&mut queues, connection, name);
    }

    let mut router = Router::default();
    route_direct(&mut router, "direct", "q1", "rk");
    router
        .registry_mut()
        .bind("direct", "q2", "rk")
        .expect("bind second queue");

    let mut destinations = Vec::with_capacity(8);
    destinations.push(QueueName::from("stale"));
    let capacity = destinations.capacity();

    let request = PublishRequest {
        exchange: "direct",
        routing_key: "rk",
        mandatory: false,
        message: message(b"payload"),
        properties: (),
    };

    let outcome =
        publish(&router, &mut queues, &mut destinations, connection, request).expect("publish");

    assert_eq!(outcome, PublishOutcome::Routed { queues: 2 });
    assert_eq!(
        destinations,
        vec![QueueName::from("q1"), QueueName::from("q2")]
    );
    assert!(!destinations.contains(&QueueName::from("stale")));
    assert_eq!(destinations.capacity(), capacity);
}

#[test]
fn mandatory_unroutable_publish_returns_body_and_properties() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    declare_queue(&mut queues, connection, "present");

    let router = Router::default();
    let body = Bytes::from_static(b"return-me");
    let pointer = body.as_ptr();
    let request = PublishRequest {
        exchange: "",
        routing_key: "missing",
        mandatory: true,
        message: Message::new(body),
        properties: Properties { id: 7 },
    };

    let mut destinations = Vec::new();
    let outcome =
        publish(&router, &mut queues, &mut destinations, connection, request).expect("publish");

    match outcome {
        PublishOutcome::Returned(returned) => {
            assert_eq!(returned.exchange, ExchangeName::from(""));
            assert_eq!(returned.routing_key, RoutingKey::from("missing"));
            assert_eq!(returned.message.body().as_ptr(), pointer);
            assert_eq!(returned.message.body().as_ref(), b"return-me");
            assert_eq!(returned.properties, Properties { id: 7 });
        }
        other => panic!("expected returned message, got {other:?}"),
    }

    assert_eq!(
        queues
            .queue("present")
            .expect("present queue")
            .message_count(),
        0
    );
}

#[test]
fn mandatory_publish_to_stale_binding_is_returned() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    let mut router = Router::default();
    route_direct(&mut router, "direct", "ghost", "rk");

    let request = PublishRequest {
        exchange: "direct",
        routing_key: "rk",
        mandatory: true,
        message: message(b"lost"),
        properties: (),
    };

    let mut destinations = Vec::new();
    let outcome =
        publish(&router, &mut queues, &mut destinations, connection, request).expect("publish");

    match outcome {
        PublishOutcome::Returned(returned) => {
            assert_eq!(returned.exchange, ExchangeName::from("direct"));
            assert_eq!(returned.routing_key, RoutingKey::from("rk"));
            assert_eq!(returned.message.body().as_ref(), b"lost");
        }
        other => panic!("expected returned message, got {other:?}"),
    }
}

#[test]
fn non_mandatory_unroutable_publish_is_dropped() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    declare_queue(&mut queues, connection, "present");

    let router = Router::default();
    let request = PublishRequest {
        exchange: "",
        routing_key: "missing",
        mandatory: false,
        message: message(b"drop-me"),
        properties: (),
    };

    let mut destinations = Vec::new();
    let outcome =
        publish(&router, &mut queues, &mut destinations, connection, request).expect("publish");

    assert_eq!(outcome, PublishOutcome::Dropped);
    assert_eq!(
        queues
            .queue("present")
            .expect("present queue")
            .message_count(),
        0
    );
}

#[test]
fn non_mandatory_stale_binding_is_dropped() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    let mut router = Router::default();
    route_direct(&mut router, "direct", "ghost", "rk");

    let request = PublishRequest {
        exchange: "direct",
        routing_key: "rk",
        mandatory: false,
        message: message(b"lost"),
        properties: (),
    };

    let mut destinations = Vec::new();
    let outcome =
        publish(&router, &mut queues, &mut destinations, connection, request).expect("publish");

    assert_eq!(outcome, PublishOutcome::Dropped);
}

#[test]
fn publish_to_unknown_exchange_returns_domain_error() {
    let connection = ConnectionId::new(1);
    let mut queues = QueueRegistry::new();
    let router = Router::default();

    let request = PublishRequest {
        exchange: "missing",
        routing_key: "rk",
        mandatory: false,
        message: message(b"payload"),
        properties: (),
    };

    let mut destinations = Vec::new();
    let error = publish(&router, &mut queues, &mut destinations, connection, request)
        .expect_err("unknown exchange");

    assert_eq!(
        error,
        DomainError::ExchangeNotFound {
            vhost: String::new(),
            exchange: "missing".to_string(),
        }
    );
}
