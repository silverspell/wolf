use bytes::Bytes;
use wolf_broker::queue::{ConnectionCloseOutcome, ConnectionId, QueueConfig, QueueRegistry};
use wolf_broker::router::Router;
use wolf_broker::{
    CancelReason, ConsumerCancel, ConsumerId, DomainError, Message, QueueArguments, QueueName,
};

fn config(durable: bool, exclusive: bool, auto_delete: bool) -> QueueConfig {
    QueueConfig {
        durable,
        exclusive,
        auto_delete,
        arguments: QueueArguments::new(),
    }
}

fn message(body: &'static str) -> Message {
    Message::new(Bytes::from_static(body.as_bytes()))
}

fn assert_resource_locked<T>(result: Result<T, DomainError>) {
    assert!(
        matches!(result, Err(DomainError::ResourceLocked { .. })),
        "expected ResourceLocked"
    );
}

#[test]
fn server_named_queue_is_registered_and_returned() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);

    let first = registry
        .declare("", QueueConfig::default(), connection)
        .expect("declare first server-named queue");
    let second = registry
        .declare("", QueueConfig::default(), connection)
        .expect("declare second server-named queue");

    assert!(!first.as_str().is_empty());
    assert!(first.as_str().starts_with("amq.gen-"));
    assert_ne!(first, second);
    assert!(registry.contains(first.as_str()));
    assert!(registry.contains(second.as_str()));
    assert_eq!(registry.len(), 2);
    assert_eq!(
        registry
            .queue(first.as_str())
            .expect("registered queue")
            .name(),
        &first
    );
}

#[test]
fn explicit_queue_name_is_preserved() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);

    let name = registry
        .declare("orders", QueueConfig::default(), connection)
        .expect("declare explicit queue");

    assert_eq!(name.as_str(), "orders");
    assert_eq!(name, QueueName::from("orders"));
    assert!(registry.contains("orders"));
}

#[test]
fn exclusive_queue_is_deleted_when_defining_connection_closes() {
    let mut registry = QueueRegistry::new();
    let owner = ConnectionId::new(1);
    let other = ConnectionId::new(2);

    let private = registry
        .declare("private", config(false, true, false), owner)
        .expect("declare exclusive queue");
    let shared = registry
        .declare("shared", QueueConfig::default(), other)
        .expect("declare shared queue");

    let outcome = registry.close_connection(owner);

    assert!(outcome.deleted_queues.contains(&private));
    assert!(!registry.contains(private.as_str()));
    assert!(registry.contains(shared.as_str()));

    let outcome = registry.close_connection(owner);
    assert!(outcome.deleted_queues.is_empty());
    assert!(registry.contains(shared.as_str()));
}

#[test]
fn auto_delete_queue_is_deleted_when_last_consumer_leaves() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("temporary", config(false, false, true), connection)
        .expect("declare auto-delete queue");

    registry
        .add_consumer(name.as_str(), connection, ConsumerId::new(10))
        .expect("add first consumer");
    registry
        .add_consumer(name.as_str(), connection, ConsumerId::new(11))
        .expect("add second consumer");

    assert!(
        !registry
            .remove_consumer(name.as_str(), ConsumerId::new(10))
            .expect("remove first consumer")
    );
    assert!(registry.contains(name.as_str()));

    assert!(
        registry
            .remove_consumer(name.as_str(), ConsumerId::new(11))
            .expect("remove last consumer")
    );
    assert!(!registry.contains(name.as_str()));
}

#[test]
fn auto_delete_is_not_triggered_by_declaration_alone() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("no-consumers", config(false, false, true), connection)
        .expect("declare auto-delete queue");

    assert!(registry.contains(name.as_str()));
    assert_eq!(registry.len(), 1);
}

#[test]
fn passive_declare_of_missing_queue_returns_queue_not_found() {
    let registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);

    let error = registry
        .declare_passive("missing", connection)
        .expect_err("passive declare must fail for a missing queue");

    assert_eq!(
        error,
        DomainError::QueueNotFound {
            vhost: String::new(),
            queue: "missing".to_string(),
        }
    );
}

#[test]
fn passive_declare_of_existing_queue_ignores_declaration_config() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("persisted", config(true, false, false), connection)
        .expect("declare queue");

    let queue = registry
        .declare_passive(name.as_str(), connection)
        .expect("passive declare of an existing queue");

    assert_eq!(queue.name(), &name);
    assert_eq!(queue.config(), &config(true, false, false));
}

#[test]
fn redeclare_with_different_arguments_returns_precondition_failed() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("stable", QueueConfig::default(), connection)
        .expect("declare queue");

    registry
        .declare(name.as_str(), QueueConfig::default(), connection)
        .expect("identical redeclare is idempotent");

    let error = registry
        .declare(name.as_str(), config(false, true, false), connection)
        .expect_err("different exclusive flag");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let error = registry
        .declare(name.as_str(), config(true, false, false), connection)
        .expect_err("different durable flag");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));

    let mut arguments = QueueArguments::new();
    arguments.insert(
        "x-message-ttl".to_string(),
        wolf_broker::ArgumentValue::Integer(1000),
    );
    let unsupported = QueueConfig {
        arguments,
        ..QueueConfig::default()
    };
    let error = registry
        .declare(name.as_str(), unsupported, connection)
        .expect_err("unsupported argument");
    assert!(matches!(error, DomainError::PreconditionFailed { .. }));
}

#[test]
fn another_connection_cannot_access_exclusive_queue() {
    let mut registry = QueueRegistry::new();
    let owner = ConnectionId::new(1);
    let intruder = ConnectionId::new(2);
    let name = registry
        .declare("owned", config(false, true, false), owner)
        .expect("declare exclusive queue");

    assert_resource_locked(registry.declare_passive(name.as_str(), intruder));
    assert_resource_locked(registry.declare(name.as_str(), config(false, true, false), intruder));
    assert_resource_locked(registry.publish(name.as_str(), intruder, message("nope")));
    assert_resource_locked(registry.purge(name.as_str(), intruder));
    assert_resource_locked(registry.delete(name.as_str(), intruder));
    assert_resource_locked(registry.add_consumer(name.as_str(), intruder, ConsumerId::new(99)));

    assert!(registry.contains(name.as_str()));
    assert_eq!(
        registry
            .queue(name.as_str())
            .expect("queue still present")
            .message_count(),
        0
    );
}

#[test]
fn purge_clears_messages_and_reports_count() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");

    for body in ["one", "two", "three"] {
        registry
            .publish(name.as_str(), connection, message(body))
            .expect("publish");
    }
    assert_eq!(
        registry
            .queue(name.as_str())
            .expect("queue")
            .message_count(),
        3
    );

    assert_eq!(registry.purge(name.as_str(), connection).expect("purge"), 3);
    assert_eq!(
        registry
            .queue(name.as_str())
            .expect("queue remains")
            .message_count(),
        0
    );
    assert!(registry.contains(name.as_str()));
    assert_eq!(registry.purge(name.as_str(), connection).expect("purge"), 0);
}

#[test]
fn delete_clears_messages_and_removes_queue() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare queue");

    registry
        .publish(name.as_str(), connection, message("one"))
        .expect("publish one");
    registry
        .publish(name.as_str(), connection, message("two"))
        .expect("publish two");

    let outcome = registry
        .delete(name.as_str(), connection)
        .expect("delete queue");

    assert_eq!(outcome.message_count(), 2);
    assert!(outcome.cancels.is_empty());
    assert!(!registry.contains(name.as_str()));

    let outcome = registry
        .delete(name.as_str(), connection)
        .expect("deleting a missing queue is idempotent");
    assert_eq!(outcome.message_count(), 0);
    assert!(outcome.cancels.is_empty());
}

#[test]
fn default_exchange_routes_every_declared_queue_name() {
    let mut registry = QueueRegistry::new();
    let router = Router::default();
    let connection = ConnectionId::new(1);

    let explicit = registry
        .declare("jobs", QueueConfig::default(), connection)
        .expect("declare explicit queue");
    let generated = registry
        .declare("", QueueConfig::default(), connection)
        .expect("declare server-named queue");

    for name in [&explicit, &generated] {
        let destinations = router
            .route("", name.as_str())
            .expect("default exchange route");
        assert_eq!(destinations, vec![name.clone()]);
        for destination in destinations {
            registry
                .publish(destination.as_str(), connection, message("payload"))
                .expect("publish via default exchange");
        }
    }

    assert_eq!(
        registry
            .queue(explicit.as_str())
            .expect("explicit queue")
            .message_count(),
        1
    );
    assert_eq!(
        registry
            .queue(generated.as_str())
            .expect("generated queue")
            .message_count(),
        1
    );
}

#[test]
fn deleting_queue_with_consumers_emits_cancel_for_each_consumer() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("fanout-target", QueueConfig::default(), connection)
        .expect("declare queue");

    registry
        .add_consumer(name.as_str(), connection, ConsumerId::new(1))
        .expect("add consumer 1");
    registry
        .add_consumer(name.as_str(), connection, ConsumerId::new(2))
        .expect("add consumer 2");

    let outcome = registry
        .delete(name.as_str(), connection)
        .expect("delete queue");

    assert_eq!(
        outcome.cancels,
        vec![
            ConsumerCancel {
                consumer: ConsumerId::new(1),
                reason: CancelReason::QueueDeleted,
            },
            ConsumerCancel {
                consumer: ConsumerId::new(2),
                reason: CancelReason::QueueDeleted,
            },
        ]
    );
}

#[test]
fn deleting_queue_without_consumers_emits_no_cancels() {
    let mut registry = QueueRegistry::new();
    let connection = ConnectionId::new(1);
    let name = registry
        .declare("empty", QueueConfig::default(), connection)
        .expect("declare queue");

    let outcome = registry
        .delete(name.as_str(), connection)
        .expect("delete queue");

    assert!(outcome.cancels.is_empty());
}

#[test]
fn connection_close_removes_consumers_from_shared_auto_delete_queue() {
    let mut registry = QueueRegistry::new();
    let first = ConnectionId::new(1);
    let second = ConnectionId::new(2);
    let name = registry
        .declare("shared", config(false, false, true), first)
        .expect("declare shared auto-delete queue");

    registry
        .add_consumer(name.as_str(), first, ConsumerId::new(10))
        .expect("add first consumer");
    registry
        .add_consumer(name.as_str(), second, ConsumerId::new(20))
        .expect("add second consumer");

    let outcome = registry.close_connection(first);

    assert_eq!(outcome, ConnectionCloseOutcome::default());
    let queue = registry.queue(name.as_str()).expect("queue survives");
    assert_eq!(
        queue.consumers().collect::<Vec<_>>(),
        vec![ConsumerId::new(20)]
    );

    let outcome = registry.close_connection(second);
    assert_eq!(outcome.deleted_queues, vec![name.clone()]);
    assert!(outcome.cancels.is_empty());
    assert!(!registry.contains(name.as_str()));
}

#[test]
fn connection_close_emits_cancels_for_exclusive_queue_consumers() {
    let mut registry = QueueRegistry::new();
    let owner = ConnectionId::new(1);
    let name = registry
        .declare("owned", config(false, true, false), owner)
        .expect("declare exclusive queue");
    registry
        .add_consumer(name.as_str(), owner, ConsumerId::new(7))
        .expect("add consumer");

    let outcome = registry.close_connection(owner);

    assert_eq!(outcome.deleted_queues, vec![name.clone()]);
    assert_eq!(
        outcome.cancels,
        vec![ConsumerCancel {
            consumer: ConsumerId::new(7),
            reason: CancelReason::QueueDeleted,
        }]
    );
    assert!(!registry.contains(name.as_str()));
}
