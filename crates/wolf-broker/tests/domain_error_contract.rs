use wolf_broker::{Broker, DomainError, ResourceKind, Vhost, VhostId, VhostName};

fn error_shape(error: &DomainError) -> (&'static str, Vec<String>) {
    match error {
        DomainError::VhostNotFound { vhost } => ("VhostNotFound", vec![vhost.clone()]),
        DomainError::ExchangeNotFound { vhost, exchange } => {
            ("ExchangeNotFound", vec![vhost.clone(), exchange.clone()])
        }
        DomainError::QueueNotFound { vhost, queue } => {
            ("QueueNotFound", vec![vhost.clone(), queue.clone()])
        }
        DomainError::AlreadyExists { kind, name } => {
            let kind = match kind {
                ResourceKind::Vhost => "Vhost",
                ResourceKind::Exchange => "Exchange",
                ResourceKind::Queue => "Queue",
                ResourceKind::Binding => "Binding",
            };
            ("AlreadyExists", vec![kind.to_string(), name.clone()])
        }
        DomainError::InvalidArgument { message } => ("InvalidArgument", vec![message.clone()]),
        DomainError::AccessRefused { message } => ("AccessRefused", vec![message.clone()]),
        DomainError::PreconditionFailed { message } => {
            ("PreconditionFailed", vec![message.clone()])
        }
        DomainError::CommandInvalid { message } => ("CommandInvalid", vec![message.clone()]),
        DomainError::BrokerClosed => ("BrokerClosed", vec![]),
    }
}

#[test]
fn queue_not_found_is_protocol_agnostic() {
    let name = VhostName::new("/").expect("valid vhost name");
    let vhost = Vhost::new(VhostId::new(1), name.clone());

    let mut broker = Broker::new();
    broker.add_vhost(vhost).expect("vhost add");

    let error = broker
        .lookup_queue(&name, "missing")
        .expect_err("queue lookup must fail without a queue registry");

    assert_eq!(
        error,
        DomainError::QueueNotFound {
            vhost: "/".to_string(),
            queue: "missing".to_string(),
        }
    );

    let (variant, fields) = error_shape(&error);
    assert_eq!(variant, "QueueNotFound");
    assert_eq!(fields, vec!["/".to_string(), "missing".to_string()]);
}

#[test]
fn error_variants_are_field_exact() {
    let errors = [
        DomainError::VhostNotFound {
            vhost: "v".to_string(),
        },
        DomainError::ExchangeNotFound {
            vhost: "v".to_string(),
            exchange: "e".to_string(),
        },
        DomainError::QueueNotFound {
            vhost: "v".to_string(),
            queue: "q".to_string(),
        },
        DomainError::AlreadyExists {
            kind: ResourceKind::Queue,
            name: "q".to_string(),
        },
        DomainError::InvalidArgument {
            message: "bad".to_string(),
        },
        DomainError::AccessRefused {
            message: "denied".to_string(),
        },
        DomainError::PreconditionFailed {
            message: "mismatch".to_string(),
        },
        DomainError::CommandInvalid {
            message: "unknown".to_string(),
        },
        DomainError::BrokerClosed,
    ];

    for error in &errors {
        assert!(!error.to_string().is_empty());
        let _: &dyn std::error::Error = error;
    }
}

#[test]
fn empty_vhost_name_is_invalid_argument() {
    let error = VhostName::new("").expect_err("empty name must be rejected");
    assert_eq!(
        error,
        DomainError::InvalidArgument {
            message: "vhost name must not be empty".to_string(),
        }
    );
}
