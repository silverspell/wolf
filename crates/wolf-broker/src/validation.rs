//! Validation of queue declaration arguments.
//!
//! AMQP clients may attach `x-*` arguments to `queue.declare`. This module
//! accepts the subset the broker supports and rejects features that are known
//! but not implemented, so unsupported declarations fail loudly rather than
//! being silently ignored.

use std::collections::BTreeMap;

use crate::error::DomainError;
use crate::exchange::ArgumentValue;

/// Protocol-neutral queue declaration arguments.
pub type QueueArguments = BTreeMap<String, ArgumentValue>;

/// The argument naming the queue type.
const QUEUE_TYPE_ARGUMENT: &str = "x-queue-type";

/// Queue arguments that are recognized but not yet supported by the broker.
pub const UNSUPPORTED_QUEUE_ARGUMENTS: &[&str] = &[
    "x-message-ttl",
    "x-expires",
    "x-dead-letter-exchange",
    "x-dead-letter-routing-key",
    "x-max-length",
    "x-max-length-bytes",
    "x-overflow",
    "x-max-priority",
    "x-single-active-consumer",
    "x-queue-mode",
    "x-queue-master-locator",
];

/// Validates the arguments of a queue declaration.
///
/// Only classic queues are supported, so a missing `x-queue-type` or the value
/// `"classic"` is accepted. Any other queue type, and any argument in
/// [`UNSUPPORTED_QUEUE_ARGUMENTS`], yields [`DomainError::PreconditionFailed`].
/// Unknown arguments are ignored so that clients may send extension arguments.
pub fn validate_queue_arguments(arguments: &QueueArguments) -> Result<(), DomainError> {
    if let Some(value) = arguments.get(QUEUE_TYPE_ARGUMENT) {
        match value {
            ArgumentValue::String(kind) if kind == "classic" => {}
            ArgumentValue::String(kind) => {
                return Err(DomainError::PreconditionFailed {
                    message: format!(
                        "queue type '{kind}' is not supported; only 'classic' queues are supported"
                    ),
                });
            }
            other => {
                return Err(DomainError::PreconditionFailed {
                    message: format!(
                        "queue argument '{QUEUE_TYPE_ARGUMENT}' must be a string, got {other:?}"
                    ),
                });
            }
        }
    }

    if let Some(name) = arguments
        .keys()
        .find(|name| UNSUPPORTED_QUEUE_ARGUMENTS.contains(&name.as_str()))
    {
        return Err(DomainError::PreconditionFailed {
            message: format!("queue argument '{name}' is not supported"),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_UNSUPPORTED: [&str; 11] = [
        "x-message-ttl",
        "x-expires",
        "x-dead-letter-exchange",
        "x-dead-letter-routing-key",
        "x-max-length",
        "x-max-length-bytes",
        "x-overflow",
        "x-max-priority",
        "x-single-active-consumer",
        "x-queue-mode",
        "x-queue-master-locator",
    ];

    fn arguments(entries: &[(&str, ArgumentValue)]) -> QueueArguments {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect()
    }

    fn expect_precondition_failed(entries: &[(&str, ArgumentValue)]) {
        let error = validate_queue_arguments(&arguments(entries))
            .expect_err("expected queue arguments to be rejected");
        assert!(
            matches!(error, DomainError::PreconditionFailed { .. }),
            "expected PreconditionFailed, got {error:?}"
        );
    }

    fn expect_accepted(entries: &[(&str, ArgumentValue)]) {
        validate_queue_arguments(&arguments(entries))
            .expect("expected queue arguments to be accepted");
    }

    #[test]
    fn unsupported_list_matches_the_documented_set() {
        assert_eq!(UNSUPPORTED_QUEUE_ARGUMENTS, ALL_UNSUPPORTED.as_slice());
    }

    #[test]
    fn stream_queue_type_is_precondition_failed() {
        expect_precondition_failed(&[("x-queue-type", ArgumentValue::String("stream".into()))]);
        expect_precondition_failed(&[
            ("x-queue-type", ArgumentValue::String("stream".into())),
            ("x-unknown", ArgumentValue::Bool(true)),
        ]);
    }

    #[test]
    fn absent_or_classic_queue_type_is_accepted() {
        expect_accepted(&[]);
        expect_accepted(&[("x-queue-type", ArgumentValue::String("classic".into()))]);
    }

    #[test]
    fn non_classic_queue_types_are_precondition_failed() {
        for value in [
            ArgumentValue::String("quorum".into()),
            ArgumentValue::String(String::new()),
            ArgumentValue::String("CLASSIC".into()),
            ArgumentValue::Bool(true),
            ArgumentValue::Integer(1),
            ArgumentValue::Bytes(b"classic".to_vec()),
        ] {
            expect_precondition_failed(&[("x-queue-type", value)]);
        }
    }

    #[test]
    fn every_unsupported_argument_is_precondition_failed() {
        for &name in UNSUPPORTED_QUEUE_ARGUMENTS {
            expect_precondition_failed(&[(name, ArgumentValue::String("value".into()))]);
        }
    }

    #[test]
    fn unsupported_argument_fails_for_all_value_types() {
        for value in [
            ArgumentValue::Bool(true),
            ArgumentValue::Integer(10),
            ArgumentValue::String("10".into()),
            ArgumentValue::Bytes(vec![1, 2, 3]),
        ] {
            expect_precondition_failed(&[("x-message-ttl", value)]);
        }
    }

    #[test]
    fn unknown_x_arguments_are_accepted() {
        for value in [
            ArgumentValue::Bool(true),
            ArgumentValue::Integer(7),
            ArgumentValue::String("x".into()),
            ArgumentValue::Bytes(vec![0, 1]),
        ] {
            expect_accepted(&[("x-unknown", value.clone())]);
            expect_accepted(&[
                ("x-queue-type", ArgumentValue::String("classic".into())),
                ("x-unknown", value),
            ]);
        }
    }

    #[test]
    fn unknown_non_x_argument_is_accepted() {
        expect_accepted(&[("priority", ArgumentValue::Integer(5))]);
    }

    #[test]
    fn unknown_arguments_do_not_mask_unsupported_arguments() {
        expect_precondition_failed(&[
            ("x-queue-type", ArgumentValue::String("classic".into())),
            ("x-unknown", ArgumentValue::Bool(true)),
            ("x-message-ttl", ArgumentValue::Integer(1000)),
        ]);
    }

    #[test]
    fn unsupported_argument_names_are_case_sensitive() {
        expect_accepted(&[("X-MESSAGE-TTL", ArgumentValue::Integer(1000))]);
    }
}
