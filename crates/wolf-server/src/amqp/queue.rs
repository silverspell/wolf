//! AMQP handlers for the queue class.

use wolf_amqp_codec::{
    BasicCancel, BasicMethod, Frame, Method, QueueBind, QueueBindOk, QueueDeclare, QueueDeclareOk,
    QueueDelete, QueueDeleteOk, QueueMethod, QueuePurge, QueuePurgeOk, QueueUnbind, QueueUnbindOk,
    ShortString, queue,
};
use wolf_broker::{ConnectionId, DomainError, QueueConfig, Vhost};

use crate::amqp::arguments;
use crate::amqp::consumer::ConsumerDirectory;
use crate::amqp::error::AmqpError;
use crate::delivery::DeliveryHub;

/// Handles `queue.declare`.
pub fn declare(
    vhost: &Vhost,
    channel: u16,
    connection: ConnectionId,
    request: QueueDeclare,
) -> Result<Vec<Frame>, AmqpError> {
    let result = if request.passive {
        vhost.declare_queue_passive(request.queue.as_str(), connection)
    } else {
        let arguments = arguments::convert(&request.arguments)
            .map_err(|error| queue_error(&error, queue::DECLARE))?;
        let config = QueueConfig {
            durable: request.durable,
            exclusive: request.exclusive,
            auto_delete: request.auto_delete,
            arguments,
        };
        vhost.declare_queue(request.queue.as_str(), config, connection)
    };
    let declaration = result.map_err(|error| queue_error(&error, queue::DECLARE))?;

    if request.nowait {
        return Ok(Vec::new());
    }
    Ok(vec![Frame::Method {
        channel,
        method: Method::Queue(QueueMethod::DeclareOk(QueueDeclareOk {
            queue: ShortString::new(declaration.name.as_str()),
            message_count: declaration.message_count,
            consumer_count: declaration.consumer_count,
        })),
    }])
}

/// Handles `queue.bind`.
pub fn bind(
    vhost: &Vhost,
    channel: u16,
    connection: ConnectionId,
    request: QueueBind,
) -> Result<Vec<Frame>, AmqpError> {
    vhost
        .bind_queue(
            request.exchange.as_str(),
            request.queue.as_str(),
            request.routing_key.as_str(),
            connection,
        )
        .map_err(|error| queue_error(&error, queue::BIND))?;

    if request.nowait {
        return Ok(Vec::new());
    }
    Ok(vec![Frame::Method {
        channel,
        method: Method::Queue(QueueMethod::BindOk(QueueBindOk)),
    }])
}

/// Handles `queue.unbind`.
pub fn unbind(
    vhost: &Vhost,
    channel: u16,
    connection: ConnectionId,
    request: QueueUnbind,
) -> Result<Vec<Frame>, AmqpError> {
    vhost
        .unbind_queue(
            request.exchange.as_str(),
            request.queue.as_str(),
            request.routing_key.as_str(),
            connection,
        )
        .map_err(|error| queue_error(&error, queue::UNBIND))?;

    Ok(vec![Frame::Method {
        channel,
        method: Method::Queue(QueueMethod::UnbindOk(QueueUnbindOk)),
    }])
}

/// Handles `queue.purge`.
pub fn purge(
    vhost: &Vhost,
    channel: u16,
    connection: ConnectionId,
    request: QueuePurge,
) -> Result<Vec<Frame>, AmqpError> {
    let message_count = vhost
        .purge_queue(request.queue.as_str(), connection)
        .map_err(|error| queue_error(&error, queue::PURGE))?;

    if request.nowait {
        return Ok(Vec::new());
    }
    Ok(vec![Frame::Method {
        channel,
        method: Method::Queue(QueueMethod::PurgeOk(QueuePurgeOk { message_count })),
    }])
}

/// Handles `queue.delete`.
///
/// Cancellations produced by the deletion are resolved through the connection's
/// [`ConsumerDirectory`] and emitted as server-initiated `basic.cancel` frames
/// before the `queue.delete-ok` reply. Consumers without a registered wire tag
/// are ignored because no `basic.consume` handler exists yet.
pub fn delete(
    vhost: &Vhost,
    channel: u16,
    connection: ConnectionId,
    request: QueueDelete,
    consumers: &mut ConsumerDirectory,
) -> Result<Vec<Frame>, AmqpError> {
    let hub = DeliveryHub::new();
    delete_with_hub(vhost, channel, connection, request, consumers, &hub)
}

/// Handles `queue.delete`, routing cross-connection cancels through the hub.
///
/// Local consumers receive a server-initiated `basic.cancel` frame before the
/// `queue.delete-ok` reply. Consumers owned by other connections receive the
/// cancellation through the delivery hub. Consumers without a registered wire
/// tag are ignored.
pub fn delete_with_hub(
    vhost: &Vhost,
    channel: u16,
    connection: ConnectionId,
    request: QueueDelete,
    consumers: &mut ConsumerDirectory,
    hub: &DeliveryHub,
) -> Result<Vec<Frame>, AmqpError> {
    let outcome = vhost
        .delete_queue(
            request.queue.as_str(),
            connection,
            request.if_unused,
            request.if_empty,
        )
        .map_err(|error| queue_error(&error, queue::DELETE))?;

    let mut frames = Vec::new();
    for cancel in outcome.cancels {
        let Some(entry) = consumers.remove(cancel.consumer) else {
            hub.cancel(cancel);
            continue;
        };
        hub.unregister(cancel.consumer);
        frames.push(Frame::Method {
            channel: entry.channel,
            method: Method::Basic(BasicMethod::Cancel(BasicCancel {
                consumer_tag: entry.tag,
                nowait: true,
            })),
        });
    }

    if !request.nowait {
        frames.push(Frame::Method {
            channel,
            method: Method::Queue(QueueMethod::DeleteOk(QueueDeleteOk {
                message_count: u32::try_from(outcome.messages).unwrap_or(u32::MAX),
            })),
        });
    }
    Ok(frames)
}

fn queue_error(error: &DomainError, method_id: u16) -> AmqpError {
    AmqpError::from_domain(error).with_offending(queue::CLASS_ID, method_id)
}

#[cfg(test)]
mod tests {
    use wolf_amqp_codec::{FieldTable, QueueMethod};
    use wolf_broker::router::Router;
    use wolf_broker::{ConsumerId, QueueConfig, QueueRegistry, Vhost, VhostId, VhostName};

    use super::*;

    fn vhost_with_consumer(queue_name: &str, consumer: ConsumerId) -> Vhost {
        let connection = ConnectionId::new(1);
        let mut queues = QueueRegistry::new();
        queues
            .declare(queue_name, QueueConfig::default(), connection)
            .expect("declare queue");
        queues
            .add_consumer(queue_name, connection, consumer)
            .expect("add consumer");
        Vhost::with_routing(
            VhostId::new(1),
            VhostName::new("/").expect("valid vhost"),
            Router::default(),
            queues,
        )
    }

    fn delete_request(queue_name: &str, nowait: bool) -> QueueDelete {
        QueueDelete {
            ticket: 0,
            queue: ShortString::new(queue_name),
            if_unused: false,
            if_empty: false,
            nowait,
        }
    }

    fn cancel_frame(frame: &Frame) -> (u16, &ShortString) {
        let Frame::Method {
            channel,
            method: Method::Basic(BasicMethod::Cancel(cancel)),
        } = frame
        else {
            panic!("expected basic.cancel, got {frame:?}");
        };
        assert!(cancel.nowait);
        (*channel, &cancel.consumer_tag)
    }

    #[test]
    fn delete_emits_cancel_before_delete_ok_for_registered_consumers() {
        let consumer = ConsumerId::new(7);
        let vhost = vhost_with_consumer("busy", consumer);
        let mut consumers = ConsumerDirectory::new();
        consumers.register(consumer, 3, ShortString::new("tag-7"));

        let frames = delete(
            &vhost,
            1,
            ConnectionId::new(1),
            delete_request("busy", false),
            &mut consumers,
        )
        .expect("delete succeeds");

        assert_eq!(frames.len(), 2);
        let (channel, tag) = cancel_frame(&frames[0]);
        assert_eq!(channel, 3);
        assert_eq!(tag.as_str(), "tag-7");
        assert!(matches!(
            frames[1],
            Frame::Method {
                channel: 1,
                method: Method::Queue(QueueMethod::DeleteOk(_)),
            }
        ));
        assert!(consumers.is_empty());
    }

    #[test]
    fn delete_of_an_unregistered_consumer_emits_only_delete_ok() {
        let consumer = ConsumerId::new(8);
        let vhost = vhost_with_consumer("busy", consumer);
        let mut consumers = ConsumerDirectory::new();

        let frames = delete(
            &vhost,
            1,
            ConnectionId::new(1),
            delete_request("busy", false),
            &mut consumers,
        )
        .expect("delete succeeds");

        assert_eq!(frames.len(), 1);
        assert!(matches!(
            frames[0],
            Frame::Method {
                channel: 1,
                method: Method::Queue(QueueMethod::DeleteOk(_)),
            }
        ));
    }

    #[test]
    fn if_unused_delete_with_a_consumer_closes_the_channel_with_406() {
        let consumer = ConsumerId::new(9);
        let vhost = vhost_with_consumer("busy", consumer);
        let mut consumers = ConsumerDirectory::new();
        consumers.register(consumer, 1, ShortString::new("tag-9"));

        let request = QueueDelete {
            if_unused: true,
            ..delete_request("busy", false)
        };
        let error = delete(&vhost, 1, ConnectionId::new(1), request, &mut consumers)
            .expect_err("consumer blocks if_unused");

        assert_eq!(error.reply_code.code(), 406);
        assert_eq!(error.offending_method_id, queue::DELETE);
        assert_eq!(consumers.len(), 1);
    }

    #[test]
    fn nowait_delete_emits_cancels_but_no_delete_ok() {
        let consumer = ConsumerId::new(10);
        let vhost = vhost_with_consumer("busy", consumer);
        let mut consumers = ConsumerDirectory::new();
        consumers.register(consumer, 2, ShortString::new("tag-10"));

        let frames = delete(
            &vhost,
            1,
            ConnectionId::new(1),
            delete_request("busy", true),
            &mut consumers,
        )
        .expect("delete succeeds");

        assert_eq!(frames.len(), 1);
        let (channel, tag) = cancel_frame(&frames[0]);
        assert_eq!(channel, 2);
        assert_eq!(tag.as_str(), "tag-10");
    }

    #[test]
    fn declare_passes_through_the_domain_arguments() {
        let vhost = Vhost::new(VhostId::new(1), VhostName::new("/").expect("valid vhost"));

        let frames = declare(
            &vhost,
            1,
            ConnectionId::new(1),
            QueueDeclare {
                ticket: 0,
                queue: ShortString::new("plain"),
                passive: false,
                durable: false,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                arguments: FieldTable::new(),
            },
        )
        .expect("declare succeeds");

        assert!(matches!(
            frames.as_slice(),
            [Frame::Method {
                channel: 1,
                method: Method::Queue(QueueMethod::DeclareOk(_)),
            }]
        ));
    }
}
