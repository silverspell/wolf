//! Handlers for `basic.ack`, `basic.nack`, `basic.reject` and recovery.
//!
//! Settlement is delegated to the broker dispatcher. Requeued messages and
//! restored prefetch credit are offered to consumers again through the
//! connection [`DeliveryHub`].

use wolf_amqp_codec::{
    BasicAck, BasicMethod, BasicNack, BasicRecover, BasicRecoverAsync, BasicRecoverOk, BasicReject,
    Frame, Method, basic, basic_ack, basic_recover,
};
use wolf_broker::{ChannelId, DeliveryTag, DomainError, Vhost};

use crate::amqp::error::{AmqpError, ReplyCode};
use crate::delivery::DeliveryHub;

/// Handles `basic.ack`. There is no reply on success.
pub fn ack(
    vhost: &Vhost,
    broker_channel: ChannelId,
    hub: &DeliveryHub,
    request: BasicAck,
) -> Result<Vec<Frame>, AmqpError> {
    vhost
        .ack(
            broker_channel,
            DeliveryTag::new(request.delivery_tag),
            request.multiple,
        )
        .map_err(|error| basic_error(&error, basic_ack::ACK))?;
    vhost
        .dispatch_all(&mut hub.sink())
        .map_err(|error| basic_error(&error, basic_ack::ACK))?;
    Ok(Vec::new())
}

/// Handles `basic.nack`. There is no reply on success.
pub fn nack(
    vhost: &Vhost,
    broker_channel: ChannelId,
    hub: &DeliveryHub,
    request: BasicNack,
) -> Result<Vec<Frame>, AmqpError> {
    vhost
        .nack(
            broker_channel,
            DeliveryTag::new(request.delivery_tag),
            request.multiple,
            request.requeue,
        )
        .map_err(|error| basic_error(&error, basic_ack::NACK))?;
    vhost
        .dispatch_all(&mut hub.sink())
        .map_err(|error| basic_error(&error, basic_ack::NACK))?;
    Ok(Vec::new())
}

/// Handles `basic.reject`. There is no reply on success.
pub fn reject(
    vhost: &Vhost,
    broker_channel: ChannelId,
    hub: &DeliveryHub,
    request: BasicReject,
) -> Result<Vec<Frame>, AmqpError> {
    vhost
        .reject(
            broker_channel,
            DeliveryTag::new(request.delivery_tag),
            request.requeue,
        )
        .map_err(|error| basic_error(&error, basic_ack::REJECT))?;
    vhost
        .dispatch_all(&mut hub.sink())
        .map_err(|error| basic_error(&error, basic_ack::REJECT))?;
    Ok(Vec::new())
}

/// Handles `basic.recover`, replying with `basic.recover-ok`.
pub fn recover(
    vhost: &Vhost,
    broker_channel: ChannelId,
    hub: &DeliveryHub,
    channel: u16,
    request: BasicRecover,
) -> Result<Vec<Frame>, AmqpError> {
    requeue_all(
        vhost,
        broker_channel,
        hub,
        request.requeue,
        basic_recover::RECOVER,
    )?;
    Ok(vec![Frame::Method {
        channel,
        method: Method::Basic(BasicMethod::RecoverOk(BasicRecoverOk)),
    }])
}

/// Handles `basic.recover-async`; no reply is sent.
pub fn recover_async(
    vhost: &Vhost,
    broker_channel: ChannelId,
    hub: &DeliveryHub,
    request: BasicRecoverAsync,
) -> Result<Vec<Frame>, AmqpError> {
    requeue_all(
        vhost,
        broker_channel,
        hub,
        request.requeue,
        basic_recover::RECOVER_ASYNC,
    )?;
    Ok(Vec::new())
}

fn requeue_all(
    vhost: &Vhost,
    broker_channel: ChannelId,
    hub: &DeliveryHub,
    requeue: bool,
    method_id: u16,
) -> Result<(), AmqpError> {
    if !requeue {
        return Err(AmqpError::connection(
            ReplyCode::NotImplemented,
            "basic.recover with requeue=false is not supported",
        )
        .with_offending(basic::CLASS_ID, method_id));
    }
    vhost
        .recover(broker_channel)
        .map_err(|error| basic_error(&error, method_id))?;
    vhost
        .dispatch_all(&mut hub.sink())
        .map_err(|error| basic_error(&error, method_id))?;
    Ok(())
}

fn basic_error(error: &DomainError, method_id: u16) -> AmqpError {
    AmqpError::from_domain(error).with_offending(basic::CLASS_ID, method_id)
}
