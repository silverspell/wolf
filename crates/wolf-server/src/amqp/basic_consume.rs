//! Handlers for `basic.qos`, `basic.consume`, `basic.cancel` and deliveries.
//!
//! Consumer state is split between the protocol-neutral broker dispatcher and
//! the connection's [`ConsumerDirectory`]. Deliveries route through the
//! connection [`DeliveryHub`], so a message published on any connection can be
//! delivered to its consumer's owning connection.

use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;
use wolf_amqp_codec::{
    BasicCancel, BasicCancelOk, BasicConsume, BasicConsumeOk, BasicDeliver, BasicMethod, BasicQos,
    BasicQosOk, ContentHeader, Frame, Method, ShortString, basic, basic_consume,
    split_content_body,
};
use wolf_broker::{
    ChannelId, ConnectionId, ConsumerId, ConsumerOptions, Delivery, DomainError, Vhost,
};

use crate::amqp::consumer::ConsumerDirectory;
use crate::amqp::error::{AmqpError, ReplyCode};
use crate::delivery::{DeliveryHub, ServerEvent};

/// Handles `basic.qos`.
pub fn qos(
    vhost: &Vhost,
    broker_channel: ChannelId,
    hub: &DeliveryHub,
    channel: u16,
    request: BasicQos,
) -> Result<Vec<Frame>, AmqpError> {
    if request.prefetch_size != 0 {
        return Err(AmqpError::connection(
            ReplyCode::NotImplemented,
            "basic.qos prefetch_size is not supported",
        )
        .with_offending(basic::CLASS_ID, basic_consume::QOS));
    }

    vhost
        .set_prefetch(
            broker_channel,
            u32::from(request.prefetch_count),
            request.global,
        )
        .map_err(|error| basic_error(&error, basic_consume::QOS))?;
    vhost
        .dispatch_all(&mut hub.sink())
        .map_err(|error| basic_error(&error, basic_consume::QOS))?;

    Ok(vec![Frame::Method {
        channel,
        method: Method::Basic(BasicMethod::QosOk(BasicQosOk)),
    }])
}

/// Handles `basic.consume`.
#[allow(clippy::too_many_arguments)]
pub fn consume(
    vhost: &Vhost,
    directory: &mut ConsumerDirectory,
    hub: &DeliveryHub,
    outbox: &UnboundedSender<ServerEvent>,
    channel: u16,
    broker_channel: ChannelId,
    connection: ConnectionId,
    request: BasicConsume,
) -> Result<Vec<Frame>, AmqpError> {
    if request.no_local {
        return Err(AmqpError::connection(
            ReplyCode::NotImplemented,
            "basic.consume with no_local=true is not supported",
        )
        .with_offending(basic::CLASS_ID, basic_consume::CONSUME));
    }
    if request.exclusive {
        return Err(AmqpError::connection(
            ReplyCode::NotImplemented,
            "basic.consume with exclusive=true is not supported",
        )
        .with_offending(basic::CLASS_ID, basic_consume::CONSUME));
    }

    let consumer = ConsumerId::generate();
    let tag = if request.consumer_tag.as_str().is_empty() {
        ShortString::new(format!("amq.ctag-{}", consumer.get()))
    } else {
        request.consumer_tag.clone()
    };
    if directory.contains_tag(channel, &tag) {
        return Err(AmqpError::channel(
            ReplyCode::NotAllowed,
            format!("attempt to reuse consumer tag '{}'", tag.as_str()),
        )
        .with_offending(basic::CLASS_ID, basic_consume::CONSUME));
    }

    vhost
        .consume(
            connection,
            broker_channel,
            consumer,
            request.queue.as_str(),
            ConsumerOptions {
                no_ack: request.no_ack,
            },
        )
        .map_err(|error| basic_error(&error, basic_consume::CONSUME))?;
    directory.register(consumer, channel, tag.clone());
    hub.register(consumer, outbox.clone());

    let frames = if request.nowait {
        Vec::new()
    } else {
        vec![Frame::Method {
            channel,
            method: Method::Basic(BasicMethod::ConsumeOk(BasicConsumeOk { consumer_tag: tag })),
        }]
    };

    vhost
        .dispatch(request.queue.as_str(), &mut hub.sink())
        .map_err(|error| basic_error(&error, basic_consume::CONSUME))?;

    Ok(frames)
}

/// Handles `basic.cancel`.
pub fn cancel(
    vhost: &Vhost,
    directory: &mut ConsumerDirectory,
    hub: &DeliveryHub,
    channel: u16,
    request: BasicCancel,
) -> Result<Vec<Frame>, AmqpError> {
    if let Some((consumer, _entry)) = directory.remove_by_tag(channel, &request.consumer_tag) {
        hub.unregister(consumer);
        vhost
            .cancel(consumer)
            .map_err(|error| basic_error(&error, basic_consume::CANCEL))?;
    }

    if request.nowait {
        return Ok(Vec::new());
    }
    Ok(vec![Frame::Method {
        channel,
        method: Method::Basic(BasicMethod::CancelOk(BasicCancelOk {
            consumer_tag: request.consumer_tag,
        })),
    }])
}

/// Builds a server-initiated `basic.cancel` frame for a delivery consumer.
pub fn cancel_frame(channel: u16, tag: ShortString) -> Frame {
    Frame::Method {
        channel,
        method: Method::Basic(BasicMethod::Cancel(BasicCancel {
            consumer_tag: tag,
            nowait: true,
        })),
    }
}

/// Builds `basic.deliver` plus content frames for one delivery.
pub fn delivery_frames(
    channel: u16,
    frame_max: u32,
    consumer_tag: &ShortString,
    delivery: &Delivery,
) -> Result<Vec<Frame>, AmqpError> {
    let metadata = delivery.message.metadata();
    let exchange = metadata.map_or("", |metadata| metadata.exchange.as_str());
    let routing_key = metadata.map_or("", |metadata| metadata.routing_key.as_str());
    let properties = metadata.map_or_else(Bytes::new, |metadata| metadata.properties.clone());

    let mut frames = vec![Frame::Method {
        channel,
        method: Method::Basic(BasicMethod::Deliver(BasicDeliver {
            consumer_tag: consumer_tag.clone(),
            delivery_tag: delivery.delivery_tag.get(),
            redelivered: delivery.redelivered,
            exchange: ShortString::new(exchange),
            routing_key: ShortString::new(routing_key),
        })),
    }];
    frames.extend(content_frames(
        channel,
        frame_max,
        delivery.message.body().clone(),
        properties,
    )?);
    Ok(frames)
}

/// Builds a content header and zero-copy body chunks for a message.
pub(crate) fn content_frames(
    channel: u16,
    frame_max: u32,
    body: Bytes,
    properties: Bytes,
) -> Result<Vec<Frame>, AmqpError> {
    let header = ContentHeader {
        class_id: basic::CLASS_ID,
        weight: 0,
        body_size: body.len() as u64,
        properties,
    };
    let mut frames = vec![Frame::ContentHeader { channel, header }];
    for chunk in split_content_body(body, frame_max).map_err(|error| {
        AmqpError::connection(
            ReplyCode::CommandInvalid,
            format!("cannot split message body: {error}"),
        )
    })? {
        frames.push(Frame::ContentBody {
            channel,
            body: chunk,
        });
    }
    Ok(frames)
}

fn basic_error(error: &DomainError, method_id: u16) -> AmqpError {
    AmqpError::from_domain(error).with_offending(basic::CLASS_ID, method_id)
}
