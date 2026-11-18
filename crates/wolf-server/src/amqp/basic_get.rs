//! Handler for `basic.get` and its `basic.get-ok` / `basic.get-empty` replies.

use bytes::Bytes;
use wolf_amqp_codec::{
    BasicGet, BasicGetEmpty, BasicGetOk, BasicMethod, Frame, Method, ShortString, basic,
    basic_consume,
};
use wolf_broker::{ChannelId, ConnectionId, DomainError, GetOutcome, Vhost};

use crate::amqp::basic_consume::content_frames;
use crate::amqp::error::AmqpError;

/// Handles `basic.get`, returning either a message with content or an empty
/// reply.
pub fn get(
    vhost: &Vhost,
    channel: u16,
    broker_channel: ChannelId,
    connection: ConnectionId,
    frame_max: u32,
    request: BasicGet,
) -> Result<Vec<Frame>, AmqpError> {
    let outcome = vhost
        .get(
            connection,
            broker_channel,
            request.queue.as_str(),
            request.no_ack,
        )
        .map_err(|error| get_error(&error))?;

    match outcome {
        GetOutcome::Empty => Ok(vec![Frame::Method {
            channel,
            method: Method::Basic(BasicMethod::GetEmpty(BasicGetEmpty {
                cluster_id: ShortString::new(""),
            })),
        }]),
        GetOutcome::Message(got) => {
            let metadata = got.message.metadata();
            let exchange = metadata.map_or("", |metadata| metadata.exchange.as_str());
            let routing_key = metadata.map_or("", |metadata| metadata.routing_key.as_str());
            let properties =
                metadata.map_or_else(Bytes::new, |metadata| metadata.properties.clone());
            let delivery_tag = got.delivery_tag.map_or(0, |tag| tag.get());

            let mut frames = vec![Frame::Method {
                channel,
                method: Method::Basic(BasicMethod::GetOk(BasicGetOk {
                    delivery_tag,
                    redelivered: got.redelivered,
                    exchange: ShortString::new(exchange),
                    routing_key: ShortString::new(routing_key),
                    message_count: u32::try_from(got.remaining).unwrap_or(u32::MAX),
                })),
            }];
            frames.extend(content_frames(
                channel,
                frame_max,
                got.message.into_body(),
                properties,
            )?);
            Ok(frames)
        }
    }
}

fn get_error(error: &DomainError) -> AmqpError {
    AmqpError::from_domain(error).with_offending(basic::CLASS_ID, basic_consume::GET)
}
