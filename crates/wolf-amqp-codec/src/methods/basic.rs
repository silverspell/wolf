//! AMQP Basic class (60) methods.

use bytes::BufMut;

use crate::error::{Error, Result};
use crate::methods::{bit_set, pack_bits};
use crate::types::{Reader, Writer};

#[path = "basic_ack.rs"]
pub mod basic_ack;
#[path = "basic_consume.rs"]
pub mod basic_consume;
#[path = "basic_publish.rs"]
pub mod basic_publish;
#[path = "basic_recover.rs"]
pub mod basic_recover;

pub use basic_ack::{ACK, BasicAck, BasicNack, BasicReject, NACK, REJECT};
pub use basic_consume::{
    BasicCancel, BasicCancelOk, BasicConsume, BasicConsumeOk, BasicDeliver, BasicGet,
    BasicGetEmpty, BasicGetOk, BasicQos, BasicQosOk, CANCEL, CANCEL_OK, CONSUME, CONSUME_OK,
    DELIVER, GET, GET_EMPTY, GET_OK, QOS, QOS_OK,
};
pub use basic_publish::{BasicPublish, BasicReturn, PUBLISH, RETURN};
pub use basic_recover::{
    BasicRecover, BasicRecoverAsync, BasicRecoverOk, RECOVER, RECOVER_ASYNC, RECOVER_OK,
};

/// AMQP Basic class id.
pub const CLASS_ID: u16 = 60;

/// A method from the AMQP Basic class.
#[derive(Debug, Clone, PartialEq)]
pub enum BasicMethod {
    /// `basic.qos`
    Qos(BasicQos),
    /// `basic.qos-ok`
    QosOk(BasicQosOk),
    /// `basic.consume`
    Consume(BasicConsume),
    /// `basic.consume-ok`
    ConsumeOk(BasicConsumeOk),
    /// `basic.cancel`
    Cancel(BasicCancel),
    /// `basic.cancel-ok`
    CancelOk(BasicCancelOk),
    /// `basic.publish`
    Publish(BasicPublish),
    /// `basic.return`
    Return(BasicReturn),
    /// `basic.deliver`
    Deliver(BasicDeliver),
    /// `basic.get`
    Get(BasicGet),
    /// `basic.get-ok`
    GetOk(BasicGetOk),
    /// `basic.get-empty`
    GetEmpty(BasicGetEmpty),
    /// `basic.ack`
    Ack(BasicAck),
    /// `basic.reject`
    Reject(BasicReject),
    /// `basic.nack`
    Nack(BasicNack),
    /// `basic.recover-async`
    RecoverAsync(BasicRecoverAsync),
    /// `basic.recover`
    Recover(BasicRecover),
    /// `basic.recover-ok`
    RecoverOk(BasicRecoverOk),
}

impl BasicMethod {
    /// Returns the AMQP class id (always 60).
    pub fn class_id(&self) -> u16 {
        CLASS_ID
    }

    /// Returns the AMQP method id.
    pub fn method_id(&self) -> u16 {
        match self {
            Self::Qos(_) => QOS,
            Self::QosOk(_) => QOS_OK,
            Self::Consume(_) => CONSUME,
            Self::ConsumeOk(_) => CONSUME_OK,
            Self::Cancel(_) => CANCEL,
            Self::CancelOk(_) => CANCEL_OK,
            Self::Publish(_) => PUBLISH,
            Self::Return(_) => RETURN,
            Self::Deliver(_) => DELIVER,
            Self::Get(_) => GET,
            Self::GetOk(_) => GET_OK,
            Self::GetEmpty(_) => GET_EMPTY,
            Self::Ack(_) => ACK,
            Self::Reject(_) => REJECT,
            Self::Nack(_) => NACK,
            Self::RecoverAsync(_) => RECOVER_ASYNC,
            Self::Recover(_) => RECOVER,
            Self::RecoverOk(_) => RECOVER_OK,
        }
    }

    pub(crate) fn encode_body_into<B: BufMut>(&self, writer: &mut Writer<B>) -> Result<()> {
        match self {
            Self::Qos(method) => {
                writer.write_u32(method.prefetch_size);
                writer.write_u16(method.prefetch_count);
                writer.write_u8(pack_bits(&[method.global]));
            }
            Self::QosOk(_) => {}
            Self::Consume(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.queue.as_str())?;
                writer.write_short_string(method.consumer_tag.as_str())?;
                writer.write_u8(pack_bits(&[
                    method.no_local,
                    method.no_ack,
                    method.exclusive,
                    method.nowait,
                ]));
                writer.write_field_table(&method.arguments, 0)?;
            }
            Self::ConsumeOk(method) => {
                writer.write_short_string(method.consumer_tag.as_str())?;
            }
            Self::Cancel(method) => {
                writer.write_short_string(method.consumer_tag.as_str())?;
                writer.write_u8(pack_bits(&[method.nowait]));
            }
            Self::CancelOk(method) => {
                writer.write_short_string(method.consumer_tag.as_str())?;
            }
            Self::Publish(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_short_string(method.routing_key.as_str())?;
                writer.write_u8(pack_bits(&[method.mandatory, method.immediate]));
            }
            Self::Return(method) => {
                writer.write_u16(method.reply_code);
                writer.write_short_string(method.reply_text.as_str())?;
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_short_string(method.routing_key.as_str())?;
            }
            Self::Deliver(method) => {
                writer.write_short_string(method.consumer_tag.as_str())?;
                writer.write_u64(method.delivery_tag);
                writer.write_u8(pack_bits(&[method.redelivered]));
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_short_string(method.routing_key.as_str())?;
            }
            Self::Get(method) => {
                writer.write_u16(method.ticket);
                writer.write_short_string(method.queue.as_str())?;
                writer.write_u8(pack_bits(&[method.no_ack]));
            }
            Self::GetOk(method) => {
                writer.write_u64(method.delivery_tag);
                writer.write_u8(pack_bits(&[method.redelivered]));
                writer.write_short_string(method.exchange.as_str())?;
                writer.write_short_string(method.routing_key.as_str())?;
                writer.write_u32(method.message_count);
            }
            Self::GetEmpty(method) => {
                writer.write_short_string(method.cluster_id.as_str())?;
            }
            Self::Ack(method) => {
                writer.write_u64(method.delivery_tag);
                writer.write_u8(pack_bits(&[method.multiple]));
            }
            Self::Reject(method) => {
                writer.write_u64(method.delivery_tag);
                writer.write_u8(pack_bits(&[method.requeue]));
            }
            Self::Nack(method) => {
                writer.write_u64(method.delivery_tag);
                writer.write_u8(pack_bits(&[method.multiple, method.requeue]));
            }
            Self::RecoverAsync(method) => {
                writer.write_u8(pack_bits(&[method.requeue]));
            }
            Self::Recover(method) => {
                writer.write_u8(pack_bits(&[method.requeue]));
            }
            Self::RecoverOk(_) => {}
        }
        Ok(())
    }

    pub(crate) fn decode(method_id: u16, reader: &mut Reader<'_>) -> Result<Self> {
        let method = match method_id {
            QOS => Self::Qos(BasicQos {
                prefetch_size: reader.read_u32()?,
                prefetch_count: reader.read_u16()?,
                global: bit_set(reader.read_u8()?, 0),
            }),
            QOS_OK => Self::QosOk(BasicQosOk),
            CONSUME => {
                let ticket = reader.read_u16()?;
                let queue = reader.read_short_string()?;
                let consumer_tag = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Consume(BasicConsume {
                    ticket,
                    queue,
                    consumer_tag,
                    no_local: bit_set(bits, 0),
                    no_ack: bit_set(bits, 1),
                    exclusive: bit_set(bits, 2),
                    nowait: bit_set(bits, 3),
                    arguments: reader.read_field_table(0)?,
                })
            }
            CONSUME_OK => Self::ConsumeOk(BasicConsumeOk {
                consumer_tag: reader.read_short_string()?,
            }),
            CANCEL => Self::Cancel(BasicCancel {
                consumer_tag: reader.read_short_string()?,
                nowait: bit_set(reader.read_u8()?, 0),
            }),
            CANCEL_OK => Self::CancelOk(BasicCancelOk {
                consumer_tag: reader.read_short_string()?,
            }),
            PUBLISH => {
                let ticket = reader.read_u16()?;
                let exchange = reader.read_short_string()?;
                let routing_key = reader.read_short_string()?;
                let bits = reader.read_u8()?;
                Self::Publish(BasicPublish {
                    ticket,
                    exchange,
                    routing_key,
                    mandatory: bit_set(bits, 0),
                    immediate: bit_set(bits, 1),
                })
            }
            RETURN => Self::Return(BasicReturn {
                reply_code: reader.read_u16()?,
                reply_text: reader.read_short_string()?,
                exchange: reader.read_short_string()?,
                routing_key: reader.read_short_string()?,
            }),
            DELIVER => Self::Deliver(BasicDeliver {
                consumer_tag: reader.read_short_string()?,
                delivery_tag: reader.read_u64()?,
                redelivered: bit_set(reader.read_u8()?, 0),
                exchange: reader.read_short_string()?,
                routing_key: reader.read_short_string()?,
            }),
            GET => {
                let ticket = reader.read_u16()?;
                let queue = reader.read_short_string()?;
                Self::Get(BasicGet {
                    ticket,
                    queue,
                    no_ack: bit_set(reader.read_u8()?, 0),
                })
            }
            GET_OK => Self::GetOk(BasicGetOk {
                delivery_tag: reader.read_u64()?,
                redelivered: bit_set(reader.read_u8()?, 0),
                exchange: reader.read_short_string()?,
                routing_key: reader.read_short_string()?,
                message_count: reader.read_u32()?,
            }),
            GET_EMPTY => Self::GetEmpty(BasicGetEmpty {
                cluster_id: reader.read_short_string()?,
            }),
            ACK => Self::Ack(BasicAck {
                delivery_tag: reader.read_u64()?,
                multiple: bit_set(reader.read_u8()?, 0),
            }),
            REJECT => Self::Reject(BasicReject {
                delivery_tag: reader.read_u64()?,
                requeue: bit_set(reader.read_u8()?, 0),
            }),
            NACK => {
                let delivery_tag = reader.read_u64()?;
                let bits = reader.read_u8()?;
                Self::Nack(BasicNack {
                    delivery_tag,
                    multiple: bit_set(bits, 0),
                    requeue: bit_set(bits, 1),
                })
            }
            RECOVER_ASYNC => Self::RecoverAsync(BasicRecoverAsync {
                requeue: bit_set(reader.read_u8()?, 0),
            }),
            RECOVER => Self::Recover(BasicRecover {
                requeue: bit_set(reader.read_u8()?, 0),
            }),
            RECOVER_OK => Self::RecoverOk(BasicRecoverOk),
            other => {
                return Err(Error::UnknownMethod {
                    class_id: CLASS_ID,
                    method_id: other,
                });
            }
        };
        Ok(method)
    }
}
