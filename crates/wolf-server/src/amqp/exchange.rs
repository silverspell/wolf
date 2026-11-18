//! AMQP handlers for the exchange class.

use wolf_amqp_codec::{
    ExchangeDeclare, ExchangeDeclareOk, ExchangeDelete, ExchangeDeleteOk, ExchangeMethod, Frame,
    Method, exchange,
};
use wolf_broker::{DomainError, ExchangeConfig, ExchangeKind, Vhost};

use crate::amqp::arguments;
use crate::amqp::error::AmqpError;

/// Handles `exchange.declare`.
pub async fn declare(
    vhost: &Vhost,
    channel: u16,
    request: ExchangeDeclare,
) -> Result<Vec<Frame>, AmqpError> {
    let result = if request.passive {
        vhost
            .declare_exchange_passive(request.exchange.as_str())
            .await
    } else {
        let kind = ExchangeKind::parse(request.exchange_type.as_str())
            .map_err(|error| exchange_error(&error, exchange::DECLARE))?;
        let arguments = arguments::convert(&request.arguments)
            .map_err(|error| exchange_error(&error, exchange::DECLARE))?;
        let config = ExchangeConfig {
            kind,
            durable: request.durable,
            auto_delete: request.auto_delete,
            internal: request.internal,
            arguments,
        };
        vhost
            .declare_exchange(request.exchange.as_str(), config)
            .await
    };

    result.map_err(|error| exchange_error(&error, exchange::DECLARE))?;

    if request.nowait {
        return Ok(Vec::new());
    }
    Ok(vec![Frame::Method {
        channel,
        method: Method::Exchange(ExchangeMethod::DeclareOk(ExchangeDeclareOk)),
    }])
}

/// Handles `exchange.delete`.
pub async fn delete(
    vhost: &Vhost,
    channel: u16,
    request: ExchangeDelete,
) -> Result<Vec<Frame>, AmqpError> {
    vhost
        .delete_exchange(request.exchange.as_str(), request.if_unused)
        .await
        .map_err(|error| exchange_error(&error, exchange::DELETE))?;

    if request.nowait {
        return Ok(Vec::new());
    }
    Ok(vec![Frame::Method {
        channel,
        method: Method::Exchange(ExchangeMethod::DeleteOk(ExchangeDeleteOk)),
    }])
}

fn exchange_error(error: &DomainError, method_id: u16) -> AmqpError {
    AmqpError::from_domain(error).with_offending(exchange::CLASS_ID, method_id)
}
