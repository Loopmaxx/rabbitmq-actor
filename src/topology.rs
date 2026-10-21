use lapin::{
    Connection,
    options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
};

use crate::{AmqpConfig, AmqpError, Result, TopologyObject, error::timed};

/// Declares all exchanges, queues and bindings from the config.
/// Called after every (re)connect; declarations are idempotent.
///
/// Every broker call is limited by `connection.operation_timeout_ms`.
pub(crate) async fn declare(connection: &Connection, config: &AmqpConfig) -> Result<()> {
    let timeout = config.connection.operation_timeout();
    let channel = timed("open channel", timeout, connection.create_channel())
        .await?
        .map_err(AmqpError::channel)?;

    // All names below were checked against the 255 byte limit of AMQP short strings
    // by `AmqpConfig::validate`, so the `.into()` conversions cannot panic.
    for exchange in &config.exchanges {
        let options = ExchangeDeclareOptions {
            durable: exchange.durable,
            auto_delete: exchange.auto_delete,
            ..Default::default()
        };
        let declare = channel.exchange_declare(
            exchange.name.as_str().into(),
            exchange.kind.to_lapin(),
            options,
            FieldTable::default(),
        );
        timed("declare exchange", timeout, declare)
            .await?
            .map_err(|e| AmqpError::declare(TopologyObject::Exchange, &exchange.name, e))?;
    }

    for queue in &config.queues {
        let options = QueueDeclareOptions {
            durable: queue.durable,
            exclusive: queue.exclusive,
            auto_delete: queue.auto_delete,
            ..Default::default()
        };
        let declare = channel.queue_declare(
            queue.name.as_str().into(),
            options,
            queue.declare_arguments(),
        );
        timed("declare queue", timeout, declare)
            .await?
            .map_err(|e| AmqpError::declare(TopologyObject::Queue, &queue.name, e))?;

        for binding in &queue.bindings {
            for key in &binding.routing_keys {
                let bind = channel.queue_bind(
                    queue.name.as_str().into(),
                    binding.exchange.as_str().into(),
                    key.as_str().into(),
                    QueueBindOptions::default(),
                    FieldTable::default(),
                );
                timed("bind queue", timeout, bind).await?.map_err(|e| {
                    AmqpError::declare(
                        TopologyObject::Binding,
                        format!("{} -> {} ({key})", binding.exchange, queue.name),
                        e,
                    )
                })?;
            }
        }
    }

    let _ = timed(
        "close channel",
        timeout,
        channel.close(200, "topology declared".into()),
    )
    .await;
    tracing::debug!(
        exchanges = config.exchanges.len(),
        queues = config.queues.len(),
        "topology declared"
    );
    Ok(())
}
