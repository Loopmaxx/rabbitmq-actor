use std::{marker::PhantomData, sync::Arc};

use serde::de::DeserializeOwned;

use crate::{AmqpError, MessageProperties};

/// What happens to a message after a handler processed it.
///
/// `#[non_exhaustive]`: new outcomes may be added in minor releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandlerResult {
    /// Processed, remove it from the queue.
    Ack,
    /// Temporary failure, put it back into the queue.
    ///
    /// On RabbitMQ 4.x a requeue does not count towards the quorum queue's
    /// `delivery_limit` (only `x-acquired-count` grows). The library therefore rejects
    /// the message instead once `consumer.max_requeues` is reached.
    Requeue,
    /// Permanent failure, drop it or move it to the dead-letter exchange.
    Reject,
}

impl HandlerResult {
    /// Combines the results of several handlers on the same queue: the most
    /// severe result wins (`Reject` > `Requeue` > `Ack`).
    pub fn combine(self, other: Self) -> Self {
        use HandlerResult::*;
        match (self, other) {
            (Reject, _) | (_, Reject) => Reject,
            (Requeue, _) | (_, Requeue) => Requeue,
            _ => Ack,
        }
    }
}

/// Everything about an incoming message except the payload.
///
/// `#[non_exhaustive]`: use [`MessageMeta::new`] to build one, e.g. in handler tests.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MessageMeta {
    /// Queue the message was consumed from.
    pub queue: String,
    /// Exchange the message was published to (empty for the default exchange).
    pub exchange: String,
    /// Routing key the message was published with.
    pub routing_key: String,
    /// The broker delivered this message before (AMQP `redelivered` flag).
    pub redelivered: bool,
    /// `x-delivery-count`: failed deliveries on a quorum queue (counts towards `delivery_limit`).
    pub delivery_count: Option<i64>,
    /// `x-acquired-count`: how often a quorum queue already handed this message out (RabbitMQ 4.x).
    pub acquired_count: Option<i64>,
    /// Message properties and headers.
    pub properties: MessageProperties,
}

impl MessageMeta {
    /// Metadata of a first delivery with empty properties.
    pub fn new(
        queue: impl Into<String>,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        Self {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            redelivered: false,
            delivery_count: None,
            acquired_count: None,
            properties: MessageProperties::default(),
        }
    }

    /// Sets the properties.
    pub fn with_properties(mut self, properties: MessageProperties) -> Self {
        self.properties = properties;
        self
    }
}

/// A received message as passed to a [`MessageHandler`].
///
/// `#[non_exhaustive]`: use [`IncomingMessage::new`] to build one, e.g. in handler tests.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct IncomingMessage {
    /// Everything except the payload.
    pub meta: MessageMeta,
    /// Raw message body.
    pub payload: Vec<u8>,
}

impl IncomingMessage {
    /// Creates a message from metadata and payload.
    pub fn new(meta: MessageMeta, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            meta,
            payload: payload.into(),
        }
    }

    /// Deserializes the payload from JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, AmqpError> {
        serde_json::from_slice(&self.payload).map_err(|source| AmqpError::Deserialization {
            routing_key: self.meta.routing_key.clone(),
            source: Arc::new(source),
        })
    }
}

/// Input trait 1: receives the raw message.
#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    /// Processes one message. A panic counts as [`HandlerResult::Requeue`].
    async fn handle(&self, msg: &IncomingMessage) -> HandlerResult;
}

/// Input trait 2: receives the payload deserialized from JSON.
///
/// Messages that cannot be deserialized are rejected (and dead-lettered if configured)
/// without calling the handler.
#[async_trait::async_trait]
pub trait TypedMessageHandler<T>: Send + Sync + 'static
where
    T: DeserializeOwned + Send + 'static,
{
    /// Processes one deserialized message. A panic counts as [`HandlerResult::Requeue`].
    async fn handle(&self, msg: T, meta: &MessageMeta) -> HandlerResult;
}

/// Turns a [`TypedMessageHandler`] into a [`MessageHandler`].
pub(crate) struct TypedHandlerAdapter<T, H> {
    handler: H,
    _type: PhantomData<fn() -> T>,
}

impl<T, H> TypedHandlerAdapter<T, H> {
    pub(crate) fn new(handler: H) -> Arc<Self> {
        Arc::new(Self {
            handler,
            _type: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<T, H> MessageHandler for TypedHandlerAdapter<T, H>
where
    T: DeserializeOwned + Send + 'static,
    H: TypedMessageHandler<T>,
{
    async fn handle(&self, msg: &IncomingMessage) -> HandlerResult {
        match msg.json::<T>() {
            Ok(value) => self.handler.handle(value, &msg.meta).await,
            Err(e) => {
                tracing::error!(queue = %msg.meta.queue, "rejecting message: {e}");
                HandlerResult::Reject
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Order {
        id: u32,
    }

    struct OrderHandler;

    #[async_trait::async_trait]
    impl TypedMessageHandler<Order> for OrderHandler {
        async fn handle(&self, msg: Order, _meta: &MessageMeta) -> HandlerResult {
            if msg.id == 0 {
                HandlerResult::Requeue
            } else {
                HandlerResult::Ack
            }
        }
    }

    fn message(payload: &str) -> IncomingMessage {
        IncomingMessage::new(
            MessageMeta::new("orders", "ex", "order.created"),
            payload.as_bytes(),
        )
    }

    #[tokio::test]
    async fn typed_adapter_deserializes_and_rejects_garbage() {
        let handler = TypedHandlerAdapter::new(OrderHandler);
        assert_eq!(
            handler.handle(&message(r#"{"id":1}"#)).await,
            HandlerResult::Ack
        );
        assert_eq!(
            handler.handle(&message(r#"{"id":0}"#)).await,
            HandlerResult::Requeue
        );
        assert_eq!(
            handler.handle(&message("not json")).await,
            HandlerResult::Reject
        );
    }

    #[test]
    fn most_severe_result_wins() {
        use HandlerResult::*;
        assert_eq!(Ack.combine(Ack), Ack);
        assert_eq!(Ack.combine(Requeue), Requeue);
        assert_eq!(Requeue.combine(Reject), Reject);
    }
}
