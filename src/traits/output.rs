use serde::Serialize;

use crate::{AmqpError, MessageProperties, Result};

/// A message to publish.
///
/// `#[non_exhaustive]`: build it with [`OutgoingMessage::new`] or [`OutgoingMessage::json`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct OutgoingMessage {
    /// Target exchange; empty for the default exchange.
    pub exchange: String,
    /// Routing key, at most 255 bytes.
    pub routing_key: String,
    /// Raw message body.
    pub payload: Vec<u8>,
    /// Message properties and headers.
    pub properties: MessageProperties,
}

impl OutgoingMessage {
    /// A message with a raw payload and default properties.
    pub fn new(
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            payload,
            properties: MessageProperties::default(),
        }
    }

    /// Serializes `data` as JSON and sets `content_type = application/json`.
    pub fn json<T: Serialize + ?Sized>(
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
        data: &T,
    ) -> Result<Self> {
        let payload = serde_json::to_vec(data).map_err(AmqpError::serialization)?;
        let mut msg = Self::new(exchange, routing_key, payload);
        msg.properties.content_type = Some("application/json".into());
        Ok(msg)
    }

    /// Replaces the properties.
    pub fn with_properties(mut self, properties: MessageProperties) -> Self {
        self.properties = properties;
        self
    }
}

/// Output trait 1: sends raw messages. Implemented by [`crate::AmqpClient`]; implement it
/// yourself to mock publishing in tests.
#[async_trait::async_trait]
pub trait MessagePublisher: Send + Sync {
    /// Resolves once the broker confirmed the message.
    ///
    /// Fails with [`AmqpError::NotConnected`] while the connection is being re-established.
    async fn publish(&self, msg: OutgoingMessage) -> Result<()>;

    /// Publishes a [`Publishable`] value.
    async fn publish_message<P>(&self, message: &P) -> Result<()>
    where
        P: Publishable + Sync,
        Self: Sized,
    {
        self.publish(message.to_outgoing()?).await
    }
}

/// Output trait 2: a type that knows where it is sent to.
pub trait Publishable: Serialize {
    /// Target exchange.
    fn exchange(&self) -> String;
    /// Routing key.
    fn routing_key(&self) -> String;

    /// Message properties; `content_type` defaults to `application/json`.
    fn properties(&self) -> MessageProperties {
        MessageProperties::default()
    }

    /// Serializes `self` into an [`OutgoingMessage`].
    fn to_outgoing(&self) -> Result<OutgoingMessage> {
        let mut msg = OutgoingMessage::json(self.exchange(), self.routing_key(), self)?;
        let content_type = msg.properties.content_type.take();
        msg.properties = self.properties();
        msg.properties.content_type = msg.properties.content_type.or(content_type);
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize)]
    struct OrderCreated {
        id: u32,
    }

    impl Publishable for OrderCreated {
        fn exchange(&self) -> String {
            "orders".into()
        }
        fn routing_key(&self) -> String {
            format!("order.created.{}", self.id)
        }
        fn properties(&self) -> MessageProperties {
            MessageProperties::default().with_message_id(self.id.to_string())
        }
    }

    #[test]
    fn publishable_builds_json_message() {
        let msg = OrderCreated { id: 7 }.to_outgoing().unwrap();
        assert_eq!(msg.exchange, "orders");
        assert_eq!(msg.routing_key, "order.created.7");
        assert_eq!(msg.payload, br#"{"id":7}"#);
        assert_eq!(
            msg.properties.content_type.as_deref(),
            Some("application/json")
        );
        assert_eq!(msg.properties.message_id.as_deref(), Some("7"));
    }
}
