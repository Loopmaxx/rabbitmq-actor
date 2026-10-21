mod input;
mod output;

pub use input::*;
pub use output::*;

use std::{collections::BTreeMap, time::Duration};

use lapin::{
    BasicProperties,
    types::{AMQPValue, ByteArray, FieldTable, LongString, ShortString},
};

use crate::{AmqpError, Result};

/// Message properties without lapin types, used for incoming and outgoing messages.
///
/// `#[non_exhaustive]`: build it with [`MessageProperties::default`] and the `with_*`
/// methods, or set the public fields on a default value.
#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct MessageProperties {
    /// MIME type of the payload, e.g. `application/json`.
    pub content_type: Option<String>,
    /// Application-defined message id.
    pub message_id: Option<String>,
    /// Id of the request this message answers.
    pub correlation_id: Option<String>,
    /// Queue the receiver should reply to.
    pub reply_to: Option<String>,
    /// Unix timestamp in seconds.
    pub timestamp: Option<u64>,
    /// Per-message TTL (`expiration`). Sent as whole milliseconds.
    pub expiration: Option<Duration>,
    /// Priority (0–255). Only honored by queues with `x-max-priority`.
    pub priority: Option<u8>,
    /// Application-defined message type (the AMQP `type` property).
    pub message_type: Option<String>,
    /// Id of the sending application.
    pub app_id: Option<String>,
    /// User id; RabbitMQ rejects messages whose `user_id` differs from the connection's user.
    pub user_id: Option<String>,
    /// Application headers. Incoming nested tables and arrays (e.g. `x-death`) and
    /// decimal values are not supported yet and are left out.
    pub headers: BTreeMap<String, HeaderValue>,
}

/// Value of an application header.
///
/// `#[non_exhaustive]`: table and array values may be added in minor releases.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum HeaderValue {
    /// UTF-8 string.
    String(String),
    /// Any signed or unsigned AMQP integer up to 64 bit, and AMQP timestamps.
    Int(i64),
    /// Boolean.
    Bool(bool),
    /// 32 or 64 bit floating point number.
    Float(f64),
    /// Raw bytes, also used for incoming strings that are not valid UTF-8.
    Bytes(Vec<u8>),
}

impl HeaderValue {
    /// The value if it is a [`HeaderValue::String`].
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    /// The value if it is a [`HeaderValue::Int`].
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(v) => Some(*v),
            _ => None,
        }
    }

    /// The value if it is a [`HeaderValue::Bool`].
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    fn from_amqp(value: &AMQPValue) -> Option<Self> {
        Some(match value {
            AMQPValue::LongString(s) => match std::str::from_utf8(s.as_bytes()) {
                Ok(s) => Self::String(s.to_owned()),
                Err(_) => Self::Bytes(s.as_bytes().to_vec()),
            },
            AMQPValue::ShortString(s) => Self::String(s.as_str().to_owned()),
            AMQPValue::Boolean(b) => Self::Bool(*b),
            AMQPValue::ShortShortInt(v) => Self::Int(i64::from(*v)),
            AMQPValue::ShortShortUInt(v) => Self::Int(i64::from(*v)),
            AMQPValue::ShortInt(v) => Self::Int(i64::from(*v)),
            AMQPValue::ShortUInt(v) => Self::Int(i64::from(*v)),
            AMQPValue::LongInt(v) => Self::Int(i64::from(*v)),
            AMQPValue::LongUInt(v) => Self::Int(i64::from(*v)),
            AMQPValue::LongLongInt(v) => Self::Int(*v),
            AMQPValue::Timestamp(v) => Self::Int(i64::try_from(*v).ok()?),
            AMQPValue::Float(v) => Self::Float(f64::from(*v)),
            AMQPValue::Double(v) => Self::Float(*v),
            AMQPValue::ByteArray(b) => Self::Bytes(b.as_slice().to_vec()),
            AMQPValue::DecimalValue(_)
            | AMQPValue::FieldArray(_)
            | AMQPValue::FieldTable(_)
            | AMQPValue::Void => return None,
        })
    }

    fn to_amqp(&self) -> AMQPValue {
        match self {
            Self::String(s) => AMQPValue::LongString(LongString::from(s.as_str())),
            Self::Int(v) => AMQPValue::LongLongInt(*v),
            Self::Bool(v) => AMQPValue::Boolean(*v),
            Self::Float(v) => AMQPValue::Double(*v),
            Self::Bytes(b) => AMQPValue::ByteArray(ByteArray::from(b.clone())),
        }
    }
}

impl From<String> for HeaderValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for HeaderValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<i64> for HeaderValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<i32> for HeaderValue {
    fn from(value: i32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<u32> for HeaderValue {
    fn from(value: u32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<bool> for HeaderValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f64> for HeaderValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<Vec<u8>> for HeaderValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl MessageProperties {
    /// Sets `content_type`.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets `message_id`.
    pub fn with_message_id(mut self, id: impl Into<String>) -> Self {
        self.message_id = Some(id.into());
        self
    }

    /// Sets `correlation_id`.
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Sets `reply_to`.
    pub fn with_reply_to(mut self, queue: impl Into<String>) -> Self {
        self.reply_to = Some(queue.into());
        self
    }

    /// Sets `timestamp` (Unix seconds).
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Sets the per-message TTL.
    pub fn with_expiration(mut self, ttl: Duration) -> Self {
        self.expiration = Some(ttl);
        self
    }

    /// Sets `priority`.
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Sets the AMQP `type` property.
    pub fn with_message_type(mut self, message_type: impl Into<String>) -> Self {
        self.message_type = Some(message_type.into());
        self
    }

    /// Sets `app_id`.
    pub fn with_app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Sets `user_id`.
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Adds a header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<HeaderValue>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub(crate) fn from_amqp(properties: &BasicProperties) -> Self {
        let string = |s: &Option<ShortString>| s.as_ref().map(|s| s.as_str().to_owned());
        let headers = properties
            .headers()
            .as_ref()
            .map(|table| {
                table
                    .inner()
                    .iter()
                    .filter_map(|(k, v)| Some((k.as_str().to_owned(), HeaderValue::from_amqp(v)?)))
                    .collect()
            })
            .unwrap_or_default();

        Self {
            content_type: string(properties.content_type()),
            message_id: string(properties.message_id()),
            correlation_id: string(properties.correlation_id()),
            reply_to: string(properties.reply_to()),
            timestamp: *properties.timestamp(),
            expiration: properties
                .expiration()
                .as_ref()
                .and_then(|ms| ms.as_str().parse().ok())
                .map(Duration::from_millis),
            priority: *properties.priority(),
            message_type: string(properties.kind()),
            app_id: string(properties.app_id()),
            user_id: string(properties.user_id()),
            headers,
        }
    }

    /// Fails with [`AmqpError::InvalidMessage`] if a string property or header name is
    /// longer than the 255 bytes AMQP allows.
    pub(crate) fn to_amqp(&self, persistent: bool) -> Result<BasicProperties> {
        let mut p = BasicProperties::default();
        if persistent {
            p = p.with_delivery_mode(2);
        }
        if let Some(v) = &self.content_type {
            p = p.with_content_type(short_string("content_type", v)?);
        }
        if let Some(v) = &self.message_id {
            p = p.with_message_id(short_string("message_id", v)?);
        }
        if let Some(v) = &self.correlation_id {
            p = p.with_correlation_id(short_string("correlation_id", v)?);
        }
        if let Some(v) = &self.reply_to {
            p = p.with_reply_to(short_string("reply_to", v)?);
        }
        if let Some(v) = self.timestamp {
            p = p.with_timestamp(v);
        }
        if let Some(v) = self.expiration {
            p = p.with_expiration(short_string("expiration", &v.as_millis().to_string())?);
        }
        if let Some(v) = self.priority {
            p = p.with_priority(v);
        }
        if let Some(v) = &self.message_type {
            p = p.with_type(short_string("message_type", v)?);
        }
        if let Some(v) = &self.app_id {
            p = p.with_app_id(short_string("app_id", v)?);
        }
        if let Some(v) = &self.user_id {
            p = p.with_user_id(short_string("user_id", v)?);
        }
        if !self.headers.is_empty() {
            let mut table = FieldTable::default();
            for (k, v) in &self.headers {
                table.insert(short_string("header name", k)?, v.to_amqp());
            }
            p = p.with_headers(table);
        }
        Ok(p)
    }
}

/// Converts to an AMQP short string (at most 255 bytes) without panicking.
pub(crate) fn short_string(field: &str, value: &str) -> Result<ShortString> {
    ShortString::try_new(value).map_err(|_| {
        AmqpError::InvalidMessage(format!(
            "{field} is {} bytes long, AMQP allows at most 255",
            value.len()
        ))
    })
}

/// Reads an integer header such as `x-delivery-count`.
pub(crate) fn integer_header(properties: &BasicProperties, key: &str) -> Option<i64> {
    let value = properties.headers().as_ref()?.inner().get(key)?;
    HeaderValue::from_amqp(value)?.as_i64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn properties_roundtrip() {
        let properties = MessageProperties::default()
            .with_content_type("application/json")
            .with_message_id("id-1")
            .with_correlation_id("corr-1")
            .with_reply_to("replies")
            .with_timestamp(1_700_000_000)
            .with_expiration(Duration::from_millis(1500))
            .with_priority(5)
            .with_message_type("order.created")
            .with_app_id("shop")
            .with_user_id("guest")
            .with_header("s", "text")
            .with_header("i", 42i64)
            .with_header("b", true)
            .with_header("f", 1.5)
            .with_header("x", vec![0xff, 0x00]);

        let amqp = properties.to_amqp(true).unwrap();
        assert_eq!(*amqp.delivery_mode(), Some(2));
        assert_eq!(MessageProperties::from_amqp(&amqp), properties);
    }

    #[test]
    fn incoming_headers_keep_their_type() {
        let mut table = FieldTable::default();
        table.insert("count".into(), AMQPValue::LongUInt(7));
        table.insert("flag".into(), AMQPValue::Boolean(true));
        table.insert(
            "binary".into(),
            AMQPValue::LongString(LongString::from(vec![0xff, 0xfe])),
        );
        table.insert(
            "nested".into(),
            AMQPValue::FieldTable(FieldTable::default()),
        );
        let amqp = BasicProperties::default().with_headers(table);

        let headers = MessageProperties::from_amqp(&amqp).headers;
        assert_eq!(headers.get("count"), Some(&HeaderValue::Int(7)));
        assert_eq!(headers.get("flag"), Some(&HeaderValue::Bool(true)));
        assert_eq!(
            headers.get("binary"),
            Some(&HeaderValue::Bytes(vec![0xff, 0xfe]))
        );
        assert!(!headers.contains_key("nested"));
        assert_eq!(integer_header(&amqp, "count"), Some(7));
    }

    #[test]
    fn too_long_property_is_an_error_not_a_panic() {
        let properties = MessageProperties::default().with_message_id("x".repeat(256));
        assert!(matches!(
            properties.to_amqp(false),
            Err(AmqpError::InvalidMessage(_))
        ));
    }
}
