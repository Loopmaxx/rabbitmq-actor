use std::collections::BTreeMap;

use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use serde::Deserialize;

use crate::{AmqpError, BindingConfig, Result};

/// A queue that is declared on every (re)connect, with its bindings and consumer settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct QueueConfig {
    /// Queue name. Server-named queues are not supported.
    pub name: String,
    /// `quorum` by default.
    #[serde(default)]
    pub queue_type: QueueType,
    /// Quorum queues must be durable.
    #[serde(default = "default_true")]
    pub durable: bool,
    /// Classic queues only.
    #[serde(default)]
    pub exclusive: bool,
    /// Classic queues only.
    #[serde(default)]
    pub auto_delete: bool,
    /// Bindings declared together with the queue.
    #[serde(default)]
    pub bindings: Vec<BindingConfig>,
    /// Quorum queue arguments.
    #[serde(default)]
    pub quorum: QuorumOptions,
    /// Consumer settings, used once a handler is registered.
    #[serde(default)]
    pub consumer: ConsumerConfig,
    /// Additional `x-*` arguments that have no dedicated field.
    /// Strings, booleans and integers are supported.
    #[serde(default)]
    pub arguments: BTreeMap<String, serde_json::Value>,
}

/// Queue type (`x-queue-type`).
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum QueueType {
    /// Classic queue.
    Classic,
    /// Replicated quorum queue. The default.
    #[default]
    Quorum,
}

/// Arguments that only apply to quorum queues.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct QuorumOptions {
    /// `x-delivery-limit`: after this many redeliveries a message is dropped or dead-lettered.
    pub delivery_limit: Option<i64>,
    /// `x-dead-letter-exchange`
    pub dead_letter_exchange: Option<String>,
    /// `x-dead-letter-routing-key`
    pub dead_letter_routing_key: Option<String>,
    /// `x-dead-letter-strategy`
    pub dead_letter_strategy: Option<DeadLetterStrategy>,
    /// `x-overflow`
    pub overflow: Option<Overflow>,
    /// `x-quorum-initial-group-size`
    pub initial_group_size: Option<i64>,
    /// `x-max-length`
    pub max_length: Option<i64>,
    /// `x-max-length-bytes`
    pub max_length_bytes: Option<i64>,
}

/// Dead-letter strategy of a quorum queue (`x-dead-letter-strategy`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum DeadLetterStrategy {
    /// `at-most-once`: messages may be lost while dead-lettering.
    AtMostOnce,
    /// `at-least-once`: requires `overflow: "reject-publish"`.
    AtLeastOnce,
}

/// What happens when a queue reaches `max_length` or `max_length_bytes` (`x-overflow`).
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum Overflow {
    /// Drop the oldest messages.
    DropHead,
    /// Reject new publishes (negative confirm).
    RejectPublish,
}

/// Consumer settings of a queue.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct ConsumerConfig {
    /// Maximum number of unacknowledged messages per consumer (`basic.qos`, never global).
    pub prefetch_count: u16,
    /// Defaults to `<queue>-consumer`.
    pub consumer_tag: Option<String>,
    /// A message whose handler returned `Requeue` this many times is rejected (and
    /// dead-lettered if configured) instead. Uses `x-acquired-count`, so it only works
    /// on quorum queues. `null` disables the limit.
    pub max_requeues: Option<u32>,
    /// A handler that runs longer is cancelled and the message is requeued (which
    /// counts towards `max_requeues`). `null` (the default) waits forever.
    pub handler_timeout_ms: Option<u64>,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            prefetch_count: 10,
            consumer_tag: None,
            max_requeues: Some(20),
            handler_timeout_ms: None,
        }
    }
}

impl QueueConfig {
    /// A durable quorum queue without bindings and with default settings.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            queue_type: QueueType::default(),
            durable: true,
            exclusive: false,
            auto_delete: false,
            bindings: Vec::new(),
            quorum: QuorumOptions::default(),
            consumer: ConsumerConfig::default(),
            arguments: BTreeMap::new(),
        }
    }

    /// Adds a binding.
    pub fn with_binding(mut self, binding: BindingConfig) -> Self {
        self.bindings.push(binding);
        self
    }

    /// The configured consumer tag or `<queue>-consumer`.
    pub fn consumer_tag(&self) -> String {
        self.consumer
            .consumer_tag
            .clone()
            .unwrap_or_else(|| format!("{}-consumer", self.name))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let invalid = |msg: &str| {
            Err(AmqpError::InvalidConfig(format!(
                "queue '{}': {msg}",
                self.name
            )))
        };

        if self.name.is_empty() {
            return Err(AmqpError::InvalidConfig(
                "queue name must not be empty (server-named queues are not supported)".into(),
            ));
        }
        if self.consumer.prefetch_count == 0 {
            return invalid("prefetch_count must be greater than 0");
        }
        for binding in &self.bindings {
            if binding.routing_keys.is_empty() {
                return invalid(&format!(
                    "binding to exchange '{}' needs at least one routing key",
                    binding.exchange
                ));
            }
        }
        for (key, value) in &self.arguments {
            json_to_amqp(value).ok_or_else(|| {
                AmqpError::InvalidConfig(format!(
                    "queue '{}': argument '{key}' must be a string, boolean or integer",
                    self.name
                ))
            })?;
        }

        match self.queue_type {
            QueueType::Quorum => {
                if !self.durable {
                    return invalid("quorum queues must be durable");
                }
                if self.exclusive {
                    return invalid("quorum queues cannot be exclusive");
                }
                if self.auto_delete {
                    return invalid("quorum queues cannot be auto-delete");
                }
                let q = &self.quorum;
                if q.dead_letter_strategy == Some(DeadLetterStrategy::AtLeastOnce)
                    && q.overflow != Some(Overflow::RejectPublish)
                {
                    return invalid(
                        "dead_letter_strategy 'at-least-once' requires overflow 'reject-publish'",
                    );
                }
                if q.dead_letter_strategy.is_some() && q.dead_letter_exchange.is_none() {
                    return invalid("dead_letter_strategy requires dead_letter_exchange");
                }
                if q.delivery_limit.is_some_and(|l| l < 0) {
                    return invalid("delivery_limit must not be negative");
                }
            }
            QueueType::Classic => {
                if self.quorum.delivery_limit.is_some()
                    || self.quorum.dead_letter_strategy.is_some()
                    || self.quorum.initial_group_size.is_some()
                {
                    return invalid(
                        "delivery_limit, dead_letter_strategy and initial_group_size only apply to quorum queues",
                    );
                }
            }
        }
        Ok(())
    }

    /// Arguments for `queue.declare`.
    pub(crate) fn declare_arguments(&self) -> FieldTable {
        let mut args = FieldTable::default();
        let mut put = |key: &str, value: AMQPValue| {
            args.insert(ShortString::from(key), value);
        };
        let string = |s: &str| AMQPValue::LongString(LongString::from(s));

        if self.queue_type == QueueType::Quorum {
            put("x-queue-type", string("quorum"));
        }
        let q = &self.quorum;
        if let Some(v) = q.delivery_limit {
            put("x-delivery-limit", AMQPValue::LongLongInt(v));
        }
        if let Some(v) = &q.dead_letter_exchange {
            put("x-dead-letter-exchange", string(v));
        }
        if let Some(v) = &q.dead_letter_routing_key {
            put("x-dead-letter-routing-key", string(v));
        }
        if let Some(v) = q.dead_letter_strategy {
            let v = match v {
                DeadLetterStrategy::AtMostOnce => "at-most-once",
                DeadLetterStrategy::AtLeastOnce => "at-least-once",
            };
            put("x-dead-letter-strategy", string(v));
        }
        if let Some(v) = q.overflow {
            let v = match v {
                Overflow::DropHead => "drop-head",
                Overflow::RejectPublish => "reject-publish",
            };
            put("x-overflow", string(v));
        }
        if let Some(v) = q.initial_group_size {
            put("x-quorum-initial-group-size", AMQPValue::LongLongInt(v));
        }
        if let Some(v) = q.max_length {
            put("x-max-length", AMQPValue::LongLongInt(v));
        }
        if let Some(v) = q.max_length_bytes {
            put("x-max-length-bytes", AMQPValue::LongLongInt(v));
        }
        for (key, value) in &self.arguments {
            if let Some(value) = json_to_amqp(value) {
                put(key, value);
            }
        }
        args
    }
}

fn json_to_amqp(value: &serde_json::Value) -> Option<AMQPValue> {
    match value {
        serde_json::Value::String(s) => Some(AMQPValue::LongString(LongString::from(s.as_str()))),
        serde_json::Value::Bool(b) => Some(AMQPValue::Boolean(*b)),
        serde_json::Value::Number(n) => n.as_i64().map(AMQPValue::LongLongInt),
        _ => None,
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(json: &str) -> QueueConfig {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn quorum_is_default_and_sets_queue_type() {
        let q = queue(r#"{ "name": "orders", "quorum": { "delivery_limit": 5 } }"#);
        assert_eq!(q.queue_type, QueueType::Quorum);
        q.validate().unwrap();
        let args = q.declare_arguments();
        let args = args.inner();
        assert_eq!(
            args.get("x-queue-type"),
            Some(&AMQPValue::LongString("quorum".into()))
        );
        assert_eq!(
            args.get("x-delivery-limit"),
            Some(&AMQPValue::LongLongInt(5))
        );
    }

    #[test]
    fn quorum_rejects_non_durable_and_exclusive() {
        assert!(
            queue(r#"{ "name": "q", "durable": false }"#)
                .validate()
                .is_err()
        );
        assert!(
            queue(r#"{ "name": "q", "exclusive": true }"#)
                .validate()
                .is_err()
        );
        assert!(
            queue(r#"{ "name": "q", "auto_delete": true }"#)
                .validate()
                .is_err()
        );
    }

    #[test]
    fn at_least_once_requires_reject_publish() {
        let q = queue(
            r#"{ "name": "q", "quorum": { "dead_letter_exchange": "dlx", "dead_letter_strategy": "at-least-once" } }"#,
        );
        assert!(q.validate().is_err());
        let q = queue(
            r#"{ "name": "q", "quorum": { "dead_letter_exchange": "dlx", "dead_letter_strategy": "at-least-once", "overflow": "reject-publish" } }"#,
        );
        q.validate().unwrap();
    }

    #[test]
    fn classic_queue_has_no_queue_type_argument() {
        let q = queue(r#"{ "name": "q", "queue_type": "classic", "exclusive": true }"#);
        q.validate().unwrap();
        assert!(q.declare_arguments().inner().get("x-queue-type").is_none());
    }

    #[test]
    fn classic_rejects_quorum_only_options() {
        let q =
            queue(r#"{ "name": "q", "queue_type": "classic", "quorum": { "delivery_limit": 3 } }"#);
        assert!(q.validate().is_err());
    }

    #[test]
    fn rejects_unsupported_argument_types() {
        let q = queue(r#"{ "name": "q", "arguments": { "x-foo": [1, 2] } }"#);
        assert!(q.validate().is_err());
    }
}
