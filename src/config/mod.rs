mod connection;
mod exchange;
mod queue;
mod reconnect;

pub use connection::*;
pub use exchange::*;
pub use queue::*;
pub use reconnect::*;

use std::{collections::HashSet, path::Path, time::Duration};

use serde::Deserialize;

use crate::{AmqpError, Result};

/// Root configuration, loaded from JSON.
///
/// See `examples/config.json` for a complete example.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AmqpConfig {
    /// Broker URLs and connection settings.
    pub connection: ConnectionConfig,
    /// Backoff between connection attempts.
    #[serde(default)]
    pub reconnect: ReconnectConfig,
    /// Publisher settings.
    #[serde(default)]
    pub publisher: PublisherConfig,
    /// Exchanges declared on every (re)connect.
    #[serde(default)]
    pub exchanges: Vec<ExchangeConfig>,
    /// Queues declared on every (re)connect.
    #[serde(default)]
    pub queues: Vec<QueueConfig>,
    /// Mailbox size of the internal actors.
    #[serde(default = "default_mailbox_capacity")]
    pub mailbox_capacity: usize,
    /// How long [`crate::AmqpClient::shutdown`] waits for running handlers and
    /// outstanding publisher confirms before it closes the connection anyway.
    /// Handlers still running then are cancelled and their messages redelivered.
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
}

/// Publisher settings.
///
/// Like all configuration structs it is `#[non_exhaustive]`, so new fields are not a
/// breaking change. Start from [`Default`] and set fields instead of using a literal:
///
/// ```compile_fail
/// let config = amqp_actor_lib::PublisherConfig {
///     confirm_timeout_ms: 1_000,
///     persistent: true,
///     mandatory: false,
/// };
/// ```
///
/// ```
/// let mut config = amqp_actor_lib::PublisherConfig::default();
/// config.confirm_timeout_ms = 1_000;
/// ```
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct PublisherConfig {
    /// How long `publish` waits for the broker's confirm.
    pub confirm_timeout_ms: u64,
    /// Sets `delivery_mode = 2` so messages survive a broker restart.
    pub persistent: bool,
    /// Sets the `mandatory` flag; unroutable messages are then returned by the broker.
    pub mandatory: bool,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            confirm_timeout_ms: 5_000,
            persistent: true,
            mandatory: false,
        }
    }
}

impl PublisherConfig {
    /// `confirm_timeout_ms` as [`Duration`].
    pub fn confirm_timeout(&self) -> Duration {
        Duration::from_millis(self.confirm_timeout_ms)
    }
}

fn default_mailbox_capacity() -> usize {
    1_024
}

fn default_shutdown_timeout_ms() -> u64 {
    10_000
}

impl AmqpConfig {
    /// Configuration for the given broker URLs with all defaults and no topology.
    /// Call [`AmqpConfig::validate`] (or let [`crate::AmqpClient::start`] do it) after
    /// changing fields.
    pub fn new(urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            connection: ConnectionConfig::new(urls),
            reconnect: ReconnectConfig::default(),
            publisher: PublisherConfig::default(),
            exchanges: Vec::new(),
            queues: Vec::new(),
            mailbox_capacity: default_mailbox_capacity(),
            shutdown_timeout_ms: default_shutdown_timeout_ms(),
        }
    }

    /// `shutdown_timeout_ms` as [`Duration`].
    pub fn shutdown_timeout(&self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms)
    }

    /// Adds an exchange.
    pub fn with_exchange(mut self, exchange: ExchangeConfig) -> Self {
        self.exchanges.push(exchange);
        self
    }

    /// Adds a queue.
    pub fn with_queue(mut self, queue: QueueConfig) -> Self {
        self.queues.push(queue);
        self
    }

    /// Parses and validates a JSON configuration.
    pub fn from_json_str(json: &str) -> Result<Self> {
        let config: Self = serde_json::from_str(json)
            .map_err(|e| AmqpError::InvalidConfig(format!("invalid JSON: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    /// Reads, parses and validates a JSON configuration file.
    pub fn from_json_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let json = std::fs::read_to_string(path).map_err(|e| {
            AmqpError::InvalidConfig(format!("cannot read {}: {e}", path.display()))
        })?;
        Self::from_json_str(&json)
    }

    /// Checks the configuration. Called by [`crate::AmqpClient::start`] as well.
    pub fn validate(&self) -> Result<()> {
        let invalid = |msg: String| Err(AmqpError::InvalidConfig(msg));

        if self.connection.urls.is_empty() {
            return invalid("connection.urls must contain at least one URL".into());
        }
        if self.connection.health_check_interval_ms == 0 {
            return invalid("connection.health_check_interval_ms must be greater than 0".into());
        }
        if self.connection.connect_timeout_ms == 0 || self.connection.operation_timeout_ms == 0 {
            return invalid(
                "connection.connect_timeout_ms and operation_timeout_ms must be greater than 0"
                    .into(),
            );
        }
        if self.connection.heartbeat_secs == Some(0) {
            return invalid(
                "connection.heartbeat_secs must be greater than 0 (or null for the broker default)"
                    .into(),
            );
        }
        for url in &self.connection.urls {
            if let Err(e) = url.parse::<lapin::uri::AMQPUri>() {
                return invalid(format!(
                    "connection.urls: '{}' is not a valid AMQP URL: {e}",
                    crate::error::redact_url(url)
                ));
            }
        }
        if let Some(name) = &self.connection.connection_name {
            check_short("connection.connection_name", name)?;
        }
        if self.mailbox_capacity == 0 {
            return invalid("mailbox_capacity must be greater than 0".into());
        }

        let r = &self.reconnect;
        if r.initial_delay_ms == 0 || r.max_delay_ms < r.initial_delay_ms {
            return invalid("reconnect: 0 < initial_delay_ms <= max_delay_ms is required".into());
        }
        if r.multiplier < 1.0 || !(0.0..=1.0).contains(&r.jitter) {
            return invalid(
                "reconnect: multiplier >= 1.0 and 0.0 <= jitter <= 1.0 are required".into(),
            );
        }
        if r.max_attempts == Some(0) {
            return invalid(
                "reconnect.max_attempts must be greater than 0 (or null for unlimited)".into(),
            );
        }
        if self.shutdown_timeout_ms == 0 {
            return invalid("shutdown_timeout_ms must be greater than 0".into());
        }
        if self.publisher.confirm_timeout_ms == 0 {
            return invalid("publisher.confirm_timeout_ms must be greater than 0".into());
        }

        let mut exchanges = HashSet::new();
        for exchange in &self.exchanges {
            if exchange.name.is_empty() || exchange.name.starts_with("amq.") {
                return invalid(format!(
                    "exchange '{}': the default and 'amq.*' exchanges cannot be declared",
                    exchange.name
                ));
            }
            check_short("exchange name", &exchange.name)?;
            if !exchanges.insert(exchange.name.as_str()) {
                return invalid(format!("exchange '{}' is configured twice", exchange.name));
            }
        }

        let mut queues = HashSet::new();
        for queue in &self.queues {
            queue.validate()?;
            short_names(queue)?;
            if queue.consumer.handler_timeout_ms == Some(0) {
                return invalid(format!(
                    "queue '{}': consumer.handler_timeout_ms must be greater than 0 (or null)",
                    queue.name
                ));
            }
            if !queues.insert(queue.name.as_str()) {
                return invalid(format!("queue '{}' is configured twice", queue.name));
            }
        }
        Ok(())
    }

    /// The queue with the given name, if configured.
    pub fn queue(&self, name: &str) -> Option<&QueueConfig> {
        self.queues.iter().find(|q| q.name == name)
    }
}

/// AMQP short strings are limited to 255 bytes; lapin panics on longer ones, so all
/// names that end up in a short string are checked here.
fn check_short(what: &str, value: &str) -> Result<()> {
    if value.len() > 255 {
        return Err(AmqpError::InvalidConfig(format!(
            "{what} '{}...' is longer than 255 bytes",
            value.chars().take(32).collect::<String>()
        )));
    }
    Ok(())
}

fn short_names(queue: &QueueConfig) -> Result<()> {
    check_short("queue name", &queue.name)?;
    check_short("consumer tag", &queue.consumer_tag())?;
    for key in queue.arguments.keys() {
        check_short("queue argument", key)?;
    }
    for binding in &queue.bindings {
        check_short("binding exchange", &binding.exchange)?;
        for key in &binding.routing_keys {
            check_short("routing key", key)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_is_valid() {
        let config = AmqpConfig::from_json_str(include_str!("../../examples/config.json")).unwrap();
        assert_eq!(config.queues.len(), 2);
    }

    #[test]
    fn default_config_file_matches_defaults() {
        let config =
            AmqpConfig::from_json_str(include_str!("../../examples/default-config.json")).unwrap();
        let queue = &config.queues[0];
        assert_eq!(queue.queue_type, QueueType::Quorum);
        assert_eq!(
            queue.consumer.prefetch_count,
            ConsumerConfig::default().prefetch_count
        );
        assert_eq!(
            queue.consumer.max_requeues,
            ConsumerConfig::default().max_requeues
        );
        assert_eq!(
            config.reconnect.max_delay_ms,
            ReconnectConfig::default().max_delay_ms
        );
        assert_eq!(
            config.publisher.confirm_timeout_ms,
            PublisherConfig::default().confirm_timeout_ms
        );
        let defaults = AmqpConfig::new(["amqp://x"]);
        assert_eq!(config.shutdown_timeout_ms, defaults.shutdown_timeout_ms);
        assert_eq!(
            config.connection.connect_timeout_ms,
            defaults.connection.connect_timeout_ms
        );
        assert_eq!(
            config.connection.operation_timeout_ms,
            defaults.connection.operation_timeout_ms
        );
        assert_eq!(
            config.connection.heartbeat_secs,
            defaults.connection.heartbeat_secs
        );
        assert_eq!(
            queue.consumer.handler_timeout_ms,
            ConsumerConfig::default().handler_timeout_ms
        );
    }

    #[test]
    fn minimal_config_uses_defaults() {
        let config =
            AmqpConfig::from_json_str(r#"{ "connection": { "urls": ["amqp://localhost"] } }"#)
                .unwrap();
        assert_eq!(config.reconnect.initial_delay_ms, 500);
        assert!(config.publisher.persistent);
    }

    #[test]
    fn rejects_empty_urls_and_unknown_fields() {
        assert!(AmqpConfig::from_json_str(r#"{ "connection": { "urls": [] } }"#).is_err());
        assert!(
            AmqpConfig::from_json_str(r#"{ "connection": { "urls": ["amqp://x"] }, "queu": [] }"#)
                .is_err()
        );
    }

    #[test]
    fn rejects_names_longer_than_255_bytes() {
        let long = "q".repeat(256);
        let config = AmqpConfig::new(["amqp://x"]).with_queue(QueueConfig::new(long.as_str()));
        assert!(matches!(
            config.validate(),
            Err(AmqpError::InvalidConfig(_))
        ));

        let config = AmqpConfig::new(["amqp://x"]).with_queue(
            QueueConfig::new("q").with_binding(BindingConfig::new("ex", [long.as_str()])),
        );
        assert!(matches!(
            config.validate(),
            Err(AmqpError::InvalidConfig(_))
        ));
    }

    #[test]
    fn builder_matches_minimal_json() {
        let config = AmqpConfig::new(["amqp://localhost"])
            .with_exchange(ExchangeConfig::new("orders", ExchangeType::Topic))
            .with_queue(
                QueueConfig::new("orders.created")
                    .with_binding(BindingConfig::new("orders", ["order.created"])),
            );
        config.validate().unwrap();
        let json = AmqpConfig::from_json_str(
            r#"{ "connection": { "urls": ["amqp://localhost"] },
                 "exchanges": [ { "name": "orders" } ],
                 "queues": [ { "name": "orders.created",
                               "bindings": [ { "exchange": "orders", "routing_keys": ["order.created"] } ] } ] }"#,
        )
        .unwrap();
        assert_eq!(format!("{config:?}"), format!("{json:?}"));
    }

    #[test]
    fn rejects_invalid_urls_and_zero_timeouts() {
        assert!(AmqpConfig::from_json_str(r#"{ "connection": { "urls": ["no url"] } }"#).is_err());
        let mut config = AmqpConfig::new(["amqp://localhost"]);
        config.connection.connect_timeout_ms = 0;
        assert!(config.validate().is_err());
        let mut config = AmqpConfig::new(["amqp://localhost"]);
        config.connection.heartbeat_secs = Some(0);
        assert!(config.validate().is_err());
        let mut config = AmqpConfig::new(["amqp://localhost"]);
        config.connection.heartbeat_secs = None;
        config.validate().unwrap();
    }

    #[test]
    fn rejects_duplicate_queues() {
        let json = r#"{ "connection": { "urls": ["amqp://x"] }, "queues": [ { "name": "a" }, { "name": "a" } ] }"#;
        assert!(AmqpConfig::from_json_str(json).is_err());
    }
}
