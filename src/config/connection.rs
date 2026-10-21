use std::time::Duration;

use serde::Deserialize;

/// Broker URLs and connection settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ConnectionConfig {
    /// Broker URLs, tried in order. After a connection loss the last working URL is tried first.
    pub urls: Vec<String>,
    /// Name shown in the RabbitMQ management UI.
    #[serde(default)]
    pub connection_name: Option<String>,
    /// Interval in which the connection status is checked in addition to lapin's error events.
    #[serde(default = "default_health_check_interval_ms")]
    pub health_check_interval_ms: u64,
    /// Upper limit for one connection attempt (TCP, TLS and AMQP handshake).
    #[serde(default = "default_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// Upper limit for a single broker operation: opening a channel, declaring an
    /// exchange, queue or binding, starting or cancelling a consumer.
    #[serde(default = "default_timeout_ms")]
    pub operation_timeout_ms: u64,
    /// Heartbeat interval requested from the broker. A silent connection loss is
    /// noticed after about twice this interval. `null` uses the broker's default
    /// (60 s on RabbitMQ). A `heartbeat` parameter in the URL takes precedence.
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_secs: Option<u16>,
}

impl ConnectionConfig {
    /// Connection to the given URLs with default settings.
    pub fn new(urls: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            urls: urls.into_iter().map(Into::into).collect(),
            connection_name: None,
            health_check_interval_ms: default_health_check_interval_ms(),
            connect_timeout_ms: default_timeout_ms(),
            operation_timeout_ms: default_timeout_ms(),
            heartbeat_secs: default_heartbeat_secs(),
        }
    }

    /// `health_check_interval_ms` as [`Duration`].
    pub fn health_check_interval(&self) -> Duration {
        Duration::from_millis(self.health_check_interval_ms)
    }

    /// `connect_timeout_ms` as [`Duration`].
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }

    /// `operation_timeout_ms` as [`Duration`].
    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }
}

fn default_health_check_interval_ms() -> u64 {
    5_000
}

fn default_timeout_ms() -> u64 {
    10_000
}

fn default_heartbeat_secs() -> Option<u16> {
    Some(10)
}
