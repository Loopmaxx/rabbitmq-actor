use std::time::Duration;

use serde::Deserialize;

/// Exponential backoff between connection attempts:
/// `min(initial_delay_ms * multiplier^attempt, max_delay_ms)`, plus/minus `jitter`.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct ReconnectConfig {
    /// Delay before the first retry. Default 500 ms.
    pub initial_delay_ms: u64,
    /// Upper limit for the delay. Default 30 s.
    pub max_delay_ms: u64,
    /// Growth factor per attempt, at least 1.0. Default 2.0.
    pub multiplier: f64,
    /// Relative jitter between 0.0 and 1.0, e.g. 0.2 = +/-20 %.
    pub jitter: f64,
    /// `None` retries forever.
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_delay_ms: 500,
            max_delay_ms: 30_000,
            multiplier: 2.0,
            jitter: 0.2,
            max_attempts: None,
        }
    }
}

impl ReconnectConfig {
    /// `initial_delay_ms` as [`Duration`].
    pub fn initial_delay(&self) -> Duration {
        Duration::from_millis(self.initial_delay_ms)
    }

    /// `max_delay_ms` as [`Duration`].
    pub fn max_delay(&self) -> Duration {
        Duration::from_millis(self.max_delay_ms)
    }
}
