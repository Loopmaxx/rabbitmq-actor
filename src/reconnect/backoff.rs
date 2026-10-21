use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::ReconnectConfig;

/// Exponential backoff with jitter. One attempt = one connection try to one URL.
pub(crate) struct Backoff {
    config: ReconnectConfig,
    attempt: u32,
    rng: u64,
}

impl Backoff {
    pub(crate) fn new(config: ReconnectConfig) -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0x9E37_79B9_7F4A_7C15);
        Self {
            config,
            attempt: 0,
            rng: seed | 1,
        }
    }

    /// Number of failed attempts since the last [`Backoff::reset`].
    pub(crate) fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Records a failed attempt and returns how long to wait before the next one,
    /// or `None` if `max_attempts` is reached.
    pub(crate) fn next_delay(&mut self) -> Option<Duration> {
        let base = self.base_delay(self.attempt);
        self.attempt = self.attempt.saturating_add(1);
        if self
            .config
            .max_attempts
            .is_some_and(|max| self.attempt >= max)
        {
            return None;
        }
        Some(self.apply_jitter(base))
    }

    fn base_delay(&self, attempt: u32) -> Duration {
        let initial = self.config.initial_delay().as_secs_f64();
        let max = self.config.max_delay().as_secs_f64();
        let delay = initial * self.config.multiplier.powi(attempt.min(64) as i32);
        Duration::from_secs_f64(delay.min(max))
    }

    fn apply_jitter(&mut self, delay: Duration) -> Duration {
        if self.config.jitter == 0.0 {
            return delay;
        }
        // xorshift64: good enough for jitter, avoids a dependency on `rand`.
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let unit = (self.rng >> 11) as f64 / (1u64 << 53) as f64; // [0, 1)
        let factor = 1.0 + self.config.jitter * (2.0 * unit - 1.0);
        delay.mul_f64(factor.max(0.0))
    }
}

/// Cycles through the configured URLs, starting with the last one that worked.
pub(crate) struct UrlRotation {
    urls: Vec<String>,
    next: usize,
    current: usize,
}

impl UrlRotation {
    pub(crate) fn new(urls: Vec<String>) -> Self {
        assert!(!urls.is_empty(), "validated by AmqpConfig::validate");
        Self {
            urls,
            next: 0,
            current: 0,
        }
    }

    pub(crate) fn next_url(&mut self) -> &str {
        self.current = self.next;
        self.next = (self.next + 1) % self.urls.len();
        &self.urls[self.current]
    }

    /// The URL returned last worked, try it first next time.
    pub(crate) fn mark_success(&mut self) {
        self.next = self.current;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_attempts: Option<u32>, jitter: f64) -> ReconnectConfig {
        ReconnectConfig {
            initial_delay_ms: 100,
            max_delay_ms: 1_000,
            multiplier: 2.0,
            jitter,
            max_attempts,
        }
    }

    #[test]
    fn grows_exponentially_and_is_capped() {
        let mut b = Backoff::new(config(None, 0.0));
        let delays: Vec<_> = (0..6)
            .map(|_| b.next_delay().unwrap().as_millis())
            .collect();
        assert_eq!(delays, [100, 200, 400, 800, 1_000, 1_000]);
        b.reset();
        assert_eq!(b.next_delay().unwrap().as_millis(), 100);
    }

    #[test]
    fn stops_after_max_attempts() {
        let mut b = Backoff::new(config(Some(3), 0.0));
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_none());
        assert_eq!(b.attempt(), 3);
    }

    #[test]
    fn jitter_stays_in_range() {
        let mut b = Backoff::new(config(None, 0.5));
        for _ in 0..100 {
            b.reset();
            let d = b.next_delay().unwrap().as_millis();
            assert!((50..=150).contains(&d), "{d}");
        }
    }

    #[test]
    fn rotation_prefers_last_working_url() {
        let mut r = UrlRotation::new(vec!["a".into(), "b".into(), "c".into()]);
        assert_eq!(r.next_url(), "a");
        assert_eq!(r.next_url(), "b");
        r.mark_success();
        assert_eq!(r.next_url(), "b");
        assert_eq!(r.next_url(), "c");
        assert_eq!(r.next_url(), "a");
    }
}
