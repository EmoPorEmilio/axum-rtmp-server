use std::time::Duration;
use crate::error::RtmpError;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub initial_interval: Duration,
    pub max_interval: Duration,
    pub max_attempts: u32,
    pub multiplier: f64,
}

impl RetryPolicy {
    pub fn from_config(
        initial_interval_ms: u64,
        max_interval_ms: u64,
        max_attempts: u32,
        multiplier: f64,
    ) -> Self {
        Self {
            initial_interval: Duration::from_millis(initial_interval_ms),
            max_interval: Duration::from_millis(max_interval_ms),
            max_attempts,
            multiplier,
        }
    }

    pub fn get_delay(&self, attempt: u32) -> Duration {
        if attempt >= self.max_attempts {
            return Duration::MAX;
        }

        let delay_ms = self.initial_interval.as_millis() as f64
            * self.multiplier.powi(attempt as i32);
        let delay_ms = delay_ms.min(self.max_interval.as_millis() as f64);
        Duration::from_millis(delay_ms as u64)
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
}
