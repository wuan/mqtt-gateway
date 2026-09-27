use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative number of events dropped because a target channel was full or
/// disconnected.
pub(crate) static EVENTS_DROPPED: AtomicU64 = AtomicU64::new(0);
/// Cumulative number of events skipped because of an invalid timestamp.
pub(crate) static INVALID_TIMESTAMP_EVENTS: AtomicU64 = AtomicU64::new(0);
/// Cumulative number of failed InfluxDB batch writes (after retries).
pub(crate) static INFLUX_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Cumulative number of failed PostgreSQL batch writes (after retries).
pub(crate) static POSTGRES_WRITE_FAILURES: AtomicU64 = AtomicU64::new(0);
/// Cumulative number of successful MQTT reconnections.
pub(crate) static MQTT_RECONNECTS: AtomicU64 = AtomicU64::new(0);

/// Increment a counter and return its new value.
pub(crate) fn increment(counter: &AtomicU64) -> u64 {
    counter.fetch_add(1, Ordering::Relaxed) + 1
}

/// Total count for a counter.
pub(crate) fn total(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

/// Returns `true` for the first occurrence and then every `every` occurrences,
/// so repetitive warnings are throttled instead of spamming the log.
pub(crate) fn should_log(count: u64, every: u64) -> bool {
    every == 0 || count == 1 || count.is_multiple_of(every)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_log_first_and_periodically() {
        assert!(should_log(1, 100));
        assert!(!should_log(2, 100));
        assert!(should_log(100, 100));
        assert!(should_log(200, 100));
    }

    #[test]
    fn test_should_log_with_zero_interval_always_logs() {
        assert!(should_log(2, 0));
    }

    #[test]
    fn test_increment_and_total() {
        let counter = AtomicU64::new(0);
        assert_eq!(increment(&counter), 1);
        assert_eq!(increment(&counter), 2);
        assert_eq!(total(&counter), 2);
    }
}
