use crate::data::LogEvent;
use crate::Number;
use crate::Shutdown;
use anyhow::Context;
use async_compat::Compat;
use influxdb::{Client, Timestamp, WriteQuery};
use log::{info, trace, warn};
#[cfg(test)]
use mockall::automock;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct InfluxConfig {
    url: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

impl InfluxConfig {
    pub fn new(
        url: String,
        database: String,
        user: Option<String>,
        password: Option<String>,
        token: Option<String>,
    ) -> Self {
        Self {
            url,
            database,
            user,
            password,
            token,
        }
    }
}

struct DefaultInfluxClient {
    client: Client,
}

impl DefaultInfluxClient {
    fn new(client: Client) -> Self {
        DefaultInfluxClient { client }
    }
}

#[cfg_attr(test, automock)]
trait InfluxClient: Sync + Send {
    fn write(&self, point: Vec<WriteQuery>) -> Result<String, influxdb::Error>;

    #[cfg(test)]
    fn wrapped(&self) -> &Client;
}

impl InfluxClient for DefaultInfluxClient {
    fn write(&self, query: Vec<WriteQuery>) -> Result<String, influxdb::Error> {
        futures::executor::block_on(Compat::new(async { self.client.query(query).await }))
    }

    #[cfg(test)]
    fn wrapped(&self) -> &Client {
        &self.client
    }
}

fn create_influxdb_client(influx_config: &InfluxConfig) -> anyhow::Result<Box<dyn InfluxClient>> {
    let mut influx_client = Client::new(influx_config.url.clone(), influx_config.database.clone());

    influx_client = if let Some(token) = influx_config.token.clone() {
        info!(
            "InfluxDB: {} {} set token",
            influx_config.url, influx_config.database
        );
        influx_client.with_token(token)
    } else if let (Some(user), Some(password)) =
        (influx_config.user.clone(), influx_config.password.clone())
    {
        info!(
            "InfluxDB: {} {} set username {} and password",
            influx_config.url, influx_config.database, user
        );
        influx_client.with_auth(user, password)
    } else {
        info!(
            "InfluxDB: {} {} no authentication",
            influx_config.url, influx_config.database
        );
        influx_client
    };

    Ok(Box::new(DefaultInfluxClient::new(influx_client)))
}

fn influxdb_writer(
    rx: Receiver<LogEvent>,
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
    shutdown: Shutdown,
) {
    let mut writer = Writer::new(
        influx_client,
        influx_config.clone(),
        Duration::from_secs(15),
    );

    loop {
        // Use shorter timeout to check shutdown flag more frequently
        match rx.recv_timeout(Duration::from_secs(1)) {
            // Process the event before reacting to a shutdown request so it is
            // not silently dropped.
            Ok(event) => {
                if let Some(query) = map_to_query(event) {
                    writer.queue(query);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => writer.flush_if_due(),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                writer.flush();
                warn!(
                    "InfluxDB: disconnected {} {}",
                    influx_config.url, influx_config.database,
                );
                break;
            }
        }

        // Check for shutdown request periodically
        if shutdown.is_requested() {
            writer.flush();
            info!(
                "InfluxDB: shutdown requested, exiting writer {} {}",
                influx_config.url, influx_config.database
            );
            break;
        }
    }

    info!(
        "InfluxDB: exiting writer {} {}",
        influx_config.url, influx_config.database
    );
}

/// Maximum write attempts (initial try + retries) before a batch is dropped.
const WRITE_MAX_ATTEMPTS: u32 = 3;
/// Base delay for exponential backoff between write attempts.
const WRITE_RETRY_BASE_DELAY: Duration = Duration::from_secs(1);

struct Writer {
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
    queries: Vec<WriteQuery>,
    accumulation_time: Duration,
    start: Instant,
    max_attempts: u32,
    retry_base_delay: Duration,
}

impl Writer {
    pub(crate) fn queue(&mut self, query: WriteQuery) {
        self.queries.push(query);

        trace!(
            "influx writer: # of points {} time {} (elapsed: {})",
            self.queries.len(),
            self.start.elapsed().as_millis(),
            self.start.elapsed() >= self.accumulation_time
        );
        if self.start.elapsed() >= self.accumulation_time {
            self.flush();
        }
    }

    /// Flush a pending batch once its accumulation window has elapsed, even
    /// when no further event arrives to trigger the flush. Without this, the
    /// last reading before a quiet period can sit unwritten indefinitely.
    fn flush_if_due(&mut self) {
        if !self.queries.is_empty() && self.start.elapsed() >= self.accumulation_time {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.queries.is_empty() {
            return;
        }

        let now = Instant::now();
        let queries = std::mem::take(&mut self.queries);
        let query_count = queries.len();

        let mut attempt = 0;
        loop {
            attempt += 1;
            trace!("before write to influx (attempt {})", attempt);
            match self.influx_client.write(queries.clone()) {
                Ok(_) => break,
                Err(error) => {
                    if attempt >= self.max_attempts {
                        let failures =
                            crate::metrics::increment(&crate::metrics::INFLUX_WRITE_FAILURES);
                        log::error!(
                            "#### Error writing to influx after {} attempt(s) \
                             ({} batch failure(s) so far): {} {}: {:?}",
                            attempt,
                            failures,
                            self.influx_config.url,
                            self.influx_config.database,
                            error
                        );
                        break;
                    }
                    let delay = self.retry_base_delay * 2u32.pow(attempt - 1);
                    warn!(
                        "InfluxDB: write failed (attempt {}/{}): {} {}: {:?}; retrying in {:?}",
                        attempt,
                        self.max_attempts,
                        self.influx_config.url,
                        self.influx_config.database,
                        error,
                        delay
                    );
                    thread::sleep(delay);
                }
            }
        }

        let duration = now.elapsed();
        info!(
            "InfluxDB: {} {} write #{} ({:.3} s)",
            self.influx_config.url,
            self.influx_config.database,
            query_count,
            duration.as_secs_f64()
        );
        self.start = now
    }
}

impl Writer {
    fn new(
        influx_client: Box<dyn InfluxClient>,
        influx_config: InfluxConfig,
        accumulation_time: Duration,
    ) -> Self {
        Self {
            influx_client,
            influx_config,
            queries: Vec::new(),
            start: Instant::now(),
            accumulation_time,
            max_attempts: WRITE_MAX_ATTEMPTS,
            retry_base_delay: WRITE_RETRY_BASE_DELAY,
        }
    }
}

pub fn spawn_influxdb_writer(
    influx_config: InfluxConfig,
    shutdown: Shutdown,
) -> anyhow::Result<(SyncSender<LogEvent>, JoinHandle<()>)> {
    let influx_client =
        create_influxdb_client(&influx_config).context("Failed to create InfluxDB client")?;

    Ok(spawn_writer(influx_client, influx_config, shutdown))
}

fn spawn_writer(
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
    shutdown: Shutdown,
) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!(
                "InfluxDB: starting writer {} {}",
                influx_config.url, influx_config.database
            );

            influxdb_writer(rx, influx_client, influx_config, shutdown);
        }),
    )
}

/// Log every invalid-timestamp event only up to this many, then every Nth.
const INVALID_TIMESTAMP_LOG_EVERY: u64 = 1000;

pub fn map_to_query(log_event: LogEvent) -> Option<WriteQuery> {
    // InfluxDB timestamps are unsigned; a negative or zero timestamp would wrap
    // to a nonsensical value, so drop the event instead.
    let timestamp = match u128::try_from(log_event.timestamp) {
        Ok(timestamp) if timestamp > 0 => timestamp,
        _ => {
            let count = crate::metrics::increment(&crate::metrics::INVALID_TIMESTAMP_EVENTS);
            if crate::metrics::should_log(count, INVALID_TIMESTAMP_LOG_EVERY) {
                warn!(
                    "InfluxDB: skipping '{}' with invalid timestamp {} (tags: {:?}, \
                     {} skipped so far)",
                    log_event.measurement, log_event.timestamp, log_event.tags, count
                );
            }
            return None;
        }
    };

    let mut write_query = WriteQuery::new(Timestamp::Seconds(timestamp), log_event.measurement);
    for (tag, value) in log_event.tags {
        write_query = write_query.add_tag(tag, value);
    }
    for (name, value) in log_event.fields {
        match value {
            Number::Int(value) => {
                write_query = write_query.add_field(name, value);
            }
            Number::UInt(value) => {
                write_query = write_query.add_field(name, value);
            }
            Number::Float(value) => {
                write_query = write_query.add_field(name, value);
            }
        }
    }
    Some(write_query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Number;
    use mockall::predicate::function;

    fn log_event() -> LogEvent {
        log_event_at(1_701_292_592)
    }

    fn log_event_at(timestamp: i64) -> LogEvent {
        LogEvent::new_value_from_ref(
            "test".to_string(),
            timestamp,
            vec![].into_iter().collect(),
            Number::Float(1.23),
        )
    }

    fn write_query() -> WriteQuery {
        map_to_query(log_event()).expect("valid event")
    }

    fn influx_config() -> InfluxConfig {
        InfluxConfig::new(
            "http://localhost:8086".to_string(),
            "test_db".to_string(),
            Some("user".to_string()),
            Some("password".to_string()),
            None,
        )
    }

    #[test]
    fn test_map_to_query_rejects_invalid_timestamps() {
        assert!(map_to_query(log_event_at(0)).is_none());
        assert!(map_to_query(log_event_at(-1)).is_none());
        assert!(map_to_query(log_event_at(i64::MIN)).is_none());
        assert!(map_to_query(log_event_at(1)).is_some());
    }

    #[test]
    fn test_influxdb_writer_retries_until_success() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let mut mock_client = Box::new(MockInfluxClient::new());
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        mock_client.expect_write().times(3).returning(move |_| {
            if attempts_clone.fetch_add(1, Ordering::SeqCst) < 2 {
                Err(influxdb::Error::ApiError(500))
            } else {
                Ok("ok".to_string())
            }
        });

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(0));
        writer.retry_base_delay = Duration::ZERO;

        writer.queue(write_query());

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_flush_if_due_writes_pending_batch() {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(1)
            .returning(|_| Ok("test_data".to_string()));

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(5));
        writer.queries.push(write_query());
        // Pretend the accumulation window has already elapsed.
        writer.start = Instant::now() - Duration::from_secs(10);

        writer.flush_if_due();
    }

    #[test]
    fn test_flush_if_due_waits_until_window_elapsed() {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client.expect_write().times(0);

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(60));
        writer.queries.push(write_query());

        writer.flush_if_due();
    }

    #[test]
    fn test_flush_if_due_is_noop_without_pending_queries() {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client.expect_write().times(0);

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(0));
        writer.start = Instant::now() - Duration::from_secs(10);

        writer.flush_if_due();
    }

    #[test]
    fn test_influxdb_writer_gives_up_after_max_attempts() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let mut mock_client = Box::new(MockInfluxClient::new());
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();
        mock_client
            .expect_write()
            .times(WRITE_MAX_ATTEMPTS as usize)
            .returning(move |_| {
                attempts_clone.fetch_add(1, Ordering::SeqCst);
                Err(influxdb::Error::ApiError(500))
            });

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(0));
        writer.retry_base_delay = Duration::ZERO;

        writer.queue(write_query());

        assert_eq!(attempts.load(Ordering::SeqCst), WRITE_MAX_ATTEMPTS);
    }

    #[test]
    fn test_influxdb_writer_internal() -> anyhow::Result<()> {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(1)
            .returning(|_| Ok("test_data".to_string()));

        // Run the `influxdb_writer` function
        let (tx, rx) = sync_channel(100);
        let join_handle = thread::spawn(move || {
            influxdb_writer(rx, mock_client, influx_config(), Shutdown::new());
        });

        // Send a test query
        tx.send(log_event())?;

        // Close the channel
        drop(tx);

        join_handle.join().expect("stopped writer");

        Ok(())
    }

    #[test]
    fn test_influxdb_writer_direct_write() -> anyhow::Result<()> {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(1)
            .with(function(|points: &Vec<WriteQuery>| points.len() == 1))
            .returning(|_| Ok("test_data".to_string()));

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(0));

        writer.queue(write_query());
        Ok(())
    }

    #[test]
    fn test_influxdb_writer_batch_write() -> anyhow::Result<()> {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(0)
            .returning(|_| Ok("test_data".to_string()));

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(5));

        writer.queue(write_query());
        Ok(())
    }

    #[test]
    fn test_influxdb_writer_forced_batch_write() -> anyhow::Result<()> {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(1)
            .with(function(|points: &Vec<WriteQuery>| points.len() == 1))
            .returning(|_| Ok("test_data".to_string()));

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(5));

        writer.queue(write_query());
        writer.flush();

        Ok(())
    }

    #[test]
    fn test_influxdb_writer_no_batch_write_on_empty_queue() -> anyhow::Result<()> {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client.expect_write().times(0);

        let mut writer = Writer::new(mock_client, influx_config(), Duration::from_secs(5));

        writer.flush();

        Ok(())
    }

    #[test]
    fn test_spawn_influxdb_writer_closing_without_sending_something() -> anyhow::Result<()> {
        let mock_client = Box::new(MockInfluxClient::new());

        let (tx, handle) = spawn_writer(mock_client, influx_config(), Shutdown::new());

        drop(tx);

        handle
            .join()
            .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))
    }

    #[test]
    fn test_spawn_influxdb_writer_closing_after_sending() -> anyhow::Result<()> {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(1)
            .with(function(|points: &Vec<WriteQuery>| points.len() == 1))
            .returning(|_| Ok("".to_string()));

        let (tx, handle) = spawn_writer(mock_client, influx_config(), Shutdown::new());

        tx.send(log_event())?;

        drop(tx);

        handle
            .join()
            .map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))
    }

    #[test]
    fn test_create_influxdb_client() {
        let config = influx_config();

        let result = create_influxdb_client(&config);

        assert!(result.is_ok());
        let wrapper = result.unwrap();
        let client = wrapper.wrapped();
        assert_eq!(client.database_name(), "test_db");
        assert_eq!(client.database_url(), "http://localhost:8086");
    }
}
