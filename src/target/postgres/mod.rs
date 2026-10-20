use crate::data::LogEvent;
use crate::Number;
use crate::Shutdown;
use log::{error, info, warn};
#[cfg(test)]
use mockall::automock;
use postgres::types::ToSql;
use postgres::Client;
use postgres::NoTls;
use std::collections::HashMap;
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How long to wait for a message before re-checking shutdown/accumulation.
const POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Maximum number of rows collected before a batch is written.
const BATCH_MAX_SIZE: usize = 100;
/// Maximum time rows may sit in a batch before being written.
const BATCH_MAX_DELAY: Duration = Duration::from_secs(1);

/// Factory used to re-establish a lost Postgres connection.
pub(crate) type ReconnectClient = Box<dyn Fn() -> anyhow::Result<Box<dyn PostgresClient>> + Send>;

#[derive(Clone)]
pub struct PostgresConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    database: String,
    tls: bool,
}

impl PostgresConfig {
    pub(crate) fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        database: String,
        tls: bool,
    ) -> Self {
        Self {
            host,
            port,
            username,
            password,
            database,
            tls,
        }
    }
}

/// Why a database write failed, from the writer's point of view.
#[derive(Debug)]
pub(crate) enum WriteError {
    /// The connection is unusable; reconnecting may let the write succeed.
    Connection(String),
    /// The server rejected the statement; reconnecting cannot help.
    Statement(String),
}

impl std::fmt::Display for WriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteError::Connection(message) => write!(formatter, "connection error: {message}"),
            WriteError::Statement(message) => write!(formatter, "statement error: {message}"),
        }
    }
}

/// Classify a failed write from the connection state and the server's SQLSTATE.
///
/// A closed connection, a failure without a SQLSTATE (transport or encoding),
/// or a connection-exception (`08`) or shutdown (`57P0x`) SQLSTATE leaves the
/// connection unusable. Every other server error is a statement failure that
/// reconnecting cannot fix.
fn classify(is_closed: bool, sqlstate: Option<&str>, message: String) -> WriteError {
    let connection_lost =
        is_closed || sqlstate.is_none_or(|code| code.starts_with("08") || code.starts_with("57P0"));
    if connection_lost {
        WriteError::Connection(message)
    } else {
        WriteError::Statement(message)
    }
}

#[cfg_attr(test, automock)]
pub trait PostgresClient: Send {
    fn execute<'a>(
        &mut self,
        query: &str,
        params: &'a [&'a (dyn ToSql + Sync)],
    ) -> Result<u64, WriteError>;
}

struct DefaultPostgresClient {
    client: Client,
}

impl DefaultPostgresClient {
    fn new(client: Client) -> Self {
        DefaultPostgresClient { client }
    }
}

impl DefaultPostgresClient {}

impl PostgresClient for DefaultPostgresClient {
    fn execute(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, WriteError> {
        self.client.execute(query, params).map_err(|error| {
            classify(
                error.is_closed(),
                error.code().map(|code| code.code()),
                error.to_string(),
            )
        })
    }
}
/// Only allow plain identifiers as table names. The measurement is derived from
/// MQTT topic segments, so anything else would allow SQL injection through the
/// quoted identifier.
fn sanitize_measurement(name: &str) -> Option<&str> {
    if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Some(name)
    } else {
        None
    }
}

/// A single row ready to be inserted, extracted from a [`LogEvent`].
struct Row {
    measurement: String,
    time: i64,
    location: String,
    sensor: String,
    value: f64,
}

/// Extract a PostgreSQL row from an event, or `None` when the event does not
/// carry the required schema (measurement, value, location, sensor).
fn row_from_event(event: &LogEvent) -> Option<Row> {
    let measurement = match sanitize_measurement(&event.measurement) {
        Some(measurement) => measurement.to_string(),
        None => {
            warn!(
                "PostgreSQL: skipping event with invalid measurement name '{}'",
                event.measurement
            );
            return None;
        }
    };

    let value = match event.fields.get("value") {
        Some(Number::Int(value)) => *value as f64,
        Some(Number::Float(value)) => *value,
        None => {
            warn!(
                "PostgreSQL: skipping '{}' event without a 'value' field",
                measurement
            );
            return None;
        }
    };

    let location = match event.tags.get("location") {
        Some(location) => location.clone(),
        None => {
            warn!(
                "PostgreSQL: skipping '{}' event without a 'location' tag",
                measurement
            );
            return None;
        }
    };

    let sensor = match event.tags.get("sensor") {
        Some(sensor) => sensor.clone(),
        None => {
            warn!(
                "PostgreSQL: skipping '{}' event without a 'sensor' tag",
                measurement
            );
            return None;
        }
    };

    Some(Row {
        measurement,
        time: event.timestamp,
        location,
        sensor,
        value,
    })
}

/// Build a multi-row `INSERT` statement with `count` parameter tuples.
fn insert_statement(measurement: &str, count: usize) -> String {
    let mut statement = format!(
        "insert into \"{}\" (time, location, sensor, value) values",
        measurement
    );
    for index in 0..count {
        if index > 0 {
            statement.push(',');
        }
        let base = index * 4;
        statement.push_str(&format!(
            " (${}, ${}, ${}, ${})",
            base + 1,
            base + 2,
            base + 3,
            base + 4
        ));
    }
    statement.push(';');
    statement
}

struct PostgresWriter {
    client: Box<dyn PostgresClient>,
    reconnect: ReconnectClient,
    shutdown: Shutdown,
    pending: Vec<Row>,
    max_size: usize,
    max_delay: Duration,
    first_queued_at: Option<Instant>,
}

impl PostgresWriter {
    fn new(
        client: Box<dyn PostgresClient>,
        reconnect: ReconnectClient,
        shutdown: Shutdown,
    ) -> Self {
        Self {
            client,
            reconnect,
            shutdown,
            pending: Vec::new(),
            max_size: BATCH_MAX_SIZE,
            max_delay: BATCH_MAX_DELAY,
            first_queued_at: None,
        }
    }

    fn push(&mut self, event: &LogEvent) {
        if let Some(row) = row_from_event(event) {
            if self.first_queued_at.is_none() {
                self.first_queued_at = Some(Instant::now());
            }
            self.pending.push(row);
        }
        // Enforce both bounds at enqueue time so a batch is written within
        // `max_delay` of its first row even while events keep arriving faster
        // than the receive timeout.
        self.flush_if_due();
    }

    /// Flush a batch once its size or age exceeds the configured limits.
    fn flush_if_due(&mut self) {
        let due = self.pending.len() >= self.max_size
            || self
                .first_queued_at
                .is_some_and(|queued| queued.elapsed() >= self.max_delay);
        if due {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }

        let rows = std::mem::take(&mut self.pending);
        self.first_queued_at = None;

        let mut by_measurement: HashMap<&str, Vec<&Row>> = HashMap::new();
        for row in &rows {
            by_measurement
                .entry(row.measurement.as_str())
                .or_default()
                .push(row);
        }

        let query_count = rows.len();
        let start = Instant::now();
        for (measurement, group) in by_measurement {
            self.write_group(measurement, &group);
        }
        info!(
            "PostgreSQL: write #{} row(s) in {:.3} s",
            query_count,
            start.elapsed().as_secs_f64()
        );
    }

    /// Write one measurement's rows with a single multi-row `INSERT`.
    fn write_group(&mut self, measurement: &str, rows: &[&Row]) {
        let statement = insert_statement(measurement, rows.len());

        // Owned storage so the borrowed parameter slice stays valid.
        let times: Vec<i64> = rows.iter().map(|row| row.time).collect();
        let locations: Vec<&str> = rows.iter().map(|row| row.location.as_str()).collect();
        let sensors: Vec<&str> = rows.iter().map(|row| row.sensor.as_str()).collect();
        let values: Vec<f64> = rows.iter().map(|row| row.value).collect();
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(rows.len() * 4);
        for index in 0..rows.len() {
            params.push(&times[index]);
            params.push(&locations[index]);
            params.push(&sensors[index]);
            params.push(&values[index]);
        }

        match self.client.execute(&statement, &params) {
            Ok(_) => {}
            Err(error @ WriteError::Connection(_)) => {
                log_write_error(measurement, &error);

                // The connection may be broken; reconnect once and retry.
                match (self.reconnect)() {
                    Ok(mut new_client) => {
                        let retry = new_client.execute(&statement, &params);
                        self.client = new_client;
                        match retry {
                            Ok(_) => {
                                info!("PostgreSQL: reconnected and retried '{}'", measurement)
                            }
                            Err(error @ WriteError::Statement(_)) => {
                                log_write_error(measurement, &error);
                                self.isolate_rows(measurement, rows, &error);
                            }
                            Err(error) => report_write_failure(measurement, &error),
                        }
                    }
                    Err(error) => report_write_failure(measurement, &error),
                }
            }
            Err(error @ WriteError::Statement(_)) => {
                log_write_error(measurement, &error);
                self.isolate_rows(measurement, rows, &error);
            }
        }
    }

    /// Write a group's rows one at a time so a row the server rejects cannot
    /// discard the rows that remain. Every row that cannot be written is
    /// reported and dropped.
    fn isolate_rows(&mut self, measurement: &str, rows: &[&Row], group_error: &WriteError) {
        warn!(
            "PostgreSQL: group write to '{}' failed ({}); isolating {} row(s)",
            measurement,
            group_error,
            rows.len()
        );
        let statement = insert_statement(measurement, 1);
        for row in rows {
            let location = row.location.as_str();
            let sensor = row.sensor.as_str();
            let params: [&(dyn ToSql + Sync); 4] = [&row.time, &location, &sensor, &row.value];
            if let Err(error) = self.client.execute(&statement, &params) {
                report_write_failure(measurement, &error);
            }
        }
    }

    fn run(&mut self, rx: Receiver<Arc<LogEvent>>) {
        loop {
            if self.shutdown.is_requested() {
                info!("PostgreSQL: shutdown requested, draining queue");
                self.drain(&rx);
                break;
            }

            match rx.recv_timeout(POLL_INTERVAL) {
                Ok(event) => self.push(&event),
                Err(RecvTimeoutError::Timeout) => self.flush_if_due(),
                Err(RecvTimeoutError::Disconnected) => {
                    warn!("PostgreSQL: channel disconnected, draining queue");
                    self.drain(&rx);
                    break;
                }
            }
        }
        info!("exiting postgres writer");
    }

    fn drain(&mut self, rx: &Receiver<Arc<LogEvent>>) {
        while let Ok(event) = rx.try_recv() {
            self.push(&event);
        }
        self.flush();
    }
}

fn log_write_error(measurement: &str, error: &dyn std::fmt::Display) {
    error!("#### Error writing to postgres: {} {}", measurement, error);
}

fn report_write_failure(measurement: &str, error: &dyn std::fmt::Display) {
    let failures = crate::metrics::increment(&crate::metrics::POSTGRES_WRITE_FAILURES);
    error!(
        "#### Error writing to postgres: {} {} ({} batch failure(s) so far)",
        measurement, error, failures
    );
}

fn start_postgres_writer(
    rx: Receiver<Arc<LogEvent>>,
    client: Box<dyn PostgresClient>,
    reconnect: ReconnectClient,
    shutdown: Shutdown,
) {
    PostgresWriter::new(client, reconnect, shutdown).run(rx);
}

pub fn spawn_postgres_writer(
    config: PostgresConfig,
    shutdown: Shutdown,
) -> anyhow::Result<(SyncSender<Arc<LogEvent>>, JoinHandle<()>)> {
    let client = create_postgres_client(&config)?;
    let reconnect_config = config.clone();
    let reconnect: ReconnectClient = Box::new(move || create_postgres_client(&reconnect_config));
    Ok(spawn_postgres_writer_internal(client, reconnect, shutdown))
}

fn create_postgres_client(config: &PostgresConfig) -> anyhow::Result<Box<dyn PostgresClient>> {
    let mut pg_config = postgres::Config::new();
    pg_config
        .host(&config.host)
        .port(config.port)
        .user(&config.username)
        .password(&config.password)
        .dbname(&config.database);

    let result = if config.tls {
        let connector = native_tls::TlsConnector::builder()
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build TLS connector: {}", e))?;
        pg_config.connect(postgres_native_tls::MakeTlsConnector::new(connector))
    } else {
        pg_config.connect(NoTls)
    };

    let client = result.map_err(|e| {
        anyhow::anyhow!(
            "Failed to connect to Postgres database at {}:{} (tls: {}): {}",
            config.host,
            config.port,
            config.tls,
            e
        )
    })?;
    Ok(Box::new(DefaultPostgresClient::new(client)))
}

pub(crate) fn spawn_postgres_writer_internal(
    client: Box<dyn PostgresClient>,
    reconnect: ReconnectClient,
    shutdown: Shutdown,
) -> (SyncSender<Arc<LogEvent>>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!("starting postgres writer");
            start_postgres_writer(rx, client, reconnect, shutdown);
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn unreachable_reconnect() -> ReconnectClient {
        Box::new(|| Err(anyhow::anyhow!("reconnect not expected in test")))
    }

    fn test_row(time: i64, location: &str) -> Row {
        Row {
            measurement: "t".to_string(),
            time,
            location: location.to_string(),
            sensor: "s".to_string(),
            value: 1.0,
        }
    }

    fn test_event(value: f64) -> LogEvent {
        LogEvent::new_value_from_ref(
            "measurement".to_string(),
            0,
            vec![("location", "location"), ("sensor", "BME680")]
                .into_iter()
                .collect(),
            Number::Float(value),
        )
    }

    #[test]
    fn test_sanitize_measurement() {
        assert_eq!(sanitize_measurement("powerdc"), Some("powerdc"));
        assert_eq!(sanitize_measurement("total_energy"), Some("total_energy"));
        assert_eq!(sanitize_measurement("invalid-name"), None);
        assert_eq!(sanitize_measurement("drop\"table"), None);
        assert_eq!(sanitize_measurement(""), None);
    }

    #[test]
    fn test_row_from_event_skips_events_without_postgres_schema() {
        // No `value` field and no `location`/`sensor` tags (e.g. BLE events).
        let event = LogEvent::new(
            "btle".to_string(),
            0,
            HashMap::from([("device".to_string(), "abc".to_string())]),
            HashMap::from([("rssi".to_string(), Number::Int(-92))]),
        );

        assert!(row_from_event(&event).is_none());
    }

    #[test]
    fn test_row_from_event_skips_invalid_measurement() {
        let event = LogEvent::new(
            "invalid;drop".to_string(),
            0,
            HashMap::from([
                ("location".to_string(), "home".to_string()),
                ("sensor".to_string(), "BME680".to_string()),
            ]),
            HashMap::from([("value".to_string(), Number::Float(1.0))]),
        );

        assert!(row_from_event(&event).is_none());
    }

    #[test]
    fn test_row_from_event_extracts_schema() {
        let event = LogEvent::new_value_from_ref(
            "temperature".to_string(),
            1_701_292_592,
            vec![("location", "home"), ("sensor", "BME680")]
                .into_iter()
                .collect(),
            Number::Float(19.5),
        );

        let row = row_from_event(&event).expect("row");
        assert_eq!(row.measurement, "temperature");
        assert_eq!(row.time, 1_701_292_592);
        assert_eq!(row.location, "home");
        assert_eq!(row.sensor, "BME680");
        assert_eq!(row.value, 19.5);
    }

    #[test]
    fn test_write_group_batches_multiple_rows_into_one_statement() {
        let mut mock = MockPostgresClient::new();
        mock.expect_execute()
            .times(1)
            .withf(|query, params| {
                query
                    == "insert into \"t\" (time, location, sensor, value) values \
                        ($1, $2, $3, $4), ($5, $6, $7, $8);"
                    && params.len() == 8
            })
            .returning(|_, _| Ok(2));

        let client: Box<dyn PostgresClient> = Box::new(mock);
        let mut writer = PostgresWriter::new(client, unreachable_reconnect(), Shutdown::new());
        writer.pending.push(Row {
            measurement: "t".to_string(),
            time: 1,
            location: "a".to_string(),
            sensor: "s".to_string(),
            value: 1.0,
        });
        writer.pending.push(Row {
            measurement: "t".to_string(),
            time: 2,
            location: "b".to_string(),
            sensor: "s".to_string(),
            value: 2.0,
        });

        writer.flush();
        assert!(writer.pending.is_empty());
    }

    #[test]
    fn test_push_flushes_batch_that_exceeds_max_delay_while_traffic_continues() {
        let mut mock = MockPostgresClient::new();
        mock.expect_execute().times(1).returning(|_, _| Ok(1));

        let client: Box<dyn PostgresClient> = Box::new(mock);
        let mut writer = PostgresWriter::new(client, unreachable_reconnect(), Shutdown::new());

        writer.push(&test_event(1.0));
        assert!(!writer.pending.is_empty());

        // Simulate a batch whose first row has waited past `max_delay` while
        // further events keep arriving: the next enqueue must flush without the
        // receive timeout ever firing.
        writer.first_queued_at = Some(Instant::now() - writer.max_delay);
        writer.push(&test_event(2.0));

        assert!(writer.pending.is_empty());
    }

    #[test]
    fn test_flush_if_due_flushes_quiet_batch_after_max_delay() {
        let mut mock = MockPostgresClient::new();
        mock.expect_execute().times(1).returning(|_, _| Ok(1));

        let client: Box<dyn PostgresClient> = Box::new(mock);
        let mut writer = PostgresWriter::new(client, unreachable_reconnect(), Shutdown::new());

        writer.push(&test_event(1.0));
        assert!(!writer.pending.is_empty());

        // The source fell silent; the timeout branch calls `flush_if_due`.
        writer.first_queued_at = Some(Instant::now() - writer.max_delay);
        writer.flush_if_due();

        assert!(writer.pending.is_empty());
    }

    #[test]
    fn test_push_defers_young_batch_and_flushes_full_batch() {
        let mut mock = MockPostgresClient::new();
        mock.expect_execute().times(1).returning(|_, _| Ok(1));

        let client: Box<dyn PostgresClient> = Box::new(mock);
        let mut writer = PostgresWriter::new(client, unreachable_reconnect(), Shutdown::new());

        let event = test_event(1.0);
        for _ in 0..writer.max_size - 1 {
            writer.push(&event);
        }
        // Below `max_size` and younger than `max_delay`: nothing is written yet.
        assert_eq!(writer.pending.len(), writer.max_size - 1);

        // Reaching `max_size` flushes immediately.
        writer.push(&event);
        assert!(writer.pending.is_empty());
    }

    #[test]
    fn test_postgres_writer_internal() -> anyhow::Result<()> {
        let log_event = LogEvent::new_value_from_ref(
            "test".to_string(),
            0i64,
            vec![("location", "location"), ("sensor", "BME680")]
                .into_iter()
                .collect(),
            Number::Float(1.23),
        );

        let sensor_reading_duplicate = log_event.clone();

        let mut mock_client = Box::new(MockPostgresClient::new());
        mock_client.expect_execute()
            .times(1)
            .withf(move |query, parameters| {
                let expected_parameters: [&dyn ToSql; 4] = [&sensor_reading_duplicate.timestamp, &"location", &"BME680", &1.23];
                query == "insert into \"measurement\" (time, location, sensor, value) values ($1, $2, $3, $4);" ||
                    parameters.len() == expected_parameters.len() &&
                        parameters.iter().zip(expected_parameters.iter()).all(|(a, b)| format!("{a:?}") == format!("{b:?}"))
            })
            .returning(|_, _| Ok(123));

        let reconnect: ReconnectClient =
            Box::new(|| Err(anyhow::anyhow!("reconnect not expected in test")));
        let (tx, join_handle) =
            spawn_postgres_writer_internal(mock_client, reconnect, Shutdown::new());

        tx.send(Arc::new(log_event)).unwrap();

        drop(tx);

        let _ = join_handle.join();

        Ok(())
    }

    #[test]
    fn test_classify_closed_and_missing_sqlstate_are_connection_errors() {
        // A closed connection is a connection failure even with a server code.
        assert!(matches!(
            classify(true, Some("23505"), "closed".to_string()),
            WriteError::Connection(_)
        ));
        // A transport failure carries no SQLSTATE.
        assert!(matches!(
            classify(false, None, "transport".to_string()),
            WriteError::Connection(_)
        ));
    }

    #[test]
    fn test_classify_connection_and_shutdown_sqlstates_are_connection_errors() {
        for code in ["08000", "08003", "08006", "57P01", "57P02", "57P03"] {
            assert!(
                matches!(
                    classify(false, Some(code), code.to_string()),
                    WriteError::Connection(_)
                ),
                "SQLSTATE {code} should be a connection error"
            );
        }
    }

    #[test]
    fn test_classify_statement_sqlstates_are_statement_errors() {
        // Constraint violations and other server errors cannot be fixed by a
        // reconnect - including class 57 codes that are not shutdown.
        for code in ["23505", "42P01", "57014", "40001"] {
            assert!(
                matches!(
                    classify(false, Some(code), code.to_string()),
                    WriteError::Statement(_)
                ),
                "SQLSTATE {code} should be a statement error"
            );
        }
    }

    #[test]
    fn test_write_group_statement_error_isolates_conflicting_row_without_reconnecting() {
        let reconnects = Arc::new(AtomicUsize::new(0));
        let reconnect_counter = Arc::clone(&reconnects);
        let reconnect: ReconnectClient = Box::new(move || {
            reconnect_counter.fetch_add(1, Ordering::SeqCst);
            Err(anyhow::anyhow!("a statement error must not reconnect"))
        });

        let mut mock = MockPostgresClient::new();
        // The three-row insert fails as a whole ...
        mock.expect_execute()
            .withf(|_, params| params.len() == 12)
            .times(1)
            .returning(|_, _| Err(WriteError::Statement("duplicate key".to_string())));
        // ... so every row is retried on its own; only the row with time 2
        // still conflicts.
        mock.expect_execute()
            .withf(|_, params| params.len() == 4)
            .times(3)
            .returning(|_, params| {
                if format!("{:?}", params[0]) == "2" {
                    Err(WriteError::Statement("duplicate key".to_string()))
                } else {
                    Ok(1)
                }
            });

        let mut writer = PostgresWriter::new(Box::new(mock), reconnect, Shutdown::new());
        writer.pending.push(test_row(1, "a"));
        writer.pending.push(test_row(2, "b"));
        writer.pending.push(test_row(3, "c"));

        writer.flush();

        assert_eq!(
            reconnects.load(Ordering::SeqCst),
            0,
            "a statement error must not reconnect"
        );
        assert!(writer.pending.is_empty());
    }

    #[test]
    fn test_write_group_connection_error_reconnects_once_and_retries() {
        let reconnects = Arc::new(AtomicUsize::new(0));
        let reconnect_counter = Arc::clone(&reconnects);
        let reconnect: ReconnectClient = Box::new(move || {
            reconnect_counter.fetch_add(1, Ordering::SeqCst);
            let mut client = MockPostgresClient::new();
            // The retry reuses the same multi-row statement and succeeds.
            client
                .expect_execute()
                .withf(|_, params| params.len() == 8)
                .times(1)
                .returning(|_, _| Ok(2));
            Ok(Box::new(client))
        });

        let mut mock = MockPostgresClient::new();
        mock.expect_execute()
            .withf(|_, params| params.len() == 8)
            .times(1)
            .returning(|_, _| Err(WriteError::Connection("connection closed".to_string())));

        let mut writer = PostgresWriter::new(Box::new(mock), reconnect, Shutdown::new());
        writer.pending.push(test_row(1, "a"));
        writer.pending.push(test_row(2, "b"));

        writer.flush();

        assert_eq!(
            reconnects.load(Ordering::SeqCst),
            1,
            "a connection error must reconnect once and retry"
        );
        assert!(writer.pending.is_empty());
    }

    #[test]
    fn test_write_group_retry_connection_error_drops_and_reports_group() {
        let before = crate::metrics::total(&crate::metrics::POSTGRES_WRITE_FAILURES);

        let reconnects = Arc::new(AtomicUsize::new(0));
        let reconnect_counter = Arc::clone(&reconnects);
        let reconnect: ReconnectClient = Box::new(move || {
            reconnect_counter.fetch_add(1, Ordering::SeqCst);
            let mut client = MockPostgresClient::new();
            // The retry fails again because the connection is still broken;
            // individual rows must not be attempted against a dead connection.
            client
                .expect_execute()
                .withf(|_, params| params.len() == 8)
                .times(1)
                .returning(|_, _| Err(WriteError::Connection("still down".to_string())));
            Ok(Box::new(client))
        });

        let mut mock = MockPostgresClient::new();
        mock.expect_execute()
            .withf(|_, params| params.len() == 8)
            .times(1)
            .returning(|_, _| Err(WriteError::Connection("connection closed".to_string())));

        let mut writer = PostgresWriter::new(Box::new(mock), reconnect, Shutdown::new());
        writer.pending.push(test_row(1, "a"));
        writer.pending.push(test_row(2, "b"));

        writer.flush();

        assert_eq!(reconnects.load(Ordering::SeqCst), 1);
        let after = crate::metrics::total(&crate::metrics::POSTGRES_WRITE_FAILURES);
        assert!(after > before, "a failed reconnect retry must be reported");
        assert!(writer.pending.is_empty(), "the group must be dropped");
    }
}
