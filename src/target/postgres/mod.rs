use crate::data::LogEvent;
use crate::Number;
use crate::Shutdown;
use log::{error, info, warn};
#[cfg(test)]
use mockall::automock;
use postgres::types::ToSql;
use postgres::Client;
use postgres::{Error, NoTls};
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

#[cfg_attr(test, automock)]
pub trait PostgresClient: Send {
    fn execute<'a>(
        &mut self,
        query: &str,
        params: &'a [&'a (dyn ToSql + Sync)],
    ) -> Result<u64, Error>;
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
    fn execute(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error> {
        self.client.execute(query, params)
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
        Some(Number::UInt(value)) => *value as f64,
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
        if self.pending.len() >= self.max_size {
            self.flush();
        }
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
        let mut statement = format!(
            "insert into \"{}\" (time, location, sensor, value) values",
            measurement
        );
        for index in 0..rows.len() {
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

        if let Err(error) = self.client.execute(&statement, &params) {
            error!(
                "#### Error writing to postgres: {} {:?}",
                measurement, error
            );

            // The connection may be broken; try to reconnect once and retry.
            match (self.reconnect)() {
                Ok(mut new_client) => {
                    match new_client.execute(&statement, &params) {
                        Ok(_) => {
                            info!("PostgreSQL: reconnected and retried '{}'", measurement)
                        }
                        Err(error) => {
                            report_write_failure(measurement, &error);
                        }
                    }
                    self.client = new_client;
                }
                Err(error) => {
                    report_write_failure(measurement, &error);
                }
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

    fn unreachable_reconnect() -> ReconnectClient {
        Box::new(|| Err(anyhow::anyhow!("reconnect not expected in test")))
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
}
