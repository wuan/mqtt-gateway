use crate::data::LogEvent;
use crate::is_shutdown_requested;
use crate::Number;
use log::{error, info, warn};
#[cfg(test)]
use mockall::automock;
use postgres::types::ToSql;
use postgres::Client;
use postgres::{Error, NoTls};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

/// Factory used to re-establish a lost Postgres connection.
pub(crate) type ReconnectClient = Box<dyn Fn() -> anyhow::Result<Box<dyn PostgresClient>> + Send>;

#[derive(Clone)]
pub struct PostgresConfig {
    host: String,
    port: u16,
    username: String,
    password: String,
    database: String,
}

impl PostgresConfig {
    pub(crate) fn new(
        host: String,
        port: u16,
        username: String,
        password: String,
        database: String,
    ) -> Self {
        Self {
            host,
            port,
            username,
            password,
            database,
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
fn start_postgres_writer(
    rx: Receiver<LogEvent>,
    mut client: Box<dyn PostgresClient>,
    reconnect: ReconnectClient,
) {
    loop {
        // Check for shutdown request
        if is_shutdown_requested() {
            info!("PostgreSQL: shutdown requested, draining queue");
            while let Ok(event) = rx.try_recv() {
                write_event(&event, &mut client, &reconnect);
            }
            break;
        }

        // Use recv_timeout to allow periodic shutdown checks
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(event) => write_event(&event, &mut client, &reconnect),
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                warn!("PostgreSQL: channel disconnected, draining queue");
                while let Ok(event) = rx.try_recv() {
                    write_event(&event, &mut client, &reconnect);
                }
                break;
            }
        }
    }
    info!("exiting postgres writer");
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

fn write_event(
    event: &LogEvent,
    client: &mut Box<dyn PostgresClient>,
    reconnect: &ReconnectClient,
) {
    let measurement = match sanitize_measurement(&event.measurement) {
        Some(measurement) => measurement,
        None => {
            warn!(
                "PostgreSQL: skipping event with invalid measurement name '{}'",
                event.measurement
            );
            return;
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
            return;
        }
    };

    let location = match event.tags.get("location") {
        Some(location) => location,
        None => {
            warn!(
                "PostgreSQL: skipping '{}' event without a 'location' tag",
                measurement
            );
            return;
        }
    };

    let sensor = match event.tags.get("sensor") {
        Some(sensor) => sensor,
        None => {
            warn!(
                "PostgreSQL: skipping '{}' event without a 'sensor' tag",
                measurement
            );
            return;
        }
    };

    let statement = format!(
        "insert into \"{}\" (time, location, sensor, value) values ($1, $2, $3, $4);",
        measurement
    );
    let params: [&(dyn ToSql + Sync); 4] = [&event.timestamp, location, sensor, &value];

    if let Err(error) = client.execute(&statement, &params) {
        error!(
            "#### Error writing to postgres: {} {:?}",
            measurement, error
        );

        // The connection may be broken; try to reconnect once and retry.
        match reconnect() {
            Ok(mut new_client) => {
                match new_client.execute(&statement, &params) {
                    Ok(_) => info!("PostgreSQL: reconnected and retried '{}'", measurement),
                    Err(error) => error!(
                        "#### Error writing to postgres after reconnect: {} {:?}",
                        measurement, error
                    ),
                }
                *client = new_client;
            }
            Err(error) => error!("PostgreSQL: reconnect failed: {}", error),
        }
    }
}

pub fn spawn_postgres_writer(
    config: PostgresConfig,
) -> anyhow::Result<(SyncSender<LogEvent>, JoinHandle<()>)> {
    let client = create_postgres_client(&config)?;
    let reconnect_config = config.clone();
    let reconnect: ReconnectClient = Box::new(move || create_postgres_client(&reconnect_config));
    Ok(spawn_postgres_writer_internal(client, reconnect))
}

fn create_postgres_client(config: &PostgresConfig) -> anyhow::Result<Box<dyn PostgresClient>> {
    let client = postgres::Config::new()
        .host(&config.host)
        .port(config.port)
        .user(&config.username)
        .password(&config.password)
        .dbname(&config.database)
        .connect(NoTls)
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to connect to Postgres database at {}:{}: {}",
                config.host,
                config.port,
                e
            )
        })?;
    Ok(Box::new(DefaultPostgresClient::new(client)))
}

pub(crate) fn spawn_postgres_writer_internal(
    client: Box<dyn PostgresClient>,
    reconnect: ReconnectClient,
) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!("starting postgres writer");
            start_postgres_writer(rx, client, reconnect);
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
    fn test_write_event_skips_events_without_postgres_schema() {
        let mut mock = MockPostgresClient::new();
        mock.expect_execute().times(0);
        let mut mock_client: Box<dyn PostgresClient> = Box::new(mock);
        let reconnect = unreachable_reconnect();

        // No `value` field and no `location`/`sensor` tags (e.g. BLE events).
        let event = LogEvent::new(
            "btle".to_string(),
            0,
            HashMap::from([("device".to_string(), "abc".to_string())]),
            HashMap::from([("rssi".to_string(), Number::Int(-92))]),
        );

        write_event(&event, &mut mock_client, &reconnect);
    }

    #[test]
    fn test_write_event_skips_invalid_measurement() {
        let mut mock = MockPostgresClient::new();
        mock.expect_execute().times(0);
        let mut mock_client: Box<dyn PostgresClient> = Box::new(mock);
        let reconnect = unreachable_reconnect();

        let event = LogEvent::new(
            "invalid;drop".to_string(),
            0,
            HashMap::from([
                ("location".to_string(), "home".to_string()),
                ("sensor".to_string(), "BME680".to_string()),
            ]),
            HashMap::from([("value".to_string(), Number::Float(1.0))]),
        );

        write_event(&event, &mut mock_client, &reconnect);
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
        let (tx, join_handle) = spawn_postgres_writer_internal(mock_client, reconnect);

        tx.send(log_event).unwrap();

        drop(tx);

        let _ = join_handle.join();

        Ok(())
    }
}
