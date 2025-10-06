use crate::data::LogEvent;
use crate::Number;
use log::{error, info, warn};
#[cfg(test)]
use mockall::automock;
use postgres::types::ToSql;
use postgres::Client;
use postgres::{Error, NoTls};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use std::thread::JoinHandle;

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
fn start_postgres_writer(rx: Receiver<LogEvent>, mut client: Box<dyn PostgresClient>) {
    loop {
        let result = rx.recv();
        let query = match result {
            Ok(query) => query,
            Err(error) => {
                warn!("error receiving query: {:?}", error);
                break;
            }
        };

        let statement = format!(
            "insert into \"{}\" (time, location, sensor, value) values ($1, $2, $3, $4);",
            query.measurement
        );
        let x1 = query.fields.get("value").unwrap();
        let value = match x1 {
            Number::Int(value) => &(*value as f64),
            Number::Float(value) => value,
        };
        let x = client.execute(
            &statement,
            &[
                &query.timestamp,
                &query.tags.get("location").unwrap(),
                &query.tags.get("sensor").unwrap(),
                value,
            ],
        );

        match x {
            Ok(_) => {}
            Err(error) => {
                error!(
                    "#### Error writing to postgres: {} {:?}",
                    query.measurement, error
                );
            }
        }
    }
    info!("exiting influx writer async");
}

pub fn spawn_postgres_writer(config: PostgresConfig) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let client = create_postgres_client(&config);
    spawn_postgres_writer_internal(client)
}

fn create_postgres_client(config: &PostgresConfig) -> Box<dyn PostgresClient> {
    let client = postgres::Config::new()
        .host(&config.host)
        .port(config.port)
        .user(&config.username)
        .password(&config.password)
        .dbname(&config.database)
        .connect(NoTls)
        .expect("failed to connect to Postgres database");
    Box::new(DefaultPostgresClient::new(client))
}

pub fn spawn_postgres_writer_internal(
    client: Box<dyn PostgresClient>,
) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!("starting postgres writer");
            start_postgres_writer(rx, client);
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let (tx, join_handle) = spawn_postgres_writer_internal(mock_client);

        tx.send(log_event).unwrap();

        drop(tx);

        let _ = join_handle.join();

        Ok(())
    }
}
