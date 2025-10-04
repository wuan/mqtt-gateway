use crate::SensorReading;
use futures::executor::block_on;
use log::{error, info, warn};
#[cfg(test)]
use mockall::automock;
use postgres::types::ToSql;
use postgres::Client;
use postgres::{Error, NoTls};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use tokio::task::JoinHandle;

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

async fn start_postgres_writer(rx: Receiver<SensorReading>, mut client: Box<dyn PostgresClient>) {
    block_on(async move {
        info!("starting postgres writer async");

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
            let x = client.execute(
                &statement,
                &[&query.time, &query.location, &query.sensor, &query.value],
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
    });

    info!("exiting influx writer");
}

pub fn spawn_postgres_writer(
    config: PostgresConfig,
) -> (SyncSender<SensorReading>, JoinHandle<()>) {
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
) -> (SyncSender<SensorReading>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        tokio::spawn(async move {
            info!("starting postgres writer");
            start_postgres_writer(rx, client).await;
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_writer_internal() -> anyhow::Result<()> {
        let sensor_reading = SensorReading {
            measurement: "measurement".to_string(),
            time: chrono::Utc::now(),
            location: "location".to_string(),
            sensor: "sensor".to_string(),
            value: 123.4,
        };

        let sensor_reading_duplicate = sensor_reading.clone();

        let mut mock_client = Box::new(MockPostgresClient::new());
        mock_client.expect_execute()
            .times(1)
            .withf(move |query, parameters| {
                let expected_parameters: [&dyn ToSql; 4] = [&sensor_reading_duplicate.time, &sensor_reading_duplicate.location, &sensor_reading_duplicate.sensor, &sensor_reading_duplicate.value];
                query == "insert into \"measurement\" (time, location, sensor, value) values ($1, $2, $3, $4);" ||
                    parameters.len() == expected_parameters.len() &&
                        parameters.iter().zip(expected_parameters.iter()).all(|(a, b)| format!("{a:?}") == format!("{b:?}"))
            })
            .returning(|_, _| Ok(123));

        let (tx, join_handle) = spawn_postgres_writer_internal(mock_client);

        tx.send(sensor_reading).unwrap();

        drop(tx);

        let _ = join_handle.join();

        Ok(())
    }
}
