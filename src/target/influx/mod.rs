use async_trait::async_trait;
//use anyhow::Result;
use futures::executor::block_on;
use influxdb::{Client, WriteQuery};
use log::{info, trace, warn};
#[cfg(test)]
use mockall::automock;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use std::thread::JoinHandle;

pub struct InfluxConfig {
    url: String,
    database: String,
    user: Option<String>,
    password: Option<String>,
}

impl InfluxConfig {
    pub fn new(
        url: String,
        database: String,
        user: Option<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            url,
            database,
            user,
            password,
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
#[async_trait]
trait InfluxClient: Sync + Send {
    async fn query(&self, write_query: WriteQuery) -> Result<String, influxdb::Error>;
}

#[async_trait]
impl InfluxClient for DefaultInfluxClient {
    async fn query(&self, write_query: WriteQuery) -> Result<String, influxdb::Error> {
        self.client.query(write_query).await
    }
}

fn create_influxdb_client(influx_config: &InfluxConfig) -> anyhow::Result<Box<dyn InfluxClient>> {
    let mut influx_client = Client::new(influx_config.url.clone(), influx_config.database.clone());

    influx_client = if let (Some(user), Some(password)) =
        (influx_config.user.clone(), influx_config.password.clone())
    {
        influx_client.with_auth(user, password)
    } else {
        influx_client
    };

    Ok(Box::new(DefaultInfluxClient::new(influx_client)))
}

fn influxdb_writer<T>(
    rx: Receiver<T>,
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
    query_mapper: fn(T) -> WriteQuery,
) {
    block_on(async move {
        info!(
            "starting influx writer async {} {}",
            &influx_config.url, &influx_config.database
        );

        loop {
            let result = rx.recv();
            let data = match result {
                Ok(query) => query,
                Err(error) => {
                    warn!("error receiving query: {:?}", error);
                    break;
                }
            };
            let query = query_mapper(data);
            trace!("write to influx");
            let result = influx_client.query(query).await;
            trace!("done");
            match result {
                Ok(_) => {}
                Err(error) => {
                    panic!(
                        "#### Error writing to influx: {} {}: {:?}",
                        &influx_config.url, &influx_config.database, error
                    );
                }
            }
        }
        info!("exiting influx writer async");
    });

    info!("exiting influx writer");
}

pub fn spawn_influxdb_writer<T: Send + 'static>(
    influx_config: InfluxConfig,
    query_mapper: fn(T) -> WriteQuery,
) -> (SyncSender<T>, JoinHandle<()>) {
    let influx_client =
        create_influxdb_client(&influx_config).expect("could not create influxdb client");
    spawn_influxdb_writer_internal(influx_client, influx_config, query_mapper)
}

fn spawn_influxdb_writer_internal<T: Send + 'static>(
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
    query_mapper: fn(T) -> WriteQuery,
) -> (SyncSender<T>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!(
                "starting influx writer {} {}",
                &influx_config.url, &influx_config.database
            );

            influxdb_writer(rx, influx_client, influx_config, query_mapper)
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use influxdb::Timestamp::Seconds;

    // A mock `WriteQuery` for testing purposes
    fn mock_write_query(data: String) -> WriteQuery {
        info!("mock write query {}", data);

        assert_eq!(data, "test_data");

        let current_timestamp = Seconds(chrono::Utc::now().timestamp() as u128);
        WriteQuery::new(current_timestamp, "measurement")
            .add_field("field", influxdb::Type::Float(1.23))
    }

    //
    #[test]
    fn test_influxdb_writer_internal() -> anyhow::Result<()> {
        let influx_config = InfluxConfig::new(
            "http://localhost:8086".to_string(),
            "test_db".to_string(),
            Some("user".to_string()),
            Some("password".to_string()),
        );

        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_query()
            .times(1)
            .returning(|_| Ok("Success".to_string()));

        // Run the `influxdb_writer` function
        let (tx, join_handle) =
            spawn_influxdb_writer_internal(mock_client, influx_config, mock_write_query);

        // Send a test query
        tx.send("test_data".to_string()).unwrap();

        // Close the channel
        drop(tx);

        join_handle.join().expect("stopped writer");

        Ok(())
    }
}
