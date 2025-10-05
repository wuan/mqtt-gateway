use crate::data::LogEvent;
use crate::Number;
use async_trait::async_trait;
use influxdb::{Client, Timestamp, WriteQuery};
use log::{debug, info, trace, warn};
#[cfg(test)]
use mockall::automock;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::time::Instant;
use tokio::task::JoinHandle;
use url::Url;

pub struct InfluxConfig {
    url: Url,
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
    ) -> anyhow::Result<Self> {
        Ok(Self {
            url: Url::parse(&url)?,
            database,
            user,
            password,
            token,
        })
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
    async fn write(&self, point: WriteQuery) -> Result<String, influxdb::Error>;
}

#[async_trait]
impl InfluxClient for DefaultInfluxClient {
    async fn write(&self, write_query: WriteQuery) -> Result<String, influxdb::Error> {
        self.client.query(write_query).await
    }
}

fn create_influxdb_client(influx_config: &InfluxConfig) -> anyhow::Result<Box<dyn InfluxClient>> {
    let mut influx_client = Client::new(
        influx_config.url.to_string().clone(),
        influx_config.database.clone(),
    );

    influx_client = if let Some(token) = influx_config.token.clone() {
        influx_client.with_token(token)
    } else if let (Some(user), Some(password)) =
        (influx_config.user.clone(), influx_config.password.clone())
    {
        influx_client.with_auth(user, password)
    } else {
        influx_client
    };

    Ok(Box::new(DefaultInfluxClient::new(influx_client)))
}

async fn influxdb_writer(
    rx: Receiver<LogEvent>,
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
) {
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
        let query = map_to_point(data);
        trace!("before write to influx");
        let start = Instant::now();
        let result = influx_client.write(query).await;
        let duration = start.elapsed();
        debug!("write to InfluxDB ({:.2})", duration.as_secs_f64());
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
}

pub fn spawn_influxdb_writer(
    influx_config: InfluxConfig,
) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let influx_client =
        create_influxdb_client(&influx_config).expect("could not create influxdb client");
    spawn_influxdb_writer_internal(influx_client, influx_config)
}

fn spawn_influxdb_writer_internal(
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);
    println!("Spawn influx writer async");

    (
        tx,
        tokio::spawn(async move {
            println!("starting influx writer async");
            info!(
                "starting influx writer {} {}",
                &influx_config.url, &influx_config.database
            );

            influxdb_writer(rx, influx_client, influx_config).await;
        }),
    )
}

pub fn map_to_point(log_event: LogEvent) -> WriteQuery {
    let mut write_query = WriteQuery::new(
        Timestamp::Seconds(log_event.timestamp as u128),
        log_event.measurement,
    );
    for (tag, value) in log_event.tags {
        write_query = write_query.add_tag(tag, value);
    }
    for (name, value) in log_event.fields {
        match value {
            Number::Int(value) => {
                write_query = write_query.add_field(name, value);
            }
            Number::Float(value) => {
                write_query = write_query.add_field(name, value);
            }
        }
    }
    write_query
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Number;

    // A mock `WriteQuery` for testing purposes
    fn mock_write_query(data: String) -> LogEvent {
        info!("mock write query {}", data);

        assert_eq!(data, "test_data");

        let current_timestamp = chrono::Utc::now().timestamp();
        LogEvent::new_value_from_ref(
            "measurement".to_string(),
            current_timestamp,
            vec![].into_iter().collect(),
            Number::Float(1.23),
        )
    }

    #[tokio::test]
    async fn test_influxdb_writer_internal() -> anyhow::Result<()> {
        let influx_config = InfluxConfig::new(
            "http://localhost:8086".to_string(),
            "test_db".to_string(),
            Some("user".to_string()),
            Some("password".to_string()),
            None,
        )?;

        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(1)
            .returning(|_| Ok("test_data".to_string()));

        // Run the `influxdb_writer` function
        let (tx, join_handle) = spawn_influxdb_writer_internal(mock_client, influx_config);

        // Send a test query
        let log_event = LogEvent::new_value_from_ref(
            "test".to_string(),
            0i64,
            vec![].into_iter().collect(),
            Number::Float(1.23),
        );
        tx.send(log_event).unwrap();

        // Close the channel
        drop(tx);

        join_handle.await.expect("stopped writer");

        Ok(())
    }
}
