use crate::data::LogEvent;
use crate::Number;
use async_compat::Compat;
use influxdb::{Client, Error, Timestamp, WriteQuery};
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
        info!("InfluxDB: Using token");
        influx_client.with_token(token)
    } else if let (Some(user), Some(password)) =
        (influx_config.user.clone(), influx_config.password.clone())
    {
        info!("InfluxDB: Using username {} and password", &user);
        influx_client.with_auth(user, password)
    } else {
        info!("InfluxDB: No authentication");
        influx_client
    };

    Ok(Box::new(DefaultInfluxClient::new(influx_client)))
}

fn influxdb_writer(
    rx: Receiver<LogEvent>,
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
) {
    let mut writer = Writer::new(influx_client, influx_config.clone(), Duration::from_secs(5));

    loop {
        let result = rx.recv_timeout(Duration::from_secs(10));

        let query = match result {
            Ok(event) => map_to_query(event),
            Err(error) => {
                writer.flush();
                match error {
                    std::sync::mpsc::RecvTimeoutError::Timeout => continue,
                    std::sync::mpsc::RecvTimeoutError::Disconnected => {
                        warn!(
                            "InfluxDB: disconnected {} {}",
                            influx_config.url, influx_config.database,
                        );
                        break;
                    }
                }
            }
        };

        writer.queue(query);
    }

    info!(
        "InfluxDB: exiting writer {} {}",
        influx_config.url, influx_config.database
    );
}

struct Writer {
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
    queries: Vec<WriteQuery>,
    accumulation_time: Duration,
    start: Instant,
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
        if self.queries.len() > 0 && self.start.elapsed() >= self.accumulation_time {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.queries.is_empty() {
            return;
        }

        let now = Instant::now();
        let query_count = self.queries.len();
        trace!("before write to influx");
        let result = self.influx_client.write(self.queries.clone());
        let duration = now.elapsed();
        info!(
            "InfluxDB: {} {} write #{} ({:.3} s)",
            self.influx_config.url,
            self.influx_config.database,
            query_count,
            duration.as_secs_f64()
        );
        match result {
            Ok(_) => {}
            Err(error) => {
                let _ = &self.panic(error);
            }
        }
        self.queries.clear();
        self.start = now
    }

    fn panic(&self, error: Error) {
        panic!(
            "#### Error writing to influx: {} {}: {:?}",
            self.influx_config.url, self.influx_config.database, error
        );
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
        }
    }
}

pub fn spawn_influxdb_writer(
    influx_config: InfluxConfig,
) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let influx_client =
        create_influxdb_client(&influx_config).expect("could not create influxdb client");

    spawn_writer(influx_client, influx_config)
}

fn spawn_writer(
    influx_client: Box<dyn InfluxClient>,
    influx_config: InfluxConfig,
) -> (SyncSender<LogEvent>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            info!(
                "InfluxDB: starting writer {} {}",
                &influx_config.url, &influx_config.database
            );

            influxdb_writer(rx, influx_client, influx_config);
        }),
    )
}

pub fn map_to_query(log_event: LogEvent) -> WriteQuery {
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
    use mockall::predicate::function;

    fn log_event() -> LogEvent {
        LogEvent::new_value_from_ref(
            "test".to_string(),
            0i64,
            vec![].into_iter().collect(),
            Number::Float(1.23),
        )
    }

    fn write_query() -> WriteQuery {
        map_to_query(log_event())
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
    fn test_influxdb_writer_internal() -> anyhow::Result<()> {
        let mut mock_client = Box::new(MockInfluxClient::new());
        mock_client
            .expect_write()
            .times(1)
            .returning(|_| Ok("test_data".to_string()));

        // Run the `influxdb_writer` function
        let (tx, rx) = sync_channel(100);
        let join_handle = thread::spawn(move || {
            influxdb_writer(rx, mock_client, influx_config());
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

        let (tx, handle) = spawn_writer(mock_client, influx_config());

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

        let (tx, handle) = spawn_writer(mock_client, influx_config());

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
