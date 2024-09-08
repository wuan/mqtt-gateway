use futures::executor::block_on;
use influxdb::{Client, WriteQuery};
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

pub fn influxdb_writer<T>(
    rx: Receiver<T>,
    influx_config: InfluxConfig,
    query_mapper: fn(T) -> WriteQuery,
) {
    let influx_url = influx_config.url.clone();
    let influx_database = influx_config.database.clone();

    let mut influx_client = Client::new(influx_config.url, influx_config.database);
    influx_client =
        if let (Some(user), Some(password)) = (influx_config.user, influx_config.password) {
            influx_client.with_auth(user, password)
        } else {
            influx_client
        };

    block_on(async move {
        println!(
            "starting influx writer async {} {}",
            &influx_url, &influx_database
        );

        loop {
            let result = rx.recv();
            let data = match result {
                Ok(query) => query,
                Err(error) => {
                    println!("error receiving query: {:?}", error);
                    break;
                }
            };
            let query = query_mapper(data);
            let result = influx_client.query(query).await;
            match result {
                Ok(_) => {}
                Err(error) => {
                    panic!(
                        "#### Error writing to influx: {} {}: {:?}",
                        &influx_url, &influx_database, error
                    );
                }
            }
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}

pub fn spawn_influxdb_writer<T: Send + 'static>(
    config: InfluxConfig,
    mapper: fn(T) -> WriteQuery,
) -> (SyncSender<T>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    (
        tx,
        thread::spawn(move || {
            println!(
                "starting influx writer {} {}",
                &config.url, &config.database
            );

            influxdb_writer(rx, config, mapper);
        }),
    )
}
