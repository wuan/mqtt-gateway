use crate::SensorReading;
use futures::executor::block_on;
use postgres::{Config, NoTls};
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

fn start_postgres_writer(rx: Receiver<SensorReading>, config: Config) {
    let mut client = config
        .connect(NoTls)
        .expect("failed to connect to postgres");

    block_on(async move {
        println!("starting postgres writer async");

        loop {
            let result = rx.recv();
            let query = match result {
                Ok(query) => query,
                Err(error) => {
                    println!("error receiving query: {:?}", error);
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
                    eprintln!(
                        "#### Error writing to postgres: {} {:?}",
                        query.measurement, error
                    );
                }
            }
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}

pub fn spawn_postgres_writer(
    config: PostgresConfig,
) -> (SyncSender<SensorReading>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    let mut db_config = postgres::Config::new();
    let _ = db_config
        .host(&config.host)
        .port(config.port)
        .user(&config.username)
        .password(config.password)
        .dbname(&config.database);

    (
        tx,
        thread::spawn(move || {
            println!("starting postgres writer");
            start_postgres_writer(rx, db_config);
        }),
    )
}
