use std::{env, process, thread, time::Duration};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread::JoinHandle;

use chrono::{DateTime, Utc};
use futures::{executor::block_on, stream::StreamExt};
use influxdb::{Client, Timestamp, WriteQuery};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;
use postgres::{Config, NoTls};

use crate::data::CheckMessage;
use crate::data::klimalogger::SensorLogger;
use crate::data::opendtu::OpenDTULogger;
use crate::data::shelly::ShellyLogger;

mod config;
mod data;


// The topics to which we subscribe.
const TOPICS: &[&str] = &["sensors/#", "shellies/#", "solar/#"];
const QOS: &[i32] = &[QOS_1, QOS_1, QOS_1];

#[derive(Debug, Clone)]
struct SensorReading {
    measurement: String,
    time: DateTime<Utc>,
    location: String,
    sensor: String,
    value: f32,
    unit: String,
    calculated: bool,
}

pub enum WriteType {
    Int(i32),
    Float(f32),
}

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::var_os("PG_HOST").unwrap().into_string().unwrap();
    let port = env::var_os("PG_PORT").unwrap().into_string().unwrap();
    let user = env::var_os("PG_USER").unwrap().into_string().unwrap();
    let password = env::var_os("PG_PASSWORD").unwrap().into_string().unwrap();
    let database = env::var_os("PG_DATABASE").unwrap().into_string().unwrap();
    let mut db_config = postgres::Config::new();
    let _ = db_config
        .host(&host)
        .port(port.parse::<u16>().unwrap())
        .user(&user)
        .password(password)
        .dbname(&database);

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://mqtt:1883".to_string());

    println!("Connecting to the MQTT server at '{}'...", host);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(host)
        .client_id("sensors_gateway")
        .finalize();

    // Create the client connection
    let mut cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    let (shelly_logger, iot_influx_writer_handle) = create_shelly_logger();

    let (sensor_logger, sensors_influx_writer_handle, sensors_postgres_writer_handle) = create_sensor_logger(db_config);

    let (opendtu_logger, solar_influx_writer_handle) = create_opendtu_logger();

    let handlers: [(String, Arc<Mutex<dyn CheckMessage>>); 3] = [
        ("shellies".to_string(), Arc::new(Mutex::new(shelly_logger))),
        ("sensors".to_string(), Arc::new(Mutex::new(sensor_logger))),
        ("solar".to_string(), Arc::new(Mutex::new(opendtu_logger))),
    ];

    if let Err(err) = block_on(async {
        // Get message stream before connecting.
        let mut strm = cli.get_stream(25);

        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
            .automatic_reconnect(Duration::from_secs(1), Duration::from_secs(300))
            .finalize();

        cli.connect(conn_opts).await?;

        println!("Subscribing to topics: {:?}", TOPICS);
        cli.subscribe_many(TOPICS, QOS).await?;

        // Just loop on incoming messages.
        println!("Waiting for messages...");

        // Note that we're not providing a way to cleanly shut down and
        // disconnect. Therefore, when you kill this app (with a ^C or
        // whatever) the server will get an unexpected drop and then
        // should emit the LWT message.


        while let Some(msg_opt) = strm.next().await {
            if let Some(msg) = msg_opt {
                let mut found = false;
                for (prefix, call) in &handlers {
                    let string = format!("{}/", prefix);
                    if msg.topic().starts_with(&string) {
                        found = true;
                        call.lock().unwrap().check_message(&msg);
                        break;
                    }
                }
                if !found {
                    println!("{} {:?}", msg.topic(), msg.payload_str());
                }
            } else {
                // A "None" means we were disconnected. Try to reconnect...
                println!("Lost connection. Attempting reconnect. {:?}", cli.is_connected());
                while let Err(err) = cli.reconnect().await {
                    println!("Error reconnecting: {}", err);
                    // For tokio use: tokio::time::delay_for()
                    async_std::task::sleep(Duration::from_millis(1000)).await;
                }
            }
        }

        iot_influx_writer_handle.join().expect("failed to join influx writer thread");
        sensors_influx_writer_handle.join().expect("failed to join influx writer thread");
        sensors_postgres_writer_handle.join().expect("failed to join influx writer thread");
        solar_influx_writer_handle.join().expect("failed to join influx writer thread");

        // Explicit return type for the async block
        Ok::<(), mqtt::Error>(())
    }) {
        eprintln!("{}", err);
    }
}

fn create_shelly_logger() -> (ShellyLogger, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    let influx_writer_handle = thread::spawn(move || {
        println!("starting influx writer");
        let database = "iot";

        start_influx_writer(rx, database, std::convert::identity);
    });

    let logger = ShellyLogger::new(tx);

    (logger, influx_writer_handle)
}

fn create_sensor_logger(db_config: Config) -> (SensorLogger, JoinHandle<()>, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    let influx_writer_handle = thread::spawn(move || {
        println!("starting influx writer");
        let database = "klima";

        fn mapper(result: SensorReading) -> WriteQuery {
            let timestamp = Timestamp::Seconds(result.time.timestamp() as u128);
            let write_query = WriteQuery::new(timestamp, "data")
                .add_tag("type", result.measurement.to_string())
                .add_tag("location", result.location.to_string())
                .add_tag("sensor", result.sensor.to_string())
                .add_tag("calculated", result.calculated)
                .add_field("value", result.value);
            if result.unit != "" {
                write_query.add_tag("unit", result.unit.to_string())
            } else {
                write_query
            }
        }

        start_influx_writer(rx, database, mapper);
    });

    let (ts_tx, ts_rx) = sync_channel(100);

    let postgres_writer_handle = thread::spawn(move || {
        println!("starting postgres writer");
        start_postgres_writer(ts_rx, db_config);
    });

    let logger = SensorLogger::new(vec![tx, ts_tx]);
    (logger, influx_writer_handle, postgres_writer_handle)
}

fn create_opendtu_logger() -> (OpenDTULogger, JoinHandle<()>) {
    let (tx, rx) = sync_channel(100);

    let influx_writer_handle = thread::spawn(move || {
        println!("starting OpenDTU influx writer");
        start_influx_writer(rx, "solar", std::convert::identity);
    });

    let logger = OpenDTULogger::new(tx);

    (logger, influx_writer_handle)
}

fn start_influx_writer<T>(iot_rx: Receiver<T>, database: &str, query_mapper: fn(T) -> WriteQuery) {
    let influx_client = Client::new("http://influx:8086", database);
    block_on(async move {
        println!("starting influx writer async");

        loop {
            let result = iot_rx.recv();
            let data = match result {
                Ok(query) => { query }
                Err(error) => {
                    println!("error receiving query: {:?}", error);
                    break;
                }
            };
            let query = query_mapper(data);
            let _ = influx_client.query(query).await.expect("failed to write to influx");
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}

fn start_postgres_writer(rx: Receiver<SensorReading>, config: Config) {
    let mut client = config.connect(NoTls).expect("failed to connect to postgres");

    block_on(async move {
        println!("starting postgres writer async");

        loop {
            let result = rx.recv();
            let query = match result {
                Ok(query) => { query }
                Err(error) => {
                    println!("error receiving query: {:?}", error);
                    break;
                }
            };

            let statement = format!("insert into \"{}\" (time, location, sensor, value, unit, calculated) values ($1, $2, $3, $4, $5, $6);", query.measurement);
            let x = client.execute(
                &statement,
                &[&query.time, &query.location, &query.sensor, &query.value, &query.unit, &query.calculated],
            );

            match x {
                Ok(_) => {}
                Err(error) => {
                    eprintln!("#### Error writing to postgres: {} {:?}", query.measurement, error);
                }
            }
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}

