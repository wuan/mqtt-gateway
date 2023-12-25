// paho-mqtt/examples/async_subscribe.rs
//
// This is a Paho MQTT Rust client, sample application.
//
//! This application is an MQTT subscriber using the asynchronous client
//! interface of the Paho Rust client library.
//! It also monitors for disconnects and performs manual re-connections.
//!
//! The sample demonstrates:
//!   - An async/await subscriber
//!   - Connecting to an MQTT server/broker.
//!   - Subscribing to topics
//!   - Receiving messages from an async stream.
//!   - Handling disconnects and attempting manual reconnects.
//!   - Using a "persistent" (non-clean) session so the broker keeps
//!     subscriptions and messages through reconnects.
//!   - Last will and testament
//!
//! Note that this example specifically does *not* handle a ^C, so breaking
//! out of the app will always result in an un-clean disconnect causing the
//! broker to emit the LWT message.

/*******************************************************************************
 * Copyright (c) 2017-2023 Frank Pagliughi <fpagliughi@mindspring.com>
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Frank Pagliughi - initial implementation and documentation
 *******************************************************************************/

use std::{env, process, thread, time::Duration};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::mpsc::{Receiver, sync_channel};

use async_std::prelude::FutureExt;
use chrono::{DateTime, Utc};
use futures::{executor::block_on, SinkExt, stream::StreamExt};
use influxdb::{Client, Query, WriteQuery};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;
use postgres::{Config, NoTls};
use serde::{Deserialize, Serialize};

use crate::data::CheckMessage;
use crate::data::klimalogger::Sensorlogger;
use crate::data::shelly::{ShellyLogger, Timestamped};

mod data;


// The topics to which we subscribe.
const TOPICS: &[&str] = &["sensors/#", "shellies/#"];
const QOS: &[i32] = &[QOS_1, QOS_1];

#[derive(Debug)]
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

    let u = match env::var_os("USER") {
        Some(v) => v.into_string().unwrap(),
        None => panic!("$USER is not set")
    };

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

    let (mut iot_tx, iot_rx) = sync_channel(100);

    let iot_influx_writer_handle = thread::spawn(move || {
        println!("starting influx writer");
        let database = "iot";

        start_influx_writer(iot_rx, database);
    });

    let (sensors_tx, sensors_rx) = sync_channel(100);

    let sensors_influx_writer_handle = thread::spawn(move || {
        println!("starting influx writer");
        let database = "klima";

        start_influx_writer(sensors_rx, database);
    });

    let (ts_sensors_tx, ts_sensors_rx) = sync_channel(100);

    let sensors_postgres_writer_handle = thread::spawn(move || {
        println!("starting postgres writer");
        start_postgres_writer(ts_sensors_rx, &db_config);
    });

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

        let shelly_logger = ShellyLogger::new(&iot_tx);
        let sensor_logger = Sensorlogger::new(sensors_tx, ts_sensors_tx);

        let iter: [(&str, &dyn CheckMessage);2] = [
            ("shellies", &shelly_logger),
            ("sensors", &sensor_logger),
        ];
        let handler_map = HashMap::<&str, &dyn CheckMessage>::from_iter(iter);

        while let Some(msg_opt) = strm.next().await {
            if let Some(msg) = msg_opt {
                let mut found = false;
                for (&prefix, &call) in handler_map.iter() {
                    let string = format!("{}/", prefix);
                    if msg.topic().starts_with(&string) {
                        found = true;
                        call.check_message(&msg);
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

        drop(iot_tx);
        iot_influx_writer_handle.join().expect("failed to join influx writer thread");
        sensors_influx_writer_handle.join().expect("failed to join influx writer thread");
        sensors_postgres_writer_handle.join().expect("failed to join influx writer thread");

        // Explicit return type for the async block
        Ok::<(), mqtt::Error>(())
    }) {
        eprintln!("{}", err);
    }
}


fn start_influx_writer(iot_rx: Receiver<WriteQuery>, database: &str) {
    let influx_client = Client::new("http://influx:8086", database);
    block_on(async move {
        println!("starting influx writer async");

        loop {
            let result = iot_rx.recv();
            let query = match result {
                Ok(query) => { query }
                Err(error) => {
                    println!("error receiving query: {:?}", error);
                    break;
                }
            };
            let _ = influx_client.query(query).await.expect("failed to write to influx");
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}

fn start_postgres_writer(rx: Receiver<SensorReading>, config: &Config) {
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

