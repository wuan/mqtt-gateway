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
use std::ops::Deref;
use std::sync::mpsc::{Receiver, sync_channel};

use async_std::prelude::FutureExt;
use futures::{executor::block_on, SinkExt, stream::StreamExt};
use influxdb::{Client, Query, Timestamp, WriteQuery};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;
use serde::{Deserialize, Serialize};

use data::shelly;

use crate::data::klimalogger;

mod data;


// The topics to which we subscribe.
const TOPICS: &[&str] = &["sensors/#", "shellies/#"];
const QOS: &[i32] = &[QOS_1, QOS_1];


fn main() {
    // Initialize the logger from the environment
    env_logger::init();

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

    let (mut iot_tx, mut iot_rx) = sync_channel(100);

    let iot_influx_writer_handle = thread::spawn(move || {
        println!("starting influx writer");
        let database = "iot";

        start_influx_writer(iot_rx, database);
    });

    let (mut sensors_tx, mut sensors_rx) = sync_channel(100);

    let sensors_influx_writer_handle = thread::spawn(move || {
        println!("starting influx writer");
        let database = "klima";

        start_influx_writer(sensors_rx, database);
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

        enum WriteType {
            Int(i32),
            Float(f32),
        }

        while let Some(msg_opt) = strm.next().await {
            if let Some(msg) = msg_opt {
                if msg.topic().ends_with("/status/switch:0") {
                    let location = msg.topic().split("/").nth(1).unwrap();

                    let result = shelly::parse(&msg)?;

                    if let Some(data) = result {
                        println!("{} {:?}", location, data);

                        let timestamp = Timestamp::Seconds(data.energy.minute_ts as u128);
                        for (measurement, value, unit) in vec![
                            ("output", WriteType::Int(data.output as i32), "bool"),
                            ("power", WriteType::Float(data.power), "W"),
                            ("current", WriteType::Float(data.current), "A"),
                            ("voltage", WriteType::Float(data.voltage), "V"),
                            ("total_energy", WriteType::Float(data.energy.total), "Wh"),
                            ("temperature", WriteType::Float(data.temperature.t_C), "°C"),
                        ] {
                            let query = WriteQuery::new(timestamp, measurement);
                            let query = unsafe {
                                match value {
                                    WriteType::Int(i) => {
                                        query.add_field("value", i)
                                    }
                                    WriteType::Float(f) => {
                                        query.add_field("value", f)
                                    }
                                }
                            };

                            let query = query.add_tag("location", location)
                                .add_tag("sensor", "shelly")
                                .add_tag("type", "switch")
                                .add_tag("unit", unit);
                            iot_tx.send(query).expect("failed to send");
                        }
                    }
                } else if msg.topic().starts_with("sensors/") {
                    let mut split = msg.topic().split("/");
                    let location = split.nth(1).unwrap();
                    let measurement = split.next().unwrap();

                    let result = klimalogger::parse(&msg)?;

                    if let Some(result) = result {
                        println!("{} {} {:?}", location, measurement, &result);
                        let timestamp = Timestamp::Seconds(result.timestamp as u128);
                        let write_query = WriteQuery::new(timestamp, "data")
                            .add_tag("type", measurement)
                            .add_tag("location", location)
                            .add_tag("sensor", result.sensor)
                            .add_tag("calculated", result.calculated)
                            .add_field("value", result.value);
                        let write_query = if result.unit != "" {
                            write_query.add_tag("unit", result.unit)
                        } else {
                            write_query
                        };
                        sensors_tx.send(write_query).expect("failed to send");
                    }
                } else {
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

        // Explicit return type for the async block
        Ok::<(), mqtt::Error>(())
    }) {
        eprintln!("{}", err);
    }
}

fn start_influx_writer(mut iot_rx: Receiver<WriteQuery>, database: &str) {
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
            println!("writing to influx: {:?}", query);
            let _ = influx_client.query(query).await.expect("failed to write to influx");
        }
        println!("exiting influx writer async");
    });

    println!("exiting influx writer");
}

