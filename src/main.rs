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

use std::{env, process, time::Duration};
use std::ops::Deref;

use futures::{executor::block_on, stream::StreamExt};
use influxdb::{Client, WriteQuery};
use paho_mqtt as mqtt;
use paho_mqtt::QOS_1;

use data::parse::parse_timestamp;

use crate::data::parse::Timestamp;

mod data;


// The topics to which we subscribe.
const TOPICS: &[&str] = &["solar/#"];
const QOS: &[i32] = &[QOS_1];

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://mqtt:1883".to_string());

    println!("Connecting to the MQTT server at '{}'...", host);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(host)
        .client_id("solar_gateway")
        .finalize();

    // Create the client connection
    let mut cli = mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    });

    let client = Client::new("http://influx:8086", "solar");

    if let Err(err) = block_on(async {
        // Get message stream before connecting.
        let mut strm = cli.get_stream(25);

        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
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

        let mut timestamp: Option<Timestamp> = None;
        let mut point: Option<WriteQuery>;

        while let Some(msg_opt) = strm.next().await {
            point = None;

            if let Some(msg) = msg_opt {
                let mut split = msg.topic().split("/");
                let _ = split.next();
                let section = split.next();
                let element = split.next();
                if let (Some(section), Some(element)) = (section, element) {
                    let field = split.next();
                    if let Some(field) = field {
                        match element {
                            "0" => {
                                println!("  inverter: {:}: {:?}", field, msg.payload_str());
                                let value: f64 = msg.payload_str().parse().unwrap();
                                point = Some(WriteQuery::new(timestamp.as_ref().unwrap().to_influxdb(), field)
                                    .add_tag("device", section)
                                    .add_tag("component", "inverter")
                                    .add_field("value", value)
                                );
                            }
                            "device" => {
                                // ignore device global data
                                // println!("  device: {:}: {:?}", field, msg.payload_str())
                            }
                            "status" => {
                                if field == "last_update" {
                                    timestamp = parse_timestamp(msg.payload_str().deref()).ok();
                                    //println!("{:?}", timestamp);
                                } else {
                                    // ignore other status data
                                    // println!("  status: {:}: {:?}", field, msg.payload_str());
                                }
                            }
                            _ => {
                                let payload = msg.payload_str();
                                println!("  string {:}: {:}: {:?}", element, field, payload);
                                if payload.len() > 0 {
                                    let value: f64 = payload.parse().unwrap();
                                    point = Some(WriteQuery::new(timestamp.as_ref().unwrap().to_influxdb(), field)
                                        .add_tag("device", section)
                                        .add_tag("component", "string")
                                        .add_tag("string", element)
                                        .add_field("value", value)
                                    );
                                }
                            }
                        }
                    } else {
                        // global options -> ignore for now
                        // println!(" global {:}.{:}: {:?}", section, element, msg.payload_str())
                    }
                }

                if let Some(mut point) = point {

                    if let Some(timestamp) = &timestamp {
                        point = point.add_tag("year", timestamp.year)
                            .add_tag("month", timestamp.month)
                            .add_tag("year_month", timestamp.month_string.clone());
                    }
                    println!("   -> {:?}", &point);
                    let result = client.query(point).await;
                    if result.is_err() {
                        println!("{:?}", result.err())
                    }
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

        // Explicit return type for the async block
        Ok::<(), mqtt::Error>(())
    }) {
        eprintln!("{}", err);
    }
}