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

mod data;

use futures::{executor::block_on, stream::StreamExt};
use paho_mqtt as mqtt;
use std::{env, process, time::Duration};
use influxdb::{Client, Timestamp, WriteQuery};
use influxdb::Timestamp::Seconds;
use paho_mqtt::QOS_1;

// The topics to which we subscribe.
const TOPICS: &[&str] = &["solar/#"];
const QOS: &[i32] = &[QOS_1];

/////////////////////////////////////////////////////////////////////////////

fn main() {
    // Initialize the logger from the environment
    env_logger::init();

    let host = env::args()
        .nth(1)
        .unwrap_or_else(|| "mqtt://mqtt:1883".to_string());

    println!("Connecting to the MQTT server at '{}'...", host);

    // Create the client. Use a Client ID for a persistent session.
    // A real system should try harder to use a unique ID.
    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(host)
        .client_id("solar_test")
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

        // Define the set of options for the connection
        let lwt = mqtt::Message::new(
            "test/lwt",
            "[LWT] Async subscriber lost connection",
            mqtt::QOS_1,
        );

        // Create the connect options, explicitly requesting MQTT v3.x
        let conn_opts = mqtt::ConnectOptionsBuilder::new_v3()
            .keep_alive_interval(Duration::from_secs(30))
            .clean_session(false)
            .will_message(lwt)
            .finalize();

        // Make the connection to the broker
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
                                point = Some(WriteQuery::new(timestamp.unwrap(), field)
                                    .add_tag("device", section)
                                    .add_tag("component", "inverter")
                                    .add_field("value", value)
                                );
                            }
                            "device" => {
                                // println!("  device: {:}: {:?}", field, msg.payload_str())
                            }
                            "status" => {
                                if field == "last_update" {
                                    let timestamp_string = msg.payload_str().to_string();
                                    timestamp = timestamp_string.parse().map(move |value| Seconds(value)).ok();
                                    println!("{:?}", timestamp);
                                } else {
                                    // println!("  status: {:}: {:?}", field, msg.payload_str());
                                }
                            }
                            _ => {
                                println!("  string {:}: {:}: {:?}", element, field, msg.payload_str());
                                let value: f64 = msg.payload_str().parse().unwrap();
                                point = Some(WriteQuery::new(timestamp.unwrap(), field)
                                    .add_tag("device", section)
                                    .add_tag("component", "string")
                                    .add_tag("string", element)
                                    .add_field("value", value)
                                );
                            }
                        }
                    } else {
                        // println!(" global {:}.{:}: {:?}", section, element, msg.payload_str())
                    }
                }

                if let Some(point) = point {
                    println!("   -> {:?}", &point);
                    let result = client.query(point).await;
                }

                // let result: serde_json::Result<LogEvent> = serde_json::from_slice(msg.payload());
                // println!("{:?}", result.unwrap());
            } else {
                // A "None" means we were disconnected. Try to reconnect...
                println!("Lost connection. Attempting reconnect.");
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