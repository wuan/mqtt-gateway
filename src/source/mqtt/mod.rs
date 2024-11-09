use paho_mqtt as mqtt;
use std::process;
use log::{error, info};

pub fn create_mqtt_client(mqtt_url: String, mqtt_client_id: String) -> mqtt::AsyncClient {
    info!("Connecting to the MQTT server at '{}'...", mqtt_url);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(mqtt_url)
        .client_id(mqtt_client_id)
        .finalize();

    mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        error!("Error creating the client: {:?}", e);
        process::exit(1);
    })
}