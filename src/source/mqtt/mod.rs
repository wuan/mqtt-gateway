use paho_mqtt as mqtt;
use std::process;

pub fn create_mqtt_client(mqtt_url: String, mqtt_client_id: String) -> mqtt::AsyncClient {
    println!("Connecting to the MQTT server at '{}'...", mqtt_url);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(mqtt_url)
        .client_id(mqtt_client_id)
        .finalize();

    mqtt::AsyncClient::new(create_opts).unwrap_or_else(|e| {
        println!("Error creating the client: {:?}", e);
        process::exit(1);
    })
}