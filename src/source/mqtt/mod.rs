use log::{error, info};
use paho_mqtt as mqtt;
use std::process;

pub fn create_mqtt_client(mqtt_url: String, mqtt_client_id: String) -> mqtt::Client {
    info!("Connecting to the MQTT server at '{}'...", mqtt_url);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(mqtt_url)
        .client_id(mqtt_client_id)
        .finalize();

    mqtt::Client::new(create_opts).unwrap_or_else(|e| {
        error!("Error creating the client: {:?}", e);
        process::exit(1);
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mqtt_client_success() {
        let client = create_mqtt_client("bad_url".to_string(), "test_client".to_string());

        assert_eq!(client.client_id(), "test_client");
    }

    #[test]
    fn test_create_mqtt_client_connect_failure() {
        let client = create_mqtt_client("bad_url".to_string(), "test_client".to_string());

        let result = client.connect(None);

        assert!(result.is_err());
    }
}
