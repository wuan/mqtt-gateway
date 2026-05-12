use anyhow::Context;
use log::info;
use paho_mqtt as mqtt;

pub fn create_mqtt_client(mqtt_url: &str, mqtt_client_id: &str) -> anyhow::Result<mqtt::Client> {
    info!("Connecting to the MQTT server at '{}'...", mqtt_url);

    let create_opts = mqtt::CreateOptionsBuilder::new_v3()
        .server_uri(mqtt_url)
        .client_id(mqtt_client_id)
        .finalize();

    mqtt::Client::new(create_opts)
        .with_context(|| format!("Failed to create MQTT client for URL: {}", mqtt_url))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_mqtt_client_success() {
        let result = create_mqtt_client("tcp://localhost:1883", "test_client");
        assert!(result.is_ok());
        let client = result.unwrap();
        assert_eq!(client.client_id(), "test_client");
    }

    #[test]
    fn test_create_mqtt_client_connect_failure() {
        // paho-mqtt accepts various URL formats at creation time
        // Connection fails later with invalid URLs
        let result = create_mqtt_client("tcp://invalid-host:1883", "test_client");
        assert!(result.is_ok());
        let client = result.unwrap();
        let connect_result = client.connect(None);
        assert!(connect_result.is_err());
    }
}
