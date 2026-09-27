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
        // paho-mqtt accepts various URL formats at creation time. Use a
        // loopback address with a closed port so the failure is immediate and
        // does not depend on DNS resolution.
        let result = create_mqtt_client("tcp://127.0.0.1:1", "test_client");
        assert!(result.is_ok());
        let client = result.unwrap();
        let connect_result = client.connect(None);
        assert!(connect_result.is_err());
    }

    #[test]
    fn test_secure_uri_detection() {
        assert!(paho_mqtt::is_secure_uri("ssl://broker:8883"));
        assert!(paho_mqtt::is_secure_uri("mqtts://broker:8883"));
        assert!(paho_mqtt::is_secure_uri("wss://broker:443"));
        assert!(!paho_mqtt::is_secure_uri("tcp://broker:1883"));
        assert!(!paho_mqtt::is_secure_uri("mqtt://broker:1883"));
    }
}
