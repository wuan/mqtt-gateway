use paho_mqtt::Message;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Clone)]
struct Data {
    #[serde(rename = "time")]
    #[serde(with = "time::serde::rfc3339")]
    timestamp: OffsetDateTime,
    host: String,
    location: String,
    #[serde(rename = "type")]
    measurement_type: String,
    unit: String,
    sensor: String,
    calculated: bool,
}

fn parse(msg: &Message) -> Result<Option<Data>, &'static str> {
    let data = serde_json::from_slice::<Data>(msg.payload()).map_err(|error| {
        eprintln!("{:?}", error);
        "could not deserialize JSON"
    })?;
    Ok(Some(data.clone()))
}

#[cfg(test)]
mod tests {
    use paho_mqtt::QOS_1;

    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_parse_timestamp() -> Result<(), &'static str> {
        let topic = "klimalogger";
        let payload = "{\"host\": \"dana\", \"location\": \"Kinderzimmer 1\", \"type\": \"temperature\", \"unit\": \"\u{00b0C}\", \"sensor\": \"BME680\", \"calculated\": false, \"time\": \"2023-11-29T21:16:32.511722+00:00\", \"value\": 19.45}";

        let message = Message::new(topic, payload, QOS_1);
        let data = parse(&message)?.unwrap();

        assert_eq!(data.timestamp.unix_timestamp(), 1701292592);

        Ok(())
    }
}