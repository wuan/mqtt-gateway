use paho_mqtt::Message;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Data {
    #[serde(rename = "time")]
    pub(crate) timestamp: i32,
    pub(crate) value: f32,
    pub(crate) unit: String,
    pub(crate) sensor: String,
    pub(crate) calculated: bool,
}

pub fn parse(msg: &Message) -> Result<Option<Data>, &'static str> {
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
    fn test_parse() -> Result<(), &'static str> {
        let topic = "klimalogger";
        let payload = "{\"host\": \"dana\", \"location\": \"Kinderzimmer 1\", \"type\": \"temperature\", \"unit\": \"\u{00b0C}\", \"sensor\": \"BME680\", \"calculated\": false, \"time\": \"2023-11-29T21:16:32.511722+00:00\", \"value\": 19.45}";

        let message = Message::new(topic, payload, QOS_1);
        let data = parse(&message)?.unwrap();

        assert_eq!(data.timestamp, 1701292592);

        Ok(())
    }

    #[test]
    fn test_parse_error() -> Result<(), &'static str> {
        let topic = "klimalogger";
        let payload = "{\"host\": \"dana\", \"location\": \"Kinderzimmer 1\", \"type\": \"temperature\", \"unit\": \"\u{00b0C}\", \"sensor\": \"BME680\", \"calculated\": false, \"time\": \"foo\", \"value\": 19.45}";

        let message = Message::new(topic, payload, QOS_1);
        let error = parse(&message).err().unwrap();

        assert_eq!(error, "could not deserialize JSON");

        Ok(())
    }
}