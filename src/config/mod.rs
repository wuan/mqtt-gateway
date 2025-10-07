use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum SourceType {
    #[serde(rename = "shelly")]
    Shelly,
    #[serde(rename = "sensor")]
    Sensor,
    #[serde(rename = "opendtu")]
    OpenDTU,
    #[serde(rename = "openmqttgateway")]
    OpenMqttGateway,
    #[serde(rename = "debug")]
    Debug,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Source {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) source_type: SourceType,
    pub(crate) prefix: String,
    pub(crate) targets: Option<Vec<Target>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
#[serde(tag = "type")]
pub enum Target {
    #[serde(rename = "influxdb")]
    InfluxDB {
        url: String,
        database: String,
        user: Option<String>,
        password: Option<String>,
        token: Option<String>,
    },
    #[serde(rename = "postgresql")]
    Postgresql {
        host: String,
        port: u16,
        user: String,
        password: String,
        database: String,
    },
    #[serde(rename = "debug")]
    Debug {},
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Config {
    pub(crate) sources: Vec<Source>,
    #[serde(rename = "mqttUrl")]
    pub(crate) mqtt_url: String,
    #[serde(rename = "mqttClientId")]
    pub(crate) mqtt_client_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use log::debug;

    #[test]
    fn test_deserialize_influxdb() -> Result<()> {
        let yaml = r#"
        type: "influxdb"
        url: "foo"
        database: "bar"
        "#;

        let result: Target = serde_yaml_ng::from_str(&yaml).unwrap();

        if let Target::InfluxDB { url, database, .. } = result {
            assert_eq!(url, "foo");
            assert_eq!(database, "bar");
        } else {
            panic!("wrong type");
        }

        Ok(())
    }

    #[test]
    fn test_deserialize_postgresql() -> Result<()> {
        let yaml = r#"
        type: "postgresql"
        host: "foo"
        port: 5432
        database: "bar"
        user: "baz"
        password: "qux"
        "#;

        let result: Target = serde_yaml_ng::from_str(&yaml).unwrap();
        debug!("{:?}", result);

        if let Target::Postgresql {
            host,
            port,
            database,
            user,
            password,
        } = result
        {
            assert_eq!(host, "foo");
            assert_eq!(port, 5432);
            assert_eq!(database, "bar");
            assert_eq!(user, "baz");
            assert_eq!(password, "qux");
        } else {
            panic!("wrong type");
        }

        Ok(())
    }

    #[test]
    fn test_deserialize_source() -> Result<()> {
        let yaml = r#"
        name: "foo"
        type: "sensor"
        prefix: "bar"
        targets:
          - type: "influxdb"
            url: "baz"
            database: "qux"
        "#;

        let result: Source = serde_yaml_ng::from_str(&yaml).unwrap();

        assert_eq!(result.name, "foo");
        assert_eq!(result.source_type, SourceType::Sensor);
        assert_eq!(result.prefix, "bar");

        let targets = result.targets.unwrap();
        assert_eq!(targets.len(), 1);
        let target = &targets[0];

        if let Target::InfluxDB { url, database, .. } = target {
            assert_eq!(url, "baz");
            assert_eq!(database, "qux");
        } else {
            panic!("wrong type");
        }

        Ok(())
    }
}
