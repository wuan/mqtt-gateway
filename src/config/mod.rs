use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum SourceType {
    #[serde(rename = "shelly")]
    Shelly,
    #[serde(rename = "sensor")]
    Sensor,
    #[serde(rename = "opendtu")]
    OpenDTU,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Source {
    name: String,
    #[serde(rename = "type")]
    pub(crate) source_type: SourceType,
    pub(crate) prefix: String,
    pub(crate) targets: Vec<Target>,
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
    },
    #[serde(rename = "postgresql")]
    Postgresql {
        host: String,
        port: u16,
        user: String,
        password: String,
        database: String,
    },
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq)]
pub struct Config {
    pub(crate) sources: Vec<Source>,
    #[serde(rename = "mqttUrl")]
    pub(crate) mqtt_url: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_influxdb() -> Result<(), &'static str> {
        let yaml = r#"
        type: "influxdb"
        host: "foo"
        port: 8086
        database: "bar"
        "#;

        let result: Target = serde_yaml::from_str(&yaml).unwrap();

        if let Target::InfluxDB { host, port, database } = result {
            assert_eq!(host, "foo");
            assert_eq!(port, 8086);
            assert_eq!(database, "bar");
        } else {
            panic!("wrong type");
        }

        Ok(())
    }

    #[test]
    fn test_deserialize_postgresql() -> Result<(), &'static str> {
        let yaml = r#"
        type: "postgresql"
        host: "foo"
        port: 5432
        database: "bar"
        user: "baz"
        password: "qux"
        "#;

        let result: Target = serde_yaml::from_str(&yaml).unwrap();
        println!("{:?}", result);

        if let Target::Postgresql { host, port, database, user, password } = result {
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
    fn test_deserialize_source() -> Result<(), &'static str> {
        let yaml = r#"
        name: "foo"
        type: "sensor"
        prefix: "bar"
        targets:
          - type: "influxdb"
            host: "baz"
            port: 8080
            database: "qux"
        "#;

        let result: Source = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(result.name, "foo");
        assert_eq!(result.source_type, SourceType::Sensor);
        assert_eq!(result.prefix, "bar");

        assert!(result.targets.len() == 1);
        let target = &result.targets[0];

        if let Target::InfluxDB { host, port, database } = target {
            assert_eq!(host, "baz");
            assert_eq!(*port, 8080u16);
            assert_eq!(database, "qux");
        } else {
            panic!("wrong type");
        }

        Ok(())
    }
}