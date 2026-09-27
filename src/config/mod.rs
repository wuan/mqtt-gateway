use regex::Regex;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::LazyLock;

/// Matches `${VAR_NAME}` placeholders in the configuration file.
static ENV_VAR_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap());

/// Expand `${VAR_NAME}` placeholders using environment variables.
///
/// This keeps secrets (passwords, tokens) out of the configuration file, so it
/// can be committed and the values supplied via the environment or Docker
/// secrets. Referencing an undefined variable is an error.
pub(crate) fn expand_env(input: &str) -> anyhow::Result<String> {
    let mut missing: Vec<String> = Vec::new();

    let output =
        ENV_VAR_REGEX.replace_all(input, |caps: &regex::Captures| match env::var(&caps[1]) {
            Ok(value) => value,
            Err(_) => {
                missing.push(caps[1].to_string());
                String::new()
            }
        });

    if !missing.is_empty() {
        anyhow::bail!(
            "undefined environment variable(s) referenced in config: {}",
            missing.join(", ")
        );
    }

    Ok(output.into_owned())
}

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
        #[serde(default)]
        tls: bool,
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

impl Config {
    /// Validate cross-field constraints that `serde` cannot express.
    ///
    /// The PostgreSQL writer stores a `(time, location, sensor, value)` row and
    /// therefore requires the sensor schema. Attaching it to a source that does
    /// not produce `location`/`sensor` tags and a `value` field would silently
    /// drop every event, so reject such a configuration up front.
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        for source in &self.sources {
            let has_postgres = source.targets.as_ref().is_some_and(|targets| {
                targets
                    .iter()
                    .any(|t| matches!(t, Target::Postgresql { .. }))
            });

            if has_postgres
                && !matches!(source.source_type, SourceType::Sensor | SourceType::Shelly)
            {
                anyhow::bail!(
                    "source '{}' (type {:?}) uses a PostgreSQL target, but only \
                     'sensor' and 'shelly' sources provide the required \
                     location/sensor/value schema",
                    source.name,
                    source.source_type
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use log::debug;
    use serial_test::serial;

    #[test]
    fn test_deserialize_influxdb() -> Result<()> {
        let yaml = r#"
        type: "influxdb"
        url: "foo"
        database: "bar"
        "#;

        let result: Target = serde_yaml_ng::from_str(yaml).unwrap();

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

        let result: Target = serde_yaml_ng::from_str(yaml).unwrap();
        debug!("{:?}", result);

        if let Target::Postgresql {
            host,
            port,
            database,
            user,
            password,
            tls,
        } = result
        {
            assert_eq!(host, "foo");
            assert_eq!(port, 5432);
            assert_eq!(database, "bar");
            assert_eq!(user, "baz");
            assert_eq!(password, "qux");
            assert!(!tls);
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

        let result: Source = serde_yaml_ng::from_str(yaml).unwrap();

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

    fn config(source_type: SourceType, target: Target) -> Config {
        Config {
            sources: vec![Source {
                name: "test".to_string(),
                source_type,
                prefix: "test".to_string(),
                targets: Some(vec![target]),
            }],
            mqtt_url: "mqtt://localhost:1883".to_string(),
            mqtt_client_id: "test".to_string(),
        }
    }

    fn postgres_target() -> Target {
        Target::Postgresql {
            host: "localhost".to_string(),
            port: 5432,
            user: "user".to_string(),
            password: "password".to_string(),
            database: "db".to_string(),
            tls: false,
        }
    }

    #[test]
    fn test_validate_postgres_allowed_for_sensor_and_shelly() {
        assert!(config(SourceType::Sensor, postgres_target())
            .validate()
            .is_ok());
        assert!(config(SourceType::Shelly, postgres_target())
            .validate()
            .is_ok());
    }

    #[test]
    fn test_validate_rejects_postgres_for_unsupported_sources() {
        for source_type in [
            SourceType::OpenDTU,
            SourceType::OpenMqttGateway,
            SourceType::Debug,
        ] {
            let result = config(source_type.clone(), postgres_target()).validate();
            assert!(
                result.is_err(),
                "expected PostgreSQL target to be rejected for {:?}",
                source_type
            );
        }
    }

    #[test]
    fn test_validate_allows_influx_for_all_sources() {
        let target = Target::InfluxDB {
            url: "http://localhost:8086".to_string(),
            database: "db".to_string(),
            user: None,
            password: None,
            token: None,
        };

        for source_type in [
            SourceType::Sensor,
            SourceType::Shelly,
            SourceType::OpenDTU,
            SourceType::OpenMqttGateway,
            SourceType::Debug,
        ] {
            assert!(config(source_type, target.clone()).validate().is_ok());
        }
    }

    #[test]
    #[serial]
    fn test_expand_env_replaces_placeholders() {
        env::set_var("MQTT_GATEWAY_TEST_SECRET", "s3cr3t");
        let expanded = expand_env("password: ${MQTT_GATEWAY_TEST_SECRET}").unwrap();
        env::remove_var("MQTT_GATEWAY_TEST_SECRET");

        assert_eq!(expanded, "password: s3cr3t");
    }

    #[test]
    fn test_expand_env_leaves_other_text_untouched() {
        assert_eq!(expand_env("plain: value").unwrap(), "plain: value");
    }

    #[test]
    #[serial]
    fn test_expand_env_missing_variable_is_error() {
        env::remove_var("MQTT_GATEWAY_TEST_MISSING");
        let result = expand_env("password: ${MQTT_GATEWAY_TEST_MISSING}");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_allows_source_without_targets() {
        let config = Config {
            sources: vec![Source {
                name: "test".to_string(),
                source_type: SourceType::OpenDTU,
                prefix: "test".to_string(),
                targets: None,
            }],
            mqtt_url: "mqtt://localhost:1883".to_string(),
            mqtt_client_id: "test".to_string(),
        };

        assert!(config.validate().is_ok());
    }
}
