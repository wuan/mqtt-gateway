# configuration Specification

## Purpose

Defines how the gateway discovers, expands, parses, and validates its `config.yml`
into the typed configuration used to connect to MQTT and route events to targets.

## Requirements

### Requirement: Configuration file discovery

The gateway SHALL locate its configuration file by searching `./config.yml` first
and then `./config/config.yml`, using the first match. If neither exists, startup
SHALL fail with an error.

#### Scenario: Configuration found in working directory
- **WHEN** `config.yml` exists in the working directory
- **THEN** the gateway loads that file

#### Scenario: Configuration found in config subdirectory
- **WHEN** `config.yml` exists only under `./config/`
- **THEN** the gateway loads `./config/config.yml`

#### Scenario: No configuration file present
- **WHEN** neither location contains `config.yml`
- **THEN** startup fails with an error explaining no configuration file was found

### Requirement: Environment variable expansion

Before parsing, the gateway SHALL replace every `${VAR_NAME}` placeholder in the
configuration text with the value of the corresponding environment variable.
Referencing an undefined variable SHALL be an error.

#### Scenario: Placeholder replaced with environment value
- **WHEN** the configuration contains `${MQTT_PASSWORD}` and that variable is set
- **THEN** the placeholder is replaced with the variable's value before parsing

#### Scenario: Undefined variable referenced
- **WHEN** the configuration references a variable that is not set
- **THEN** startup fails with an error naming the missing variable(s)

### Requirement: MQTT connection configuration

The configuration SHALL require `mqttUrl` and `mqttClientId`, and SHALL accept
optional `mqttUsername` and `mqttPassword`. The credentials SHALL be absent when
not specified.

#### Scenario: Configuration without credentials
- **WHEN** only `mqttUrl` and `mqttClientId` are provided
- **THEN** the configuration loads with no username or password

#### Scenario: Configuration with credentials
- **WHEN** `mqttUsername` and `mqttPassword` are provided
- **THEN** the configuration loads with those credentials

### Requirement: Source and target configuration

The configuration SHALL define a list of sources, each with a `name`, a `type`
marked by the `type` field, a `prefix`, and optional `targets`. Each target SHALL
be discriminated by its `type` field into `influxdb`, `postgresql`, or `debug`.

#### Scenario: Source with an InfluxDB target
- **WHEN** a source declares a target with `type: influxdb` and connection fields
- **THEN** the configuration loads that target as an InfluxDB target

#### Scenario: Source without targets
- **WHEN** a source declares no targets
- **THEN** the configuration loads the source with an empty target set

### Requirement: PostgreSQL target validation

A `postgresql` target SHALL only be accepted for `sensor` and `shelly` sources,
because only those sources provide the required `location`/`sensor`/`value`
schema. Any other source type using a PostgreSQL target SHALL be rejected.

#### Scenario: PostgreSQL target for a supported source
- **WHEN** a `sensor` or `shelly` source uses a `postgresql` target
- **THEN** validation succeeds

#### Scenario: PostgreSQL target for an unsupported source
- **WHEN** an `opendtu`, `openmqttgateway`, or `debug` source uses a `postgresql` target
- **THEN** startup fails with an error explaining the source lacks the required schema

### Requirement: Malformed configuration is rejected safely

The gateway SHALL reject configuration that is not valid YAML or does not match
the expected schema by returning an error. The parser SHALL NOT panic on malformed
input.

#### Scenario: Invalid YAML syntax
- **WHEN** the configuration file contains malformed YAML
- **THEN** startup fails with a parse error rather than panicking
