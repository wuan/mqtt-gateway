# Spec Delta

## MODIFIED Requirements

### Requirement: Source and target configuration

The configuration SHALL define a list of sources, each with a `name`, a `type`
marked by the `type` field, a `prefix`, optional `targets`, and an optional
`patterns` list. Each target SHALL be discriminated by its `type` field into
`influxdb`, `postgresql`, or `debug`. Each pattern SHALL have a `topic` filter, a
`format` of `json` (default), `float`, or `int`, and an optional `sensor` tag
name.

#### Scenario: Source with an InfluxDB target
- **WHEN** a source declares a target with `type: influxdb` and connection fields
- **THEN** the configuration loads that target as an InfluxDB target

#### Scenario: Source without targets
- **WHEN** a source declares no targets
- **THEN** the configuration loads the source with an empty target set

#### Scenario: Source with float patterns
- **WHEN** a sensor source declares a `patterns` entry with `topic: "sensors/+/temperature"` and `format: "float"`
- **THEN** the configuration loads that pattern with format `float`

#### Scenario: Source with integer patterns
- **WHEN** a sensor source declares a `patterns` entry with `topic: "sensors/+/count"` and `format: "int"`
- **THEN** the configuration loads that pattern with format `int`

#### Scenario: Pattern format defaults to JSON
- **WHEN** a pattern declares only a `topic`
- **THEN** the configuration loads that pattern with format `json`
