# Spec Delta

## MODIFIED Requirements

### Requirement: Source and target configuration

The configuration SHALL define a list of sources, each with a `name`, a `type`
marked by the `type` field, a `prefix`, optional `targets`, and an optional
`patterns` list. Each target SHALL be discriminated by its `type` field into
`influxdb`, `postgresql`, or `debug`. Each pattern SHALL have a `topic` filter, a
`format` of `json` (default) or `float`, and an optional `sensor` tag name.

#### Scenario: Source with an InfluxDB target
- **WHEN** a source declares a target with `type: influxdb` and connection fields
- **THEN** the configuration loads that target as an InfluxDB target

#### Scenario: Source without targets
- **WHEN** a source declares no targets
- **THEN** the configuration loads the source with an empty target set

#### Scenario: Source with float patterns
- **WHEN** a sensor source declares a `patterns` entry with `topic: "sensors/+/temperature"` and `format: "float"`
- **THEN** the configuration loads that pattern with format `float`

#### Scenario: Pattern format defaults to JSON
- **WHEN** a pattern declares only a `topic`
- **THEN** the configuration loads that pattern with format `json`

## ADDED Requirements

### Requirement: Pattern topic validation

The configuration SHALL reject a source pattern whose `topic` is empty, contains
an empty segment (a leading, trailing, or doubled `/`), or uses `#` anywhere but
the final segment. The error SHALL name the offending source.

#### Scenario: Empty pattern topic is rejected
- **WHEN** a source declares a pattern with an empty `topic`
- **THEN** validation fails with a message naming the source

#### Scenario: Pattern with an empty segment is rejected
- **WHEN** a source declares a pattern topic such as `/sensors/+`, `sensors//+`, or `sensors/+/`
- **THEN** validation fails with a message naming the source

#### Scenario: Non-final multi-level wildcard is rejected
- **WHEN** a source declares a pattern topic such as `sensors/#/temperature`
- **THEN** validation fails with a message naming the source

#### Scenario: Valid pattern topics are accepted
- **WHEN** a source declares the pattern topics `sensors/+/temperature`, `sensors/#`, or `#`
- **THEN** validation succeeds
