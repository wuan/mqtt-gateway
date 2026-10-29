# Spec Delta

## Purpose

Decodes messages received under a `sensor` source prefix into timestamped
measurement events, using the default JSON object encoding or configured
plain-number patterns.

## ADDED Requirements

### Requirement: JSON sensor payloads

The gateway SHALL decode a sensor message as a JSON object
`{ sensor, time, value }` when no configured pattern selects it, and SHALL emit
one event whose measurement is the topic's measurement segment, tagged with
`location` and `sensor` and stamped with the payload `time`.

#### Scenario: JSON payload emits an event

- **WHEN** a JSON payload `{"sensor":"BME680","time":1701292592,"value":19.45}` arrives on `sensors/kitchen/temperature`
- **THEN** an event with measurement `temperature`, tags `location=kitchen` and `sensor=BME680`, and value `19.45` is emitted

#### Scenario: Stale JSON timestamp is dropped

- **WHEN** a JSON payload whose `time` differs from the receive time by more than 60 seconds arrives
- **THEN** no event is emitted

### Requirement: Pattern-based topic selection

The gateway SHALL select the first configured pattern whose `topic` filter
matches the message topic, where `+` matches exactly one topic level and `#`
matches the remaining levels. A message that matches no pattern SHALL use the
default JSON decoding.

#### Scenario: First matching pattern wins

- **WHEN** patterns `sensors/+/temperature` and `sensors/#` are both configured and a message arrives on `sensors/kitchen/temperature`
- **THEN** the `sensors/+/temperature` pattern is applied

#### Scenario: Single-level wildcard matches one segment

- **WHEN** the pattern `sensors/+/temperature` is configured and a message arrives on `sensors/kitchen/temperature`
- **THEN** the pattern matches

#### Scenario: Multi-level wildcard matches the remaining segments

- **WHEN** the pattern `sensors/#` is configured and a message arrives on `sensors/kitchen/temperature`
- **THEN** the pattern matches

#### Scenario: Unmatched topic falls back to JSON

- **WHEN** only `sensors/+/temperature` is configured and a message arrives on `sensors/kitchen/humidity`
- **THEN** the message is decoded with the default JSON decoding

### Requirement: Plain float payloads

For a pattern with `format` `float`, the gateway SHALL parse the payload as a
floating point number and SHALL emit an event stamped with the receive time,
measured by the topic's measurement segment and tagged with `location` and a
`sensor` tag that is the pattern's `sensor` when set and the location segment
otherwise.

#### Scenario: Float payload emits a receive-time event

- **WHEN** the pattern `sensors/+/temperature` has format `float` and the payload `19.45` arrives on `sensors/kitchen/temperature`
- **THEN** an event with measurement `temperature`, tags `location=kitchen` and `sensor=kitchen`, value `19.45`, and the receive-time timestamp is emitted

#### Scenario: Pattern sensor tag overrides the location default

- **WHEN** the matching `float` pattern declares `sensor: "raw"` and a payload arrives on `sensors/kitchen/temperature`
- **THEN** the emitted event carries `sensor=raw`

#### Scenario: Non-numeric float payload is dropped

- **WHEN** a payload that is not a number arrives on a topic selected by a `float` pattern
- **THEN** no event is emitted and a warning is logged

#### Scenario: Float payload without location and measurement is dropped

- **WHEN** a message on a topic that has no location or measurement segment matches a `float` pattern
- **THEN** no event is emitted and a warning is logged
