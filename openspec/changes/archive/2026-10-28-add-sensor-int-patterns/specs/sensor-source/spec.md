# Spec Delta

## ADDED Requirements

### Requirement: Plain integer payloads

For a pattern with `format` `int`, the gateway SHALL parse the payload as a
signed integer and SHALL emit an event stamped with the receive time, measured
by the topic's measurement segment and tagged with `location` and a `sensor` tag
that is the pattern's `sensor` when set and the location segment otherwise. A
payload that is not a strict integer SHALL be dropped with a warning.

#### Scenario: Integer payload emits a receive-time event

- **WHEN** the pattern `sensors/+/temperature` has format `int` and the payload `42` arrives on `sensors/kitchen/temperature`
- **THEN** an event with measurement `temperature`, tags `location=kitchen` and `sensor=kitchen`, value `42`, and the receive-time timestamp is emitted

#### Scenario: Negative integer payload is accepted

- **WHEN** the pattern `sensors/+/temperature` has format `int` and the payload `-3` arrives on `sensors/kitchen/temperature`
- **THEN** an event with value `-3` is emitted

#### Scenario: Int pattern sensor tag overrides the location default

- **WHEN** the matching `int` pattern declares `sensor: "raw"` and a payload arrives on `sensors/kitchen/temperature`
- **THEN** the emitted event carries `sensor=raw`

#### Scenario: Non-integer payload is dropped

- **WHEN** a payload such as `19.0`, `1.9e1`, or `not-a-number` arrives on a topic selected by an `int` pattern
- **THEN** no event is emitted and a warning is logged

#### Scenario: Int payload without location and measurement is dropped

- **WHEN** a message on a topic that has no location or measurement segment matches an `int` pattern
- **THEN** no event is emitted and a warning is logged
