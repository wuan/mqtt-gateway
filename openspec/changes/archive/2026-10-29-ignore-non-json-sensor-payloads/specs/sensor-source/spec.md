# Spec Delta

## ADDED Requirements

### Requirement: Non-JSON sensor payloads are ignored

Under the default JSON decoding, when a message payload is not valid JSON the
gateway SHALL ignore the message: no event is emitted and the message is
recorded at debug level only, without a warning. A payload that is valid JSON
but does not decode to the `{ sensor, time, value }` object SHALL be dropped
with a warning, because it signals a producer or configuration problem.

#### Scenario: Non-JSON payload is ignored quietly

- **WHEN** a payload that is not valid JSON (for example `heating`) arrives on a topic decoded with the default JSON decoding
- **THEN** no event is emitted and the message is logged at debug level, not as a warning

#### Scenario: Valid JSON with the wrong shape still warns

- **WHEN** a payload that is valid JSON but not a `{ sensor, time, value }` object (for example `42` or `{"a":1}`) arrives on a topic decoded with the default JSON decoding
- **THEN** no event is emitted and a warning is logged
