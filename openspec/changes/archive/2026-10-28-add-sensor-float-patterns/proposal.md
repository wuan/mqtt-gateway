# Proposal

## Why

The `sensor` source only accepts JSON payloads shaped as `{ sensor, time, value }`.
Sensors that publish a bare number (for example a `19.45` temperature reading)
cannot be ingested, even though the gateway already handles plain-text payloads
for OpenDTU. Supporting simple scalar payloads lets these devices feed the same
InfluxDB/PostgreSQL pipeline without requiring a JSON wrapper.

## What Changes

- Add an optional `patterns` list to a `sensor` source. Each pattern has a
  `topic` filter (`+` matches one level, `#` matches the remaining levels), a
  `format` of `json` (default) or `float`, and an optional `sensor` tag name.
- When a message matches a `float` pattern, parse the payload as a bare number,
  stamp the event with the receive time, and emit a measurement tagged with
  `location` and `sensor`. Topics that match no pattern keep the existing JSON
  decoding.
- Validate pattern topics at startup (non-empty, no empty segments, `#` only as
  the final segment) and reject invalid configuration, naming the source.
- No breaking changes: existing configurations without `patterns` behave exactly
  as before.

## Capabilities

### New Capabilities
- `sensor-source`: how the sensor source decodes JSON payloads and configured
  plain-float payloads into timestamped measurement events.

### Modified Capabilities
- `configuration`: the source schema gains an optional `patterns` list and the
  matching pattern-topic validation.

## Impact

- Config schema (`src/config/mod.rs`): new `Pattern` and `ValueFormat` types and
  an optional `patterns` field on `Source`.
- Sensor parser (`src/data/klimalogger/mod.rs`): pattern matching and float
  decoding; `create_logger` accepts the patterns.
- Wiring (`src/domain/sources.rs`): passes a source's patterns to the sensor
  logger.
- Documentation: README configuration notes and the `config.yml` example.
