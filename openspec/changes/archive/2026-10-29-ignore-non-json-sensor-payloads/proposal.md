# Proposal

## Why

When a `sensor` source subscribes to a topic prefix, that prefix can also carry
messages that are not the expected `{ sensor, time, value }` JSON object (for
example plain-text state strings). The default JSON decode currently logs a
`WARN ... FAILED: ... Err(expected value at line 1 column 1)` line for every one
of these messages, drowning the log in noise for payloads that are simply not
JSON and were never meant to be decoded by the sensor logger.

## What Changes

- When the default sensor JSON decode fails because the payload is not valid
  JSON at all (a JSON syntax or unexpected-end-of-input error), silently ignore
  the message and log it at `debug` only, instead of `warn`.
- Keep warning when the payload is valid JSON but does not match the expected
  `{ sensor, time, value }` shape, since that indicates a producer or
  configuration problem rather than a foreign topic.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `sensor-source`: the JSON decoding requirement gains behavior for
  payloads that are not valid JSON — they are ignored (debug-logged) rather
  than warned about.

## Impact

- Sensor parser (`src/data/klimalogger/mod.rs`): `handle_json` and `parse`
  distinguish "not JSON" decode failures from "wrong shape" decode failures.
- Tests (`src/data/klimalogger/mod.rs`): add coverage for ignoring a non-JSON
  payload and for still warning on a JSON payload with the wrong shape.
- No breaking changes: valid JSON payloads continue to behave exactly as
  before, and non-JSON payloads never produced an event anyway.
