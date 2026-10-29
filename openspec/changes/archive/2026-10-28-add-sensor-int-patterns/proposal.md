# Proposal

## Why

Sensor patterns currently decode non-JSON payloads only as `float` or `json`.
Some devices publish whole numbers (for example counters, states, or setpoints)
where a floating point representation is misleading and, for integer-only
targets, loses the intent that the reading is an integer. A dedicated `int`
format lets operators declare those topics explicitly.

## What Changes

- Add an `int` value to the sensor pattern `format` enum.
- For an `int` pattern, parse the payload as a signed integer and emit the event
  with an integer value, otherwise following the same rules as `float`
  (receive-time stamp, `location`/`sensor` tags, first-match wins, receive
  prefix routing).
- Reject payloads that are not strict integers (for example `19.0` or `1.9e1`)
  with a warning and no event, mirroring how a `float` pattern drops a
  non-numeric payload.
- Document the `int` format in the README configuration notes and the commented
  `config.yml` example.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `sensor-source`: add a plain integer payload format alongside the existing
  plain float format, including the parsing rules and drop behavior.
- `configuration`: the pattern `format` enum accepts `int` in addition to
  `json` and `float`.

## Impact

- Configuration schema (`src/config/mod.rs`): `ValueFormat` gains `Int`.
- Sensor parser (`src/data/klimalogger/mod.rs`): dispatch and an integer
  decode path; the emitted value uses the existing `Number::Int` field type.
- Documentation (`README.md`, `config.yml`).
- No breaking changes: existing configurations are unaffected and non-integer
  payloads on an `int` pattern are dropped rather than coerced.
