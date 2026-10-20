# Proposal

## Why

`OpenDTUParser` keeps one cached `timestamp: Option<i64>` for every inverter
and string. When a second inverter publishes its own
`solar/<serialB>/status/last_update`, that value replaces the first inverter's
timestamp, so readings from `solar/<serialA>/...` are then stamped with
inverter B's time (issue #407). The readings are written to InfluxDB at the
wrong time or overwrite earlier points that share the same tags
(`device`/`component`), silently corrupting the series.

## What Changes

- Track the last observed `status/last_update` timestamp per device section
  instead of in a single parser-wide field.
- Stamp every inverter and string reading with the timestamp of its own device
  section, so one inverter's `last_update` never affects another inverter.
- Keep the existing behavior of dropping a reading whose device has not yet
  published a `status/last_update` (no timestamp available).
- Preserve topic parsing, tag construction, and the target write path. No
  configuration, API, or database-format change.

## Capabilities

### New Capabilities

- `opendtu-source`: how the gateway parses OpenDTU `solar/<device>/...` topics
  into timestamped measurements, including how each device's `last_update`
  timestamp is tracked and applied.

### Modified Capabilities

<!-- None: the project has no existing opendtu-source spec. -->

## Impact

- `src/data/opendtu/mod.rs` - the `OpenDTUParser` timestamp storage and the
  `parse` dispatch that stamps inverter and string readings.
- Tests in `src/data/opendtu/mod.rs` covering multi-device timestamping.
- No new dependency, configuration option, or schema change.
