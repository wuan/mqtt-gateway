# Proposal

## Why

When shutdown is requested (for example on SIGTERM or a redeploy), the
InfluxDB writer flushes only the batch it has already assembled and then exits.
Events still buffered in its `sync_channel(100)` are never read, so up to 100
queued readings are silently lost (issue #408). The PostgreSQL writer avoids
this by draining its channel before the final flush.

## What Changes

- Drain the InfluxDB writer's event channel with `try_recv()` before the final
  flush when shutdown is requested, so buffered events are mapped, queued, and
  written instead of dropped.
- Apply the same drain when the channel disconnects, matching the PostgreSQL
  writer.
- Keep the existing batching, accumulation-window flush, retry/backoff,
  timestamp validation, and topic-to-query mapping unchanged.

Out of scope: PostgreSQL behavior, any form of persistence or write-ahead log,
changing the channel capacity, and changing when shutdown is requested.

## Capabilities

### New Capabilities

- `influx-target`: how the gateway buffers events for an `influxdb` target and
  guarantees that queued events are written rather than lost when the writer
  stops.

### Modified Capabilities

<!-- None: no existing spec covers the influxdb target. -->

## Impact

- `src/target/influx/mod.rs` - add a `Writer::drain` method and call it on
  shutdown and on channel disconnect in place of the bare final `flush`.
- Tests in `src/target/influx/mod.rs` covering draining of buffered events.
- No new dependency, configuration option, or schema change.
