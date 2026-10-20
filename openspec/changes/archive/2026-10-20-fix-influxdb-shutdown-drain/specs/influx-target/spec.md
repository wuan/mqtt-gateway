# Spec Delta

## Purpose

Defines how the gateway buffers events destined for an `influxdb` target and
makes sure queued events are written rather than lost when the writer stops.

## ADDED Requirements

### Requirement: Queued events are drained on shutdown

The gateway SHALL, when shutdown is requested, write every event still buffered
in the InfluxDB target's channel and every batch not yet written before the
writer exits, so no queued event is lost.

#### Scenario: Buffered events are written on shutdown

- **WHEN** shutdown is requested while events are still buffered in the InfluxDB channel
- **THEN** the writer drains the buffered events, writes them, and exits without dropping any

#### Scenario: Partially accumulated batch is written on shutdown

- **WHEN** shutdown is requested while a batch is accumulated but not yet written and no further event is buffered
- **THEN** the accumulated batch is written before the writer exits

#### Scenario: Shutdown with nothing queued exits cleanly

- **WHEN** shutdown is requested with no buffered events and no unwritten batch
- **THEN** the writer exits without a write

### Requirement: Queued events are drained on channel disconnect

The gateway SHALL write every event still buffered in the InfluxDB target's
channel before the writer exits when the event channel disconnects.

#### Scenario: Disconnect writes buffered events

- **WHEN** the channel disconnects while events are still buffered in the InfluxDB channel
- **THEN** the writer drains the buffered events and writes them before exiting
