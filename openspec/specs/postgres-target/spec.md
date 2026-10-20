# postgres-target Specification

## Purpose
Defines how the gateway batches events destined for a `postgresql` target and
when it writes them, so queued readings reach the database within a bounded
delay instead of waiting for a full batch.

## Requirements

### Requirement: Bounded batch flush delay

The gateway SHALL write a PostgreSQL target's queued events once the batch
reaches its maximum size or once the maximum delay has elapsed since the first
event was queued, whichever comes first. The maximum delay SHALL be enforced
while events keep arriving, not only once the source falls silent.

#### Scenario: Steady traffic flushes within the maximum delay

- **WHEN** fewer events than the maximum batch size are queued while further events keep arriving and the maximum delay elapses since the first event was queued
- **THEN** the queued events are written without waiting for the batch to reach its maximum size

#### Scenario: Quiet source flushes within the maximum delay

- **WHEN** fewer events than the maximum batch size are queued and no further event arrives while the maximum delay elapses
- **THEN** the queued events are written

#### Scenario: Full batch is written immediately

- **WHEN** the number of queued events reaches the maximum batch size before the maximum delay elapses
- **THEN** the batch is written immediately

#### Scenario: Nothing is written before a bound is reached

- **WHEN** fewer events than the maximum batch size are queued and the maximum delay has not elapsed since the first event was queued
- **THEN** no write occurs
