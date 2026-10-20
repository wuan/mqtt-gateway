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

### Requirement: Connection-aware reconnection

The PostgreSQL writer SHALL re-establish its database connection only when a
write fails because the connection is unusable. Server-reported statement
errors, such as a constraint violation, SHALL NOT trigger a reconnect.

#### Scenario: Connection failure reconnects and retries

- **WHEN** a group write fails because the connection is unusable
- **THEN** the writer opens a new connection and retries the group once

#### Scenario: Statement error does not reconnect

- **WHEN** a group write fails with a server-reported statement error such as a unique-constraint violation
- **THEN** the writer does not open a new connection

### Requirement: Row failure does not discard unaffected rows

The PostgreSQL writer SHALL write the rows of a measurement group that can be
written when another row in the same group cannot, and SHALL report each row it
drops instead of discarding the rows that remain.

#### Scenario: Conflicting row is isolated

- **WHEN** a group write fails because one row violates a constraint
- **THEN** the rows that do not violate it are written and only the offending row is dropped and reported

#### Scenario: Whole group is written when no row fails

- **WHEN** every row in a group can be written
- **THEN** the group is written and no row failure is reported
