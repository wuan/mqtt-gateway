# Spec Delta

## ADDED Requirements

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
