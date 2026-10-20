# Proposal

## Why

The PostgreSQL target treats every failed batch write as a broken connection.
`PostgresWriter::write_group` reconnects the client, retries the same multi-row
`INSERT`, and when that also fails it drops every row in the measurement group.
A server-reported error that can never succeed on retry - such as a duplicate
key on `(time, location, sensor)` from Shelly status messages that share a
`minute_ts` - therefore opens a new TCP/TLS connection on every flush and
silently loses the whole group, including rows that had no conflict at all
(issue #405).

## What Changes

- Reconnect and retry a group write only when the failure means the connection
  is unusable. Server-reported statement errors such as constraint violations
  no longer trigger a reconnect.
- When a group's `INSERT` fails with a statement error, preserve the rows that
  can be written by falling back to one statement per row; only the rows that
  genuinely cannot be written are dropped, and each is reported.
- Keep the existing batching bounds and the generated SQL shape unchanged. No
  configuration, API, or database-schema change.

## Capabilities

### New Capabilities
<!-- None: the behavior belongs to the existing `postgres-target` capability. -->

### Modified Capabilities
- `postgres-target`: adds requirements for connection-aware reconnection and for
  isolating a failed row so it does not discard the rest of its batch.

## Impact

- `src/target/postgres/mod.rs` - the `PostgresClient` error type and
  classification, `PostgresWriter::write_group`, `report_write_failure`, and the
  module tests.
- `src/metrics.rs` - the `POSTGRES_WRITE_FAILURES` doc comment now also covers
  isolated row failures.
- No new dependency, configuration option, or schema change.
