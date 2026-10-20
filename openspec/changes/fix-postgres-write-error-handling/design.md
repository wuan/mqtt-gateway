# Design

## Context

`PostgresWriter::write_group` builds one multi-row `INSERT` per measurement and
calls `PostgresClient::execute` - a crate-local trait wrapping
`postgres::Client::execute`, mocked with `mockall` in tests. On any `Err` the
writer calls its `reconnect` closure, retries the identical statement, and on a
second failure drops the whole group. `postgres::Error` exposes `is_closed()`,
`as_db_error()`, and `code()`, but has no public constructor, so a test cannot
fabricate one. See proposal.md - Why.

## Goals / Non-Goals

**Goals:**

- Reconnect only when the connection is unusable.
- Preserve rows that are unaffected by another row's failure in the same group.
- Make both behaviors unit-testable without a live server.

**Non-Goals:**

- Changing the batch size/delay bounds or the generated SQL shape.
- Making the duplicate/conflict policy configurable.
- Persistent buffering or cross-flush deduplication: a duplicate already present
  in the table is still a row that cannot be written.
- Unbounded retries: after reconnecting, a group is retried at most once.

## Decisions

**Decision: return a crate-local `WriteError` from `PostgresClient::execute`.**
The writer must branch on *why* a write failed, so the error type carries a
`Connection` or `Statement` variant. `DefaultPostgresClient` converts
`postgres::Error` through a pure `classify` helper; mocks construct either
variant directly. Rationale: `postgres::Error` cannot be built in tests, so
keeping it as the trait error would leave the new branches untestable.
Alternatives: classify inside the writer (same testability problem), or add a
`classify(&Error)` trait method (still needs a real `Error` to exercise it).

**Decision: classify with a pure helper over SQLSTATE availability.** A failure
with no server error (transport, closed connection, encoding) or with SQLSTATE
class `08` (connection exception) or `57P0x` (shutdown) is a connection failure;
every other server error is a statement failure. Rationale: transport failures
carry no SQLSTATE, and the server reports connection loss through class `08`/`57`
before closing. Trade-off: a parameter-serialization error also has no SQLSTATE
and is classified as a connection failure; the row values are fixed scalar types
(`i64`, `&str`, `f64`), so this cannot occur in practice.

**Decision: on a statement failure, isolate rows instead of reconnecting.**
Retry the group's rows one at a time; rows that succeed are written and rows
that fail are dropped and reported. Rationale: a statement error (for example a
unique violation) fails identically on retry, and per-row isolation keeps every
unaffected row. Alternative: `INSERT ... ON CONFLICT DO NOTHING`, rejected
because it silently swallows every unique/exclusion conflict - including ones
unrelated to the expected `(time, location, sensor)` dedupe - and hides data
loss for deployments that rely on conflicts surfacing. Alternative: recursively
split the group in half, rejected as more round trips and code for the same
outcome as per-row isolation on an error path.

**Decision: retry the group once after a successful reconnect, then classify the
retry.** A statement failure on the retry isolates rows; another connection
failure reports and drops the group. Rationale: inserting individual rows
against a dead connection would only multiply failures.

## Risks / Trade-offs

- [A conflict that repeats every flush now reports once per row] -> throttle the
  row-level logs with `metrics::should_log`, and count each dropped row so the
  loss stays visible; silently ignoring conflicts is the rejected alternative.
- [Duplicates already in the table are still dropped] -> expected: the unique key
  defines them as duplicates; the change only stops them from taking unrelated
  rows with them.
- [Per-row fallback issues up to 100 statements for a bad group] -> it runs only
  on the error path and replaces a guaranteed whole-group loss.
- [Changing the trait error type touches every mock usage] -> mechanical; the
  compiler and the existing writer tests catch omissions.

## Migration Plan

No migration. Behavior-only change with no configuration, API, or data change;
rollback is reverting the commit.
