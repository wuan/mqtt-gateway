# Proposal

## Why

A `postgresql` target accumulates events in memory and writes them when the batch
reaches 100 rows or when a 1-second maximum delay has elapsed since the first
queued row. The delay is only checked when the writer's 1-second `recv_timeout`
fires, so a source that keeps producing events at least once per second never
triggers the check. Rows then wait for a full batch - about 50 seconds at one
event every 0.5 seconds - while the configured delay promises roughly one, and up
to 99 rows are lost on a hard kill instead of roughly one second's worth
(issue #404).

## What Changes

- Enforce the batch maximum delay whenever an event is queued, not only when the
  receive timeout fires, so a batch is written within the maximum delay of its
  first event regardless of the arrival rate.
- Keep the existing size bound: a batch is still written as soon as it reaches
  100 rows, and the delay check decides only batches that stay below it.
- Keep `recv_timeout` on the current interval so the quiet-period check and the
  shutdown poll are unchanged. No configuration, API, or database-schema change.

## Capabilities

### New Capabilities

- `postgres-target`: how the gateway batches events destined for a `postgresql`
  target and writes them, including the size and maximum-delay flush bounds.

### Modified Capabilities

<!-- None: the batching behavior has no existing spec. -->

## Impact

- `src/target/postgres/mod.rs` - `PostgresWriter::push` / `flush_if_due`.
- Unit and writer-level tests in the same module.
- No dependency, configuration, or documentation change: the flush bounds are
  internal constants and the fix makes behavior match what the code already
  advertises.
