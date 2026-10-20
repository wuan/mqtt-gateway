# Design

## Context

`influxdb_writer` in `src/target/influx/mod.rs` owns a `Writer` that buffers
`WriteQuery`s and a `Receiver<Arc<LogEvent>>` from a `sync_channel(100)`
(`spawn_writer`). Its loop (around `src/target/influx/mod.rs:112`) receives one
event per iteration, queues the mapped query, and separately checks
`shutdown.is_requested()`. On shutdown it calls `writer.flush()` and breaks,
which writes only the `Writer`'s own pending batch. Events still sitting in the
channel buffer are never read and are dropped when the thread exits. The
PostgreSQL writer does not have this gap: its `run` checks shutdown first and
calls `drain` (`src/target/postgres/mod.rs:392`), which drains `try_recv()`
into the batch and then flushes. See proposal.md - Why for the reported impact.

The channel is a `std::sync::mpsc::sync_channel(100)`, so at most 100 events can
be buffered and `try_recv` never blocks.

## Goals / Non-Goals

**Goals:**

- On shutdown and on channel disconnect, write every event already buffered in
  the channel plus the pending batch before the writer thread exits.

**Non-Goals:**

- Changing batching, the accumulation window, retry/backoff, timestamp
  validation, or query mapping.
- Persisting events across process restarts, changing the channel capacity, or
  adding backpressure.
- Reordering the receiver side or the PostgreSQL writer.

## Decisions

**Decision: add `Writer::drain(&mut self, rx: &Receiver<Arc<LogEvent>>)` that
maps every buffered event with `try_recv()` and then calls `flush()`.**
This mirrors `PostgresWriter::drain` and keeps the mapping
(`map_to_query(&event)`) identical to the receive path, so a drained event is
written exactly as it would have been had it been received normally. Wrapping
the drain in `Writer` keeps the receiver/queue logic in one place.

**Decision: call `drain` on both the shutdown branch and the `Disconnected`
branch, replacing the bare `flush`.**
On `Disconnected` the buffer is normally already empty (mpsc yields buffered
messages before reporting disconnect), so `drain` is equivalent to `flush`
there; using one path for both avoids a second, subtly different shutdown
route and matches the PostgreSQL writer. The `Disconnected` branch also keeps
its existing warning log.

**Decision: keep the existing loop shape (process-or-timeout, then check
shutdown) rather than adopting PostgreSQL's check-first shape.**
Moving the check to the top would consume no buffered events on the first
iteration and then drain them, which also works, but it changes when the
"process the event before reacting to shutdown" guarantee applies and is a
larger edit for no behavioral gain. Calling `drain` in the existing shutdown
branch fixes the loss with the smallest, most reviewable diff.

**Alternative considered: increase the channel capacity or switch to an
unbounded channel.** Rejected: it only reduces the number of lost events and
does not address the exit-without-draining root cause.

**Alternative considered: have the receiver flush/await the writer.** Rejected:
the receiver does not own the channel buffer and already joins the writer
threads; the loss is entirely inside the writer.

## Risks / Trade-offs

- [Draining more events lengthens shutdown slightly] -> The buffer holds at
  most 100 events and `flush` batches them into one write, so the added cost is
  one extra write attempt (with the existing bounded retry budget), not one
  request per event.
- [A drain write can fail like any other write] -> It uses the same `flush`
  path and therefore the same `WRITE_MAX_ATTEMPTS` retry and failure metric;
  behavior on failure is unchanged.
- [Blocking on `try_recv` is impossible] -> `try_recv` is non-blocking and
  terminates on both `Empty` and `Disconnected` via `while let Ok(..)`, so the
  drain cannot hang shutdown.

## Migration Plan

No data migration. After upgrade, events queued at shutdown are written rather
than dropped. No configuration or schema changes; rollback is a code revert.
