# Design

## Context

`PostgresWriter` keeps queued rows in `pending`, remembers when the first pending
row was queued in `first_queued_at`, and holds `max_size` (100) and `max_delay`
(1 second). `run` blocks on `recv_timeout(POLL_INTERVAL)` (1 second): an arriving
event goes through `push`, which flushes only when the batch reaches `max_size`,
while `flush_if_due` - the check that also honors `max_delay` - runs only on the
timeout branch. A source that keeps events flowing at least once per
`POLL_INTERVAL` therefore never reaches `flush_if_due`. See proposal.md - Why.

## Goals / Non-Goals

**Goals:**

- Guarantee a queued batch is written within `max_delay` of its first row at any
  arrival rate.
- Keep the size bound and the shutdown drain behavior unchanged.

**Non-Goals:**

- Making `max_delay` or `max_size` configurable.
- Crash-safe or persistent buffering: a hard kill can still lose the in-memory
  batch, but only the part that is less than `max_delay` old.
- Changing the retry/reconnect path or the generated SQL.

## Decisions

**Decision: enforce the delay inside `push`, and keep the timeout call in `run`.**
`push` appends the row and then calls `flush_if_due`, which already applies both
the size and age bounds; the inline `pending.len() >= max_size` check in `push`
becomes redundant and is removed. `run`'s timeout branch keeps calling
`flush_if_due` so a batch still flushes when the source falls silent. Rationale:
the enqueue point is where the clock starts, so that is where the bound belongs;
every caller (`run`, `drain`) then gets the guaranteed delay, and the behavior is
unit-testable by setting `first_queued_at` without depending on wall-clock
timing. Considered alternative: the issue's one-line fix calling
`flush_if_due` after `push` in `run`'s `Ok` branch. It also fixes the reported
case, but leaves delay enforcement split between `run` and `push` and is harder
to exercise deterministically in a unit test.

**Decision: reuse `flush_if_due` as the single "is a flush due" predicate.**
Keeping size and age in one function avoids two places that can drift apart.

**Decision: leave `POLL_INTERVAL` and `recv_timeout` unchanged.** The timeout
still serves the quiet-period flush and the shutdown check; the fix removes the
dependence on it for steady traffic rather than shortening it.

## Risks / Trade-offs

- [An extra elapsed-time comparison per event] -> negligible next to the
  existing row extraction and later SQL write.
- [A hard kill still loses the current in-memory batch] -> The loss window is now
  bounded by `max_delay` (about one second) instead of the time to fill
  `max_size`; durable buffering stays out of scope and would need a different
  design.
- [`drain` may now flush a due batch mid-drain] -> Harmless: `drain` still writes
  any remainder with its final `flush`, and flushing earlier reduces loss on
  shutdown.

## Migration Plan

No migration. Behavior-only change with no configuration, API, or data change;
rollback is reverting the commit.
