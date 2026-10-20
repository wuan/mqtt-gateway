# Tasks

## 1. Enforce the delay when enqueuing

- [ ] 1.1 In `PostgresWriter::push`, call `flush_if_due` after enqueuing a row and remove the now-redundant inline `pending.len() >= max_size` check, then verify the existing PostgreSQL tests still pass with `cargo test target::postgres`
- [ ] 1.2 Add a unit test that simulates steady arrivals by pushing events while `first_queued_at` is older than `max_delay` and verifies the batch is written without the receive timeout firing, and verify it passes
- [ ] 1.3 Add a unit test that the timeout path still flushes a quiet batch by calling `flush_if_due` on a writer whose `first_queued_at` predates `max_delay`, and verify it passes
- [ ] 1.4 Add a unit test that a batch below `max_size` and younger than `max_delay` is not written while one reaching `max_size` is written immediately, and verify it passes

## 2. Integration verification

- [ ] 2.1 Run `cargo test` across the whole crate and confirm every test, including the PostgreSQL writer tests, passes
- [ ] 2.2 Run `cargo fmt -- --check` and `cargo clippy --all-targets -- -D warnings` and confirm both are clean

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
