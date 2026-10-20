# Tasks

## 1. Classify write failures at the client boundary

- [ ] 1.1 Add a crate-local `WriteError` with `Connection` and `Statement` variants, return it from `PostgresClient::execute`, and convert `postgres::Error` in `DefaultPostgresClient` through a pure `classify` helper; verify with unit tests that exercise the helper for a closed/transport error and for SQLSTATE classes `08`, `57`, and other codes
- [ ] 1.2 Update the `MockPostgresClient` usages and existing writer tests to the new error type; verify `cargo test target::postgres` passes

## 2. Reconnect only on connection failures

- [ ] 2.1 In `write_group`, reconnect and retry the group once only for `WriteError::Connection`; verify with a unit test that a `WriteError::Statement` result never invokes the reconnect closure and that a `WriteError::Connection` result reconnects once and retries the same statement
- [ ] 2.2 Verify the reconnect-then-retry path reports and drops the group when the retry also fails with `WriteError::Connection`

## 3. Isolate row failures

- [ ] 3.1 When a group write fails with `WriteError::Statement`, retry the group's rows individually, write the rows that succeed, and report each row that fails; verify with a unit test where one conflicting row is dropped while the other rows are written
- [ ] 3.2 Update the `POSTGRES_WRITE_FAILURES` doc comment to cover isolated row failures and verify `cargo test metrics` passes

## 4. Integration verification

- [ ] 4.1 Run `cargo test` across the whole crate and confirm every test, including the PostgreSQL writer tests, passes
- [ ] 4.2 Run `cargo fmt -- --check` and `cargo clippy --all-targets -- -D warnings` and confirm both are clean

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
