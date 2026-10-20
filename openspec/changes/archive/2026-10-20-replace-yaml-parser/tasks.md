# Tasks

## 1. Swap the YAML dependency

- [x] 1.1 In `Cargo.toml`, replace `serde_yaml_ng` with
  `serde-saphyr = { version = "2", default-features = false, features = ["deserialize"] }`;
  verify the manifest lists the new dependency with `default-features = false`
- [x] 1.2 Regenerate `Cargo.lock` (build or `cargo update`) and verify
  `cargo tree -i serde_yaml_ng` and `cargo tree -i unsafe-libyaml` both report
  the package is not in the graph

## 2. Migrate the parsing call sites

- [x] 2.1 Update the config load in `src/main.rs` to call
  `serde_saphyr::from_str`, keeping the existing `anyhow` context; verify
  `cargo build` succeeds
- [x] 2.2 Update the five `from_str` test call sites in `src/config/mod.rs`;
  verify `cargo test config` passes

## 3. Verify behavior and policy

- [x] 3.1 Run the full `cargo test` suite and verify all tests pass
- [x] 3.2 Parse a real `config.yml` end-to-end: verify the binary starts and
  logs the loaded source count, and that a malformed config file fails with a
  parse error rather than panicking
- [x] 3.3 Run `cargo deny check advisories` and verify `RUSTSEC-2024-0449` /
  `unsafe-libyaml` no longer appears (the unrelated `rustls` advisories may
  remain; they are out of scope for this change)
- [x] 3.4 Run `cargo fmt -- --check` and
  `cargo clippy --all-targets -- -D warnings`; verify both are clean

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
