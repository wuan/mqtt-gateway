# Tasks

## 1. Longest-prefix routing

- [ ] 1.1 Add a helper on `Sources` that returns the configured prefix matching a topic on a segment boundary with the longest match, and verify it with unit tests covering `home/shelly` vs `home`, partial-segment `home/shellybox`, and the exact-topic case
- [ ] 1.2 Use that helper in `Sources::handle` instead of `split('/').next()` and verify a multi-segment prefix delivers its message while unrelated topics stay "unhandled"
- [ ] 1.3 Keep `get_handler` lookups by exact prefix working for existing tests and verify `cargo test` passes for the `domain::sources` module

## 2. Configuration validation

- [ ] 2.1 Extend `Config::validate` to reject empty prefixes, leading/trailing `/`, and `+`/`#` in a prefix, and verify each rejection with a unit test that asserts the error names the source
- [ ] 2.2 Verify `Config::validate` still accepts single-segment and multi-segment prefixes such as `home/shelly` with a unit test

## 3. Documentation

- [ ] 3.1 Document the accepted `prefix` format (segment-separated, no wildcards, no leading/trailing slash) in the README configuration notes and verify the described examples match the validation rules

## 4. Integration verification

- [ ] 4.1 Add a receiver-level test that a message on a multi-segment prefix reaches its handler and run `cargo test`
- [ ] 4.2 Run `cargo fmt -- --check` and `cargo clippy --all-targets -- -D warnings` and confirm both are clean

## Workflow follow-up

- Archive the change after the project's review requirements are satisfied.
