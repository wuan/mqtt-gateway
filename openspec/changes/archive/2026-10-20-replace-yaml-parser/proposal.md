# Proposal

## Why

The configuration loader parses `config.yml` with `serde_yaml_ng`, which parses
through `unsafe-libyaml` — a `c2rust` machine translation of C libyaml whose
repository was archived in March 2024. It carries RUSTSEC-2024-0449
("unmaintained", no patched version), so `cargo deny check advisories` fails.
Replacing the parser removes the dependency instead of ignoring the advisory.

## What Changes

- Replace `serde_yaml_ng` with `serde-saphyr`, a pure-Rust, `unsafe`-forbidden,
  fuzz-tested YAML deserializer that is the alternative named by the RustSec
  advisory itself.
- Depend on the `deserialize` surface only
  (`default-features = false, features = ["deserialize"]`), since the gateway
  only reads YAML.
- Update the six `from_str` call sites (`src/main.rs`, `src/config/mod.rs`
  tests) to the new crate path.
- Regenerate `Cargo.lock`; the `serde_yaml_ng` and `unsafe-libyaml` entries
  disappear from the dependency graph.
- User-visible configuration behavior is unchanged: same `config.yml` schema,
  same `${VAR_NAME}` expansion, same cross-field validation.

Out of scope: the unrelated `rustls 0.23.45` advisories that also make
`cargo deny` red. That is a separate change.

## Capabilities

### New Capabilities
- `configuration`: Loading, environment-variable expansion, parsing, and
  validation of the gateway's `config.yml` into the typed `Config` model.

### Modified Capabilities
<!-- None: the project has no existing specs. -->

## Impact

- **Code**: `src/main.rs` (config load), `src/config/mod.rs` (tests). The
  `Config`/`Source`/`Target` model and `validate()` are unchanged.
- **Dependencies**: remove `serde_yaml_ng` (and transitively `unsafe-libyaml`);
  add `serde-saphyr` (and transitively the pure-Rust `granit-parser`).
  `deny.toml` needs no change — MIT OR Apache-2.0 is already allowed.
- **Build**: `serde-saphyr` requires Rust 1.89; CI and local toolchains are on
  stable (1.98), so no change needed.
- **Risk**: a from-scratch parser may differ on malformed input or unusual
  scalars, and its error type/messages change. Existing config tests plus a
  real-config parse cover this.
