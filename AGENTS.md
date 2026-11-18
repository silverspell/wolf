# AGENTS.md

Guidance for automated agents working in this repository.

## LoopFlow

This repository is configured to build and validate inside a dedicated
devcontainer at `.devcontainer/loopflow/devcontainer.json`. The team's own
`.devcontainer/devcontainer.json` is not used by LoopFlow.

### Build and test

All commands below run from the repository root and match `.loopflow.yml`:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

The workspace uses `resolver = "3"`, edition 2024, and a pinned `stable`
toolchain from `rust-toolchain.toml`. To run the broker binary:

```sh
cargo run -p wolf-server
```

### Coding conventions

- Rust 2024 edition; formatting is enforced by `rustfmt` (`rustfmt.toml` sets
  `edition = "2024"`) via `cargo fmt`.
- Lints are workspace-wide: `unsafe_code = "deny"` and clippy `all` at `warn`,
  promoted to errors with `-D warnings` in validation. Every crate opts in with
  `[lints] workspace = true`.
- Crates live under `crates/` and are named `wolf-*`. Each crate keeps a short
  module-level doc comment in `src/lib.rs` describing its responsibility.
- Architecture boundaries: `wolf-amqp-codec`, `wolf-stream-codec` and
  `wolf-storage` must not depend on `wolf-broker`; `wolf-broker` must not know
  about wire protocols. `wolf-server` wires codecs, broker and listeners
  together as the `wolf` binary.
- Codec crates are pure encoding/decoding with no I/O. Storage sits behind the
  `Storage` trait. Hot paths must avoid per-message heap allocation and body
  copying; message bodies are shared `bytes::Bytes`.
- Prefer idiomatic Rust naming (`snake_case` items, `CamelCase` types) and keep
  public items documented with `//!`/`///` doc comments where behavior is not
  obvious.

### Invariants

Draft — extracted from the design spec and workspace lints; violating any of
these requires a human decision.

- `unsafe_code` is denied across the workspace. Do not introduce `unsafe`.
- Codec crates (`wolf-amqp-codec`, `wolf-stream-codec`) and `wolf-storage` do
  not depend on `wolf-broker`; `wolf-broker` remains protocol-agnostic.
- RabbitMQ client compatibility is a hard requirement: features advertised as
  supported must behave like RabbitMQ. Silent divergence is worse than
  rejecting an unsupported feature.
- Durability semantics are at-least-once with group commit; publisher confirms
  return after the write reaches the OS, and fsync is periodic. Unacked state
  may be checkpointed, so redelivery is expected.
- All persistent state lives under a single configured `data_dir`; no external
  database is used for metadata.
- Tests must pass on both Linux and macOS development environments.

### ADRs

No dedicated ADR directory exists yet. Design decisions and their rationale are
recorded in
`docs/superpowers/specs/2026-11-18-wolf-broker-design.md`.
