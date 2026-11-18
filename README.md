# Wolf

A RabbitMQ-compatible message broker written in Rust, built for high throughput
and a low footprint. Existing RabbitMQ Java and Go clients (AMQP 0-9-1 and the
Stream protocol) connect to Wolf without code changes.

Status: early development. See the design:
[`docs/superpowers/specs/2026-11-18-wolf-broker-design.md`](docs/superpowers/specs/2026-11-18-wolf-broker-design.md).

## Layout

| Crate | Purpose |
|---|---|
| `crates/wolf-amqp-codec` | AMQP 0-9-1 codec (no I/O) |
| `crates/wolf-stream-codec` | RabbitMQ Stream protocol codec (no I/O) |
| `crates/wolf-storage` | Segmented append-only log storage |
| `crates/wolf-broker` | Broker domain: routing, queues, streams, TTL, DLX |
| `crates/wolf-server` | The `wolf` binary |

## Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p wolf-server
```

## License

MIT, see [`LICENSE`](LICENSE).
