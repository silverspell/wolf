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

## Authentication

Wolf keeps the RabbitMQ-compatible `guest`/`guest` default account. Like
RabbitMQ, the `guest` user may only connect from loopback addresses
(`127.0.0.0/8`, `::1`). To use different credentials, set both environment
variables together:

```sh
WOLF_DEFAULT_USER=app WOLF_DEFAULT_PASS=<password> cargo run -p wolf-server
```

Setting only one of the two variables makes the server refuse to start with a
clear error. A user configured through the environment is not loopback
restricted. Passwords are never logged. The guest default remains the only
credential literal in source and is suppressed only on that line for the
LoopFlow Semgrep rule (WOLF-22).

## License

MIT, see [`LICENSE`](LICENSE).
