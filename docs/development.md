# Development

`kiek` is built in Rust. Building it locally requires the standard [Rust toolchain](https://www.rust-lang.org/tools/install).

### Build

Simply run:
```bash
cargo build
```

### Tests

Tests run against a local [Redpanda](https://docs.redpanda.com/current/home/) instance. Running it requires [Docker](https://docs.docker.com/get-started/get-docker/).

Start Redpanda:
```bash
docker compose up
```
In a separate terminal:
```bash
cargo test
```