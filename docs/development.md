# Development

`kiek` is built in Rust. Building it locally requires the standard [Rust toolchain](https://www.rust-lang.org/tools/install).

### Build

Simply run:
```bash
cargo build
```

### Redpanda for Integration Tests

Integration Tests run against a local [Redpanda](https://docs.redpanda.com/current/home/) instance. Running it requires [Docker](https://docs.docker.com/get-started/get-docker/).

Start Redpanda:
```bash
docker compose up
```

#### Running Tests

We use [cargo-nextest](https://nexte.st/) for better test execution. Install it first:
```bash
cargo install cargo-nextest
```

**Unit Tests** (fast, no external dependencies):
```bash
# Run only unit tests - great for frequent testing during development
cargo nextest run --lib
```

**All Tests** (unit & integration, requires Redpanda):
```bash
# Run all tests with default profile
cargo nextest run

# Run tests with CI profile (fail-fast, more conservative)
cargo nextest run --profile ci
```

You can still use the standard cargo test if preferred:
```bash
cargo test
```