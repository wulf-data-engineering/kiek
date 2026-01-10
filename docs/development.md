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

**Only Apple Silicon and x86 64-bit Linux are supported for now.**

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

#### When port `9092` is blocked (e.g. Antigravity IDE)

If port `9092` is blocked (e.g. Antigravity uses it for its agent server), you can start Redpanda on a different port and tell the tests to use it:

```bash
# Start Redpanda on port 29092
REDPANDA_PORT=29092 docker compose up -d

# Run tests against that port
KIEK_TEST_BOOTSTRAP_SERVERS=127.0.0.1:29092 cargo nextest run
```
