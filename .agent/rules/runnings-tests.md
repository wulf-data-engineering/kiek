---
trigger: always_on
---

Start docker compose first.
Run tests using `cargo nextest run`

# Antigravity

If you are in Antigravity IDE port 9092 is blocked. Use:

```bash
REDPANDA_PORT=29092 docker compose up -d
KIEK_TEST_BOOTSTRAP_SERVERS=127.0.0.1:29092 cargo nextest run
```