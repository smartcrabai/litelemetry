# litelemetry

A lightweight, OpenTelemetry (OTLP/HTTP) compatible telemetry collection and visualization tool. Ships as a single Rust binary.

## Features

- **OTLP/HTTP compatible** — Receives Traces, Metrics, and Logs via `/v1/traces`, `/v1/metrics`, `/v1/logs` endpoints
- **In-memory storage with Redis Streams** — Telemetry data is stored in Redis; old data is automatically evicted via `allkeys-lru`
- **PostgreSQL for master data** — Viewer definitions and cursor snapshots are persisted in PostgreSQL
- **Viewer Runtime** — Updates multiple Viewers with a single Redis read per signal (fan-out)
- **Resume on startup** — Restores cursor positions from PostgreSQL snapshots and fetches only the diff

## Architecture

```
OTLP/HTTP Client
       │
       ▼
  ┌─────────┐     ┌───────────────┐     ┌──────────────┐
  │  Axum   │────▶│ Redis Streams │────▶│ViewerRuntime │
  │ Server  │     │ (per signal)  │     │  (fan-out)   │
  └─────────┘     └───────────────┘     └──────┬───────┘
                                               │
                                        ┌──────▼───────┐
                                        │  PostgreSQL   │
                                        │ (definitions, │
                                        │  snapshots)   │
                                        └──────────────┘
```

1. The Axum server receives OTLP/HTTP requests and converts them into `NormalizedEntry`
2. Entries are appended to per-signal Redis Streams (`lt:stream:traces`, etc.) via `XADD`
3. The ViewerRuntime periodically reads diffs from Redis with `XREAD` and fans out to all Viewers
4. Each Viewer accepts only entries matching its signal mask and prunes entries outside the lookback window
5. Cursor positions are saved to PostgreSQL as snapshots

## Prerequisites

- Rust (edition 2024)
- Redis
- PostgreSQL
- Docker (for integration tests)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL |
| `DATABASE_URL` | — | PostgreSQL connection URL |
| `HTTP_PORT` | `8080` | HTTP server listen port |

## Build & Run

```bash
# Build
cargo build --release

# Run
REDIS_URL=redis://127.0.0.1:6379 \
DATABASE_URL=postgres://user:pass@localhost/litelemetry \
HTTP_PORT=8080 \
cargo run --release
```

## Docker

```bash
# Build
docker build -t litelemetry .

# Run
docker run -p 8080:8080 \
  -e REDIS_URL=redis://redis:6379 \
  -e DATABASE_URL=postgres://user:pass@postgres/litelemetry \
  litelemetry
```

A pre-built image is available on GHCR:

```bash
docker pull ghcr.io/smartcrabai/litelemetry:latest
```

## Testing

```bash
# Unit tests
cargo test

# Integration tests (requires Docker)
cargo test -- --ignored
```

Integration tests use [testcontainers](https://github.com/testcontainers/testcontainers-rs) to automatically spin up Redis and PostgreSQL.

## Project Structure

```
src/
├── main.rs                    # Entrypoint
├── lib.rs                     # Module definitions
├── server.rs                  # Axum router & OTLP endpoints
├── domain/
│   ├── telemetry.rs           # Signal, SignalMask, NormalizedEntry
│   └── viewer.rs              # ViewerDefinition, ViewerStatus
├── ingest/
│   ├── decode.rs              # Content-Type parser
│   └── otlp_http.rs           # OTLP/HTTP request parser
├── storage/
│   ├── redis.rs               # Redis Stream read/write
│   └── postgres.rs            # Viewer definitions & snapshot CRUD
└── viewer_runtime/
    ├── compiler.rs            # ViewerDefinition → CompiledViewer
    ├── state.rs               # StreamCursor, ViewerState
    ├── reducer.rs             # Entry application & stale data pruning
    └── runtime.rs             # Fan-out, diff updates & resume logic
```

## License

[Apache License 2.0](LICENSE)
