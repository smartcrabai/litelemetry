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
- Docker (for integration tests and Docker Compose startup)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL |
| `DATABASE_URL` | — | PostgreSQL connection URL. When set, the app bootstraps the PostgreSQL schema and starts the viewer runtime. |
| `HTTP_PORT` | `8080` | HTTP server listen port |
| `VIEWER_RUNTIME_POLL_MS` | `1000` | Poll interval for the background viewer runtime |

## Build & Run

```bash
# Build
cargo build --release

# Run (full stack: Redis + PostgreSQL)
REDIS_URL=redis://127.0.0.1:6379 \
DATABASE_URL=postgres://user:pass@localhost/litelemetry \
HTTP_PORT=8080 \
cargo run --release
```

If you only want OTLP ingest into Redis, `DATABASE_URL` is optional and the app will start in ingest-only mode.

## Docker

```bash
# Build
docker build -t litelemetry .

# Run a single container against existing Redis/PostgreSQL instances
docker run -p 8080:8080 \
  -e REDIS_URL=redis://redis:6379 \
  -e DATABASE_URL=postgres://user:pass@postgres/litelemetry \
  litelemetry
```

A pre-built image is available on GHCR:

```bash
docker pull ghcr.io/smartcrabai/litelemetry:latest
```

### Docker Compose

The repository now ships with `compose.yml`, which starts Redis, PostgreSQL, and the app together:

```bash
docker compose up --build
```

Services exposed locally:

- `http://localhost:8080` — litelemetry OTLP/HTTP ingest
- `localhost:6379` — Redis
- `localhost:5432` — PostgreSQL (`postgres/postgres`, DB=`litelemetry`)

If any of those ports are already in use, override them when starting Compose:

```bash
LITELEMETRY_HTTP_PORT=18080 \
LITELEMETRY_REDIS_PORT=16379 \
LITELEMETRY_POSTGRES_PORT=15432 \
docker compose up --build
```

When `DATABASE_URL` is set (as it is in `compose.yml`), the app automatically creates the `viewer_definitions` and `viewer_snapshots` tables and starts the background viewer runtime.

Note: viewer definitions are still managed directly in PostgreSQL; there is not yet an HTTP API for creating them.

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
