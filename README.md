# litelemetry

A lightweight, OpenTelemetry (OTLP/HTTP) compatible telemetry collection and visualization tool. Ships as a single Rust binary.

## Features

- **OTLP/HTTP and OTLP/gRPC compatible** — Receives Traces, Metrics, and Logs via `/v1/traces`, `/v1/metrics`, `/v1/logs` endpoints (HTTP) and the standard OTLP gRPC service paths (also accepting Connect and gRPC-Web on the same port)
- **In-memory storage with Redis Streams** — Telemetry data is stored in Redis; per-signal stream length is bounded via `XADD MAXLEN ~ N` (configurable through `REDIS_STREAM_MAX_ENTRIES`)
- **PostgreSQL for master data** — Viewer definitions and cursor snapshots are persisted in PostgreSQL
- **Viewer Runtime** — Updates multiple Viewers with a single Redis read per signal (fan-out)
- **Resume on startup** — Restores cursor positions from PostgreSQL snapshots and fetches only the diff

## Architecture

```
OTLP/HTTP Client
       │
       v
  ┌─────────┐     ┌───────────────┐     ┌──────────────┐
  │  Axum   │────▶│ Redis Streams │────▶│ViewerRuntime │
  │ Server  │     │ (per signal)  │     │  (fan-out)   │
  └─────────┘     └───────────────┘     └──────┬───────┘
                                               │
                                        ┌──────v───────┐
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

## OTLP ingest endpoints

The server accepts OpenTelemetry data via two transports on the **same** HTTP port:

- **OTLP/HTTP** — `POST /v1/traces`, `/v1/metrics`, `/v1/logs` with `application/x-protobuf` or `application/json` body.
- **OTLP/gRPC** — gRPC, gRPC-Web, and ConnectRPC clients can call the standard OTLP service paths:
  - `/opentelemetry.proto.collector.trace.v1.TraceService/Export`
  - `/opentelemetry.proto.collector.metrics.v1.MetricsService/Export`
  - `/opentelemetry.proto.collector.logs.v1.LogsService/Export`

  Implemented with [`buffa`](https://github.com/anthropics/buffa) (protobuf) and [`connectrpc`](https://github.com/anthropics/connect-rust); registered into the existing axum router via `fallback_service`. Point any OTLP client at `http://<host>:<port>` (no path, no separate gRPC port).

## API authentication

All `/api/*` REST endpoints can be protected with Bearer-token API keys. OTLP ingest endpoints (`/v1/traces`, `/v1/metrics`, `/v1/logs`, OTLP gRPC paths), the healthz endpoint, and the static UI are **not** affected.

Set `API_KEYS_ENABLED=true` to turn auth on. It defaults to `false` for backward compatibility.

### Key format and storage

- Keys are generated server-side in the form `lt_<128 hex chars>` (two UUID v4s, 256-bit entropy).
- Only the SHA-256 hash is persisted (in `api_keys` table for Postgres mode, or in-memory for standalone). **The raw key is returned exactly once** in the create response and cannot be retrieved later.
- The `api_keys` table is created automatically by `docker/postgres/initdb/001-init.sql` on first startup.

### Bootstrap

When `API_KEYS_ENABLED=true` and the key store is empty, `POST /api/api-keys` is reachable without authentication so an initial key can be created. As soon as one key exists, all `/api/*` requests (including further key management) require a valid `Authorization: Bearer <key>` header.

### Key management endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/api-keys` | Create a key. Body: `{"name": "<label>"}`. Response includes the raw `key` (returned once). |
| `GET`  | `/api/api-keys` | List keys (id, name, created_at — no hashes, no raw values). |
| `DELETE` | `/api/api-keys/{id}` | Revoke a key. |

Example bootstrap flow:

```bash
# 1. Create the first key (no auth required because the store is empty)
curl -X POST http://localhost:8080/api/api-keys \
  -H 'Content-Type: application/json' \
  -d '{"name":"admin"}'
# => {"id":"...","name":"admin","key":"lt_<128 hex>","created_at":"..."}

# 2. Subsequent requests must include the Bearer token
curl http://localhost:8080/api/api-keys \
  -H 'Authorization: Bearer lt_<128 hex>'
```

In standalone mode the key store lives in memory and is wiped on every restart — re-run the bootstrap flow each time. Use Postgres (`STANDALONE=false`) for durable keys.

## Prerequisites

- Rust (edition 2024)
- [bacon](https://github.com/Canop/bacon) for local app startup (`cargo install --locked bacon`)
- `protoc` on PATH at build time (`brew install protobuf` on macOS, `apk add protoc protobuf-dev` on Alpine)
- Redis and PostgreSQL (only required for full mode; standalone mode needs neither)
- Docker (for integration tests and Docker Compose startup)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STANDALONE` | `true` | Accepts `true` / `1` for in-memory mode and `false` / `0` for Redis + PostgreSQL mode. |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL (used only when `STANDALONE=false`) |
| `DATABASE_URL` | — | PostgreSQL connection URL (used only when `STANDALONE=false`). When set, the app bootstraps the PostgreSQL schema and starts the viewer runtime. |
| `PORT` | `8080` | HTTP server listen port (shorthand; overridden by `HTTP_PORT`) |
| `HTTP_PORT` | — | HTTP server listen port (takes precedence over `PORT`) |
| `VIEWER_RUNTIME_POLL_MS` | `1000` | Poll interval for the background viewer runtime |
| `MEMORY_STREAM_MAX_ENTRIES` | `100000` | Maximum number of in-memory stream entries retained per signal when `STANDALONE=true`. |
| `REDIS_STREAM_MAX_ENTRIES` | `100000` | Maximum number of Redis stream entries retained per signal (used only when `STANDALONE=false`). Each `XADD` is issued with `MAXLEN ~ N` for approximate trimming. Set to `0` to disable trimming. |
| `API_KEYS_ENABLED` | `false` | When `true` / `1`, all `/api/*` endpoints require `Authorization: Bearer <key>`. OTLP, healthz, and the static UI remain unauthenticated. See [API authentication](#api-authentication). |

When set, numeric environment variables must parse cleanly. Invalid values fail fast at startup instead of silently falling back to defaults.

## Build & Run

```bash
# Install bacon once
cargo install --locked bacon

# Run the app locally with auto-restart on changes (no external services required)
bacon serve
```

The `serve` job uses `scripts/run-local-with-bacon.sh`. By default, it starts in **standalone (in-memory) mode** (`STANDALONE=true`) — no Redis or PostgreSQL is needed.

To run on a different port (e.g. in a git worktree):

```bash
PORT=8081 bacon serve
```

### Full mode (Redis + PostgreSQL)

To run with real external services:

```bash
# Start Redis + PostgreSQL
docker compose up -d

# Run in full mode
STANDALONE=false bacon serve
```

If you override Compose ports, export `LITELEMETRY_REDIS_PORT` / `LITELEMETRY_POSTGRES_PORT` before `bacon serve`, or set `REDIS_URL` / `DATABASE_URL` directly.

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

The repository now ships with `compose.yml`, which starts Redis 8, PostgreSQL 18, and a one-shot Redis seeder for local app development:

```bash
docker compose up -d
```

Services exposed locally:

- `localhost:6379` — Redis
- `localhost:5432` — PostgreSQL (`postgres/postgres`, DB=`litelemetry`)

On a fresh `docker compose up -d`, PostgreSQL runs `docker/postgres/initdb/001-init.sql` and seeds demo traces / metrics / logs viewers with a 24-hour lookback, while the `redis-seeder` service pushes sample telemetry for each signal into Redis for the locally running app.

If any of those ports are already in use, override them when starting Compose:

```bash
LITELEMETRY_REDIS_PORT=16379 \
LITELEMETRY_POSTGRES_PORT=15432 \
docker compose up -d
```

After Compose is up, start the app locally:

```bash
bacon serve
```

When running in full mode (`STANDALONE=false`), `DATABASE_URL` is set and the app automatically creates the `viewer_definitions` and `viewer_snapshots` tables and starts the background viewer runtime.

If you want a fresh copy of the seeded demo data, recreate the containers first:

```bash
docker compose down
docker compose up -d
```

Open `http://localhost:8080` to access the built-in viewer workspace. From that page you can:

- inspect the seeded `Compose Seed Traces`, `Compose Seed Metrics`, and `Compose Seed Logs` viewers immediately after startup
- create a traces / metrics / logs viewer from the browser
- send a sample OTLP payload to `/v1/traces`, `/v1/metrics`, or `/v1/logs`
- confirm the reflected entries in a table view backed by the in-memory viewer runtime

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
