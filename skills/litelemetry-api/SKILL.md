---
name: litelemetry-api
description: Call the litelemetry REST API (auth-protected /api/* endpoints for traces, viewers, dashboards, alerts, SLOs, ad-hoc SQL). Use when interacting with a running litelemetry server via HTTP — fetching traces/spans, managing viewers/dashboards/alerts/SLOs, running ad-hoc SQL queries, or bootstrapping API keys with Bearer authentication.
---

# litelemetry REST API skill

A thin HTTP wrapper plus reference docs for litelemetry's `/api/*` endpoints. OTLP ingest (`/v1/traces`, `/v1/metrics`, `/v1/logs`) and the gRPC collector are **not** in scope; those paths are unauthenticated and intentionally untouched.

## Prerequisites

- A running litelemetry server reachable over HTTP.
- `curl` on the PATH. `jq` is optional but recommended for pretty-printing.
- Two environment variables:
  - `LITELEMETRY_BASE_URL` (e.g. `http://localhost:8080`). Required for every call.
  - `LITELEMETRY_API_KEY` in the form `lt_<128hex>`. Required when the server runs with `API_KEYS_ENABLED=true`. If unset, `scripts/litelemetry.sh` warns to stderr and proceeds without the header — fine for the default auth-off mode.

## Quick start

```
export LITELEMETRY_BASE_URL=http://localhost:8080
export LITELEMETRY_API_KEY=lt_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

bash scripts/litelemetry.sh GET /api/services
bash scripts/litelemetry.sh POST /api/query '{"sql":"SELECT trace_id, service_name FROM traces LIMIT 10"}'
```

Output is the raw response body on stdout. Non-2xx exits with status `1` and details on stderr. Misuse (missing args, bad method) exits with status `2`.

## First-time setup (bootstrap)

When the server is started with `API_KEYS_ENABLED=true` for the first time, the key store is empty. The auth middleware grants exactly one exception: `POST /api/api-keys` is allowed without a Bearer header while the store is empty. Use `scripts/bootstrap.sh` for this one call:

```
export LITELEMETRY_BASE_URL=http://localhost:8080
bash scripts/bootstrap.sh production
# stdout: lt_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# stderr: a one-shot reminder showing the matching `export LITELEMETRY_API_KEY=...`
```

The raw key is shown **only here**. Store it before the terminal scrolls. Standalone (memory) mode loses the key on every restart — re-bootstrap. Postgres mode persists it. Details: `reference/auth.md`.

## Finding the right endpoint

1. Open `reference/endpoints.md` — a single table with every route's one-line summary and the matching detail file.
2. From there, open one of:
   - `reference/api-keys.md`, `reference/auth.md`
   - `reference/query.md` (ad-hoc SQL)
   - `reference/viewers.md`, `reference/dashboards.md`
   - `reference/traces.md`, `reference/apm.md`
   - `reference/alerts.md`, `reference/slos.md`, `reference/anomaly.md`
   - `reference/attributes.md` (services / attribute discovery)
   - `reference/misc.md` (incidents, notification channels, exemplars, rollups)
3. For an example response shape, see the matching file in `examples/` (index in `examples/README.md`).

Do **not** read all reference files upfront. Each detail page is self-contained — load only the category you need to keep context lean.

## Common workflows

### "Show me slow traces in service X, then drill in"

```
bash scripts/litelemetry.sh GET /api/services
bash scripts/litelemetry.sh GET '/api/traces/search?service=checkout&min_duration_ms=500'
bash scripts/litelemetry.sh GET /api/traces/<trace_id>/waterfall
```

### "Run a SQL query against recent data"

```
bash scripts/litelemetry.sh POST /api/query \
  '{"sql":"SELECT service_name, COUNT(*) AS n FROM traces WHERE status_code = 2 GROUP BY service_name ORDER BY n DESC","lookback_ms":3600000}'
```

The `lookback_ms` field caps the scan window. Maximum 24h. The response carries `scanned`, `matched`, `returned`, `truncated` — check `truncated` before trusting cardinality.

### "Create a viewer, then alert on it"

```
viewer_id=$(bash scripts/litelemetry.sh POST /api/viewers '{
  "name":"payments errors",
  "signal":"traces",
  "filters":[{"field":"service_name","op":"eq","value":"payments"},
             {"field":"status_code","op":"eq","value":"2"}],
  "filter_mode":"and"
}' | jq -r .id)

bash scripts/litelemetry.sh POST /api/alerts "$(jq -n --arg vid "$viewer_id" '{
  name:"payments error spike",
  viewer_id:$vid,
  condition:{threshold:{metric:"count",op:"gt",value:50}},
  severity:"warn",
  evaluation_interval_ms:60000
}')"
```

### "Copy a dashboard between environments"

```
bash scripts/litelemetry.sh GET /api/dashboards/<id>/export > dashboard.json

# point env vars at the other environment, then:
bash scripts/litelemetry.sh POST /api/dashboards/import @dashboard.json
```

`@<filename>` syntax in `scripts/litelemetry.sh` reads the body from a file.

## Error handling

`scripts/litelemetry.sh` exits non-zero on any non-2xx response and writes a one-line summary plus the first 500 bytes of the body to stderr. A few common cases:

| Status | Likely cause | Fix |
| --- | --- | --- |
| 401 | `LITELEMETRY_API_KEY` unset / wrong / revoked. | Re-export the correct key, or `scripts/bootstrap.sh` if the store is fresh. |
| 400 | Invalid params (bad signal, empty name, bad SQL, lookback out of range). | Check the request body against the relevant `reference/*.md`. |
| 404 | Resource not found. | Verify the UUID and that the resource still exists. |
| 503 | Required backing store unavailable. | Check server config (`API_KEYS_ENABLED`, `DATABASE_URL`, `STANDALONE`). |

## Scope and non-goals

- **In scope**: every `/api/*` endpoint, plus API-key bootstrap.
- **Not in scope**: OTLP HTTP ingest (`/v1/traces`, `/v1/metrics`, `/v1/logs`) — these are unauthenticated and protobuf-shaped, designed for the OpenTelemetry SDKs. The skill never touches them. The same applies to the OTLP gRPC service paths.
- **Not in scope**: the HTML UI (`GET /`) and `/healthz`.

## Reference index

| File | Endpoints covered |
| --- | --- |
| `reference/endpoints.md` | Index of all `/api/*` routes |
| `reference/auth.md` | Authentication, bootstrap, key formats |
| `reference/api-keys.md` | `POST/GET /api/api-keys`, `DELETE /api/api-keys/{id}` |
| `reference/query.md` | `POST /api/query` |
| `reference/viewers.md` | 6 viewer endpoints |
| `reference/dashboards.md` | 9 dashboard / template endpoints |
| `reference/traces.md` | 3 trace endpoints |
| `reference/apm.md` | service map, error groups, slow queries, N+1 |
| `reference/alerts.md` | 5 alert endpoints |
| `reference/slos.md` | 5 SLO endpoints (including budget) |
| `reference/anomaly.md` | `POST /api/anomaly/evaluate` |
| `reference/attributes.md` | services + attribute key/value discovery |
| `reference/misc.md` | incidents, notification channels, exemplars, rollups |
| `examples/README.md` | Index of sample JSON response bodies |

## Layout

```
SKILL.md                 # this file
scripts/
  litelemetry.sh         # METHOD + PATH wrapper (Bearer auth, body @file support)
  bootstrap.sh           # one-shot first-key issuance (no auth header)
reference/               # per-category endpoint docs
examples/                # sample JSON responses
```

The implementation referenced by each detail page lives in `src/server.rs` of the litelemetry repo; line numbers are cited so an agent that needs the exact behaviour can read the source.
