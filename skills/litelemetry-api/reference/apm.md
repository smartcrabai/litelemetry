# APM

Pre-computed aggregations over recent trace data: service topology, error fingerprints, slow DB queries, and N+1 patterns. All of these share `lookback_ms` semantics.

---

## GET /api/service-map

Service-to-service call graph with throughput and latency.

**Location**: `src/server.rs:7936-7962`

### Query params (`ServiceMapQuery`)

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `lookback_ms` | integer | no | Default `600000` (10 minutes). |

### Response 200 (`ServiceMap`)

| Field | Type | Notes |
| --- | --- | --- |
| `nodes` | array of `ServiceNode` | One per observed service. |
| `edges` | array of `ServiceEdge` | One per observed caller→callee pair. |

`ServiceNode`:

| Field | Type | Notes |
| --- | --- | --- |
| `name` | string | |
| `span_count` | integer | All spans owned by this service. |
| `error_count` | integer | Spans with error status. |
| `p95_duration_ms` | number | |

`ServiceEdge`:

| Field | Type | Notes |
| --- | --- | --- |
| `from` | string | Caller service. |
| `to` | string | Called service. |
| `calls` | integer | |
| `error_rate` | number | `0.0` – `1.0`. |

Sample: [examples/service-map.json](../examples/service-map.json).

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 | Stream read failure. |

### Example

```
scripts/litelemetry.sh GET '/api/service-map?lookback_ms=1800000'
```

---

## GET /api/error-groups

Errors collapsed by fingerprint, so 1000 occurrences of one bug show up as one row.

**Location**: `src/server.rs:7977-7995`

### Query params (`ErrorGroupQuery`)

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `lookback_ms` | integer | no | Default `3600000` (1 hour). Must be `> 0`. |

### Response 200 (`ErrorGroupsResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `error_groups` | array of `ErrorGroup` | |

`ErrorGroup`:

| Field | Type | Notes |
| --- | --- | --- |
| `fingerprint` | string | First 16 hex chars of the SHA-256 fingerprint. |
| `signature` | string | Human form, e.g. `"RuntimeError: connection refused"`. |
| `count` | integer | |
| `first_seen` | RFC 3339 timestamp | UTC. |
| `last_seen` | RFC 3339 timestamp | UTC. |
| `sample_payload` | JSON value | Raw OTLP payload of one occurrence. |

Sample: [examples/error-groups.list.json](../examples/error-groups.list.json).

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 | Query failed. |
| 503 | Error group store not configured. |

### Example

```
scripts/litelemetry.sh GET '/api/error-groups?lookback_ms=900000'
```

---

## GET /api/slow-queries

Database statements aggregated across the lookback window, ranked by p95.

**Location**: `src/server.rs:9871-9883`

### Query params (`SlowQueriesQuery`)

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `lookback_ms` | integer | no | Default `300000` (5 minutes). |
| `min_p95_ms` | integer | no | Default `100`. |

### Response 200 (array of `SlowQueryStats`)

| Field | Type | Notes |
| --- | --- | --- |
| `statement` | string | Normalized (parameters stripped). |
| `system` | string \| null | E.g. `mysql`, `postgresql`. |
| `count` | integer | |
| `p50_ms` | integer | |
| `p95_ms` | integer | |
| `p99_ms` | integer | |
| `max_ms` | integer | |

The response body is a bare array, not wrapped.

Sample: [examples/slow-queries.json](../examples/slow-queries.json).

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 | Entry collection failed. |

### Example

```
scripts/litelemetry.sh GET '/api/slow-queries?lookback_ms=900000&min_p95_ms=250'
```

---

## GET /api/n-plus-one

Suspected N+1 patterns: a single normalized statement repeated many times inside one trace, often signaling a query-in-a-loop.

**Location**: `src/server.rs:9885-9899`

### Query params (`NPlusOneQuery`)

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `lookback_ms` | integer | no | Default `300000` (5 minutes). |
| `min_repetitions` | integer | no | Default `5`. Minimum `1`. |

### Response 200 (array of `NPlusOneStats`)

| Field | Type | Notes |
| --- | --- | --- |
| `trace_id` | string | Drill in via `GET /api/traces/{trace_id}/waterfall`. |
| `statement` | string | Normalized SQL. |
| `system` | string \| null | |
| `count` | integer | Repetitions inside the trace. |
| `services` | array of string | Services that executed the statement. |

The response body is a bare array.

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 | Entry collection failed. |

### Example

```
scripts/litelemetry.sh GET '/api/n-plus-one?lookback_ms=1800000&min_repetitions=10'
```
