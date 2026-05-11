# Viewers

A *viewer* is a saved chart definition over one signal type (`traces`, `metrics`, or `logs`). Each viewer carries a query, filters, and optional aggregation, and is rendered as a table or chart.

---

## GET /api/viewers

List all viewers with their latest data snapshots.

**Location**: `src/server.rs:7997-8022`

### Response 200 (`ViewerListResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `viewers` | array of `ViewerSummary` | One per saved viewer. |

`ViewerSummary` (also returned by `GET /api/viewers/{id}` and embedded inside dashboard panels):

| Field | Type | Notes |
| --- | --- | --- |
| `id` | UUID | |
| `slug` | string | URL-friendly identifier. |
| `name` | string | |
| `signals` | array of string | E.g. `["traces"]`. |
| `chart_type` | string | `table`, `line`, etc. Valid set depends on the signal. |
| `query` | string \| null | Compiled query expression. |
| `filters` | JSON value \| null | Saved filter clauses. |
| `filter_mode` | string \| null | `"and"` or `"or"`. |
| `aggregation` | JSON value \| null | `{fn, bucket_ms, group_by}` block when set. |
| `refresh_interval_ms` | integer | How often the runtime re-evaluates. |
| `lookback_ms` | integer | Time window covered by `entries`. |
| `entry_count` | integer | Total entries in the current snapshot. |
| `status` | `ViewerStatus` | Runtime state — `ready`, `loading`, or `failed` with detail. |
| `entries` | array of `ViewerEntryRow` | The current row buffer (capped). |
| `traces` | array of `TraceSummary` | Trace-shaped projections when relevant. |
| `aggregated_buckets` | array of `Bucket` | Time-bucketed numeric values. |
| `exemplars` | array of `ExemplarPair` | Sample trace ids tied to buckets (metric viewers). |

`ViewerEntryRow`: `observed_at`, `signal`, `service_name?`, `payload_size_bytes`, `payload_preview`, `metric_name?`, `metric_value?`.

### Example

```
scripts/litelemetry.sh GET /api/viewers
```

Sample response: [examples/viewers.list.json](../examples/viewers.list.json).

---

## POST /api/viewers

Create a viewer.

**Location**: `src/server.rs:8122-8176`

### Request body (`CreateViewerRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | yes | 1–80 chars. |
| `signal` | string | yes | One of `traces`, `metrics`, `logs`. |
| `chart_type` | string | no | Defaults to `"table"`. The valid set depends on the signal (e.g. `line` requires metric aggregation). |
| `query` | string | no | Source-form query expression. |
| `filters` | array of `ViewerFilterInput` | no | See below. |
| `filter_mode` | string | no | `"and"` or `"or"`. |
| `aggregation` | `AggregationInput` | no | See below. |

`ViewerFilterInput`:

| Field | Type | Notes |
| --- | --- | --- |
| `field` | string | E.g. `service_name`, `status_code`, `attributes.http.status_code`. |
| `op` | string | `eq`, `neq`, `contains`, `gt`, `gte`, `lt`, `lte` (see compiler). |
| `value` | string | Always sent as string; the compiler coerces by field type. |

`AggregationInput`:

| Field | Type | Notes |
| --- | --- | --- |
| `fn` | string | `count`, `sum`, `avg`, `p50`, `p95`, `p99`, `max`. |
| `bucket_ms` | integer | Bucket size in milliseconds. |
| `group_by` | array of string | Zero or more group keys. |

### Response 201 (`CreateViewerResponse`)

| Field | Type |
| --- | --- |
| `id` | UUID |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad name length, invalid signal, unsupported `chart_type` for the signal, or query compile error. |
| 401 | Bearer auth required. |
| 500 | Store / runtime insert failed. |
| 503 | Viewer store or runtime not configured. |

### Example

```
scripts/litelemetry.sh POST /api/viewers '{
  "name": "checkout errors",
  "signal": "traces",
  "chart_type": "table",
  "filters": [
    {"field": "service_name", "op": "eq", "value": "checkout"},
    {"field": "status_code", "op": "eq", "value": "2"}
  ],
  "filter_mode": "and"
}'
```

---

## POST /api/viewers/preview

Try a viewer query without saving anything. Useful for "is my filter correct" iteration.

**Location**: `src/server.rs:8292-8378`

### Request body (`PreviewViewerRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `signal` | string | yes | `traces`, `metrics`, or `logs`. |
| `query` | string | no | |
| `filters` | array of `ViewerFilterInput` | no | |
| `filter_mode` | string | no | |

### Response 200 (`ViewerPreviewResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `signal` | string | Echo. |
| `query` | string \| null | Echo. |
| `filters` | JSON value \| null | Echo. |
| `filter_mode` | string \| null | Echo. |
| `entry_count` | integer | Total matching entries. |
| `entries` | array of `ViewerEntryRow` | Up to the preview cap. |
| `traces` | array of `TraceSummary` | Projection for trace signal. |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Invalid signal, compile error. |
| 401 | Bearer auth required. |
| 500 | Read failure. |

### Example

```
scripts/litelemetry.sh POST /api/viewers/preview '{
  "signal":"logs",
  "filters":[{"field":"severity","op":"eq","value":"ERROR"}]
}'
```

---

## GET /api/viewers/{id}

Fetch one viewer.

**Location**: `src/server.rs:7679-7719`

### Path params

| Param | Type |
| --- | --- |
| `id` | UUID |

### Response 200

Same `ViewerSummary` schema as `GET /api/viewers`.

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 404 | No viewer with that id. |
| 500 | Runtime failure. |

---

## PATCH /api/viewers/{id}

Partial update. Any subset of fields may be supplied.

**Location**: `src/server.rs:8178-8290`

### Path params

| Param | Type |
| --- | --- |
| `id` | UUID |

### Request body (`PatchViewerRequest`)

| Field | Type | Notes |
| --- | --- | --- |
| `chart_type` | string \| null | |
| `query` | string \| null | Pass `null` to clear. |
| `filters` | array of `ViewerFilterInput` | Empty array clears all. |
| `filter_mode` | string \| null | |
| `aggregation` | `AggregationInput` \| null | |

Note: `signal` is **not** patchable — recreate the viewer to change it.

### Response 200

Empty body.

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad `chart_type`, signal/chart-type mismatch, compile error. |
| 401 | Bearer auth required. |
| 404 | Viewer not found. |
| 500 / 503 | Store / runtime failures. |

### Example

```
scripts/litelemetry.sh PATCH /api/viewers/3f1.../viewer '{"chart_type":"line"}'
```

---

## DELETE /api/viewers/{id}

**Location**: `src/server.rs:9148-9167`

### Path params

| Param | Type |
| --- | --- |
| `id` | UUID |

### Response 204

Empty body. Dashboards that referenced this viewer keep the placeholder slot but render no data — clean them up via `PATCH /api/dashboards/{id}`.

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 404 | Viewer not found. |
| 500 / 503 | Store / runtime failures. |
