# Traces

Retrieve individual traces and their span trees. Time units are mixed (nanoseconds for raw OTLP fields, milliseconds for derived display fields) — note the field suffix when consuming.

---

## GET /api/traces

Look up a single trace by id. The trace id must be supplied as a query parameter, not a path segment.

**Location**: `src/server.rs:10563-10579`

### Query params (`TraceLookupQuery`)

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `trace_id` | string | yes | Trimmed; empty rejected. |

### Response 200 (`TraceDetail`)

| Field | Type | Notes |
| --- | --- | --- |
| `trace_id` | string | |
| `root_span_name` | string \| null | |
| `service_names` | array of string | Sorted + deduplicated. |
| `span_count` | integer | |
| `duration_ns` | integer | Total trace duration in nanoseconds. |
| `duration_ms` | integer | Same value rounded to milliseconds. |
| `started_at_ns` | integer | Unix nanoseconds. |
| `has_error` | bool | At least one span has `status_code == 2`. |
| `spans` | array of `TraceDetailSpan` | All spans, including non-root. |

`TraceDetailSpan`:

| Field | Type | Notes |
| --- | --- | --- |
| `trace_id` | string | Echoes the parent. |
| `span_id` | string | |
| `parent_span_id` | string | Empty for the root. |
| `service_name` | string | |
| `name` | string | Operation name. |
| `kind` | integer | OTLP span kind. |
| `start_time_unix_nano` | integer | |
| `end_time_unix_nano` | integer | |
| `duration_ns` | integer | `end - start`. |
| `status_code` | integer | `0` unset, `1` ok, `2` error. |
| `status_message` | string \| null | |
| `resource_attributes` | array of OTLP `KeyValue` | Raw protobuf-shaped JSON. |
| `span_attributes` | array of OTLP `KeyValue` | Raw protobuf-shaped JSON. |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Missing / empty `trace_id`. |
| 401 | Bearer auth required. |
| 404 | Trace not found. |
| 500 | Stream read failure. |

### Example

```
scripts/litelemetry.sh GET '/api/traces?trace_id=4bf92f3577b34da6a3ce929d0e0e4736'
```

---

## GET /api/traces/search

List recent traces matching coarse filters. Use this to discover trace ids first, then drill into `GET /api/traces` or `GET /api/traces/{id}/waterfall`.

**Location**: `src/server.rs:10581-10598`

### Query params (`TraceSearchQuery`)

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `service` | string | no | Exact match on `service_name`. |
| `min_duration_ms` | integer | no | Drop traces shorter than this. |

### Response 200 (`TraceSearchResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `traces` | array of `TraceListItem` | |

`TraceListItem`: `trace_id`, `root_span_name?`, `service_names`, `span_count`, `duration_ns`, `duration_ms`, `started_at_ns`, `has_error`.

Sample: [examples/traces.search.json](../examples/traces.search.json).

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 | Stream read failure. |

### Example

```
scripts/litelemetry.sh GET '/api/traces/search?service=checkout&min_duration_ms=500'
```

---

## GET /api/traces/{trace_id}/waterfall

Trace flattened into the linear waterfall layout used by the UI: spans in DFS order, sibling order by `start_ms`, with offsets relative to the earliest span. Useful for rendering a Gantt-style view without recomputing offsets.

**Location**: `src/server.rs:9404-9428`

### Path params

| Param | Type | Notes |
| --- | --- | --- |
| `trace_id` | string | Non-empty after trimming. |

### Response 200 (`TraceWaterfall`)

| Field | Type | Notes |
| --- | --- | --- |
| `trace_id` | string | |
| `spans` | array of `WaterfallSpan` | DFS order. |

`WaterfallSpan`:

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Span id. |
| `parent_id` | string | Empty when this is the root. |
| `name` | string | |
| `service_name` | string | |
| `start_ms` | number | Offset from the trace's minimum start, in milliseconds. |
| `duration_ms` | number | `>= 0`. |
| `status` | integer | OTLP status (`0` unset, `1` ok, `2` error). |
| `attributes` | object | Span attributes coerced to JSON strings. |

Sample: [examples/traces.waterfall.json](../examples/traces.waterfall.json).

### Errors

| Code | Cause |
| --- | --- |
| 400 | Empty `trace_id`. |
| 401 | Bearer auth required. |
| 404 | Trace not found. |
| 500 | Stream read failure. |

### Example

```
scripts/litelemetry.sh GET /api/traces/4bf92f3577b34da6a3ce929d0e0e4736/waterfall
```
