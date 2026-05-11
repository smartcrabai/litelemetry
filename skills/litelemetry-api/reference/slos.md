# SLOs

Service Level Objectives. Each SLO defines a `target_pct` and a rolling `window_ms`, with two filter clauses describing which events count as "successful" and which count as "total". The `/budget` endpoint reports current burn against the target.

---

## GET /api/slos

**Location**: `src/server.rs:10912-10921`

### Response 200 (`SloListResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `slos` | array of `SloSummary` | |

`SloSummary`:

| Field | Type | Notes |
| --- | --- | --- |
| `id` | UUID | |
| `name` | string | |
| `viewer_id` | UUID \| null | Optional viewer to render alongside. |
| `target_pct` | number | `0.0` – `100.0`. |
| `window_ms` | integer | Rolling evaluation window. |
| `success_filters` | array of `SloFilterClause` | Events that count as "good". |
| `total_filters` | array of `SloFilterClause` | Events that count toward the denominator. |
| `enabled` | bool | |

`SloFilterClause` is defined in `src/domain/slo.rs` (filter shape mirrors `ViewerFilterInput` from `viewers.md` — `field`, `op`, `value`).

Sample: [examples/slos.list.json](../examples/slos.list.json).

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 / 503 | Store failure. |

---

## POST /api/slos

**Location**: `src/server.rs:10923-10963`

### Request body (`CreateSloRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | yes | 1–120 chars. |
| `viewer_id` | UUID | no | Display-only association. |
| `target_pct` | number | yes | `0.0` – `100.0`. |
| `window_ms` | integer | yes | |
| `success_filters` | array of `SloFilterClause` | no | Default `[]`. |
| `total_filters` | array of `SloFilterClause` | no | Default `[]`. |

### Response 201

| Field | Type |
| --- | --- |
| `id` | UUID |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad name length, filter compile error. |
| 401 | Bearer auth required. |
| 500 / 503 | Store failure. |

### Example

```
scripts/litelemetry.sh POST /api/slos '{
  "name": "checkout 99.5",
  "target_pct": 99.5,
  "window_ms": 2592000000,
  "success_filters": [{"field":"status_code","op":"neq","value":"2"}],
  "total_filters":   [{"field":"name","op":"eq","value":"checkout.submit"}]
}'
```

---

## GET /api/slos/{id}

**Location**: `src/server.rs:10965-10979`

### Response 200

A single `SloSummary`.

---

## DELETE /api/slos/{id}

**Location**: `src/server.rs:10981-10995`

### Response 204

Empty body.

---

## GET /api/slos/{id}/budget

Compute the error budget for the SLO using the configured filters against the current data.

**Location**: `src/server.rs:10997-11022`

### Response 200 (`ErrorBudget`)

The exact shape is defined in `src/domain/slo.rs`. Expect fields along the lines of:

| Field | Type | Notes |
| --- | --- | --- |
| `total` | integer | Total events in the window. |
| `successes` | integer | Successful events. |
| `success_rate` | number | `successes / total`. |
| `target_pct` | number | Echoes the SLO target. |
| `error_budget_remaining_pct` | number | Headroom before breach. |
| `burn_rate` | number | Multiplier vs target. |

When the SLO has been quiet (zero events), the rate fields can be `null` — check before division.

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 404 | SLO not found. |
| 500 / 503 | Compile or evaluation error. |

### Example

```
scripts/litelemetry.sh GET /api/slos/<id>/budget
```
