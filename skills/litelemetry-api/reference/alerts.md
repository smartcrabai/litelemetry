# Alerts

Alerts evaluate a viewer's recent data on a schedule and open an incident when the condition matches. Conditions are arbitrary JSON validated server-side; consult the in-app docs or `src/alert` for the supported shape.

---

## GET /api/alerts

**Location**: `src/server.rs:9521-9529`

### Response 200 (`AlertListResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `alerts` | array of `AlertListItem` | |

`AlertListItem`:

| Field | Type | Notes |
| --- | --- | --- |
| `id` | UUID | |
| `name` | string | |
| `viewer_id` | UUID | Must reference an existing viewer. |
| `condition` | JSON value | E.g. `{"threshold": {"metric": "p95_ms", "op": "gt", "value": 500}}`. |
| `severity` | string | |
| `evaluation_interval_ms` | integer | |
| `enabled` | bool | |
| `revision` | integer | Bumps on every successful update. Useful for optimistic UI. |

Sample: [examples/alerts.list.json](../examples/alerts.list.json).

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 / 503 | Store failure. |

### Example

```
scripts/litelemetry.sh GET /api/alerts
```

---

## GET /api/alerts/{id}

**Location**: `src/server.rs:9531-9545`

### Path params

| Param | Type |
| --- | --- |
| `id` | UUID |

### Response 200

Single `AlertListItem`.

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 404 | Not found. |
| 500 / 503 | Store failure. |

---

## POST /api/alerts

**Location**: `src/server.rs:9547-9585`

### Request body (`CreateAlertRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | yes | 1–80 chars. |
| `viewer_id` | UUID | yes | Must exist. |
| `condition` | JSON value | yes | Validated by the alert engine. |
| `severity` | string | yes | One of the allowed severity values (e.g. `info`, `warn`, `critical`). |
| `evaluation_interval_ms` | integer | yes | Minimum `1000`. |
| `enabled` | bool | no | Default `true`. |

### Response 201

| Field | Type |
| --- | --- |
| `id` | UUID |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad name, interval, severity, condition, or unknown `viewer_id`. |
| 401 | Bearer auth required. |
| 500 / 503 | Store failure. |

### Example

```
scripts/litelemetry.sh POST /api/alerts '{
  "name": "checkout p95 high",
  "viewer_id": "...",
  "condition": {"threshold": {"metric": "p95_ms", "op": "gt", "value": 500}},
  "severity": "warn",
  "evaluation_interval_ms": 60000
}'
```

---

## PATCH /api/alerts/{id}

Partial update.

**Location**: `src/server.rs:9587-9629`

### Request body (`PatchAlertRequest`)

Any subset of:

| Field | Type | Notes |
| --- | --- | --- |
| `name` | string | |
| `viewer_id` | UUID | |
| `condition` | JSON value | |
| `severity` | string | |
| `evaluation_interval_ms` | integer | |
| `enabled` | bool | |

### Response 200

Empty body.

### Errors

| Code | Cause |
| --- | --- |
| 400 | Validation failure. |
| 401 | Bearer auth required. |
| 404 | Not found. |
| 500 / 503 | Store failure. |

### Example: temporarily pause an alert

```
scripts/litelemetry.sh PATCH /api/alerts/<id> '{"enabled": false}'
```

---

## DELETE /api/alerts/{id}

**Location**: `src/server.rs:9631-9644`

### Response 204

Empty body. Already-open incidents linked to this alert remain in the incident store.
