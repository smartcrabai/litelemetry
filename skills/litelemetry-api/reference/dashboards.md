# Dashboards

Dashboards arrange existing viewers into a grid layout. They can be exported / imported as JSON (e.g. for promotion between environments) or materialized from bundled templates.

---

## GET /api/dashboards

List dashboards.

**Location**: `src/server.rs:8670-8694`

### Response 200 (`DashboardListResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `dashboards` | array of `DashboardListItem` | |

`DashboardListItem`:

| Field | Type | Notes |
| --- | --- | --- |
| `id` | UUID | |
| `slug` | string | |
| `name` | string | |
| `panel_count` | integer | Number of panels (viewers). |
| `viewer_ids` | array of UUID | Viewers referenced by the panels, in panel order. |

Sample: [examples/dashboards.list.json](../examples/dashboards.list.json).

### Example

```
scripts/litelemetry.sh GET /api/dashboards
```

---

## POST /api/dashboards

Create a dashboard from one or more existing viewers.

**Location**: `src/server.rs:8696-8736`

### Request body (`CreateDashboardRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | yes | 1–80 chars. |
| `viewer_ids` | array of UUID | no | Quick path: laid out 1×1 in order. Default `[]`. |
| `panels` | array of `PanelRequestItem` | no | Explicit layout. If both `viewer_ids` and `panels` are supplied, `panels` wins. |
| `columns` | integer | no | Grid width, 1–24. Default 12. |

`PanelRequestItem`:

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `viewer_id` | UUID | — | Must reference an existing viewer. |
| `col_span` | integer | 1 | 1 ≤ `col_span` ≤ `columns`. |
| `row_span` | integer | 1 | ≥ 1. |

### Response 201

| Field | Type |
| --- | --- |
| `id` | UUID |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Name length, invalid `columns`, oversized `col_span`. |
| 401 | Bearer auth required. |
| 500 / 503 | Store failures. |

### Example

```
scripts/litelemetry.sh POST /api/dashboards '{
  "name": "Checkout overview",
  "columns": 12,
  "panels": [
    {"viewer_id": "...", "col_span": 6, "row_span": 1},
    {"viewer_id": "...", "col_span": 6, "row_span": 1}
  ]
}'
```

---

## GET /api/dashboards/{id}

Fetch a dashboard with each panel's rendered viewer data attached.

**Location**: `src/server.rs:8932-9060`

### Path params

| Param | Type |
| --- | --- |
| `id` | UUID |

### Query params

| Param | Type | Notes |
| --- | --- | --- |
| `lookback_ms` | integer | Override window for all panels. Must be positive and not exceed any viewer's max. |
| `service_name` | string | Comma-separated list of service names to filter every panel by. |
| `query` | string | Extra query expression layered on top of each viewer. |

### Response 200 (`DashboardDetailResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `id` | UUID | |
| `slug` | string | |
| `name` | string | |
| `columns` | integer | |
| `max_lookback_ms` | integer \| null | The smallest cap across panels, useful for UI sliders. |
| `panels` | array of `DashboardPanel` | |

`DashboardPanel`:

| Field | Type | Notes |
| --- | --- | --- |
| `viewer_id` | UUID | |
| `position` | integer | Order in the layout. |
| `col_span` | integer | |
| `row_span` | integer | |
| `viewer` | `ViewerSummary` \| null | `null` if the viewer was deleted. |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Invalid `lookback_ms`. |
| 401 | Bearer auth required. |
| 404 | Dashboard not found. |
| 500 / 503 | Load failure. |

---

## PATCH /api/dashboards/{id}

Partial update. Any subset of name, columns, or panel layout.

**Location**: `src/server.rs:9062-9146`

### Request body (`PatchDashboardRequest`)

| Field | Type | Notes |
| --- | --- | --- |
| `name` | string \| null | 1–80 chars when provided. |
| `viewer_ids` | array of UUID \| null | Reorder existing panels. Preserves any matching panel's existing `col_span`/`row_span`. |
| `panels` | array of `PanelRequestItem` \| null | Wholesale replace layout. |
| `columns` | integer \| null | |

### Response 200

Empty body.

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad name, columns, or layout. |
| 401 | Bearer auth required. |
| 404 | Dashboard not found. |
| 500 / 503 | Store failure. |

---

## DELETE /api/dashboards/{id}

Removes the dashboard but **does not delete the referenced viewers**.

**Location**: `src/server.rs:9169-9188`

### Response 204

Empty body.

---

## GET /api/dashboards/{id}/export

Produce a self-contained envelope: the dashboard metadata plus every viewer it references. Designed for `POST /api/dashboards/import` on a different deployment.

**Location**: `src/server.rs:9189-9284`

### Response 200 (`DashboardExportEnvelope`)

| Field | Type | Notes |
| --- | --- | --- |
| `version` | integer | Format version. Currently `1`. |
| `dashboard` | `DashboardExportData` | Name, columns, panels. |
| `viewers` | array of `ViewerExportData` | Full viewer definitions. |

`DashboardExportData` carries `name`, `columns`, and `panels` (each with `viewer_id`, `col_span`, `row_span`).
`ViewerExportData` carries `id`, `name`, `signal`, `chart_type`, `lookback_ms`, `query?`, `filters` (array), `filter_mode` (default `"and"`).

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 404 | Dashboard not found. |
| 500 / 503 | Load failure. |

### Example

```
scripts/litelemetry.sh GET /api/dashboards/<id>/export > dashboard.json
```

---

## POST /api/dashboards/import

Reverse of `export`. Creates new viewers and a new dashboard from an envelope.

**Location**: `src/server.rs:9286-9397`

### Request body

A `DashboardExportEnvelope`. Viewer ids inside the envelope are remapped to fresh UUIDs on the importing side, so re-import is safe.

### Response 201

| Field | Type | Notes |
| --- | --- | --- |
| `dashboard_id` | UUID | The freshly created dashboard. |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad envelope version, name lengths, invalid signals / chart types, or compile errors. |
| 401 | Bearer auth required. |
| 500 / 503 | Store / runtime failure. |

### Example

```
scripts/litelemetry.sh POST /api/dashboards/import @dashboard.json
```

---

## GET /api/templates

List bundled templates (e.g. "HTTP service overview"). No auth-state-changing side effects.

**Location**: `src/server.rs:8738-8751`

### Response 200 (`TemplateListResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `templates` | array of `TemplateInfo` | |

`TemplateInfo`: `id`, `name`, `description`, `signal_types` (array), `viewer_count`, `columns`.

---

## POST /api/dashboards/from-template

Materialize a template — creates the underlying viewers, then a dashboard referencing them.

**Location**: `src/server.rs:8765-8932`

### Request body (`CreateDashboardFromTemplateRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `template_id` | string | yes | From `GET /api/templates`. |
| `name` | string | no | Defaults to the template's bundled name. |

### Response 201

| Field | Type | Notes |
| --- | --- | --- |
| `dashboard_id` | UUID | |
| `viewer_ids` | array of UUID | Newly created viewers, in panel order. |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad name length. |
| 404 | Unknown `template_id`. |
| 500 / 503 | Store / runtime failure. |
