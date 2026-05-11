# Miscellaneous

Incidents, notification channels, exemplars, rollups. These are usually called from the UI but are exposed for completeness.

---

## Incidents

Incidents are state machines: `open` → `acknowledged` → `resolved`. Created by alert firing or manually via the API.

### GET /api/incidents

Location: `src/server.rs:10756-10773`.

Query params:

| Param | Type | Notes |
| --- | --- | --- |
| `status` | string | Filter by `open`, `acknowledged`, or `resolved`. |

Response 200 (`IncidentListResponse`): `{ "incidents": [Incident, ...] }`. `Incident` fields: `id`, `alert_id?`, `status`, `severity`, `opened_at`, `acknowledged_at?`, `resolved_at?`, `details_json`.

Errors: 400 (bad status), 401, 500.

```
scripts/litelemetry.sh GET '/api/incidents?status=open'
```

### POST /api/incidents

Location: `src/server.rs:10791-10823`. Body (`CreateIncidentRequest`):

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `alert_id` | UUID | no | Link to an alert if applicable. |
| `severity` | string | yes | 1–32 chars after trimming. |
| `details_json` | JSON value | no | Default `{}`. |

Response 201: full `Incident` with `status="open"`. Errors: 400, 401, 500.

```
scripts/litelemetry.sh POST /api/incidents '{"severity":"warn","details_json":{"note":"manual"}}'
```

### GET /api/incidents/{id}

Location: `src/server.rs:10775-10789`. Response 200: `Incident`. Errors: 401, 404, 500.

### PATCH /api/incidents/{id}

Location: `src/server.rs:10825-10858`. Body (`PatchIncidentRequest`): `{"status": "acknowledged" | "resolved"}`. Forward-only transitions; you cannot move `resolved` back to `acknowledged`.

Response 200: updated `Incident`. Errors: 400 (bad/illegal transition), 401, 404, 500.

```
scripts/litelemetry.sh PATCH /api/incidents/<id> '{"status":"acknowledged"}'
```

---

## Notification Channels

Currently only the `webhook` kind is supported.

### GET /api/notification-channels

Location: `src/server.rs:9701-9712`. Response 200 (`NotificationChannelListResponse`): `{ "channels": [NotificationChannelSummary, ...] }`. Each item: `id`, `name`, `kind`, `config`, `enabled`. Errors: 401, 500, 503.

### POST /api/notification-channels

Location: `src/server.rs:9714-9757`. Body (`CreateNotificationChannelRequest`):

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | yes | 1–80 chars. |
| `kind` | string | yes | Must be `"webhook"`. |
| `config` | JSON object | yes | For `webhook`: `{"url": "https://..."}`. |
| `enabled` | bool | no | Default `true`. |

Response 201: `{ "id": "<uuid>" }`. Errors: 400 (bad name/kind/config), 401, 500, 503.

```
scripts/litelemetry.sh POST /api/notification-channels '{
  "name": "ops-channel",
  "kind": "webhook",
  "config": {"url": "https://hooks.example.com/abc"}
}'
```

### DELETE /api/notification-channels/{id}

Location: `src/server.rs:9759-9773`. Response 204. Errors: 401, 404, 500, 503.

### POST /api/notification-channels/{id}/test

Location: `src/server.rs:9775-9809`. Triggers a test delivery and returns the result synchronously.

Response 200 (`TestNotificationChannelResponse`): `{ "delivered": bool, "detail": "..."? }`. When delivery fails the server returns 502 with `delivered: false`. Errors: 401, 404, 502 (delivery failed), 500, 503.

```
scripts/litelemetry.sh POST /api/notification-channels/<id>/test ''
```

---

## Exemplars

### GET /api/exemplars

Sample trace ids bucketed alongside a metric viewer — used to jump from a chart spike to representative traces.

Location: `src/server.rs:7825-7870`.

Query params (`ExemplarsQuery`):

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `metric_viewer_id` | UUID | yes | Must point at a metric viewer. |
| `lookback_ms` | integer | no | Defaults from the viewer's own setting. |
| `bucket_ms` | integer | no | Default `60000`. |
| `max_samples` | integer | no | Per bucket; library default applies otherwise. |

Response 200 (`ExemplarsResponse`):

| Field | Type | Notes |
| --- | --- | --- |
| `metric_viewer_id` | UUID | Echo. |
| `bucket_ms` | integer | Echo / default. |
| `lookback_ms` | integer | Echo / default. |
| `buckets` | array of `ExemplarBucket` | Each carries a `bucket_start_ms` and a list of trace ids. |

Errors: 400 (bad viewer / params), 401, 500, 503.

```
scripts/litelemetry.sh GET '/api/exemplars?metric_viewer_id=<uuid>&lookback_ms=900000'
```

---

## Rollups

### GET /api/rollups

Pre-aggregated entry counts per service across time buckets.

Location: `src/server.rs:10025-10070`.

Query params (`RollupQueryParams`):

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `signal` | string | yes | `traces`, `metrics`, or `logs`. |
| `resolution` | string | no | `"1m"`, `"5m"`, or `"1h"`. Default `"1m"`. |
| `from` | integer | yes | Epoch milliseconds. |
| `to` | integer | yes | Epoch milliseconds. Must be `> from`. |

Response 200 (`RollupResponse`):

| Field | Type | Notes |
| --- | --- | --- |
| `signal` | string | Echo. |
| `resolution` | string | Echo. |
| `from_ms` | integer | Echo. |
| `to_ms` | integer | Echo. |
| `buckets` | array of `RollupBucketRow` | |

`RollupBucketRow`: `bucket_start_ms`, `count`, `by_service` (`{service_name: count}` map).

Errors: 400 (bad signal/resolution/range), 401, 500, 503.

```
NOW_MS=$(date +%s)000
FROM_MS=$((NOW_MS - 3600000))
scripts/litelemetry.sh GET "/api/rollups?signal=traces&resolution=1m&from=$FROM_MS&to=$NOW_MS"
```
