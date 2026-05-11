# litelemetry REST API — Endpoint Index

This is the index. Find the row matching what you want to do, then open the linked detail file.

All routes are mounted under the same `LITELEMETRY_BASE_URL` and share the same `Authorization: Bearer lt_...` middleware. OTLP ingest (`/v1/traces`, `/v1/metrics`, `/v1/logs`) is **outside this skill's scope** and is never auth-protected.

## API Keys — see [api-keys.md](api-keys.md)

| Method | Path | Summary |
| --- | --- | --- |
| `POST` | `/api/api-keys` | Create an API key. Bootstrap-only when the store is empty. |
| `GET` | `/api/api-keys` | List API keys (without the secret). |
| `DELETE` | `/api/api-keys/{id}` | Revoke an API key. |

## Query — see [query.md](query.md)

| Method | Path | Summary |
| --- | --- | --- |
| `POST` | `/api/query` | Ad-hoc SQL over recent signals. |

## Viewers — see [viewers.md](viewers.md)

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/viewers` | List viewers with their current data snapshots. |
| `POST` | `/api/viewers` | Create a viewer (chart definition). |
| `POST` | `/api/viewers/preview` | Preview a viewer query without saving. |
| `GET` | `/api/viewers/{id}` | Fetch a single viewer with its data. |
| `PATCH` | `/api/viewers/{id}` | Update viewer definition fields. |
| `DELETE` | `/api/viewers/{id}` | Delete a viewer. |

## Dashboards — see [dashboards.md](dashboards.md)

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/dashboards` | List dashboards. |
| `POST` | `/api/dashboards` | Create a dashboard from existing viewers. |
| `GET` | `/api/dashboards/{id}` | Fetch a dashboard with rendered panel data. |
| `PATCH` | `/api/dashboards/{id}` | Update name / columns / panel layout. |
| `DELETE` | `/api/dashboards/{id}` | Delete a dashboard (viewers remain). |
| `GET` | `/api/dashboards/{id}/export` | Export dashboard + embedded viewers as JSON. |
| `POST` | `/api/dashboards/import` | Import a previously exported dashboard. |
| `GET` | `/api/templates` | List bundled dashboard templates. |
| `POST` | `/api/dashboards/from-template` | Materialize a template into a new dashboard. |

## Traces — see [traces.md](traces.md)

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/traces?trace_id=...` | Fetch one trace with all spans expanded. |
| `GET` | `/api/traces/search` | List recent traces, filtered by service / min duration. |
| `GET` | `/api/traces/{trace_id}/waterfall` | Get the span waterfall view of a single trace. |

## APM — see [apm.md](apm.md)

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/service-map` | Service-to-service call graph with p95 latency / error rate. |
| `GET` | `/api/error-groups` | Error fingerprints with counts and samples. |
| `GET` | `/api/slow-queries` | DB statements aggregated by p50/p95/p99 duration. |
| `GET` | `/api/n-plus-one` | Suspected N+1 patterns (repeated SQL in one trace). |

## Attributes — see [attributes.md](attributes.md)

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/services` | Distinct service names seen recently. |
| `GET` | `/api/attributes` | All attribute keys observed. |
| `GET` | `/api/attributes/{key}/values` | Distinct values for one attribute key. |

## Alerts — see [alerts.md](alerts.md)

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/alerts` | List alerts. |
| `POST` | `/api/alerts` | Create an alert tied to a viewer. |
| `GET` | `/api/alerts/{id}` | Fetch one alert. |
| `PATCH` | `/api/alerts/{id}` | Update an alert (any subset of fields). |
| `DELETE` | `/api/alerts/{id}` | Delete an alert. |

## SLOs — see [slos.md](slos.md)

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/slos` | List SLOs. |
| `POST` | `/api/slos` | Create an SLO with success / total filter clauses. |
| `GET` | `/api/slos/{id}` | Fetch one SLO. |
| `DELETE` | `/api/slos/{id}` | Delete an SLO. |
| `GET` | `/api/slos/{id}/budget` | Compute the current error budget for an SLO. |

## Anomaly — see [anomaly.md](anomaly.md)

| Method | Path | Summary |
| --- | --- | --- |
| `POST` | `/api/anomaly/evaluate` | Evaluate an anomaly detector against a viewer's data. |

## Miscellaneous — see [misc.md](misc.md)

Incidents, notification channels, exemplars, rollups. Most are read-heavy or rarely touched by agents; see the file for curl examples.

| Method | Path | Summary |
| --- | --- | --- |
| `GET` | `/api/incidents` | List incidents, optionally filtered by status. |
| `POST` | `/api/incidents` | Open a new incident. |
| `GET` | `/api/incidents/{id}` | Fetch one incident. |
| `PATCH` | `/api/incidents/{id}` | Transition incident status (open → acknowledged → resolved). |
| `GET` | `/api/notification-channels` | List webhook destinations. |
| `POST` | `/api/notification-channels` | Create a webhook channel. |
| `DELETE` | `/api/notification-channels/{id}` | Delete a channel. |
| `POST` | `/api/notification-channels/{id}/test` | Send a test payload to a channel. |
| `GET` | `/api/exemplars` | Trace sample IDs bucketed alongside a metric viewer. |
| `GET` | `/api/rollups` | Time-bucketed entry counts per service. |

## Auth & bootstrap

For the `Authorization: Bearer lt_...` rules and the bootstrap-once exception, read [auth.md](auth.md) and use `scripts/bootstrap.sh`.

## Source

Route definitions live at `src/server.rs:7593-7661`; each detail page cites the per-handler line range.
