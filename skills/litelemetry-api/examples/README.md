# Sample responses

The files in this directory show the JSON shape returned by each endpoint. They are illustrative — the field types match the source structs, but the values are fabricated and not pulled from any specific deployment.

Use these to scaffold code that walks the responses (especially the nested ones like waterfalls and service maps). For exact field meanings, see the matching `reference/<category>.md`.

| File | Endpoint | Reference |
| --- | --- | --- |
| `api-keys.create.json` | `POST /api/api-keys` (response 201) | `reference/api-keys.md` |
| `api-keys.list.json` | `GET /api/api-keys` | `reference/api-keys.md` |
| `viewers.list.json` | `GET /api/viewers` | `reference/viewers.md` |
| `dashboards.list.json` | `GET /api/dashboards` | `reference/dashboards.md` |
| `traces.search.json` | `GET /api/traces/search` | `reference/traces.md` |
| `traces.waterfall.json` | `GET /api/traces/{trace_id}/waterfall` | `reference/traces.md` |
| `service-map.json` | `GET /api/service-map` | `reference/apm.md` |
| `error-groups.list.json` | `GET /api/error-groups` | `reference/apm.md` |
| `slow-queries.json` | `GET /api/slow-queries` (bare array) | `reference/apm.md` |
| `alerts.list.json` | `GET /api/alerts` | `reference/alerts.md` |
| `slos.list.json` | `GET /api/slos` | `reference/slos.md` |
| `query.response.json` | `POST /api/query` (response 200) | `reference/query.md` |

Endpoints not represented here are either trivial (single id / 204 / echo) or shape-identical to one already shown (e.g. `GET /api/viewers/{id}` returns one item of the `viewers.list.json` array).
