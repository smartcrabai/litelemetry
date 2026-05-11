# Query

Ad-hoc SQL across recent traces / metrics / logs entries. Use this when no built-in endpoint matches your shape.

---

## POST /api/query

**Location**: `src/server.rs:8421-8496`

### Request body (`QueryRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `sql` | string | yes | Non-empty SQL. The engine surface is litelemetry's signal table dialect (see in-app docs); standard `SELECT … FROM <signal> WHERE …` works. |
| `lookback_ms` | integer | no | How far back to scan (milliseconds). Defaults to 5 minutes. **Max 24 hours** — larger values are clamped or rejected. |

### Response 200 (`QueryResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `columns` | array of string | Column names in result order. |
| `rows` | array of arrays | Each inner array has one element per column. Cell type is JSON-native (string, number, bool, null, object). |
| `scanned` | integer | Total entries inspected within the lookback window. |
| `matched` | integer | Entries that passed the SQL `WHERE`. |
| `returned` | integer | Entries actually emitted (may be `< matched` if truncated). |
| `truncated` | bool | True if the result was capped (~100k rows). When true, narrow the `WHERE` clause or shrink `lookback_ms`. |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Empty SQL, parse error, or execution error. |
| 401 | Bearer auth required. |
| 500 | Stream read failure. |

### Examples

Most recent error spans:

```
scripts/litelemetry.sh POST /api/query \
  '{"sql":"SELECT trace_id, service_name, name FROM traces WHERE status_code = 2 LIMIT 50","lookback_ms":900000}'
```

Top services by span volume over 1 hour:

```
scripts/litelemetry.sh POST /api/query \
  '{"sql":"SELECT service_name, COUNT(*) AS n FROM traces GROUP BY service_name ORDER BY n DESC LIMIT 10","lookback_ms":3600000}'
```

Sample response shape: [examples/query.response.json](../examples/query.response.json).

### Pitfalls

- The lookback window is strictly bounded. Don't try to scan more than 24h in a single request — page by adjusting `lookback_ms` or filter further.
- `rows` cells can be arbitrary JSON (e.g. a span's `attributes` map). Pipe stdout through `jq` to walk nested structures.
- When `truncated: true`, the absolute counts in `scanned` / `matched` are still accurate; only `rows` is cut.
