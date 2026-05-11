# Anomaly

One-shot detector evaluation against a viewer's current data buffer. Useful for "score this viewer right now" interactions.

---

## POST /api/anomaly/evaluate

**Location**: `src/server.rs:7912-7926`

### Request body (`EvaluateAnomalyRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `viewer_id` | UUID | yes | The viewer whose entries are scored. |
| `detector` | `DetectorSpec` | yes | Detector configuration. |

`DetectorSpec` is defined in `crate::anomaly`. The discriminator is typically a `type` field selecting the detector family (e.g. `"zscore"`, `"iqr"`); each family has its own params like `threshold`, `min_samples`, `window_ms`. Refer to the source for the current discriminated-union shape.

### Response 200 (`DetectorResult`)

Detector-family-dependent. Common fields:

| Field | Type | Notes |
| --- | --- | --- |
| `score` | number | Anomaly score / z-value. |
| `is_anomaly` | bool | |
| `points` | array | Optional series of scored buckets. |

Treat it as opaque JSON and inspect the shape returned for the specific detector you chose.

### Errors

| Code | Cause |
| --- | --- |
| 400 | Bad detector spec. |
| 401 | Bearer auth required. |
| 404 | Viewer not found. |
| 500 / 503 | Runtime failure. |

### Example

```
scripts/litelemetry.sh POST /api/anomaly/evaluate '{
  "viewer_id": "...",
  "detector": {"type": "zscore", "threshold": 3.0, "min_samples": 30}
}'
```
