# Services & Attributes

Discovery endpoints. Use these to populate dropdowns and to spell `service_name` / attribute keys correctly before crafting filters or SQL.

---

## GET /api/services

Distinct service names observed recently.

**Location**: `src/server.rs:7764-7770`

### Response 200 (`ServicesResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `services` | array of string | Sorted. |

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 | Runtime not initialized. |

### Example

```
scripts/litelemetry.sh GET /api/services
```

---

## GET /api/attributes

All attribute keys the attribute index has seen.

**Location**: `src/server.rs:7772-7783`

### Response 200 (`AttributeKeysResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `keys` | array of string | |

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 / 503 | Attribute index not configured / query failed. |

### Example

```
scripts/litelemetry.sh GET /api/attributes
```

---

## GET /api/attributes/{key}/values

Distinct values for one attribute key, optionally narrowed by prefix. Suitable for autocomplete UIs.

**Location**: `src/server.rs:7785-7802`

### Path params

| Param | Type | Notes |
| --- | --- | --- |
| `key` | string | URL-encoded. Empty rejected. |

### Query params (`AttributeValuesQuery`)

| Param | Type | Required | Notes |
| --- | --- | --- | --- |
| `prefix` | string | no | Case-sensitive prefix filter. |

### Response 200 (`AttributeValuesResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `key` | string | Echo. |
| `values` | array of string | |

### Errors

| Code | Cause |
| --- | --- |
| 400 | Empty `key`. |
| 401 | Bearer auth required. |
| 500 / 503 | Attribute index not configured. |

### Example

```
scripts/litelemetry.sh GET '/api/attributes/http.method/values?prefix=P'
```
