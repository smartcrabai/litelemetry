# API Keys

Manage the `Bearer lt_...` tokens used by every other `/api/*` endpoint. Authentication rules and the bootstrap exception are described in [auth.md](auth.md).

---

## POST /api/api-keys

Create a new API key. Bootstrap-only path when the store is empty; otherwise requires an existing valid key.

**Location**: `src/server.rs:11049-11101`

### Request body (`CreateApiKeyRequest`)

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `name` | string | yes | Human-readable label. Trimmed; empty is rejected. |

### Response 201 (`CreateApiKeyResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `id` | UUID | Stable identifier for revocation. |
| `name` | string | Echo of input. |
| `key` | string | Raw `lt_<128hex>` token. **Shown only here. Store it now.** |
| `created_at` | RFC 3339 timestamp | UTC. |

Response is sent with `Cache-Control: no-store, no-cache, must-revalidate, private` to prevent intermediary caching.

### Errors

| Code | Cause |
| --- | --- |
| 400 | `name` is empty after trimming. |
| 401 | Store is non-empty and the Bearer token is missing/invalid. |
| 500 | Insert failed in the backing store. |
| 503 | `API_KEYS_ENABLED=true` but no store is configured. |

### Examples

Bootstrap (no auth header — works only when store is empty):

```
LITELEMETRY_BASE_URL=http://localhost:8080 \
  scripts/bootstrap.sh production
```

Rotate / add another key after bootstrap:

```
scripts/litelemetry.sh POST /api/api-keys '{"name":"ci-runner"}'
```

Sample response: [examples/api-keys.create.json](../examples/api-keys.create.json).

---

## GET /api/api-keys

List existing keys without the secret material.

**Location**: `src/server.rs:11103-11123`

### Response 200 (`ApiKeyListResponse`)

| Field | Type | Notes |
| --- | --- | --- |
| `api_keys` | array of `ApiKeyListItem` | One row per key. |

`ApiKeyListItem`:

| Field | Type | Notes |
| --- | --- | --- |
| `id` | UUID | Use with `DELETE /api/api-keys/{id}`. |
| `name` | string | |
| `created_at` | RFC 3339 timestamp | UTC. |

The raw key and the SHA-256 hash are intentionally omitted.

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 500 | Store query failed. |
| 503 | Store not configured. |

### Example

```
scripts/litelemetry.sh GET /api/api-keys
```

Sample response: [examples/api-keys.list.json](../examples/api-keys.list.json).

---

## DELETE /api/api-keys/{id}

Revoke a key by its UUID. The deletion is immediate: subsequent requests with that key get 401.

**Location**: `src/server.rs:11125-11141`

### Path params

| Param | Type | Notes |
| --- | --- | --- |
| `id` | UUID | From a prior `GET /api/api-keys` row. |

### Response 204

Empty body.

### Errors

| Code | Cause |
| --- | --- |
| 401 | Bearer auth required. |
| 404 | No key with that id. |
| 500 | Store delete failed. |
| 503 | Store not configured. |

### Example

```
scripts/litelemetry.sh DELETE /api/api-keys/550e8400-e29b-41d4-a716-446655440000
```

If you delete the key you are currently authenticated with, the next request you make will fail with 401 — make sure another valid key exists before revoking your own.
