# Authentication

All `/api/*` endpoints share a single Bearer-token middleware. OTLP ingest (`/v1/...`) and `/healthz` are never auth-protected.

## Enable / disable

Authentication is **off by default** for backwards compatibility. Turn it on by setting the env var on the litelemetry process:

```
API_KEYS_ENABLED=true
```

When the env var is unset or evaluates to a falsy value, every `/api/*` request passes through with no Authorization header inspection (`src/api_auth.rs:16-18`). `scripts/litelemetry.sh` is compatible with this mode — it warns to stderr if `LITELEMETRY_API_KEY` is unset but still sends the request.

Recognized truthy values for `API_KEYS_ENABLED`: `true` (case-insensitive) and `1`. See `src/main.rs:57-60`.

## Key format

- Prefix `lt_` followed by **128 hex characters** (two concatenated UUID v4 values).
- 256 bits of entropy.
- Generated server-side; never accept user-supplied key strings.
- The raw key is shown **exactly once**, in the `POST /api/api-keys` response body. Only its SHA-256 hash is stored in the backing store (`src/domain/api_key.rs:5-15`).
- Treat keys as secrets — they grant full `/api/*` access.

## Sending the key

Single header, exact-case scheme:

```
Authorization: Bearer lt_<128hex>
```

The middleware rejects everything that does not match this exact pattern:
- Missing `Authorization` header → 401.
- A different scheme (e.g. `Basic`) → 401.
- Lowercase `bearer` → 401. The scheme is **case-sensitive** here (`src/api_auth.rs:51-61`).
- `Bearer ` followed by nothing (empty token) → 401.
- Any token whose SHA-256 hash does not match a stored key → 401.

The wrapper `scripts/litelemetry.sh` always sends `Authorization: Bearer ${LITELEMETRY_API_KEY}` when the env var is set, so writing keys correctly is the operator's responsibility.

## Bootstrap: issuing the first key

When `API_KEYS_ENABLED=true` is freshly turned on, the key store starts empty. The middleware grants exactly one special case:

> A `POST /api/api-keys` request is allowed through **without authentication** as long as the store still contains zero keys (`src/api_auth.rs:20-32`).

Use `scripts/bootstrap.sh` for this single call. After the first key exists, every subsequent request (including additional `POST /api/api-keys`) requires a valid Bearer token. If you lose the bootstrap key in standalone mode (memory store), restart litelemetry to clear it and bootstrap again.

## Storage modes and key lifetime

| Mode | Store | Persistence | When to use |
| --- | --- | --- | --- |
| Standalone (default) | `MemoryApiKeyStore` | **Cleared on restart** | Local development. Bootstrap every restart. |
| Postgres | `PostgresApiKeyStore` (`api_keys` table) | Persistent | Production. Auto-creates the table at startup. |

The Postgres mode is selected when `STANDALONE=false` and `DATABASE_URL` is set. Schema: `src/storage/api_key_store.rs:20-75` and DDL at `docker/postgres/initdb/001-init.sql:174-181`.

## Status code reference

| Code | Meaning |
| --- | --- |
| 200 / 201 / 204 | Request succeeded. |
| 401 | Missing/wrong-form Authorization header, or the key hash did not match. |
| 500 | Internal failure during key lookup (logged server-side). |
| 503 | `API_KEYS_ENABLED=true` but the backing store is not configured. |

## Implementation entry points

- Middleware: `src/api_auth.rs:11-49`
- Bearer parsing: `src/api_auth.rs:51-61`
- Hash function: `src/domain/api_key.rs:5-7`
- Store trait + impls: `src/storage/api_key_store.rs`
- Env wiring: `src/main.rs:57-60, 128, 193-195`
