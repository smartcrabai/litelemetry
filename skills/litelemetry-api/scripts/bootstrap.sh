#!/usr/bin/env bash
# bootstrap.sh — Issue the first litelemetry API key.
#
# Usage:
#   bootstrap.sh [KEY_NAME]
#
#   KEY_NAME  Human-readable label for the new key. Defaults to "bootstrap".
#
# Environment:
#   LITELEMETRY_BASE_URL   Required. e.g. http://localhost:8080
#
# This script is intentionally limited to the bootstrap path: it calls
# POST /api/api-keys *without* an Authorization header. The server accepts
# this only when the key store is empty (see src/api_auth.rs:20-32). Once a
# key exists, use scripts/litelemetry.sh with LITELEMETRY_API_KEY set instead.
#
# Output:
#   stdout: the raw "lt_..." key string on success (so you can pipe it).
#   stderr: status, helper messages, and an export hint.

set -euo pipefail

die() {
    printf 'bootstrap.sh: %s\n' "$*" >&2
    exit 2
}

key_name=${1:-bootstrap}

if [ -z "${LITELEMETRY_BASE_URL:-}" ]; then
    die "LITELEMETRY_BASE_URL is not set (e.g. export LITELEMETRY_BASE_URL=http://localhost:8080)"
fi

base_url=${LITELEMETRY_BASE_URL%/}

# Escape backslashes and double quotes for safe JSON embedding.
escaped_name=${key_name//\\/\\\\}
escaped_name=${escaped_name//\"/\\\"}

response=$(curl --silent --show-error --request POST \
    --write-out "\n%{http_code}" \
    --header "Content-Type: application/json" \
    --data "{\"name\":\"${escaped_name}\"}" \
    "${base_url}/api/api-keys")

status=${response##*$'\n'}
body=${response%$'\n'*}

case "$status" in
    2*) ;;
    401)
        {
            printf 'bootstrap.sh: HTTP 401 from POST /api/api-keys\n'
            printf '  The key store is not empty, so the bootstrap path is closed.\n'
            printf '  Use an existing key with scripts/litelemetry.sh, or rotate via\n'
            printf '    LITELEMETRY_API_KEY=<existing> scripts/litelemetry.sh POST /api/api-keys '"'"'{"name":"%s"}'"'"'\n' "$key_name"
            printf '  Body: %s\n' "${body:0:500}"
        } >&2
        exit 1
        ;;
    503)
        {
            printf 'bootstrap.sh: HTTP 503 from POST /api/api-keys\n'
            printf '  The API key store is not configured. Start litelemetry with API_KEYS_ENABLED=true\n'
            printf '  and ensure DATABASE_URL is set (Postgres mode) or use STANDALONE=true (memory).\n'
            printf '  Body: %s\n' "${body:0:500}"
        } >&2
        exit 1
        ;;
    *)
        {
            printf 'bootstrap.sh: HTTP %s from POST /api/api-keys\n' "$status"
            printf '  Body: %s\n' "${body:0:500}"
        } >&2
        exit 1
        ;;
esac

extract_key() {
    if command -v jq >/dev/null 2>&1; then
        printf '%s' "$1" | jq -r '.key'
    else
        printf '%s' "$1" | grep -o '"key"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n1 | sed -E 's/.*"key"[[:space:]]*:[[:space:]]*"([^"]*)".*/\1/'
    fi
}

key=$(extract_key "$body")

if [ -z "$key" ] || [ "$key" = "null" ]; then
    {
        printf 'bootstrap.sh: server returned 2xx but no "key" field was found.\n'
        printf '  Body: %s\n' "${body:0:500}"
    } >&2
    exit 1
fi

printf '%s\n' "$key"

{
    printf '\n'
    printf 'bootstrap.sh: created API key "%s".\n' "$key_name"
    printf '  Export it for the wrapper script:\n'
    printf '    export LITELEMETRY_API_KEY=%s\n' "$key"
    printf '  Keep this value secret — the server no longer stores the plaintext.\n'
} >&2
