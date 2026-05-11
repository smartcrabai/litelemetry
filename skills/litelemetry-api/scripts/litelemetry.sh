#!/usr/bin/env bash
# litelemetry.sh — Authenticated HTTP wrapper for the litelemetry REST API.
#
# Usage:
#   litelemetry.sh <METHOD> <PATH> [BODY]
#
#   METHOD  GET | POST | PATCH | PUT | DELETE
#   PATH    Path starting with "/api/..." (query string allowed).
#   BODY    Optional. Inline JSON, or "@file.json" to load from a file.
#
# Environment:
#   LITELEMETRY_BASE_URL   Required. e.g. http://localhost:8080
#   LITELEMETRY_API_KEY    Required when the server runs with API_KEYS_ENABLED=true.
#                          When unset, the script proceeds without an Authorization
#                          header (compatible with the default auth-off mode) and
#                          emits a warning to stderr.
#
# Output:
#   stdout: raw response body (pipe to jq if you want pretty JSON).
#   stderr: warnings, and on non-2xx the HTTP status with a truncated body.
#   exit:   0 on 2xx, 1 on any other status or transport error, 2 on misuse.

set -euo pipefail

die() {
    printf 'litelemetry.sh: %s\n' "$*" >&2
    exit 2
}

if [ "$#" -lt 2 ]; then
    die "usage: litelemetry.sh <METHOD> <PATH> [BODY]"
fi

method=$1
path=$2
body=${3:-}

case "$method" in
    GET|POST|PATCH|PUT|DELETE|HEAD) ;;
    *) die "unsupported method: $method (expected GET|POST|PATCH|PUT|DELETE|HEAD)" ;;
esac

case "$path" in
    /*) ;;
    *) die "path must start with '/': $path" ;;
esac

if [ -z "${LITELEMETRY_BASE_URL:-}" ]; then
    die "LITELEMETRY_BASE_URL is not set (e.g. export LITELEMETRY_BASE_URL=http://localhost:8080)"
fi

# Strip a trailing slash so concatenation produces clean URLs.
base_url=${LITELEMETRY_BASE_URL%/}
url=${base_url}${path}

curl_args=(--silent --show-error --request "$method" --write-out "\n%{http_code}")

if [ -n "${LITELEMETRY_API_KEY:-}" ]; then
    curl_args+=(--header "Authorization: Bearer ${LITELEMETRY_API_KEY}")
else
    printf 'litelemetry.sh: warning: LITELEMETRY_API_KEY is not set; sending request without Authorization header (works only when API_KEYS_ENABLED is false).\n' >&2
fi

if [ -n "$body" ]; then
    curl_args+=(--header "Content-Type: application/json")
    case "$body" in
        @*)
            file=${body#@}
            if [ ! -r "$file" ]; then
                die "cannot read body file: $file"
            fi
            curl_args+=(--data-binary "@$file")
            ;;
        *)
            curl_args+=(--data "$body")
            ;;
    esac
fi

response=$(curl "${curl_args[@]}" "$url")
status=${response##*$'\n'}
body_out=${response%$'\n'*}

case "$status" in
    2*)
        printf '%s' "$body_out"
        # Ensure trailing newline for terminal readability when body is non-empty.
        if [ -n "$body_out" ]; then printf '\n'; fi
        exit 0
        ;;
    401)
        {
            printf 'litelemetry.sh: HTTP 401 from %s %s\n' "$method" "$path"
            if [ -z "${LITELEMETRY_API_KEY:-}" ]; then
                printf '  LITELEMETRY_API_KEY is not set. Run scripts/bootstrap.sh to issue the first key.\n'
            else
                printf '  The API key was rejected. Verify LITELEMETRY_API_KEY or rotate via POST /api/api-keys.\n'
            fi
            printf '  Body: %s\n' "${body_out:0:500}"
        } >&2
        exit 1
        ;;
    *)
        {
            printf 'litelemetry.sh: HTTP %s from %s %s\n' "$status" "$method" "$path"
            printf '  Body: %s\n' "${body_out:0:500}"
        } >&2
        exit 1
        ;;
esac
