#!/bin/sh
set -eu

REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"
SEED_MARKER_KEY="${SEED_MARKER_KEY:-lt:seed:compose-demo:v1}"
STREAM_KEY="${STREAM_KEY:-lt:stream:traces}"

redis_cli() {
  redis-cli --raw -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
}

while ! redis_cli ping >/dev/null 2>&1; do
  sleep 1
done

if [ "$(redis_cli SET "$SEED_MARKER_KEY" 1 NX)" != "OK" ]; then
  exit 0
fi

add_trace() {
  service_name="$1"
  span_name="$2"
  trace_id="$3"
  span_id="$4"
  observed_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  payload="$(printf '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"%s"}}]},"scopeSpans":[{"scope":{"name":"compose-seed"},"spans":[{"traceId":"%s","spanId":"%s","name":"%s","kind":1}]}]}]}' "$service_name" "$trace_id" "$span_id" "$span_name")"
  redis_cli XADD "$STREAM_KEY" '*' observed_at "$observed_at" service_name "$service_name" payload "$payload" >/dev/null
}

add_trace frontend-web http_request 00000000000000000000000000000001 0000000000000001
add_trace checkout-api validate_cart 00000000000000000000000000000002 0000000000000002
add_trace worker-billing persist_receipt 00000000000000000000000000000003 0000000000000003
