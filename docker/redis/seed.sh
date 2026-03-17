#!/bin/sh
set -eu

REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"
SEED_MARKER_KEY="${SEED_MARKER_KEY:-lt:seed:compose-demo:v3}"
TRACES_STREAM_KEY="${TRACES_STREAM_KEY:-lt:stream:traces}"
METRICS_STREAM_KEY="${METRICS_STREAM_KEY:-lt:stream:metrics}"
LOGS_STREAM_KEY="${LOGS_STREAM_KEY:-lt:stream:logs}"

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
  redis_cli XADD "$TRACES_STREAM_KEY" '*' observed_at "$observed_at" service_name "$service_name" payload "$payload" >/dev/null
}

add_metric_at() {
  service_name="$1"
  metric_name="$2"
  metric_value="$3"
  offset_sec="${4:-0}"
  now_epoch="$(date +%s)"
  past_epoch="$((now_epoch - offset_sec))"
  observed_at="$(date -u -d "@${past_epoch}" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u -r "$past_epoch" +"%Y-%m-%dT%H:%M:%SZ")"
  time_unix_nano="$((past_epoch * 1000000000))"
  payload="$(printf '{"resourceMetrics":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"%s"}}]},"scopeMetrics":[{"scope":{"name":"compose-seed"},"metrics":[{"name":"%s","sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"asInt":"%s","timeUnixNano":"%s"}]}}]}]}]}' "$service_name" "$metric_name" "$metric_value" "$time_unix_nano")"
  redis_cli XADD "$METRICS_STREAM_KEY" '*' observed_at "$observed_at" service_name "$service_name" payload "$payload" >/dev/null
}

add_metric() {
  add_metric_at "$1" "$2" "$3" 0
}

add_log() {
  service_name="$1"
  severity_text="$2"
  message="$3"
  observed_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  time_unix_nano="$(($(date +%s) * 1000000000))"
  payload="$(printf '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"%s"}}]},"scopeLogs":[{"scope":{"name":"compose-seed"},"logRecords":[{"timeUnixNano":"%s","observedTimeUnixNano":"%s","severityNumber":9,"severityText":"%s","body":{"stringValue":"%s"}}]}]}]}' "$service_name" "$time_unix_nano" "$time_unix_nano" "$severity_text" "$message")"
  redis_cli XADD "$LOGS_STREAM_KEY" '*' observed_at "$observed_at" service_name "$service_name" payload "$payload" >/dev/null
}

add_trace frontend-web http_request 00000000000000000000000000000001 0000000000000001
add_trace checkout-api validate_cart 00000000000000000000000000000002 0000000000000002
add_trace worker-billing persist_receipt 00000000000000000000000000000003 0000000000000003

add_metric orders-api http.server.requests 42
add_metric checkout-api queue.depth 7

add_log worker-billing INFO payment_authorized
add_log frontend-web WARN checkout_latency_high

# ─── チャート用メトリクスデータ (直近5分間に分散) ─────────────────────────
# http.server.requests — 3サービス × 6ポイント (30秒間隔)
add_metric_at orders-api    http.server.requests 120 270
add_metric_at orders-api    http.server.requests 135 240
add_metric_at orders-api    http.server.requests 148 210
add_metric_at orders-api    http.server.requests 160 180
add_metric_at orders-api    http.server.requests 175 150
add_metric_at orders-api    http.server.requests 190 120

add_metric_at checkout-api  http.server.requests  85 270
add_metric_at checkout-api  http.server.requests  92 240
add_metric_at checkout-api  http.server.requests  78 210
add_metric_at checkout-api  http.server.requests 105 180
add_metric_at checkout-api  http.server.requests 110 150
add_metric_at checkout-api  http.server.requests  95 120

add_metric_at frontend-web  http.server.requests 200 270
add_metric_at frontend-web  http.server.requests 210 240
add_metric_at frontend-web  http.server.requests 195 210
add_metric_at frontend-web  http.server.requests 220 180
add_metric_at frontend-web  http.server.requests 240 150
add_metric_at frontend-web  http.server.requests 230 120

# http.server.duration_ms — レイテンシ推移 (折れ線向き)
add_metric_at orders-api    http.server.duration_ms  45 270
add_metric_at orders-api    http.server.duration_ms  52 240
add_metric_at orders-api    http.server.duration_ms  48 210
add_metric_at orders-api    http.server.duration_ms  61 180
add_metric_at orders-api    http.server.duration_ms  55 150
add_metric_at orders-api    http.server.duration_ms  50 120

add_metric_at checkout-api  http.server.duration_ms  30 270
add_metric_at checkout-api  http.server.duration_ms  35 240
add_metric_at checkout-api  http.server.duration_ms  28 210
add_metric_at checkout-api  http.server.duration_ms  42 180
add_metric_at checkout-api  http.server.duration_ms  38 150
add_metric_at checkout-api  http.server.duration_ms  33 120
