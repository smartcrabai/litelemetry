#!/bin/sh
set -eu

REDIS_HOST="${REDIS_HOST:-redis}"
REDIS_PORT="${REDIS_PORT:-6379}"
SEED_MARKER_KEY="${SEED_MARKER_KEY:-lt:seed:compose-demo:v4}"
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
  parent_span_id="${5:-}"
  start_ns="${6:-}"
  end_ns="${7:-}"
  status_code="${8:-0}"
  observed_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  parent_field=""
  if [ -n "$parent_span_id" ]; then
    parent_field="$(printf ',"parentSpanId":"%s"' "$parent_span_id")"
  fi
  time_fields=""
  if [ -n "$start_ns" ] && [ -n "$end_ns" ]; then
    time_fields="$(printf ',"startTimeUnixNano":"%s","endTimeUnixNano":"%s"' "$start_ns" "$end_ns")"
  fi
  status_field=""
  if [ "$status_code" != "0" ]; then
    status_field="$(printf ',"status":{"code":%s}' "$status_code")"
  fi
  payload="$(printf '{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"%s"}}]},"scopeSpans":[{"scope":{"name":"compose-seed"},"spans":[{"traceId":"%s","spanId":"%s"%s,"name":"%s","kind":2%s%s}]}]}]}' "$service_name" "$trace_id" "$span_id" "$parent_field" "$span_name" "$time_fields" "$status_field")"
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

# ─── Trace 1: frontend → customer-api → mysql (normal case) ─────────────────
# All timestamps share the same base. base = now - 30 sec (nanoseconds)
now_epoch="$(date +%s)"
base_ns="$(( (now_epoch - 30) * 1000000000 ))"

TRACE1="aaaaaaaaaaaaaaaaaaaaaaaaaaaa0001"

# frontend: HTTP GET /dispatch (root, 0–700ms)
add_trace frontend    "HTTP GET /dispatch"     "$TRACE1" a000000000000001 "" \
  "$((base_ns))" "$((base_ns + 700000000))"

#   frontend: HTTP GET: /customer (10–320ms)
add_trace frontend    "HTTP GET: /customer"    "$TRACE1" a000000000000002 a000000000000001 \
  "$((base_ns +  10000000))" "$((base_ns + 320000000))"

#     frontend: HTTP GET (12–318ms)
add_trace frontend    "HTTP GET"               "$TRACE1" a000000000000003 a000000000000002 \
  "$((base_ns +  12000000))" "$((base_ns + 318000000))"

#       customer-api: HTTP GET /customer (15–315ms)
add_trace customer    "HTTP GET /customer"     "$TRACE1" a000000000000004 a000000000000003 \
  "$((base_ns +  15000000))" "$((base_ns + 315000000))"

#         mysql: SQL SELECT (18–310ms)
add_trace mysql       "SQL SELECT"             "$TRACE1" a000000000000005 a000000000000004 \
  "$((base_ns +  18000000))" "$((base_ns + 310000000))"

#   frontend: Driver::findNearest (330–530ms)
add_trace frontend    "Driver::findNearest"    "$TRACE1" a000000000000006 a000000000000001 \
  "$((base_ns + 330000000))" "$((base_ns + 530000000))"

#     driver: Driver::findNearest (332–528ms)
add_trace driver      "Driver::findNearest"    "$TRACE1" a000000000000007 a000000000000006 \
  "$((base_ns + 332000000))" "$((base_ns + 528000000))"

#       redis: FindDriverIDs (335–355ms)
add_trace redis       "FindDriverIDs"          "$TRACE1" a000000000000008 a000000000000007 \
  "$((base_ns + 335000000))" "$((base_ns + 355000000))"

#       redis: GetDriver (360–372ms)
add_trace redis       "GetDriver"              "$TRACE1" a000000000000009 a000000000000007 \
  "$((base_ns + 360000000))" "$((base_ns + 372000000))"

#       redis: GetDriver (375–410ms, ERROR)
add_trace redis       "GetDriver"              "$TRACE1" a00000000000000a a000000000000007 \
  "$((base_ns + 375000000))" "$((base_ns + 410000000))" 2

#       redis: GetDriver (415–426ms)
add_trace redis       "GetDriver"              "$TRACE1" a00000000000000b a000000000000007 \
  "$((base_ns + 415000000))" "$((base_ns + 426000000))"

#       redis: GetDriver (430–442ms)
add_trace redis       "GetDriver"              "$TRACE1" a00000000000000c a000000000000007 \
  "$((base_ns + 430000000))" "$((base_ns + 442000000))"

#       redis: GetDriver (445–458ms)
add_trace redis       "GetDriver"              "$TRACE1" a00000000000000d a000000000000007 \
  "$((base_ns + 445000000))" "$((base_ns + 458000000))"

#       redis: GetDriver (462–478ms)
add_trace redis       "GetDriver"              "$TRACE1" a00000000000000e a000000000000007 \
  "$((base_ns + 462000000))" "$((base_ns + 478000000))"

#       redis: GetDriver (480–520ms)
add_trace redis       "GetDriver"              "$TRACE1" a00000000000000f a000000000000007 \
  "$((base_ns + 480000000))" "$((base_ns + 520000000))"

#   frontend: HTTP GET: /route (540–590ms)
add_trace frontend    "HTTP GET: /route"       "$TRACE1" a000000000000010 a000000000000001 \
  "$((base_ns + 540000000))" "$((base_ns + 590000000))"

#     frontend: HTTP GET (541–588ms)
add_trace frontend    "HTTP GET"               "$TRACE1" a000000000000011 a000000000000010 \
  "$((base_ns + 541000000))" "$((base_ns + 588000000))"

#       route: HTTP GET /route (543–585ms)
add_trace route       "HTTP GET /route"        "$TRACE1" a000000000000012 a000000000000011 \
  "$((base_ns + 543000000))" "$((base_ns + 585000000))"

#   frontend: HTTP GET: /route (600–650ms)
add_trace frontend    "HTTP GET: /route"       "$TRACE1" a000000000000013 a000000000000001 \
  "$((base_ns + 600000000))" "$((base_ns + 650000000))"

#     frontend: HTTP GET (601–648ms)
add_trace frontend    "HTTP GET"               "$TRACE1" a000000000000014 a000000000000013 \
  "$((base_ns + 601000000))" "$((base_ns + 648000000))"

#       route: HTTP GET /route (603–645ms)
add_trace route       "HTTP GET /route"        "$TRACE1" a000000000000015 a000000000000014 \
  "$((base_ns + 603000000))" "$((base_ns + 645000000))"

# ─── Trace 2: checkout-api → payment (simple normal trace) ─────────────
TRACE2="aaaaaaaaaaaaaaaaaaaaaaaaaaaa0002"

add_trace checkout-api "POST /checkout"        "$TRACE2" b000000000000001 "" \
  "$((base_ns + 100000000))" "$((base_ns + 450000000))"
add_trace checkout-api "validate_cart"         "$TRACE2" b000000000000002 b000000000000001 \
  "$((base_ns + 105000000))" "$((base_ns + 160000000))"
add_trace payment-svc  "charge"               "$TRACE2" b000000000000003 b000000000000001 \
  "$((base_ns + 170000000))" "$((base_ns + 380000000))"
add_trace payment-svc  "fraud_check"          "$TRACE2" b000000000000004 b000000000000003 \
  "$((base_ns + 175000000))" "$((base_ns + 250000000))"
add_trace payment-svc  "gateway_call"         "$TRACE2" b000000000000005 b000000000000003 \
  "$((base_ns + 260000000))" "$((base_ns + 370000000))"
add_trace checkout-api "send_confirmation"    "$TRACE2" b000000000000006 b000000000000001 \
  "$((base_ns + 390000000))" "$((base_ns + 440000000))"

# ─── Trace 3: worker-billing (with error) ─────────────────────────────
TRACE3="aaaaaaaaaaaaaaaaaaaaaaaaaaaa0003"

add_trace worker-billing "process_receipt"    "$TRACE3" c000000000000001 "" \
  "$((base_ns + 200000000))" "$((base_ns + 500000000))"
add_trace worker-billing "persist_receipt"    "$TRACE3" c000000000000002 c000000000000001 \
  "$((base_ns + 210000000))" "$((base_ns + 350000000))"
add_trace mysql          "INSERT INTO receipts" "$TRACE3" c000000000000003 c000000000000002 \
  "$((base_ns + 215000000))" "$((base_ns + 340000000))"
add_trace worker-billing "notify_downstream"  "$TRACE3" c000000000000004 c000000000000001 \
  "$((base_ns + 360000000))" "$((base_ns + 490000000))" 2
add_trace kafka          "produce"            "$TRACE3" c000000000000005 c000000000000004 \
  "$((base_ns + 365000000))" "$((base_ns + 485000000))" 2

add_metric orders-api http.server.requests 42
add_metric checkout-api queue.depth 7

add_log worker-billing INFO payment_authorized
add_log frontend-web WARN checkout_latency_high

# ─── Chart metric data (distributed over last 5 minutes) ─────────────────────────
# http.server.requests — 3 services x 6 points (30s interval)
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

# http.server.duration_ms — latency trend (line chart)
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
