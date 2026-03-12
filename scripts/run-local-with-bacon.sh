#!/bin/sh
set -eu

redis_port="${LITELEMETRY_REDIS_PORT:-6379}"
postgres_port="${LITELEMETRY_POSTGRES_PORT:-5432}"

: "${REDIS_URL:=redis://127.0.0.1:${redis_port}}"
: "${DATABASE_URL:=postgres://postgres:postgres@127.0.0.1:${postgres_port}/litelemetry}"
: "${HTTP_PORT:=8080}"
: "${VIEWER_RUNTIME_POLL_MS:=1000}"
: "${RUST_LOG:=litelemetry=info}"

export REDIS_URL
export DATABASE_URL
export HTTP_PORT
export VIEWER_RUNTIME_POLL_MS
export RUST_LOG

exec cargo run --bin litelemetry "$@"
