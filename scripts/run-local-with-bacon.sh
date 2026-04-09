#!/bin/sh
set -eu

: "${STANDALONE:=true}"
: "${HTTP_PORT:=${PORT:-8080}}"
: "${VIEWER_RUNTIME_POLL_MS:=1000}"
: "${RUST_LOG:=litelemetry=info}"

case "$STANDALONE" in
  [Tt][Rr][Uu][Ee]|1) ;;
  *)
    redis_port="${LITELEMETRY_REDIS_PORT:-6379}"
    postgres_port="${LITELEMETRY_POSTGRES_PORT:-5432}"
    : "${REDIS_URL:=redis://127.0.0.1:${redis_port}}"
    : "${DATABASE_URL:=postgres://postgres:postgres@127.0.0.1:${postgres_port}/litelemetry}"
    export REDIS_URL DATABASE_URL
    ;;
esac

export STANDALONE HTTP_PORT VIEWER_RUNTIME_POLL_MS RUST_LOG
exec cargo run --bin litelemetry "$@"
