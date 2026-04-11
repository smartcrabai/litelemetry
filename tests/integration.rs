//! Integration tests -- requires Redis and PostgreSQL
//!
//! Starts real Redis / PostgreSQL instances via testcontainers and validates.
//! Requires Docker to run:
//!   cargo test --test integration -- --include-ignored
//!
//! Skipped in normal CI with `#[ignore]`,
//! run with `cargo test --test integration -- --include-ignored`.

use axum::http::{Request, StatusCode};
use bytes::Bytes;
use chrono::{Duration, Utc};
use litelemetry::domain::dashboard::DashboardDefinition;
use litelemetry::domain::telemetry::{NormalizedEntry, Signal, SignalMask};
use litelemetry::domain::viewer::ViewerDefinition;
use litelemetry::server::{build_app, build_app_with_services};
use litelemetry::storage::memory::{MemoryStreamStore, MemoryViewerStore};
use litelemetry::storage::postgres::{PostgresStore, ViewerSnapshotRow};
use litelemetry::storage::redis::RedisStore;
use litelemetry::storage::{StreamStore, ViewerStore};
use litelemetry::viewer_runtime::runtime::ViewerRuntime;
use litelemetry::viewer_runtime::state::StreamCursor;
use serde_json::json;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

// --- Helpers ----------------------------------------------------------------

async fn make_redis_store(port: u16) -> RedisStore {
    let url = format!("redis://127.0.0.1:{port}");
    RedisStore::new(&url)
        .await
        .expect("Redis connection failed")
}

async fn make_postgres_store(port: u16) -> PostgresStore {
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    PostgresStore::new(&url)
        .await
        .expect("PostgreSQL connection failed")
}

/// Helper that sets up Redis + PostgreSQL + ViewerRuntime + App in one go.
/// Holds container references so they are not dropped before the test ends.
struct ViewerTestEnv {
    app: axum::Router,
    _redis_container: testcontainers::ContainerAsync<Redis>,
    _pg_container: testcontainers::ContainerAsync<Postgres>,
}

async fn setup_viewer_app() -> ViewerTestEnv {
    let (redis_container, pg_container) =
        tokio::join!(Redis::default().start(), Postgres::default().start(),);
    let redis_container = redis_container.unwrap();
    let pg_container = pg_container.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let runtime = ViewerRuntime::build(
        ViewerStore::Postgres(pg.clone()),
        StreamStore::Redis(redis.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Redis(redis),
        Some(ViewerStore::Postgres(pg)),
        Some(runtime),
    );

    ViewerTestEnv {
        app,
        _redis_container: redis_container,
        _pg_container: pg_container,
    }
}

fn make_viewer_def(signal_mask: SignalMask, lookback_ms: i64, revision: i64) -> ViewerDefinition {
    ViewerDefinition {
        id: Uuid::new_v4(),
        slug: format!("viewer-{}", Uuid::new_v4()),
        name: "Test Viewer".to_string(),
        refresh_interval_ms: 5_000,
        lookback_ms,
        signal_mask,
        definition_json: json!({}),
        layout_json: json!({}),
        revision,
        enabled: true,
    }
}

fn make_traces_entry(age_ms: i64) -> NormalizedEntry {
    NormalizedEntry {
        signal: Signal::Traces,
        observed_at: Utc::now() - Duration::milliseconds(age_ms),
        service_name: Some("test-svc".to_string()),
        payload: Bytes::from_static(b"\x0a\x01\x02"),
    }
}

fn make_trace_payload(service_name: &str, span_name: &str) -> Bytes {
    Bytes::from(
        json!({
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {
                                    "stringValue": service_name
                                }
                            }
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {
                                "name": "integration-test"
                            },
                            "spans": [
                                {
                                    "traceId": "00000000000000000000000000000001",
                                    "spanId": "0000000000000001",
                                    "name": span_name,
                                    "kind": 1
                                }
                            ]
                        }
                    ]
                }
            ]
        })
        .to_string(),
    )
}

fn make_metric_payload(service_name: &str, metric_name: &str, metric_value: u64) -> Bytes {
    Bytes::from(
        json!({
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {
                                    "stringValue": service_name
                                }
                            }
                        ]
                    },
                    "scopeMetrics": [
                        {
                            "scope": {
                                "name": "integration-test"
                            },
                            "metrics": [
                                {
                                    "name": metric_name,
                                    "sum": {
                                        "aggregationTemporality": 2,
                                        "isMonotonic": true,
                                        "dataPoints": [
                                            {
                                                "asInt": metric_value.to_string(),
                                                "timeUnixNano": "1"
                                            }
                                        ]
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        })
        .to_string(),
    )
}

fn make_log_payload(service_name: &str, severity_text: &str, message: &str) -> Bytes {
    Bytes::from(
        json!({
            "resourceLogs": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {
                                    "stringValue": service_name
                                }
                            }
                        ]
                    },
                    "scopeLogs": [
                        {
                            "scope": {
                                "name": "integration-test"
                            },
                            "logRecords": [
                                {
                                    "severityText": severity_text,
                                    "body": {
                                        "stringValue": message
                                    }
                                }
                            ]
                        }
                    ]
                }
            ]
        })
        .to_string(),
    )
}

async fn send_json_request(
    app: &axum::Router,
    method: &str,
    uri: impl Into<String>,
    payload: serde_json::Value,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri.into())
                .header("content-type", "application/json")
                .body(axum::body::Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn send_get_request(app: &axum::Router, uri: impl Into<String>) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri.into())
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn response_json(response: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

async fn create_viewer_id(app: &axum::Router, payload: serde_json::Value) -> String {
    let response = send_json_request(app, "POST", "/api/viewers", payload).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    response_json(response).await["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn fetch_viewer_payload(app: &axum::Router, viewer_id: &str) -> serde_json::Value {
    let response = send_get_request(app, format!("/api/viewers/{viewer_id}")).await;
    assert_eq!(response.status(), StatusCode::OK);
    response_json(response).await
}

async fn assert_viewer_chart_type(app: &axum::Router, viewer_id: &str, expected_chart_type: &str) {
    let payload = fetch_viewer_payload(app, viewer_id).await;
    assert_eq!(
        payload["chart_type"], expected_chart_type,
        "chart_type should be {expected_chart_type}"
    );
}

async fn patch_viewer_chart_type_status(
    app: &axum::Router,
    viewer_id: &str,
    chart_type: &str,
) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/viewers/{viewer_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "chart_type": chart_type }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn patch_viewer_chart_type(app: &axum::Router, viewer_id: &str, chart_type: &str) {
    let status = patch_viewer_chart_type_status(app, viewer_id, chart_type).await;
    assert_eq!(status, StatusCode::OK, "PATCH should return 200 OK");
}
// --- startup resume ---------------------------------------------------------

/// Startup resume: restore state from PostgreSQL snapshot + Redis diff
///
/// Scenario:
///   1. Insert viewer definition into PG
///   2. Save snapshot (revision match) to PG
///   3. Add telemetry after snapshot to Redis
///   4. Start viewer runtime
///   5. Verify state is correctly built from "snapshot + Redis diff"
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_startup_resume_from_snapshot_and_redis_diff() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. Insert viewer definition into PG
    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def_id = def.id;
    pg.insert_viewer_definition(&def).await.unwrap();

    // 2. Add 5 entries to Redis and get cursor
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..5 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }
    // Cursor: get the 5th entry ID (last ID via XRANGE)
    let snapshot_entries = redis_write
        .read_entries_since(Signal::Traces, None, 5)
        .await
        .unwrap();
    let snapshot_cursor_id = snapshot_entries.last().unwrap().0.clone();

    // Save snapshot to PG (cursor = ID of 5th entry)
    let mut cursor = StreamCursor::default();
    cursor.set(Signal::Traces, snapshot_cursor_id.clone());
    let snapshot = ViewerSnapshotRow {
        viewer_id: def_id,
        revision: 1,
        last_cursor_json: serde_json::to_value(&cursor).unwrap(),
        status: litelemetry::domain::viewer::ViewerStatus::Ok,
        generated_at: Utc::now(),
    };
    pg.upsert_snapshot(&snapshot).await.unwrap();

    // 3. Add 3 more entries to Redis (after snapshot cursor)
    for _ in 0..3 {
        redis_write
            .append_entry(&make_traces_entry(5_000))
            .await
            .unwrap();
    }

    // 4. Start viewer runtime
    let runtime = ViewerRuntime::build(
        ViewerStore::Postgres(pg),
        StreamStore::Redis(make_redis_store(redis_port).await),
    )
    .await
    .unwrap();

    // 5. Verify state is built with "3 entries after snapshot cursor"
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1, "should have 1 viewer");
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        3,
        "only the 3 entries after snapshot cursor should be loaded (got: {})",
        state.entries.len()
    );
    assert!(
        state.last_cursor.traces.is_some(),
        "traces cursor should be updated"
    );
    assert_ne!(
        state.last_cursor.traces.as_deref(),
        Some(snapshot_cursor_id.as_str()),
        "cursor should have advanced past the snapshot cursor"
    );
}

/// Startup resume: no snapshot -> falls back to full Redis replay
///
/// Scenario:
///   1. Insert viewer definition into PG (no snapshot)
///   2. Add telemetry to Redis
///   3. Start viewer runtime
///   4. Verify state is built by replaying from Redis retained range
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_startup_resume_no_snapshot_falls_back_to_replay() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. Viewer definition only (no snapshot)
    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    pg.insert_viewer_definition(&def).await.unwrap();

    // 2. Add 4 entries to Redis
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..4 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    // 3. Start viewer runtime
    let runtime = ViewerRuntime::build(
        ViewerStore::Postgres(pg),
        StreamStore::Redis(make_redis_store(redis_port).await),
    )
    .await
    .unwrap();

    // 4. Verify state is built with 4 entries via full replay
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        4,
        "no snapshot -> all 4 entries from Redis should be replayed"
    );
}

/// Startup resume: revision mismatch -> falls back to replay
///
/// Scenario:
///   1. Insert viewer definition with revision=2 into PG
///   2. Save stale snapshot with revision=1 to PG
///   3. Start viewer runtime
///   4. Verify revision mismatch is detected and falls back to replay
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_startup_resume_revision_mismatch_falls_back_to_replay() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. Viewer definition revision=2
    let def = make_viewer_def(Signal::Traces.into(), 300_000, 2);
    let def_id = def.id;
    pg.insert_viewer_definition(&def).await.unwrap();

    // 2. Save snapshot with revision=1 (with cursor)
    let mut cursor = StreamCursor::default();
    cursor.set(Signal::Traces, "9999999999999-0".to_string()); // future ID -> results in 0 entries if used
    let snapshot = ViewerSnapshotRow {
        viewer_id: def_id,
        revision: 1, // mismatch!
        last_cursor_json: serde_json::to_value(&cursor).unwrap(),
        status: litelemetry::domain::viewer::ViewerStatus::Ok,
        generated_at: Utc::now(),
    };
    pg.upsert_snapshot(&snapshot).await.unwrap();

    // Add 3 entries to Redis
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..3 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    // 3. Start viewer runtime
    let runtime = ViewerRuntime::build(
        ViewerStore::Postgres(pg),
        StreamStore::Redis(make_redis_store(redis_port).await),
    )
    .await
    .unwrap();

    // 4. revision mismatch -> reset cursor and full replay -> 3 entries
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        3,
        "revision mismatch -> cursor reset, all 3 Redis entries should be replayed"
    );
}

// --- diff update ------------------------------------------------------------

/// Diff update: update multiple viewers in a single Redis scan
///
/// Scenario:
///   1. Register 2 viewers targeting traces
///   2. Add traces telemetry to Redis
///   3. Fire diff batch
///   4. Verify 2 viewers' state is updated with a single Redis read
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_diff_update_one_pass_fan_out_to_multiple_viewers() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. Register 2 viewers targeting traces
    let def1 = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def2 = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    pg.insert_viewer_definition(&def1).await.unwrap();
    pg.insert_viewer_definition(&def2).await.unwrap();

    // Start empty runtime
    let mut runtime = ViewerRuntime::build(
        ViewerStore::Postgres(pg),
        StreamStore::Redis(make_redis_store(redis_port).await),
    )
    .await
    .unwrap();

    // Verify both viewers initially have 0 entries
    for (_, state) in runtime.viewers() {
        assert_eq!(state.entries.len(), 0);
    }

    // 2. Add 5 entries to Redis
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..5 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    // 3. Fire diff batch
    runtime.apply_diff_batch().await.unwrap();

    // 4. Verify 5 entries are reflected in each viewer
    for (_, state) in runtime.viewers() {
        assert_eq!(
            state.entries.len(),
            5,
            "5 entries should be reflected in each viewer"
        );
    }
}

/// Snapshot upsert after diff update
///
/// Scenario:
///   1. Register viewer and fire diff update
///   2. Verify latest state is upserted into PG viewer_snapshots
///   3. Verify snapshot can be resumed after restart
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_snapshot_upsert_after_diff_update() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def_id = def.id;
    pg.insert_viewer_definition(&def).await.unwrap();

    let pg2 = make_postgres_store(pg_port).await;
    let mut runtime = ViewerRuntime::build(
        ViewerStore::Postgres(pg),
        StreamStore::Redis(make_redis_store(redis_port).await),
    )
    .await
    .unwrap();

    // Add 3 entries to Redis and run diff batch
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..3 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }
    runtime.apply_diff_batch().await.unwrap();

    // 2. Verify snapshot is upserted in PG
    let mut snapshots = pg2.load_snapshots(&[def_id]).await.unwrap();
    assert!(!snapshots.is_empty(), "snapshot should be upserted in PG");
    let snapshot = snapshots.remove(0);
    assert_eq!(snapshot.revision, 1);
    assert!(
        snapshot.last_cursor_json.get("traces").is_some(),
        "traces cursor should be saved in snapshot"
    );

    // 3. Resume from snapshot after restart (no new entries -> diff 0)
    let runtime2 = ViewerRuntime::build(
        ViewerStore::Postgres(make_postgres_store(pg_port).await),
        StreamStore::Redis(make_redis_store(redis_port).await),
    )
    .await
    .unwrap();
    let viewers2 = runtime2.viewers();
    assert_eq!(viewers2.len(), 1);
    let (_, state2) = &viewers2[0];
    // Snapshot cursor is set so Redis diff is 0 entries
    assert_eq!(
        state2.entries.len(),
        0,
        "after restart, only diff after snapshot cursor (0 entries) should be loaded"
    );
}

/// Old entries outside viewer lookback are pruned
///
/// Scenario:
///   1. Register viewer (lookback=60s)
///   2. Add old telemetry (90s ago) and new telemetry (10s ago) to Redis
///   3. Fire diff update
///   4. Verify state does not contain old entries
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_diff_update_prunes_entries_outside_lookback() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. Register viewer with lookback=60s
    let def = make_viewer_def(Signal::Traces.into(), 60_000, 1);
    pg.insert_viewer_definition(&def).await.unwrap();

    let mut runtime = ViewerRuntime::build(
        ViewerStore::Postgres(pg),
        StreamStore::Redis(make_redis_store(redis_port).await),
    )
    .await
    .unwrap();

    // 2. Add old entries (90s ago) and new entries (10s ago) to Redis
    let mut redis_write = make_redis_store(redis_port).await;
    // Old (90s ago -> exceeds lookback of 60s)
    redis_write
        .append_entry(&make_traces_entry(90_000))
        .await
        .unwrap();
    // New (10s ago -> within lookback)
    redis_write
        .append_entry(&make_traces_entry(10_000))
        .await
        .unwrap();

    // 3. Fire diff update
    runtime.apply_diff_batch().await.unwrap();

    // 4. Verify old entries are pruned and only 1 new entry remains
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        1,
        "old entries outside lookback should be pruned, leaving 1 entry (got: {})",
        state.entries.len()
    );
}

// --- OTLP/HTTP ingest endpoint -----------------------------------------------

/// Ingest via OTLP/HTTP traces endpoint
///
/// Scenario:
///   1. Start server
///   2. Send protobuf payload to POST /v1/traces
///   3. Returns 200 OK
///   4. Verify entry is added to Redis lt:stream:traces
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_traces_via_otlp_http() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(StreamStore::Redis(redis));

    // 2. POST /v1/traces
    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(axum::body::Body::from(Bytes::from_static(b"\x0a\x0b\x0c")))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    // 3. 200 OK
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "traces ingest should return 200 OK"
    );

    // 4. Verify entry is added to Redis
    let mut redis_check = make_redis_store(redis_port).await;
    let entries = redis_check
        .read_entries_since(Signal::Traces, None, 10)
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "1 entry should be added to Redis traces stream"
    );
    assert_eq!(entries[0].1.signal, Signal::Traces);
}

/// Viewer UI API: verify traces are reflected in list API after viewer is created
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_then_trace_is_reflected_in_viewer_api() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let trace_request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_trace_payload(
            "checkout-ui",
            "render-checkout",
        )))
        .unwrap();

    let trace_response = app.clone().oneshot(trace_request).await.unwrap();
    assert_eq!(trace_response.status(), StatusCode::OK);

    let create_request = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Checkout traces", "signal": "traces" }).to_string(),
        ))
        .unwrap();

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let viewer_id = create_payload["id"].as_str().unwrap().to_string();

    // Check entry_count in list API
    let list_request = Request::builder()
        .method("GET")
        .uri("/api/viewers")
        .body(axum::body::Body::empty())
        .unwrap();

    let list_response = app.clone().oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewers = payload["viewers"].as_array().unwrap();

    assert_eq!(viewers.len(), 1, "1 viewer should be created");
    assert_eq!(viewers[0]["entry_count"], 1, "1 trace should be reflected");

    // Check entries via individual viewer API
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/api/viewers/{viewer_id}"))
        .body(axum::body::Body::empty())
        .unwrap();

    let get_response = app.oneshot(get_request).await.unwrap();
    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    assert_eq!(viewer["entries"][0]["signal"], "traces");
    assert_eq!(viewer["entries"][0]["service_name"], "checkout-ui");

    let preview = viewer["entries"][0]["payload_preview"].as_str().unwrap();
    assert!(
        preview.contains("render-checkout"),
        "payload preview should contain span name: {preview}"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_then_metric_is_reflected_in_viewer_api() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let metric_request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_metric_payload(
            "orders-api",
            "http.server.requests",
            42,
        )))
        .unwrap();

    let metric_response = app.clone().oneshot(metric_request).await.unwrap();
    assert_eq!(metric_response.status(), StatusCode::OK);

    let create_request = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Orders metrics", "signal": "metrics" }).to_string(),
        ))
        .unwrap();

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let viewer_id = create_payload["id"].as_str().unwrap().to_string();

    // Check entry_count in list API
    let list_request = Request::builder()
        .method("GET")
        .uri("/api/viewers")
        .body(axum::body::Body::empty())
        .unwrap();

    let list_response = app.clone().oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewers = payload["viewers"].as_array().unwrap();

    assert_eq!(viewers.len(), 1, "1 viewer should be created");
    assert_eq!(viewers[0]["signals"][0], "metrics");
    assert_eq!(viewers[0]["entry_count"], 1, "1 metric should be reflected");

    // Check entries via individual viewer API
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/api/viewers/{viewer_id}"))
        .body(axum::body::Body::empty())
        .unwrap();

    let get_response = app.oneshot(get_request).await.unwrap();
    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    assert_eq!(viewer["entries"][0]["signal"], "metrics");
    assert_eq!(viewer["entries"][0]["service_name"], "orders-api");

    let preview = viewer["entries"][0]["payload_preview"].as_str().unwrap();
    assert!(
        preview.contains("http.server.requests"),
        "payload preview should contain metric name: {preview}"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_then_log_is_reflected_in_viewer_api() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let log_request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_log_payload(
            "worker-billing",
            "INFO",
            "payment authorized",
        )))
        .unwrap();

    let log_response = app.clone().oneshot(log_request).await.unwrap();
    assert_eq!(log_response.status(), StatusCode::OK);

    let create_request = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Billing logs", "signal": "logs" }).to_string(),
        ))
        .unwrap();

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let viewer_id = create_payload["id"].as_str().unwrap().to_string();

    // Check entry_count in list API
    let list_request = Request::builder()
        .method("GET")
        .uri("/api/viewers")
        .body(axum::body::Body::empty())
        .unwrap();

    let list_response = app.clone().oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewers = payload["viewers"].as_array().unwrap();

    assert_eq!(viewers.len(), 1, "1 viewer should be created");
    assert_eq!(viewers[0]["signals"][0], "logs");
    assert_eq!(viewers[0]["entry_count"], 1, "1 log should be reflected");

    // Check entries via individual viewer API
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/api/viewers/{viewer_id}"))
        .body(axum::body::Body::empty())
        .unwrap();

    let get_response = app.oneshot(get_request).await.unwrap();
    let get_body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    assert_eq!(viewer["entries"][0]["signal"], "logs");
    assert_eq!(viewer["entries"][0]["service_name"], "worker-billing");

    let preview = viewer["entries"][0]["payload_preview"].as_str().unwrap();
    assert!(
        preview.contains("payment authorized"),
        "payload preview should contain log message: {preview}"
    );
}

/// Ingest via OTLP/HTTP metrics endpoint
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_metrics_via_otlp_http() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(StreamStore::Redis(redis));

    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(axum::body::Body::from(Bytes::from_static(b"\x0a\x0b")))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut redis_check = make_redis_store(redis_port).await;
    let entries = redis_check
        .read_entries_since(Signal::Metrics, None, 10)
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "1 entry should be added to Redis metrics stream"
    );
}

/// Ingest via OTLP/HTTP logs endpoint
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_logs_via_otlp_http() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(StreamStore::Redis(redis));

    let request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(Bytes::from_static(b"{}")))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut redis_check = make_redis_store(redis_port).await;
    let entries = redis_check
        .read_entries_since(Signal::Logs, None, 10)
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "1 entry should be added to Redis logs stream"
    );
}

/// Returns 415 Unsupported Media Type when sending unsupported content-type
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_unsupported_content_type_returns_415() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(StreamStore::Redis(redis));

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "text/plain")
        .body(axum::body::Body::from(Bytes::from_static(b"hello")))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "text/plain should return 415"
    );
}

// --- GET /api/viewers/:id ----------------------------------------------------

/// GET /api/viewers/:id returns the correct viewer summary.
///
/// Scenario:
///   1. Ingest one trace
///   2. Create a viewer (add_viewer reads Redis history, entry_count becomes 1)
///   3. Fetch single viewer summary via GET /api/viewers/:id
///   4. Verify name / signals / entry_count are correct
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_get_viewer_by_id_returns_viewer_summary() {
    let env = setup_viewer_app().await;
    let app = env.app;

    // 1. Ingest one trace
    let trace_request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_trace_payload(
            "detail-svc",
            "render-detail",
        )))
        .unwrap();
    let trace_response = app.clone().oneshot(trace_request).await.unwrap();
    assert_eq!(trace_response.status(), StatusCode::OK);

    // 2. Create a viewer (add_viewer reads Redis history)
    let create_request = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Detail Traces Viewer", "signal": "traces" }).to_string(),
        ))
        .unwrap();

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let viewer_id = create_payload["id"].as_str().unwrap().to_string();

    // 3. Fetch viewer by id
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/api/viewers/{viewer_id}"))
        .body(axum::body::Body::empty())
        .unwrap();

    let get_response = app.oneshot(get_request).await.unwrap();
    assert_eq!(
        get_response.status(),
        StatusCode::OK,
        "GET /api/viewers/:id should return 200 OK"
    );

    let body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // 4. Verify response fields
    assert_eq!(
        payload["name"], "Detail Traces Viewer",
        "viewer name should match"
    );
    assert_eq!(payload["signals"][0], "traces", "signal should be traces");
    assert_eq!(
        payload["entry_count"], 1,
        "one trace should be reflected in entry_count"
    );
    assert_eq!(
        payload["entries"][0]["service_name"], "detail-svc",
        "service_name should match"
    );

    let preview = payload["entries"][0]["payload_preview"].as_str().unwrap();
    assert!(
        preview.contains("render-detail"),
        "payload_preview should contain span name: {preview}"
    );
}

/// GET /api/viewers/:id returns 404 for an unknown ID.
///
/// Scenario:
///   1. Start runtime with no viewers
///   2. Call GET /api/viewers/:id with a random UUID
///   3. Expect 404 Not Found
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_get_viewer_by_id_not_found_returns_404() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let unknown_id = Uuid::new_v4();
    let request = Request::builder()
        .method("GET")
        .uri(format!("/api/viewers/{unknown_id}"))
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "unknown viewer ID should return 404"
    );
}

/// GET /api/viewers/:id returns 503 when viewer runtime is not configured.
///
/// Scenario:
///   1. Start app without viewer runtime (build_app)
///   2. Call GET /api/viewers/:id
///   3. Expect 503 Service Unavailable
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_get_viewer_by_id_without_runtime_returns_503() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let app = build_app(StreamStore::Redis(make_redis_store(redis_port).await));

    let viewer_id = Uuid::new_v4();
    let request = Request::builder()
        .method("GET")
        .uri(format!("/api/viewers/{viewer_id}"))
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "should return 503 when runtime is not configured"
    );
}

/// GET /api/viewers/:id returns 400 Bad Request when given an invalid UUID.
///
/// Scenario:
///   1. Start app with runtime
///   2. Call GET /api/viewers/not-a-uuid
///   3. Expect 400 Bad Request (Axum's Path extractor fails to parse UUID)
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_get_viewer_by_id_invalid_uuid_returns_400() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let request = Request::builder()
        .method("GET")
        .uri("/api/viewers/not-a-uuid")
        .body(axum::body::Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "invalid UUID should return 400"
    );
}

// --- chart_type & PATCH ------------------------------------------------------

/// Creating viewer with chart_type is reflected in response chart_type
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_with_chart_type() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Metrics bar", "signal": "metrics", "chart_type": "stacked_bar" }),
    )
    .await;

    assert_viewer_chart_type(&app, &viewer_id, "stacked_bar").await;
}

/// Creating metrics viewer with area chart_type is reflected in response chart_type
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_with_area_chart_type() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Metrics area", "signal": "metrics", "chart_type": "area" }),
    )
    .await;

    assert_viewer_chart_type(&app, &viewer_id, "area").await;
}

/// Creating viewer with pie chart_type is reflected in response chart_type
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_with_pie_chart_type() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Metrics pie", "signal": "metrics", "chart_type": "pie" }),
    )
    .await;

    assert_viewer_chart_type(&app, &viewer_id, "pie").await;
}

/// Omitting chart_type defaults to "table"
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_default_chart_type_is_table() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id =
        create_viewer_id(&app, json!({ "name": "Default chart", "signal": "traces" })).await;

    assert_viewer_chart_type(&app, &viewer_id, "table").await;
}

/// Change chart_type via PATCH /api/viewers/:id -> reflected in subsequent GET
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_viewer_chart_type() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id =
        create_viewer_id(&app, json!({ "name": "Patch test", "signal": "metrics" })).await;

    patch_viewer_chart_type(&app, &viewer_id, "line").await;
    assert_viewer_chart_type(&app, &viewer_id, "line").await;
}

/// Change chart_type to area via PATCH /api/viewers/:id -> reflected in subsequent GET
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_viewer_chart_type_to_area() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Patch area test", "signal": "metrics" }),
    )
    .await;

    patch_viewer_chart_type(&app, &viewer_id, "area").await;
    assert_viewer_chart_type(&app, &viewer_id, "area").await;
}

/// Change chart_type to donut via PATCH /api/viewers/:id -> reflected in subsequent GET
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_viewer_chart_type_to_donut() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Patch donut test", "signal": "metrics" }),
    )
    .await;

    patch_viewer_chart_type(&app, &viewer_id, "donut").await;
    assert_viewer_chart_type(&app, &viewer_id, "donut").await;
}

/// Create with invalid chart_type -> 400 BAD REQUEST
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_invalid_chart_type_returns_400() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let response = send_json_request(
        &app,
        "POST",
        "/api/viewers",
        json!({ "name": "Bad chart", "signal": "metrics", "chart_type": "heatmap" }),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "invalid chart_type should return 400"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_logs_viewer_with_non_table_chart_type_returns_400() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let response = send_json_request(
        &app,
        "POST",
        "/api/viewers",
        json!({ "name": "Logs line", "signal": "logs", "chart_type": "line" }),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "non-metrics viewers should reject non-table chart types"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_traces_viewer_with_non_table_chart_type_returns_400() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let response = send_json_request(
        &app,
        "POST",
        "/api/viewers",
        json!({ "name": "Bad trace chart", "signal": "traces", "chart_type": "pie" }),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "non-metrics viewers should reject non-table chart types"
    );
}

/// Creating viewer with chart_type = "billboard" is reflected in GET
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_viewer_with_billboard_chart_type() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let create_request = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Metrics billboard", "signal": "metrics", "chart_type": "billboard" })
                .to_string(),
        ))
        .unwrap();

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);
    let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let viewer_id = create_payload["id"].as_str().unwrap().to_string();

    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["chart_type"], "billboard",
        "chart_type should be billboard"
    );
}

/// PATCH viewer to chart_type = "billboard" is reflected in subsequent GET
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_viewer_chart_type_to_billboard() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let create_request = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Billboard patch test", "signal": "metrics" }).to_string(),
        ))
        .unwrap();
    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);
    let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let patch_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/viewers/{viewer_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "chart_type": "billboard" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        patch_response.status(),
        StatusCode::OK,
        "PATCH to billboard should return 200 OK"
    );

    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(
        payload["chart_type"], "billboard",
        "chart_type should be updated to billboard"
    );
}

/// PATCH to nonexistent ID -> 404
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_nonexistent_viewer_returns_404() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let unknown_id = Uuid::new_v4();
    let response = send_json_request(
        &app,
        "PATCH",
        format!("/api/viewers/{unknown_id}"),
        json!({ "chart_type": "line" }),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "PATCH on unknown ID should return 404"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_logs_viewer_with_non_table_chart_type_returns_400() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(&app, json!({ "name": "Logs table", "signal": "logs" })).await;

    let status = patch_viewer_chart_type_status(&app, &viewer_id, "area").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "non-metrics viewers should reject non-table chart types"
    );
    assert_viewer_chart_type(&app, &viewer_id, "table").await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_traces_viewer_to_non_table_chart_type_returns_400() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let viewer_id =
        create_viewer_id(&app, json!({ "name": "Trace viewer", "signal": "traces" })).await;

    let status = patch_viewer_chart_type_status(&app, &viewer_id, "donut").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "non-metrics viewers should reject non-table chart types"
    );
    assert_viewer_chart_type(&app, &viewer_id, "table").await;
}

/// Entries after metrics ingest contain metric_name / metric_value
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_metric_entries_contain_metric_name_and_value() {
    let env = setup_viewer_app().await;
    let app = env.app;

    // Ingest metrics
    let metric_request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_metric_payload(
            "web-api",
            "http.request.duration",
            150,
        )))
        .unwrap();

    let metric_response = app.clone().oneshot(metric_request).await.unwrap();
    assert_eq!(metric_response.status(), StatusCode::OK);

    // Create metrics viewer
    let create_request = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Duration metrics", "signal": "metrics", "chart_type": "line" })
                .to_string(),
        ))
        .unwrap();

    let create_response = app.clone().oneshot(create_request).await.unwrap();
    assert_eq!(create_response.status(), StatusCode::CREATED);

    let create_body = axum::body::to_bytes(create_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&create_body).unwrap();
    let viewer_id = create_payload["id"].as_str().unwrap().to_string();

    // Verify entries contain metric_name / metric_value via GET /api/viewers/:id
    let get_request = Request::builder()
        .method("GET")
        .uri(format!("/api/viewers/{viewer_id}"))
        .body(axum::body::Body::empty())
        .unwrap();

    let get_response = app.oneshot(get_request).await.unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload["entry_count"], 1);
    let entry = &payload["entries"][0];
    assert_eq!(
        entry["metric_name"], "http.request.duration",
        "metric_name should be extracted"
    );
    assert_eq!(
        entry["metric_value"], 150.0,
        "metric_value should be extracted as f64"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_delete_viewer() {
    let env = setup_viewer_app().await;
    let app = env.app;

    // Given: a viewer exists
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "To Delete", "signal": "traces" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // When: DELETE /api/viewers/{id}
    let del_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Then: 204 No Content
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);

    // And: subsequent GET returns 404
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_delete_nonexistent_viewer_returns_404() {
    let env = setup_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/viewers/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// --- Memory store (no Docker required) -------------------------------------

struct MemoryViewerTestEnv {
    app: axum::Router,
}

async fn setup_memory_viewer_app() -> MemoryViewerTestEnv {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Memory(stream_store.clone()),
        Some(ViewerStore::Memory(viewer_store.clone())),
        Some(runtime),
    );

    MemoryViewerTestEnv { app }
}

// --- startup resume (memory) -------------------------------------------------

#[tokio::test]
async fn test_startup_resume_from_snapshot_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def_id = def.id;
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let mut stream_write = stream_store.clone();
    for _ in 0..5 {
        stream_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }
    let snapshot_entries = stream_write
        .read_entries_since(Signal::Traces, None, 5)
        .await
        .unwrap();
    let cursor_id = snapshot_entries.last().unwrap().0.clone();

    let mut cursor = StreamCursor::default();
    cursor.set(Signal::Traces, cursor_id.clone());
    let snapshot = ViewerSnapshotRow {
        viewer_id: def_id,
        revision: 1,
        last_cursor_json: serde_json::to_value(&cursor).unwrap(),
        status: litelemetry::domain::viewer::ViewerStatus::Ok,
        generated_at: Utc::now(),
    };
    viewer_store.upsert_snapshot(&snapshot).await.unwrap();

    for _ in 0..3 {
        stream_write
            .append_entry(&make_traces_entry(5_000))
            .await
            .unwrap();
    }

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store),
        StreamStore::Memory(stream_store),
    )
    .await
    .unwrap();

    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        3,
        "only the 3 entries after snapshot cursor should be loaded"
    );
    assert!(state.last_cursor.traces.is_some());
    assert_ne!(
        state.last_cursor.traces.as_deref(),
        Some(cursor_id.as_str()),
        "cursor should have advanced past the snapshot cursor"
    );
}

#[tokio::test]
async fn test_startup_resume_no_snapshot_falls_back_to_replay_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let mut stream_write = stream_store.clone();
    for _ in 0..4 {
        stream_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store),
        StreamStore::Memory(stream_store),
    )
    .await
    .unwrap();

    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        4,
        "no snapshot -> all 4 memory entries should be replayed"
    );
}

#[tokio::test]
async fn test_startup_resume_revision_mismatch_falls_back_to_replay_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 300_000, 2);
    let def_id = def.id;
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    // Old snapshot with revision=1 (future cursor -> results in 0 entries)
    let mut cursor = StreamCursor::default();
    cursor.set(Signal::Traces, "9999999999999-0".to_string());
    let snapshot = ViewerSnapshotRow {
        viewer_id: def_id,
        revision: 1,
        last_cursor_json: serde_json::to_value(&cursor).unwrap(),
        status: litelemetry::domain::viewer::ViewerStatus::Ok,
        generated_at: Utc::now(),
    };
    viewer_store.upsert_snapshot(&snapshot).await.unwrap();

    let mut stream_write = stream_store.clone();
    for _ in 0..3 {
        stream_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store),
        StreamStore::Memory(stream_store),
    )
    .await
    .unwrap();

    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        3,
        "revision mismatch -> cursor reset, all 3 entries should be replayed"
    );
}

// --- diff update (memory) -----------------------------------------------------

#[tokio::test]
async fn test_diff_update_one_pass_fan_out_to_multiple_viewers_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def1 = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def2 = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    viewer_store.insert_viewer_definition(&def1).await.unwrap();
    viewer_store.insert_viewer_definition(&def2).await.unwrap();

    let mut runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();

    for (_, state) in runtime.viewers() {
        assert_eq!(state.entries.len(), 0);
    }

    let mut stream_write = stream_store.clone();
    for _ in 0..5 {
        stream_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    runtime.apply_diff_batch().await.unwrap();

    for (_, state) in runtime.viewers() {
        assert_eq!(
            state.entries.len(),
            5,
            "5 entries should be reflected in each viewer"
        );
    }
}

#[tokio::test]
async fn test_snapshot_upsert_after_diff_update_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def_id = def.id;
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let mut runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();

    let mut stream_write = stream_store.clone();
    for _ in 0..3 {
        stream_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }
    runtime.apply_diff_batch().await.unwrap();

    // Verify snapshot is upserted in memory viewer_store
    let snapshots = viewer_store.load_snapshots(&[def_id]).await.unwrap();
    assert!(!snapshots.is_empty(), "snapshot should be saved");
    let snapshot = &snapshots[0];
    assert_eq!(snapshot.revision, 1);
    assert!(
        snapshot.last_cursor_json.get("traces").is_some(),
        "traces cursor should be saved in snapshot"
    );

    // Resume from snapshot after restart (no new entries -> diff 0)
    let runtime2 = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store),
        StreamStore::Memory(stream_store),
    )
    .await
    .unwrap();
    let (_, state2) = &runtime2.viewers()[0];
    assert_eq!(
        state2.entries.len(),
        0,
        "diff after snapshot cursor (0 entries)"
    );
}

#[tokio::test]
async fn test_diff_update_prunes_entries_outside_lookback_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 60_000, 1);
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let mut runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();

    let mut stream_write = stream_store.clone();
    stream_write
        .append_entry(&make_traces_entry(90_000))
        .await
        .unwrap(); // old
    stream_write
        .append_entry(&make_traces_entry(10_000))
        .await
        .unwrap(); // new

    runtime.apply_diff_batch().await.unwrap();

    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        1,
        "old entries outside lookback should be pruned, leaving 1 entry"
    );
}

// --- OTLP ingest (memory) ----------------------------------------------------

#[tokio::test]
async fn test_ingest_traces_via_otlp_http_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let app = build_app(StreamStore::Memory(stream_store.clone()));

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/x-protobuf")
        .body(axum::body::Body::from(Bytes::from_static(b"\x0a\x0b\x0c")))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let entries = stream_store
        .clone()
        .read_entries_since(Signal::Traces, None, 10)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].1.signal, Signal::Traces);
}

#[tokio::test]
async fn test_ingest_metrics_via_otlp_http_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let app = build_app(StreamStore::Memory(stream_store.clone()));

    let request = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/x-protobuf")
        .body(axum::body::Body::from(Bytes::from_static(b"\x0a\x0b")))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let entries = stream_store
        .clone()
        .read_entries_since(Signal::Metrics, None, 10)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn test_ingest_logs_via_otlp_http_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let app = build_app(StreamStore::Memory(stream_store.clone()));

    let request = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(Bytes::from_static(b"{}")))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let entries = stream_store
        .clone()
        .read_entries_since(Signal::Logs, None, 10)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn test_ingest_unsupported_content_type_returns_415_memory() {
    let app = build_app(StreamStore::Memory(MemoryStreamStore::new(100_000)));

    let request = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "text/plain")
        .body(axum::body::Body::from(Bytes::from_static(b"hello")))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

// --- viewer API (memory) -----------------------------------------------------

#[tokio::test]
async fn test_create_viewer_then_trace_is_reflected_in_viewer_api_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let trace_req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_trace_payload(
            "checkout-ui",
            "render-checkout",
        )))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(trace_req).await.unwrap().status(),
        StatusCode::OK
    );

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Checkout traces", "signal": "traces" }).to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let list_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri("/api/viewers")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    assert_eq!(payload["viewers"].as_array().unwrap().len(), 1);
    assert_eq!(payload["viewers"][0]["entry_count"], 1);

    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(viewer["entries"][0]["signal"], "traces");
    assert_eq!(viewer["entries"][0]["service_name"], "checkout-ui");
    let preview = viewer["entries"][0]["payload_preview"].as_str().unwrap();
    assert!(preview.contains("render-checkout"));
}

#[tokio::test]
async fn test_create_viewer_then_metric_is_reflected_in_viewer_api_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let metric_req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_metric_payload(
            "orders-api",
            "http.server.requests",
            42,
        )))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(metric_req).await.unwrap().status(),
        StatusCode::OK
    );

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Orders metrics", "signal": "metrics" }).to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(viewer["entry_count"], 1);
    assert_eq!(viewer["entries"][0]["signal"], "metrics");
    assert_eq!(viewer["entries"][0]["service_name"], "orders-api");
}

#[tokio::test]
async fn test_create_viewer_then_log_is_reflected_in_viewer_api_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let log_req = Request::builder()
        .method("POST")
        .uri("/v1/logs")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_log_payload(
            "worker-billing",
            "INFO",
            "payment authorized",
        )))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(log_req).await.unwrap().status(),
        StatusCode::OK
    );

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Billing logs", "signal": "logs" }).to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(viewer["entries"][0]["signal"], "logs");
    assert_eq!(viewer["entries"][0]["service_name"], "worker-billing");
    let preview = viewer["entries"][0]["payload_preview"].as_str().unwrap();
    assert!(preview.contains("payment authorized"));
}

#[tokio::test]
async fn test_get_viewer_by_id_returns_viewer_summary_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let trace_req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_trace_payload(
            "detail-svc",
            "render-detail",
        )))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(trace_req).await.unwrap().status(),
        StatusCode::OK
    );

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Detail Traces Viewer", "signal": "traces" }).to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["name"], "Detail Traces Viewer");
    assert_eq!(payload["signals"][0], "traces");
    assert_eq!(payload["entry_count"], 1);
    assert_eq!(payload["entries"][0]["service_name"], "detail-svc");
}

#[tokio::test]
async fn test_get_viewer_by_id_not_found_returns_404_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_viewer_by_id_without_runtime_returns_503_memory() {
    let app = build_app(StreamStore::Memory(MemoryStreamStore::new(100_000)));

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_get_viewer_by_id_invalid_uuid_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/viewers/not-a-uuid")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_viewer_by_id_defaults_missing_chart_type_to_table_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let viewer_id = def.id;
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Memory(stream_store),
        Some(ViewerStore::Memory(viewer_store)),
        Some(runtime),
    );

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["chart_type"], "table");
}

// --- chart_type & PATCH (memory) -------------------------------------------

#[tokio::test]
async fn test_create_viewer_with_chart_type_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Metrics bar", "signal": "metrics", "chart_type": "stacked_bar" }),
    )
    .await;

    assert_viewer_chart_type(&app, &viewer_id, "stacked_bar").await;
}

#[tokio::test]
async fn test_create_viewer_with_area_chart_type_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Metrics area", "signal": "metrics", "chart_type": "area" }),
    )
    .await;

    assert_viewer_chart_type(&app, &viewer_id, "area").await;
}

#[tokio::test]
async fn test_create_viewer_with_pie_chart_type_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Metrics pie", "signal": "metrics", "chart_type": "pie" }),
    )
    .await;
    assert_viewer_chart_type(&app, &viewer_id, "pie").await;
}

#[tokio::test]
async fn test_create_viewer_default_chart_type_is_table_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id =
        create_viewer_id(&app, json!({ "name": "Default chart", "signal": "traces" })).await;

    assert_viewer_chart_type(&app, &viewer_id, "table").await;
}

#[tokio::test]
async fn test_patch_viewer_chart_type_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id =
        create_viewer_id(&app, json!({ "name": "Patch test", "signal": "metrics" })).await;

    patch_viewer_chart_type(&app, &viewer_id, "line").await;
    assert_viewer_chart_type(&app, &viewer_id, "line").await;
}

#[tokio::test]
async fn test_patch_viewer_chart_type_to_area_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Patch area test", "signal": "metrics" }),
    )
    .await;

    patch_viewer_chart_type(&app, &viewer_id, "area").await;
    assert_viewer_chart_type(&app, &viewer_id, "area").await;
}

#[tokio::test]
async fn test_patch_viewer_chart_type_to_donut_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(
        &app,
        json!({ "name": "Patch donut test", "signal": "metrics" }),
    )
    .await;

    patch_viewer_chart_type(&app, &viewer_id, "donut").await;
    assert_viewer_chart_type(&app, &viewer_id, "donut").await;
}

#[tokio::test]
async fn test_create_viewer_invalid_chart_type_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = send_json_request(
        &env.app,
        "POST",
        "/api/viewers",
        json!({ "name": "Bad chart", "signal": "metrics", "chart_type": "heatmap" }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_create_logs_viewer_with_non_table_chart_type_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = send_json_request(
        &env.app,
        "POST",
        "/api/viewers",
        json!({ "name": "Logs line", "signal": "logs", "chart_type": "line" }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_create_traces_viewer_with_non_table_chart_type_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = send_json_request(
        &env.app,
        "POST",
        "/api/viewers",
        json!({ "name": "Bad trace chart", "signal": "traces", "chart_type": "pie" }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_create_viewer_with_billboard_chart_type_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: a create request with chart_type = "billboard"
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Metrics billboard", "signal": "metrics", "chart_type": "billboard" })
                .to_string(),
        ))
        .unwrap();

    // When: the request is sent
    let create_resp = app.clone().oneshot(create_req).await.unwrap();

    // Then: 201 Created
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let create_payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewer_id = create_payload["id"].as_str().unwrap().to_string();

    // And: GET returns chart_type = "billboard"
    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(
        payload["chart_type"], "billboard",
        "chart_type should be billboard"
    );
}

#[tokio::test]
async fn test_patch_viewer_chart_type_to_billboard_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: a metrics viewer with default table chart type
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Billboard patch test", "signal": "metrics" }).to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // When: PATCH changes chart_type to "billboard"
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/viewers/{viewer_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "chart_type": "billboard" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    // Then: subsequent GET returns chart_type = "billboard"
    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(
        payload["chart_type"], "billboard",
        "chart_type should be updated to billboard"
    );
}

#[tokio::test]
async fn test_patch_nonexistent_viewer_returns_404_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = send_json_request(
        &env.app,
        "PATCH",
        format!("/api/viewers/{}", Uuid::new_v4()),
        json!({ "chart_type": "line" }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_patch_logs_viewer_with_non_table_chart_type_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id = create_viewer_id(&app, json!({ "name": "Logs table", "signal": "logs" })).await;

    let status = patch_viewer_chart_type_status(&app, &viewer_id, "area").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_viewer_chart_type(&app, &viewer_id, "table").await;
}

#[tokio::test]
async fn test_patch_traces_viewer_to_non_table_chart_type_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let viewer_id =
        create_viewer_id(&app, json!({ "name": "Trace viewer", "signal": "traces" })).await;

    let status = patch_viewer_chart_type_status(&app, &viewer_id, "donut").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_viewer_chart_type(&app, &viewer_id, "table").await;
}

#[tokio::test]
async fn test_metric_entries_contain_metric_name_and_value_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let metric_req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_metric_payload(
            "web-api",
            "http.request.duration",
            150,
        )))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(metric_req).await.unwrap().status(),
        StatusCode::OK
    );

    let create_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Duration metrics", "signal": "metrics", "chart_type": "line" })
                .to_string(),
        ))
        .unwrap();
    let create_resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(payload["entry_count"], 1);
    assert_eq!(
        payload["entries"][0]["metric_name"],
        "http.request.duration"
    );
    assert_eq!(payload["entries"][0]["metric_value"], 150.0);
}

// --- delete viewer (memory) -----------------------------------------------

#[tokio::test]
async fn test_delete_viewer_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: a viewer exists
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "To Delete", "signal": "traces" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // When: DELETE /api/viewers/{id}
    let del_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Then: 204 No Content
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);

    // And: subsequent GET returns 404
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_nonexistent_viewer_returns_404_memory() {
    let env = setup_memory_viewer_app().await;
    // When: DELETE for a UUID that does not exist
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/viewers/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Then: 404 Not Found
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ===========================================================================
// --- Dashboard CRUD tests (Memory -- no Docker required) ------------------------
// ===========================================================================

#[tokio::test]
async fn test_create_dashboard_and_list_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Create 1 viewer
    let create_viewer_req = Request::builder()
        .method("POST")
        .uri("/api/viewers")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "Test Viewer", "signal": "traces" }).to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(create_viewer_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Create dashboard
    let create_req = Request::builder()
        .method("POST")
        .uri("/api/dashboards")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(
            json!({ "name": "My Dashboard", "viewer_ids": [viewer_id], "columns": 2 }).to_string(),
        ))
        .unwrap();
    let resp = app.clone().oneshot(create_req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Check dashboard list
    let list_resp = app
        .oneshot(
            Request::builder()
                .uri("/api/dashboards")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let dashboards = payload["dashboards"].as_array().unwrap();
    assert!(dashboards.iter().any(|d| d["id"] == dash_id));
}

#[tokio::test]
async fn test_get_dashboard_defaults_missing_columns_to_two_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let dashboard = DashboardDefinition {
        id: Uuid::new_v4(),
        slug: "legacy-dashboard".to_string(),
        name: "Legacy Dashboard".to_string(),
        layout_json: json!({ "panels": [] }),
        revision: 1,
        enabled: true,
    };
    let dashboard_id = dashboard.id;
    viewer_store.insert_dashboard(&dashboard).await.unwrap();

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Memory(stream_store),
        Some(ViewerStore::Memory(viewer_store)),
        Some(runtime),
    );

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/api/dashboards/{dashboard_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["columns"], 2);
}

#[tokio::test]
async fn test_create_dashboard_invalid_columns_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    for invalid_columns in [0, 5] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/dashboards")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": "Invalid Columns", "columns": invalid_columns })
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "columns={invalid_columns} should be rejected"
        );
    }
}

#[tokio::test]
async fn test_create_dashboard_with_viewers_then_get_detail_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Ingest trace data
    let trace_req = Request::builder()
        .method("POST")
        .uri("/v1/traces")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_trace_payload("svc-a", "op-a")))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(trace_req).await.unwrap().status(),
        StatusCode::OK
    );

    // Create viewer
    let create_viewer_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Traces", "signal": "traces" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_viewer_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_viewer_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Create dashboard
    let create_dash_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Trace Dashboard", "viewer_ids": [viewer_id] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_dash_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_dash_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Get dashboard detail
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["name"], "Trace Dashboard");
    let panels = payload["panels"].as_array().unwrap();
    assert_eq!(panels.len(), 1);
    assert_eq!(panels[0]["viewer_id"], viewer_id);
    assert!(panels[0]["viewer"].is_object());
    assert_eq!(panels[0]["viewer"]["name"], "Traces");
}

#[tokio::test]
async fn test_get_dashboard_not_found_returns_404_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_dashboard_invalid_uuid_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .uri("/api/dashboards/not-a-uuid")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_get_dashboard_with_lookback_ms_filters_entries_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let mut stream_write = stream_store.clone();
    stream_write
        .append_entry(&make_traces_entry(120_000))
        .await
        .unwrap();
    stream_write
        .append_entry(&make_traces_entry(90_000))
        .await
        .unwrap();
    stream_write
        .append_entry(&make_traces_entry(30_000))
        .await
        .unwrap();
    stream_write
        .append_entry(&make_traces_entry(10_000))
        .await
        .unwrap();

    let dashboard = DashboardDefinition {
        id: Uuid::new_v4(),
        slug: "test-lookback".to_string(),
        name: "Test Lookback".to_string(),
        layout_json: json!({ "panels": [{ "viewer_id": def.id, "position": 0 }] }),
        revision: 1,
        enabled: true,
    };
    viewer_store.insert_dashboard(&dashboard).await.unwrap();
    let dashboard_id = dashboard.id;

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Memory(stream_store),
        Some(ViewerStore::Memory(viewer_store)),
        Some(runtime),
    );

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dashboard_id}?lookback_ms=60000"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let panels = payload["panels"].as_array().unwrap();
    assert_eq!(panels.len(), 1);
    assert_eq!(
        panels[0]["viewer"]["entry_count"], 2,
        "only entries within 60s lookback should be included (30s and 10s old)"
    );
}

#[tokio::test]
async fn test_get_dashboard_with_invalid_lookback_ms_returns_400_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let dashboard = DashboardDefinition {
        id: Uuid::new_v4(),
        slug: "test-invalid".to_string(),
        name: "Test Invalid".to_string(),
        layout_json: json!({ "panels": [] }),
        revision: 1,
        enabled: true,
    };
    viewer_store.insert_dashboard(&dashboard).await.unwrap();
    let dashboard_id = dashboard.id;

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Memory(stream_store),
        Some(ViewerStore::Memory(viewer_store)),
        Some(runtime),
    );

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dashboard_id}?lookback_ms=0"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "lookback_ms=0 should be rejected"
    );

    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dashboard_id}?lookback_ms=-5000"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "negative lookback_ms should be rejected"
    );
}

#[tokio::test]
async fn test_get_dashboard_rejects_lookback_ms_above_dashboard_limit_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 60_000, 1);
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let dashboard = DashboardDefinition {
        id: Uuid::new_v4(),
        slug: "test-too-large".to_string(),
        name: "Test Too Large".to_string(),
        layout_json: json!({ "panels": [{ "viewer_id": def.id, "position": 0 }] }),
        revision: 1,
        enabled: true,
    };
    viewer_store.insert_dashboard(&dashboard).await.unwrap();
    let dashboard_id = dashboard.id;

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Memory(stream_store),
        Some(ViewerStore::Memory(viewer_store)),
        Some(runtime),
    );

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dashboard_id}?lookback_ms=120000"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "lookback_ms above the dashboard limit should be rejected"
    );
}

#[tokio::test]
async fn test_get_dashboard_without_lookback_ms_uses_viewer_default_memory() {
    let stream_store = MemoryStreamStore::new(100_000);
    let viewer_store = MemoryViewerStore::new();

    let def = make_viewer_def(Signal::Traces.into(), 60000, 1);
    viewer_store.insert_viewer_definition(&def).await.unwrap();

    let mut stream_write = stream_store.clone();
    stream_write
        .append_entry(&make_traces_entry(120_000))
        .await
        .unwrap();
    stream_write
        .append_entry(&make_traces_entry(90_000))
        .await
        .unwrap();
    stream_write
        .append_entry(&make_traces_entry(30_000))
        .await
        .unwrap();
    stream_write
        .append_entry(&make_traces_entry(10_000))
        .await
        .unwrap();

    let dashboard = DashboardDefinition {
        id: Uuid::new_v4(),
        slug: "test-default".to_string(),
        name: "Test Default".to_string(),
        layout_json: json!({ "panels": [{ "viewer_id": def.id, "position": 0 }] }),
        revision: 1,
        enabled: true,
    };
    viewer_store.insert_dashboard(&dashboard).await.unwrap();
    let dashboard_id = dashboard.id;

    let runtime = ViewerRuntime::build(
        ViewerStore::Memory(viewer_store.clone()),
        StreamStore::Memory(stream_store.clone()),
    )
    .await
    .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(
        StreamStore::Memory(stream_store),
        Some(ViewerStore::Memory(viewer_store)),
        Some(runtime),
    );

    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dashboard_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let panels = payload["panels"].as_array().unwrap();
    assert_eq!(
        panels[0]["viewer"]["entry_count"], 2,
        "viewer with 60s lookback should only include entries within 60s"
    );
    assert_eq!(
        panels[0]["viewer"]["lookback_ms"], 60000,
        "response should include the viewer's lookback_ms"
    );
    assert_eq!(
        payload["max_lookback_ms"], 60000,
        "response should expose the dashboard-wide lookback ceiling"
    );
}

#[tokio::test]
async fn test_patch_dashboard_name_and_columns_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Create
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Original Name", "columns": 2 }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // PATCH
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/dashboards/{dash_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Updated Name", "columns": 3 }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    // Verify via GET
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["name"], "Updated Name");
    assert_eq!(payload["columns"], 3);
}

#[tokio::test]
async fn test_patch_dashboard_invalid_columns_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Original Name", "columns": 2 }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    for invalid_columns in [0, 5] {
        let patch_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/api/dashboards/{dash_id}"))
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "columns": invalid_columns }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            patch_resp.status(),
            StatusCode::BAD_REQUEST,
            "columns={invalid_columns} should be rejected"
        );
    }
}

#[tokio::test]
async fn test_patch_dashboard_viewer_ids_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // viewer 1
    let resp1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "V1", "signal": "traces" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body1 = axum::body::to_bytes(resp1.into_body(), usize::MAX)
        .await
        .unwrap();
    let id1 = serde_json::from_slice::<serde_json::Value>(&body1).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // viewer 2
    let resp2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "V2", "signal": "logs" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body2 = axum::body::to_bytes(resp2.into_body(), usize::MAX)
        .await
        .unwrap();
    let id2 = serde_json::from_slice::<serde_json::Value>(&body2).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Create dashboard (id1 only)
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "D", "viewer_ids": [id1] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Change to id2 via PATCH
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/dashboards/{dash_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "viewer_ids": [id2] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    // Verify via GET
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewer_ids: Vec<&str> = payload["panels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["viewer_id"].as_str().unwrap())
        .collect();
    assert!(viewer_ids.contains(&id2.as_str()));
    assert!(!viewer_ids.contains(&id1.as_str()));
}

#[tokio::test]
async fn test_patch_nonexistent_dashboard_returns_404_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/dashboards/{}", Uuid::new_v4()))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(json!({ "name": "X" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_dashboard_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Create
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "To Delete" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // DELETE
    let del_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);

    // GET returns 404
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_nonexistent_dashboard_returns_404_memory() {
    let env = setup_memory_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/dashboards/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_dashboard_skips_missing_viewers_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Create dashboard with nonexistent viewer_id
    let nonexistent_viewer_id = Uuid::new_v4().to_string();
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Sparse Dashboard", "viewer_ids": [nonexistent_viewer_id] })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // GET detail -- panels are empty (nonexistent viewer_id is skipped)
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Nonexistent viewer_id is skipped so panels are empty
    let panels = payload["panels"].as_array().unwrap();
    assert!(panels.is_empty());
}

#[tokio::test]
async fn test_create_dashboard_without_runtime_returns_503_memory() {
    // ingest-only mode (no viewer_store)
    let app = build_app(StreamStore::Memory(MemoryStreamStore::new(100_000)));

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(json!({ "name": "D" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// --- Dashboard Viewer order tests (memory -- no Docker required) ------------

/// Given: a dashboard with 3 Viewers created in order [v1, v2, v3]
/// When: fetching the dashboard via GET /api/dashboards/{id}
/// Then: panels have positions 0, 1, 2 respectively
#[tokio::test]
async fn test_create_dashboard_panels_have_positions_in_order_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Create 3 Viewers
    let mut viewer_ids = Vec::new();
    for name in ["V1", "V2", "V3"] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": name, "signal": "traces" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        viewer_ids.push(id);
    }

    // Create dashboard in order [v1, v2, v3]
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Order Test", "viewer_ids": viewer_ids }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Verify positions via GET
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let panels = payload["panels"].as_array().unwrap();
    assert_eq!(panels.len(), 3);

    // Sort by position and verify order
    let mut sorted_panels = panels.clone();
    sorted_panels.sort_by_key(|p| p["position"].as_u64().unwrap());
    for (i, panel) in sorted_panels.iter().enumerate() {
        assert_eq!(
            panel["position"].as_u64().unwrap(),
            i as u64,
            "panel at sorted index {i} should have position {i}"
        );
        assert_eq!(
            panel["viewer_id"].as_str().unwrap(),
            viewer_ids[i],
            "panel at position {i} should have viewer_ids[{i}]"
        );
    }
}

/// Given: a dashboard created with [v1, v2]
/// When: changing viewer_ids to [v2, v1] (reversed) via PATCH
/// Then: GET shows v2.position == 0, v1.position == 1
#[tokio::test]
async fn test_patch_dashboard_reorder_viewers_updates_positions_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Create 2 Viewers
    let mut viewer_ids = Vec::new();
    for name in ["Alpha", "Beta"] {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": name, "signal": "traces" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        viewer_ids.push(id);
    }
    let (id_alpha, id_beta) = (viewer_ids[0].clone(), viewer_ids[1].clone());

    // Create dashboard in order [alpha, beta]
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Reorder Test", "viewer_ids": [&id_alpha, &id_beta] })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Change to reversed order [beta, alpha] via PATCH
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/dashboards/{dash_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "viewer_ids": [&id_beta, &id_alpha] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    // Verify positions are updated via GET
    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let panels = payload["panels"].as_array().unwrap();
    assert_eq!(panels.len(), 2);

    // Sort by position and verify beta comes first, alpha second
    let mut sorted = panels.clone();
    sorted.sort_by_key(|p| p["position"].as_u64().unwrap());
    assert_eq!(
        sorted[0]["viewer_id"].as_str().unwrap(),
        id_beta,
        "after reorder, beta (position 0) should come first"
    );
    assert_eq!(
        sorted[1]["viewer_id"].as_str().unwrap(),
        id_alpha,
        "after reorder, alpha (position 1) should come second"
    );
}

// ===========================================================================
// --- Dashboard CRUD tests (Docker -- requires Docker) ----------------------
// ===========================================================================

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_dashboard_and_list() {
    let env = setup_viewer_app().await;
    let app = env.app;

    // Create viewer
    let create_viewer_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Docker Viewer", "signal": "traces" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_viewer_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_viewer_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Create dashboard
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Docker Dashboard", "viewer_ids": [viewer_id], "columns": 2 })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Check list
    let list_resp = app
        .oneshot(
            Request::builder()
                .uri("/api/dashboards")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(list_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let dashboards = payload["dashboards"].as_array().unwrap();
    assert!(dashboards.iter().any(|d| d["id"] == dash_id));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_get_dashboard_not_found_returns_404() {
    let env = setup_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_get_dashboard_invalid_uuid_returns_400() {
    let env = setup_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .uri("/api/dashboards/not-a-uuid")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_dashboard_name_and_columns() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Original", "columns": 2 }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/dashboards/{dash_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Patched", "columns": 4 }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["name"], "Patched");
    assert_eq!(payload["columns"], 4);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_patch_nonexistent_dashboard_returns_404() {
    let env = setup_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/dashboards/{}", Uuid::new_v4()))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(json!({ "name": "X" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_delete_dashboard() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "To Delete" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let del_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del_resp.status(), StatusCode::NO_CONTENT);

    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_delete_nonexistent_dashboard_returns_404() {
    let env = setup_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/dashboards/{}", Uuid::new_v4()))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_dashboard_skips_missing_viewers() {
    let env = setup_viewer_app().await;
    let app = env.app;

    let nonexistent_viewer_id = Uuid::new_v4().to_string();
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Sparse", "viewer_ids": [nonexistent_viewer_id] }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let dash_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/dashboards/{dash_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(payload["panels"].as_array().unwrap().is_empty());
}

// ===========================================================================
// --- Viewer query filter tests (Memory -- no Docker required) --------------
// ===========================================================================

/// Create viewer with query -> only matching entries appear in detail
#[tokio::test]
async fn test_create_viewer_with_query_filters_matching_entries_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: two traces ingested -- one from "checkout-ui", one from "orders-api"
    for (service, span) in [
        ("checkout-ui", "render-checkout"),
        ("orders-api", "process-order"),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    // When: create viewer with query="checkout-ui"
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Checkout only", "signal": "traces", "query": "checkout-ui" })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Then: viewer detail shows only the checkout-ui entry
    let get_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    assert_eq!(
        viewer["entry_count"], 1,
        "only matching entry should be counted"
    );
    assert_eq!(viewer["entries"][0]["service_name"], "checkout-ui");
    assert_eq!(
        viewer["query"], "checkout-ui",
        "viewer summary should expose the current query"
    );
}

/// Create viewer with `filters` (AND mode) — only entries matching all filters appear
#[tokio::test]
async fn test_create_viewer_with_filters_and_mode_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: traces from three services
    for (service, span) in [
        ("checkout-ui", "render"),
        ("checkout-ui-extra", "render-extra"),
        ("orders-api", "process"),
    ] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    // When: create viewer with AND filters: service_name eq "checkout-ui" AND payload contains "render"
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({
                        "name": "Checkout exact",
                        "signal": "traces",
                        "filters": [
                            { "field": "service_name", "op": "eq", "value": "checkout-ui" },
                            { "field": "payload", "op": "contains", "value": "render" }
                        ],
                        "filter_mode": "and"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    // Then: only "checkout-ui" with "render" span matches (not "checkout-ui-extra")
    assert_eq!(
        viewer["entry_count"], 1,
        "AND mode: only exact match should pass"
    );
    assert_eq!(viewer["entries"][0]["service_name"], "checkout-ui");
    assert_eq!(viewer["filter_mode"], "and");
    assert_eq!(viewer["filters"][0]["field"], "service_name");
    assert_eq!(viewer["filters"][0]["op"], "eq");
}

/// Create viewer with `filters` (OR mode) — entries matching any filter appear
#[tokio::test]
async fn test_create_viewer_with_filters_or_mode_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: traces from two services
    for (service, span) in [("frontend", "render"), ("backend", "process")] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    // When: create viewer with OR filters: service_name eq "frontend" OR service_name eq "backend"
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({
                        "name": "Frontend or Backend",
                        "signal": "traces",
                        "filters": [
                            { "field": "service_name", "op": "eq", "value": "frontend" },
                            { "field": "service_name", "op": "eq", "value": "backend" }
                        ],
                        "filter_mode": "or"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    // Then: both entries match (OR logic)
    assert_eq!(
        viewer["entry_count"], 2,
        "OR mode: both matching entries should appear"
    );
    assert_eq!(viewer["filter_mode"], "or");
}

/// Preview viewer with filters — filters apply to preview results
#[tokio::test]
async fn test_preview_viewer_with_filters_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: traces from two services
    for (service, span) in [("alpha-svc", "span-a"), ("beta-svc", "span-b")] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    // When: preview with filter service_name eq "alpha-svc"
    let preview_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers/preview")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({
                        "signal": "traces",
                        "filters": [{ "field": "service_name", "op": "eq", "value": "alpha-svc" }],
                        "filter_mode": "and"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preview_resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(preview_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let preview: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Then: only alpha-svc entry matches
    assert_eq!(
        preview["entry_count"], 1,
        "preview should only return matching entry"
    );
    assert_eq!(preview["entries"][0]["service_name"], "alpha-svc");
    assert_eq!(preview["filters"][0]["field"], "service_name");
}

/// Patch viewer from query-only to filters
#[tokio::test]
async fn test_patch_viewer_query_to_filters_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: traces from two services
    for (service, span) in [("svc-a", "op-a"), ("svc-b", "op-b")] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    // Create viewer with query
    let create_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({"name": "Query only viewer", "signal": "traces", "query": "svc-a"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Patch to use filters instead
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/viewers/{viewer_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({
                        "filters": [{ "field": "service_name", "op": "eq", "value": "svc-b" }],
                        "filter_mode": "and"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    let get_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    // Then: svc-b filter matches only svc-b entry
    assert_eq!(
        viewer["entry_count"], 1,
        "after patch to filters, only svc-b should match"
    );
    assert_eq!(viewer["entries"][0]["service_name"], "svc-b");
}

/// Patch viewer to remove all filters (pass empty filters array)
#[tokio::test]
async fn test_patch_viewer_remove_filters_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: traces from two services
    for (service, span) in [("svc-x", "op-x"), ("svc-y", "op-y")] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    // Create viewer with filter
    let create_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({
                            "name": "Filtered viewer",
                            "signal": "traces",
                            "filters": [{ "field": "service_name", "op": "eq", "value": "svc-x" }],
                            "filter_mode": "and"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Patch with empty filters to remove filters
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/viewers/{viewer_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(json!({ "filters": [] }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    let get_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    // Then: no filters → match-all → both entries visible
    assert_eq!(
        viewer["entry_count"], 2,
        "after clearing filters, all entries should match"
    );
    assert!(
        viewer["filters"].is_null(),
        "filters field should be absent after clear"
    );
}

/// Create viewer with invalid regex filter — should return 400
#[tokio::test]
async fn test_create_viewer_with_invalid_regex_returns_400_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({
                        "name": "Bad regex viewer",
                        "signal": "traces",
                        "filters": [{ "field": "service_name", "op": "regex", "value": "[invalid" }]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

/// `query` backward compat — existing query field still filters when no filters present
#[tokio::test]
async fn test_query_backward_compat_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    for (service, span) in [("legacy-svc", "op"), ("new-svc", "op")] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    let create_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({"name": "Legacy query viewer", "signal": "traces", "query": "legacy-svc"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    assert_eq!(
        viewer["entry_count"], 1,
        "query should still filter entries backward-compatibly"
    );
    assert_eq!(viewer["entries"][0]["service_name"], "legacy-svc");
    assert_eq!(viewer["query"], "legacy-svc");
    assert!(
        viewer["filters"].is_null(),
        "no filters field should be present"
    );
}

/// When both `query` and `filters` are present, `filters` take priority
#[tokio::test]
async fn test_filters_take_priority_over_query_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    for (service, span) in [("alpha", "op"), ("beta", "op")] {
        assert_eq!(
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/traces")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(make_trace_payload(service, span)))
                        .unwrap()
                )
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
    }

    // query="beta" but filter=service_name eq "alpha" → filter wins, only alpha appears
    let create_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({
                            "name": "Filter priority test",
                            "signal": "traces",
                            "query": "beta",
                            "filters": [{ "field": "service_name", "op": "eq", "value": "alpha" }],
                            "filter_mode": "and"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let get_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();

    // filters take priority: filter matches "alpha" only, not "beta"
    assert_eq!(
        viewer["entry_count"], 1,
        "filters must take priority over query"
    );
    assert_eq!(viewer["entries"][0]["service_name"], "alpha");
}

/// Create viewer with query -> non-matching entries excluded from entry_count and entries list
#[tokio::test]
async fn test_create_viewer_with_query_excludes_non_matching_from_entry_count_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: ingest 3 traces from different services
    for (service, span) in [
        ("svc-alpha", "op-alpha"),
        ("svc-beta", "op-beta"),
        ("svc-gamma", "op-gamma"),
    ] {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(make_trace_payload(service, span)))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // When: create viewer with query matching only "svc-alpha"
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "Alpha only", "signal": "traces", "query": "svc-alpha" })
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Then: entry_count = 1 (only svc-alpha), not 3
    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(viewer["entry_count"], 1);
    assert_eq!(viewer["entries"][0]["service_name"], "svc-alpha");
}

/// Create viewer without query -> all entries pass (backward-compatible match-all)
#[tokio::test]
async fn test_create_viewer_without_query_shows_all_entries_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: ingest 2 traces from different services
    for (svc, span) in [("svc-x", "op-x"), ("svc-y", "op-y")] {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(make_trace_payload(svc, span)))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // When: create viewer with NO query
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "All traces", "signal": "traces" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Then: all 2 entries appear
    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(
        viewer["entry_count"], 2,
        "without query all entries should be included"
    );
    assert_eq!(
        viewer["query"],
        serde_json::Value::Null,
        "query field should be absent when not set"
    );
}

/// PATCH viewer query -> viewer state is rebuilt and old non-matching entries are removed
#[tokio::test]
async fn test_patch_viewer_query_rebuilds_state_removes_non_matching_entries_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: ingest traces from two services
    for (svc, span) in [
        ("checkout-ui", "render-checkout"),
        ("orders-api", "process-order"),
    ] {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(make_trace_payload(svc, span)))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // Create a viewer with no query -> sees both entries
    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "name": "All traces", "signal": "traces" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_resp.status(), StatusCode::CREATED);
    let body = axum::body::to_bytes(create_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Confirm both entries are present before patch
    let pre_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/viewers/{viewer_id}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let pre: serde_json::Value = serde_json::from_slice(&pre_body).unwrap();
    assert_eq!(pre["entry_count"], 2, "should see 2 entries before patch");

    // When: PATCH to add query="checkout-ui"
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/viewers/{viewer_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "query": "checkout-ui" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    // Then: viewer now shows only checkout-ui entry
    let post_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let post: serde_json::Value = serde_json::from_slice(&post_body).unwrap();
    assert_eq!(
        post["entry_count"], 1,
        "after patching query, only matching entry should remain"
    );
    assert_eq!(post["entries"][0]["service_name"], "checkout-ui");
    assert_eq!(post["query"], "checkout-ui");
}

/// POST /api/viewers/preview -> returns matching entries without persisting a viewer
#[tokio::test]
async fn test_preview_viewer_returns_matching_entries_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: two traces ingested
    for (svc, span) in [
        ("checkout-ui", "render-checkout"),
        ("orders-api", "process-order"),
    ] {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(make_trace_payload(svc, span)))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // When: POST /api/viewers/preview with signal=traces, query=checkout-ui
    let preview_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers/preview")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "signal": "traces", "query": "checkout-ui" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preview_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(preview_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let preview: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Then: only checkout-ui entry is returned
    assert_eq!(preview["entry_count"], 1);
    assert_eq!(preview["entries"][0]["service_name"], "checkout-ui");
    assert_eq!(preview["signal"], "traces");
    assert_eq!(preview["query"], "checkout-ui");
}

/// POST /api/viewers/preview -> does not create a viewer or increase viewer list count
#[tokio::test]
async fn test_preview_viewer_does_not_persist_viewer_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: ingest a trace
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(make_trace_payload("svc-a", "op-a")))
                .unwrap(),
        )
        .await
        .unwrap();

    // When: POST /api/viewers/preview
    let preview_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers/preview")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "signal": "traces", "query": "svc-a" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preview_resp.status(), StatusCode::OK);

    // Then: viewer list is still empty -- preview did not persist anything
    let list_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri("/api/viewers")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let list: serde_json::Value = serde_json::from_slice(&list_body).unwrap();
    assert_eq!(
        list["viewers"].as_array().unwrap().len(),
        0,
        "preview must not persist a viewer"
    );
}

/// Preview and viewer detail must agree on matching entries for the same signal/query
#[tokio::test]
async fn test_preview_and_viewer_detail_are_consistent_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Given: ingest two traces
    for (svc, span) in [
        ("checkout-ui", "render-checkout"),
        ("orders-api", "process-order"),
    ] {
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(make_trace_payload(svc, span)))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // Run preview with query="checkout-ui"
    let preview_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers/preview")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "signal": "traces", "query": "checkout-ui" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let preview: serde_json::Value = serde_json::from_slice(&preview_body).unwrap();

    // Create a viewer with the same signal/query
    let create_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": "Checkout viewer", "signal": "traces", "query": "checkout-ui" })
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let detail_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let detail: serde_json::Value = serde_json::from_slice(&detail_body).unwrap();

    // Then: preview and detail must report the same entry_count
    assert_eq!(
        preview["entry_count"], detail["entry_count"],
        "preview and viewer detail must agree on match count"
    );
}

/// Existing chart_type PATCH still works when query is not provided
#[tokio::test]
async fn test_patch_chart_type_without_query_still_works_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    // Create viewer (no query)
    let create_body = axum::body::to_bytes(
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/viewers")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        json!({ "name": "Chart test", "signal": "metrics" }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer_id = serde_json::from_slice::<serde_json::Value>(&create_body).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // PATCH only chart_type (no query field)
    let patch_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/api/viewers/{viewer_id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "chart_type": "line" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch_resp.status(), StatusCode::OK);

    // Verify chart_type changed
    let get_body = axum::body::to_bytes(
        app.oneshot(
            Request::builder()
                .uri(format!("/api/viewers/{viewer_id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .into_body(),
        usize::MAX,
    )
    .await
    .unwrap();
    let viewer: serde_json::Value = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(viewer["chart_type"], "line");
}

/// POST /api/viewers/preview with signal=metrics returns entries with metric_name and metric_value
#[tokio::test]
async fn test_preview_viewer_returns_metric_name_and_value_memory() {
    let env = setup_memory_viewer_app().await;
    let app = env.app;

    let metric_req = Request::builder()
        .method("POST")
        .uri("/v1/metrics")
        .header("content-type", "application/json")
        .body(axum::body::Body::from(make_metric_payload(
            "metrics-svc",
            "cpu.usage",
            42,
        )))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(metric_req).await.unwrap().status(),
        StatusCode::OK
    );

    let preview_resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/viewers/preview")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    json!({ "signal": "metrics" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(preview_resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(preview_resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let preview: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(preview["entry_count"], 1);
    assert_eq!(
        preview["entries"][0]["metric_name"], "cpu.usage",
        "metric_name must be present in preview entries"
    );
    assert_eq!(
        preview["entries"][0]["metric_value"], 42.0,
        "metric_value must be present in preview entries"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_create_dashboard_without_runtime_returns_503() {
    // ingest-only mode
    let redis_container = testcontainers_modules::redis::Redis::default()
        .start()
        .await
        .unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let redis = make_redis_store(redis_port).await;
    let app = build_app(StreamStore::Redis(redis));

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/dashboards")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(json!({ "name": "D" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// --- UI HTML content (query filter / preview) ---------------------------------

/// GET / HTML contains all query filter and preview UI elements:
/// - viewer-query-input (with data-testid)
/// - preview-viewer-button (with data-testid)
/// - viewer-preview-panel with its inner elements
/// - Query column header in the viewer table
/// - viewer-detail-query-row with its inner elements
#[tokio::test]
async fn test_index_html_contains_query_filter_and_preview_elements() {
    let env = setup_memory_viewer_app().await;
    let resp = env
        .app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(bytes.to_vec()).expect("HTML body should be valid UTF-8");

    // query input
    assert!(
        html.contains("id=\"viewer-query-input\""),
        "HTML should contain viewer-query-input element"
    );
    assert!(
        html.contains("data-testid=\"viewer-query-input\""),
        "viewer-query-input should have data-testid attribute"
    );

    // preview button
    assert!(
        html.contains("id=\"preview-viewer-button\""),
        "HTML should contain preview-viewer-button element"
    );
    assert!(
        html.contains("data-testid=\"preview-viewer-button\""),
        "preview-viewer-button should have data-testid attribute"
    );

    // preview panel
    assert!(
        html.contains("id=\"viewer-preview-panel\""),
        "HTML should contain viewer-preview-panel section"
    );
    assert!(
        html.contains("id=\"viewer-preview-entries-body\""),
        "preview panel should contain entries tbody"
    );
    assert!(
        html.contains("id=\"viewer-preview-traces-body\""),
        "preview panel should contain traces tbody"
    );
    assert!(
        html.contains("id=\"viewer-preview-count\""),
        "preview panel should contain entry count span"
    );
    assert!(
        html.contains("id=\"viewer-preview-close\""),
        "preview panel should contain close button"
    );

    // viewer table Query column
    assert!(
        html.contains("<th>Query</th>"),
        "viewer table header should contain Query column"
    );

    // viewer detail query edit row
    assert!(
        html.contains("id=\"viewer-detail-query-row\""),
        "viewer detail section should contain query edit row"
    );
    assert!(
        html.contains("id=\"viewer-detail-query-input\""),
        "query edit row should contain input field"
    );
    assert!(
        html.contains("id=\"viewer-detail-query-update\""),
        "query edit row should contain update button"
    );
}
