//! 統合テスト — Redis と PostgreSQL を必要とする
//!
//! testcontainers で実際の Redis / PostgreSQL インスタンスを起動して検証する。
//! 実行には Docker が必要:
//!   cargo test --test integration -- --include-ignored
//!
//! 通常の CI では `#[ignore]` でスキップされ、
//! `cargo test --test integration -- --include-ignored` で実行できる。

use axum::http::{Request, StatusCode};
use bytes::Bytes;
use chrono::{Duration, Utc};
use litelemetry::domain::telemetry::{NormalizedEntry, Signal, SignalMask};
use litelemetry::domain::viewer::ViewerDefinition;
use litelemetry::server::{build_app, build_app_with_services};
use litelemetry::storage::postgres::{PostgresStore, ViewerSnapshotRow};
use litelemetry::storage::redis::RedisStore;
use litelemetry::viewer_runtime::runtime::ViewerRuntime;
use litelemetry::viewer_runtime::state::StreamCursor;
use serde_json::json;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

// ─── ヘルパー ────────────────────────────────────────────────────────────────

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
    let runtime = ViewerRuntime::build(pg.clone(), redis.clone())
        .await
        .unwrap();
    let runtime = Arc::new(Mutex::new(runtime));

    let app = build_app_with_services(redis, Some(pg), Some(runtime));

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

// ─── startup resume ─────────────────────────────────────────────────────────

/// 起動時 resume: PostgreSQL snapshot + Redis 差分から状態を復元する
///
/// シナリオ:
///   1. viewer 定義を PG に挿入
///   2. snapshot (revision 一致) を PG に保存
///   3. Redis に snapshot 以降の telemetry を追加
///   4. viewer runtime を起動
///   5. state が "snapshot + Redis diff" で正しく構築されることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_startup_resume_from_snapshot_and_redis_diff() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. viewer 定義を PG に挿入
    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def_id = def.id;
    pg.insert_viewer_definition(&def).await.unwrap();

    // 2. Redis に 5 件追加してカーソルを取得
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..5 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }
    // カーソル: 最初の 5 件目の ID を取得 (XRANGE して最後の ID)
    let snapshot_entries = redis_write
        .read_entries_since(Signal::Traces, None, 5)
        .await
        .unwrap();
    let snapshot_cursor_id = snapshot_entries.last().unwrap().0.clone();

    // snapshot (cursor = 最初の 5 件目の ID) を PG に保存
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

    // 3. Redis にさらに 3 件追加 (snapshot cursor より後)
    for _ in 0..3 {
        redis_write
            .append_entry(&make_traces_entry(5_000))
            .await
            .unwrap();
    }

    // 4. viewer runtime を起動
    let runtime = ViewerRuntime::build(pg, make_redis_store(redis_port).await)
        .await
        .unwrap();

    // 5. state が "snapshot cursor 以降の 3 件" で構築されることを確認
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1, "viewer が 1 件あること");
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        3,
        "snapshot cursor 以降の 3 件だけが取り込まれること (got: {})",
        state.entries.len()
    );
    assert!(
        state.last_cursor.traces.is_some(),
        "traces カーソルが更新されていること"
    );
    assert_ne!(
        state.last_cursor.traces.as_deref(),
        Some(snapshot_cursor_id.as_str()),
        "カーソルが snapshot cursor より先に進んでいること"
    );
}

/// 起動時 resume: snapshot なし → Redis 全量 replay にフォールバック
///
/// シナリオ:
///   1. viewer 定義を PG に挿入 (snapshot はなし)
///   2. Redis に telemetry を追加
///   3. viewer runtime を起動
///   4. Redis retained 範囲から replay して state が構築されることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_startup_resume_no_snapshot_falls_back_to_replay() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. viewer 定義のみ (snapshot なし)
    let def = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    pg.insert_viewer_definition(&def).await.unwrap();

    // 2. Redis に 4 件追加
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..4 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    // 3. viewer runtime を起動
    let runtime = ViewerRuntime::build(pg, make_redis_store(redis_port).await)
        .await
        .unwrap();

    // 4. 全量 replay で 4 件の state が構築されることを確認
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        4,
        "スナップショットなし → Redis 全量 4 件が replay されること"
    );
}

/// 起動時 resume: revision mismatch → replay にフォールバック
///
/// シナリオ:
///   1. viewer 定義 revision=2 を PG に挿入
///   2. revision=1 の古い snapshot を PG に保存
///   3. viewer runtime を起動
///   4. revision mismatch を検出し replay にフォールバックすることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_startup_resume_revision_mismatch_falls_back_to_replay() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. viewer 定義 revision=2
    let def = make_viewer_def(Signal::Traces.into(), 300_000, 2);
    let def_id = def.id;
    pg.insert_viewer_definition(&def).await.unwrap();

    // 2. revision=1 の snapshot (cursor 付き) を保存
    let mut cursor = StreamCursor::default();
    cursor.set(Signal::Traces, "9999999999999-0".to_string()); // 未来の ID → これを使うと 0 件になる
    let snapshot = ViewerSnapshotRow {
        viewer_id: def_id,
        revision: 1, // mismatch!
        last_cursor_json: serde_json::to_value(&cursor).unwrap(),
        status: litelemetry::domain::viewer::ViewerStatus::Ok,
        generated_at: Utc::now(),
    };
    pg.upsert_snapshot(&snapshot).await.unwrap();

    // Redis に 3 件追加
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..3 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    // 3. viewer runtime を起動
    let runtime = ViewerRuntime::build(pg, make_redis_store(redis_port).await)
        .await
        .unwrap();

    // 4. revision mismatch → cursor をリセットして全量 replay → 3 件
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        3,
        "revision mismatch → カーソルをリセットして Redis 全量 3 件が replay されること"
    );
}

// ─── diff update ────────────────────────────────────────────────────────────

/// diff 更新: 複数 viewer を 1 回の Redis 走査でまとめて更新する
///
/// シナリオ:
///   1. traces を対象とする viewer を 2 件登録
///   2. Redis に traces telemetry を追加
///   3. diff batch を発火
///   4. Redis から 1 回だけ読み出して 2 viewer の state が更新されることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_diff_update_one_pass_fan_out_to_multiple_viewers() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. traces を対象とする viewer を 2 件登録
    let def1 = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    let def2 = make_viewer_def(Signal::Traces.into(), 300_000, 1);
    pg.insert_viewer_definition(&def1).await.unwrap();
    pg.insert_viewer_definition(&def2).await.unwrap();

    // 空の runtime を起動
    let mut runtime = ViewerRuntime::build(pg, make_redis_store(redis_port).await)
        .await
        .unwrap();

    // 両 viewer の初期 entries が 0 件であることを確認
    for (_, state) in runtime.viewers() {
        assert_eq!(state.entries.len(), 0);
    }

    // 2. Redis に 5 件追加
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..5 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }

    // 3. diff batch を発火
    runtime.apply_diff_batch().await.unwrap();

    // 4. 両 viewer に 5 件ずつ反映されていることを確認
    for (_, state) in runtime.viewers() {
        assert_eq!(state.entries.len(), 5, "両 viewer に 5 件が反映されること");
    }
}

/// diff 更新後の snapshot upsert
///
/// シナリオ:
///   1. viewer を登録してから diff 更新を発火
///   2. PG の viewer_snapshots に最新状態が upsert されることを確認
///   3. 再起動後に snapshot から resume できることを確認
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
    let mut runtime = ViewerRuntime::build(pg, make_redis_store(redis_port).await)
        .await
        .unwrap();

    // Redis に 3 件追加して diff batch
    let mut redis_write = make_redis_store(redis_port).await;
    for _ in 0..3 {
        redis_write
            .append_entry(&make_traces_entry(10_000))
            .await
            .unwrap();
    }
    runtime.apply_diff_batch().await.unwrap();

    // 2. PG の snapshot が upsert されていることを確認
    let mut snapshots = pg2.load_snapshots(&[def_id]).await.unwrap();
    assert!(
        !snapshots.is_empty(),
        "snapshot が PG に upsert されていること"
    );
    let snapshot = snapshots.remove(0);
    assert_eq!(snapshot.revision, 1);
    assert!(
        snapshot.last_cursor_json.get("traces").is_some(),
        "traces カーソルが snapshot に保存されていること"
    );

    // 3. 再起動後に snapshot から resume (追加エントリなし → diff 0件)
    let runtime2 = ViewerRuntime::build(
        make_postgres_store(pg_port).await,
        make_redis_store(redis_port).await,
    )
    .await
    .unwrap();
    let viewers2 = runtime2.viewers();
    assert_eq!(viewers2.len(), 1);
    let (_, state2) = &viewers2[0];
    // snapshot cursor が設定されているので Redis 差分は 0 件
    assert_eq!(
        state2.entries.len(),
        0,
        "再起動後は snapshot cursor 以降の差分 (0 件) のみ取り込まれること"
    );
}

/// viewer の lookback を超えた古いエントリが prune される
///
/// シナリオ:
///   1. viewer (lookback=60s) を登録
///   2. 古い telemetry (90s前) と新しい telemetry (10s前) を Redis に追加
///   3. diff 更新を発火
///   4. state に古いエントリが含まれないことを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_diff_update_prunes_entries_outside_lookback() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
    let pg_container = Postgres::default().start().await.unwrap();
    let pg_port = pg_container.get_host_port_ipv4(5432).await.unwrap();

    let pg = make_postgres_store(pg_port).await;
    pg.create_schema().await.unwrap();

    // 1. lookback=60s の viewer を登録
    let def = make_viewer_def(Signal::Traces.into(), 60_000, 1);
    pg.insert_viewer_definition(&def).await.unwrap();

    let mut runtime = ViewerRuntime::build(pg, make_redis_store(redis_port).await)
        .await
        .unwrap();

    // 2. 古いエントリ (90s前) と新しいエントリ (10s前) を Redis に追加
    let mut redis_write = make_redis_store(redis_port).await;
    // 古い (90s前 → lookback 60s を超える)
    redis_write
        .append_entry(&make_traces_entry(90_000))
        .await
        .unwrap();
    // 新しい (10s前 → lookback 内)
    redis_write
        .append_entry(&make_traces_entry(10_000))
        .await
        .unwrap();

    // 3. diff 更新を発火
    runtime.apply_diff_batch().await.unwrap();

    // 4. 古いエントリが prune されて新しい 1 件だけ残ることを確認
    let viewers = runtime.viewers();
    assert_eq!(viewers.len(), 1);
    let (_, state) = &viewers[0];
    assert_eq!(
        state.entries.len(),
        1,
        "lookback 外の古いエントリが prune されて 1 件だけ残ること (got: {})",
        state.entries.len()
    );
}

// ─── OTLP/HTTP ingest endpoint ───────────────────────────────────────────────

/// OTLP/HTTP traces エンドポイントへの ingest
///
/// シナリオ:
///   1. サーバを起動
///   2. POST /v1/traces に protobuf ペイロードを送信
///   3. 200 OK が返ること
///   4. Redis の lt:stream:traces に entry が追加されることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_traces_via_otlp_http() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(redis);

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
        "traces ingest は 200 OK を返すこと"
    );

    // 4. Redis に entry が追加されていることを確認
    let mut redis_check = make_redis_store(redis_port).await;
    let entries = redis_check
        .read_entries_since(Signal::Traces, None, 10)
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "Redis の traces stream に 1 件追加されること"
    );
    assert_eq!(entries[0].1.signal, Signal::Traces);
}

/// viewer UI 用 API: viewer 作成後に traces が一覧 API へ反映されることを確認
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

    let list_request = Request::builder()
        .method("GET")
        .uri("/api/viewers")
        .body(axum::body::Body::empty())
        .unwrap();

    let list_response = app.oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewers = payload["viewers"].as_array().unwrap();

    assert_eq!(viewers.len(), 1, "viewer が 1 件作成されること");
    assert_eq!(viewers[0]["entry_count"], 1, "trace が 1 件反映されること");
    assert_eq!(viewers[0]["entries"][0]["signal"], "traces");
    assert_eq!(viewers[0]["entries"][0]["service_name"], "checkout-ui");

    let preview = viewers[0]["entries"][0]["payload_preview"]
        .as_str()
        .unwrap();
    assert!(
        preview.contains("render-checkout"),
        "payload preview に span name が含まれること: {preview}"
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

    let list_request = Request::builder()
        .method("GET")
        .uri("/api/viewers")
        .body(axum::body::Body::empty())
        .unwrap();

    let list_response = app.oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewers = payload["viewers"].as_array().unwrap();

    assert_eq!(viewers.len(), 1, "viewer が 1 件作成されること");
    assert_eq!(viewers[0]["signals"][0], "metrics");
    assert_eq!(
        viewers[0]["entry_count"], 1,
        "metrics が 1 件反映されること"
    );
    assert_eq!(viewers[0]["entries"][0]["signal"], "metrics");
    assert_eq!(viewers[0]["entries"][0]["service_name"], "orders-api");

    let preview = viewers[0]["entries"][0]["payload_preview"]
        .as_str()
        .unwrap();
    assert!(
        preview.contains("http.server.requests"),
        "payload preview に metric name が含まれること: {preview}"
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

    let list_request = Request::builder()
        .method("GET")
        .uri("/api/viewers")
        .body(axum::body::Body::empty())
        .unwrap();

    let list_response = app.oneshot(list_request).await.unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(list_response.into_body(), usize::MAX)
        .await
        .unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let viewers = payload["viewers"].as_array().unwrap();

    assert_eq!(viewers.len(), 1, "viewer が 1 件作成されること");
    assert_eq!(viewers[0]["signals"][0], "logs");
    assert_eq!(viewers[0]["entry_count"], 1, "log が 1 件反映されること");
    assert_eq!(viewers[0]["entries"][0]["signal"], "logs");
    assert_eq!(viewers[0]["entries"][0]["service_name"], "worker-billing");

    let preview = viewers[0]["entries"][0]["payload_preview"]
        .as_str()
        .unwrap();
    assert!(
        preview.contains("payment authorized"),
        "payload preview に log message が含まれること: {preview}"
    );
}

/// OTLP/HTTP metrics エンドポイントへの ingest
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_metrics_via_otlp_http() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(redis);

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
        "Redis の metrics stream に 1 件追加されること"
    );
}

/// OTLP/HTTP logs エンドポイントへの ingest
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_logs_via_otlp_http() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(redis);

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
        "Redis の logs stream に 1 件追加されること"
    );
}

/// 未対応の content-type を送信した場合に 415 Unsupported Media Type が返る
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_unsupported_content_type_returns_415() {
    let redis_container = Redis::default().start().await.unwrap();
    let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();

    let redis = make_redis_store(redis_port).await;
    let app = build_app(redis);

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

// ─── GET /api/viewers/:id ────────────────────────────────────────────────────

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

    let app = build_app(make_redis_store(redis_port).await);

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
