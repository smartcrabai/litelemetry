//! 統合テスト — Redis と PostgreSQL を必要とする
//!
//! testcontainers で実際の Redis / PostgreSQL インスタンスを起動して検証する。
//! 実行には Docker が必要:
//!   cargo test --test integration
//!
//! 通常の CI では `#[ignore]` でスキップされ、
//! `cargo test --test integration -- --include-ignored` で実行できる。

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
    todo!("implement after production code is ready")
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
    todo!("implement after production code is ready")
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
    todo!("implement after production code is ready")
}

// ─── diff update ────────────────────────────────────────────────────────────

/// diff 更新: 複数 viewer を 1 回の Redis 走査でまとめて更新する
///
/// シナリオ:
///   1. traces を対象とする viewer を 2 件登録
///   2. Redis に traces telemetry を追加
///   3. due batch を発火
///   4. Redis から 1 回だけ読み出して 2 viewer の state が更新されることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_diff_update_one_pass_fan_out_to_multiple_viewers() {
    todo!("implement after production code is ready")
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
    todo!("implement after production code is ready")
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
    todo!("implement after production code is ready")
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
    todo!("implement after production code is ready")
}

/// OTLP/HTTP metrics エンドポイントへの ingest
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_metrics_via_otlp_http() {
    todo!("implement after production code is ready")
}

/// OTLP/HTTP logs エンドポイントへの ingest
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_logs_via_otlp_http() {
    todo!("implement after production code is ready")
}

/// 未対応の content-type を送信した場合に 415 Unsupported Media Type が返る
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_ingest_unsupported_content_type_returns_415() {
    todo!("implement after production code is ready")
}
