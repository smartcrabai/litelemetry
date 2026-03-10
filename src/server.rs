use crate::domain::telemetry::Signal;
use crate::ingest::decode::DecodeError;
use crate::ingest::otlp_http::parse_ingest_request;
use crate::storage::redis::RedisStore;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};

/// OTLP ペイロードの最大ボディサイズ (4 MiB)。
/// 単一バッチとして現実的なサイズ上限。
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Axum 共有 state
///
/// RedisStore は MultiplexedConnection をラップしており Clone で並行使用できるため、
/// Arc<Mutex<>> でラップする必要はない。Axum は各リクエストで State を clone する。
#[derive(Clone)]
pub struct AppState {
    pub redis: RedisStore,
}

/// Axum app を構築して返す
pub fn build_app(redis: RedisStore) -> Router {
    let state = AppState { redis };
    Router::new()
        .route("/v1/traces", post(ingest_traces))
        .route("/v1/metrics", post(ingest_metrics))
        .route("/v1/logs", post(ingest_logs))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn ingest_traces(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    handle_ingest(state, Signal::Traces, &headers, body).await
}

async fn ingest_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    handle_ingest(state, Signal::Metrics, &headers, body).await
}

async fn ingest_logs(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> StatusCode {
    handle_ingest(state, Signal::Logs, &headers, body).await
}

async fn handle_ingest(
    state: AppState,
    signal: Signal,
    headers: &HeaderMap,
    body: Bytes,
) -> StatusCode {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());

    match parse_ingest_request(signal, content_type, body) {
        Ok(entry) => {
            let mut redis = state.redis;
            match redis.append_entry(&entry).await {
                Ok(_) => StatusCode::OK,
                Err(e) => {
                    tracing::error!("redis append_entry failed: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        Err(DecodeError::UnsupportedContentType(_)) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
    }
}
