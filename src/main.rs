use litelemetry::server::{SharedViewerRuntime, build_app_with_services};
use litelemetry::storage::memory::{
    MemoryStreamStore, MemoryViewerStore, default_dashboard_definitions, default_viewer_definitions,
};
use litelemetry::storage::postgres::PostgresStore;
use litelemetry::storage::redis::RedisStore;
use litelemetry::storage::{StreamStore, ViewerStore};
use litelemetry::viewer_runtime::runtime::ViewerRuntime;
use std::time::Duration;
use tokio::sync::Mutex;

const DEFAULT_HTTP_PORT: u16 = 8080;
const DEFAULT_VIEWER_RUNTIME_POLL_MS: u64 = 1_000;
const DEFAULT_MEMORY_STREAM_MAX_ENTRIES: usize = 100_000;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "litelemetry=info".into()),
        )
        .init();

    let port: u16 = std::env::var("HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_HTTP_PORT);
    let viewer_runtime_poll_ms: u64 = std::env::var("VIEWER_RUNTIME_POLL_MS")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_VIEWER_RUNTIME_POLL_MS);
    let standalone = std::env::var("STANDALONE")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    let (stream_store, viewer_store, viewer_runtime) = if standalone {
        tracing::info!("starting in standalone (in-memory) mode");

        let max_entries = std::env::var("MEMORY_STREAM_MAX_ENTRIES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MEMORY_STREAM_MAX_ENTRIES);

        let memory_viewer_store = MemoryViewerStore::new();
        let viewer_defs = default_viewer_definitions();
        let viewer_ids: Vec<uuid::Uuid> = viewer_defs.iter().map(|d| d.id).collect();
        for def in &viewer_defs {
            memory_viewer_store
                .insert_viewer_definition(def)
                .await
                .expect("failed to insert default viewer definition");
        }
        for dash in default_dashboard_definitions(&viewer_ids) {
            memory_viewer_store
                .insert_dashboard(&dash)
                .await
                .expect("failed to insert default dashboard");
        }

        let stream_store = StreamStore::Memory(MemoryStreamStore::new(max_entries));
        let viewer_store = ViewerStore::Memory(memory_viewer_store);

        let runtime = build_and_spawn_viewer_runtime(
            viewer_store.clone(),
            stream_store.clone(),
            viewer_runtime_poll_ms,
        )
        .await;

        (stream_store, Some(viewer_store), Some(runtime))
    } else {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        let database_url = std::env::var("DATABASE_URL").ok();

        tracing::info!(
            "connecting to Redis: {}",
            redact_redis_url_for_log(&redis_url)
        );
        let redis = RedisStore::new(&redis_url)
            .await
            .expect("failed to connect to Redis");
        let stream_store = StreamStore::Redis(redis);

        let mut viewer_store = None;
        let mut viewer_runtime: Option<SharedViewerRuntime> = None;

        if let Some(database_url) = database_url.as_deref() {
            tracing::info!("connecting to PostgreSQL");
            let postgres_store = PostgresStore::new(database_url)
                .await
                .expect("failed to connect to PostgreSQL");
            postgres_store
                .create_schema()
                .await
                .expect("failed to create PostgreSQL schema");

            let vs = ViewerStore::Postgres(postgres_store);
            let runtime = build_and_spawn_viewer_runtime(
                vs.clone(),
                stream_store.clone(),
                viewer_runtime_poll_ms,
            )
            .await;
            viewer_store = Some(vs);
            viewer_runtime = Some(runtime);
        } else {
            tracing::info!("DATABASE_URL is not set; starting in ingest-only mode");
        }

        (stream_store, viewer_store, viewer_runtime)
    };

    let app = build_app_with_services(stream_store, viewer_store, viewer_runtime);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind");
    tracing::info!("listening on 0.0.0.0:{port}");
    axum::serve(listener, app).await.expect("server error");
}

async fn build_and_spawn_viewer_runtime(
    viewer_store: ViewerStore,
    stream_store: StreamStore,
    poll_ms: u64,
) -> SharedViewerRuntime {
    let runtime = ViewerRuntime::build(viewer_store, stream_store)
        .await
        .expect("failed to build viewer runtime");
    let runtime = std::sync::Arc::new(Mutex::new(runtime));
    spawn_viewer_runtime(runtime.clone(), poll_ms);
    tracing::info!(
        "viewer runtime started with {} ms polling interval",
        poll_ms
    );
    runtime
}

fn redact_redis_url_for_log(redis_url: &str) -> String {
    let Some((scheme, rest)) = redis_url.split_once("://") else {
        return "<redacted redis url>".to_string();
    };

    let authority_end = rest
        .find(|c| ['/', '?', '#'].contains(&c))
        .unwrap_or(rest.len());
    let (authority, suffix) = rest.split_at(authority_end);
    let sanitized_authority = authority
        .rsplit_once('@')
        .map(|(_, host)| format!("[REDACTED]@{host}"))
        .unwrap_or_else(|| authority.to_string());

    format!("{scheme}://{sanitized_authority}{suffix}")
}

fn spawn_viewer_runtime(runtime: SharedViewerRuntime, poll_ms: u64) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(poll_ms.max(1)));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            if let Err(e) = runtime.lock().await.apply_diff_batch().await {
                tracing::error!("viewer runtime diff batch failed: {e}");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::redact_redis_url_for_log;

    #[test]
    fn redis_url_without_credentials_is_logged_as_is() {
        assert_eq!(
            redact_redis_url_for_log("redis://127.0.0.1:6379"),
            "redis://127.0.0.1:6379"
        );
    }

    #[test]
    fn redis_url_with_credentials_is_redacted() {
        assert_eq!(
            redact_redis_url_for_log("redis://user:secret@redis.example.com:6379/1"),
            "redis://[REDACTED]@redis.example.com:6379/1"
        );
    }
}
