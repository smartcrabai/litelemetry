use litelemetry::server::{SharedViewerRuntime, build_app_with_services};
use litelemetry::storage::postgres::PostgresStore;
use litelemetry::storage::redis::RedisStore;
use litelemetry::viewer_runtime::runtime::ViewerRuntime;
use std::time::Duration;
use tokio::sync::Mutex;

const DEFAULT_HTTP_PORT: u16 = 8080;
const DEFAULT_VIEWER_RUNTIME_POLL_MS: u64 = 1_000;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "litelemetry=info".into()),
        )
        .init();

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let database_url = std::env::var("DATABASE_URL").ok();
    let port: u16 = std::env::var("HTTP_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_HTTP_PORT);
    let viewer_runtime_poll_ms: u64 = std::env::var("VIEWER_RUNTIME_POLL_MS")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_VIEWER_RUNTIME_POLL_MS);

    tracing::info!("connecting to Redis: {redis_url}");
    let redis = RedisStore::new(&redis_url)
        .await
        .expect("failed to connect to Redis");
    let mut postgres = None;
    let mut viewer_runtime = None;

    if let Some(database_url) = database_url.as_deref() {
        tracing::info!("connecting to PostgreSQL");
        let postgres_store = PostgresStore::new(database_url)
            .await
            .expect("failed to connect to PostgreSQL");
        postgres_store
            .create_schema()
            .await
            .expect("failed to create PostgreSQL schema");

        let runtime = ViewerRuntime::build(postgres_store.clone(), redis.clone())
            .await
            .expect("failed to build viewer runtime");
        let runtime = std::sync::Arc::new(Mutex::new(runtime));
        spawn_viewer_runtime(runtime.clone(), viewer_runtime_poll_ms);
        tracing::info!(
            "viewer runtime started with {} ms polling interval",
            viewer_runtime_poll_ms
        );
        postgres = Some(postgres_store);
        viewer_runtime = Some(runtime);
    } else {
        tracing::info!("DATABASE_URL is not set; starting in ingest-only mode");
    }

    let app = build_app_with_services(redis, postgres, viewer_runtime);
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("failed to bind");
    tracing::info!("listening on 0.0.0.0:{port}");
    axum::serve(listener, app).await.expect("server error");
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
