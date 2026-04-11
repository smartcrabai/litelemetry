use litelemetry::server::{SharedViewerRuntime, build_app_with_services};
use litelemetry::storage::memory::{
    MemoryStreamStore, MemoryViewerStore, default_dashboard_definitions, default_viewer_definitions,
};
use litelemetry::storage::postgres::PostgresStore;
use litelemetry::storage::redis::RedisStore;
use litelemetry::storage::{StreamStore, ViewerStore};
use litelemetry::viewer_runtime::runtime::ViewerRuntime;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;

const DEFAULT_HTTP_PORT: u16 = 8080;
const DEFAULT_VIEWER_RUNTIME_POLL_MS: u64 = 1_000;
const DEFAULT_MEMORY_STREAM_MAX_ENTRIES: usize = 100_000;

#[derive(Debug, Error, PartialEq, Eq)]
enum ConfigError {
    #[error("{var_name} must be {expected}, got {value:?}")]
    InvalidEnv {
        var_name: &'static str,
        expected: &'static str,
        value: String,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "litelemetry=info".into()),
        )
        .init();

    let http_port = std::env::var("HTTP_PORT").ok();
    let port = std::env::var("PORT").ok();
    let viewer_runtime_poll_ms_raw = std::env::var("VIEWER_RUNTIME_POLL_MS").ok();
    let standalone_raw = std::env::var("STANDALONE").ok();

    let port = exit_on_config_error(read_http_port(http_port.as_deref(), port.as_deref()));
    let viewer_runtime_poll_ms = exit_on_config_error(read_viewer_runtime_poll_ms(
        viewer_runtime_poll_ms_raw.as_deref(),
    ));
    let standalone = exit_on_config_error(read_standalone(standalone_raw.as_deref()));

    let (stream_store, viewer_store, viewer_runtime) = if standalone {
        tracing::warn!(
            "starting in standalone (in-memory) mode; set STANDALONE=false to use persistent storage"
        );

        let memory_stream_max_entries_raw = std::env::var("MEMORY_STREAM_MAX_ENTRIES").ok();
        let max_entries = exit_on_config_error(read_memory_stream_max_entries(
            memory_stream_max_entries_raw.as_deref(),
        ));

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
        let mut interval = tokio::time::interval(Duration::from_millis(poll_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            if let Err(e) = runtime.lock().await.apply_diff_batch().await {
                tracing::error!("viewer runtime diff batch failed: {e}");
            }
        }
    });
}

fn exit_on_config_error<T>(result: Result<T, ConfigError>) -> T {
    result.unwrap_or_else(|error| {
        tracing::error!("{error}");
        std::process::exit(2);
    })
}

fn read_http_port(http_port: Option<&str>, port: Option<&str>) -> Result<u16, ConfigError> {
    match http_port {
        Some(value) => parse_env_with_message(
            "HTTP_PORT",
            Some(value),
            DEFAULT_HTTP_PORT,
            "a valid u16 TCP port",
        ),
        None => parse_env_with_message("PORT", port, DEFAULT_HTTP_PORT, "a valid u16 TCP port"),
    }
}

fn read_viewer_runtime_poll_ms(raw: Option<&str>) -> Result<u64, ConfigError> {
    let poll_ms = parse_env_with_message(
        "VIEWER_RUNTIME_POLL_MS",
        raw,
        DEFAULT_VIEWER_RUNTIME_POLL_MS,
        "a positive integer",
    )?;

    if poll_ms == 0 {
        return Err(invalid_env(
            "VIEWER_RUNTIME_POLL_MS",
            "a positive integer",
            "0",
        ));
    }

    Ok(poll_ms)
}

fn read_standalone(raw: Option<&str>) -> Result<bool, ConfigError> {
    match raw {
        None => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Some(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Some(value) => Err(invalid_env(
            "STANDALONE",
            "`true`, `false`, `1`, or `0`",
            value,
        )),
    }
}

fn read_memory_stream_max_entries(raw: Option<&str>) -> Result<usize, ConfigError> {
    parse_env_with_message(
        "MEMORY_STREAM_MAX_ENTRIES",
        raw,
        DEFAULT_MEMORY_STREAM_MAX_ENTRIES,
        "a non-negative integer",
    )
}

fn parse_env_with_message<T>(
    var_name: &'static str,
    raw: Option<&str>,
    default: T,
    expected: &'static str,
) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
{
    match raw {
        Some(value) => value
            .parse()
            .map_err(|_| invalid_env(var_name, expected, value)),
        None => Ok(default),
    }
}

fn invalid_env(var_name: &'static str, expected: &'static str, value: &str) -> ConfigError {
    ConfigError::InvalidEnv {
        var_name,
        expected,
        value: value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        invalid_env, read_http_port, read_standalone, read_viewer_runtime_poll_ms,
        redact_redis_url_for_log,
    };

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

    #[test]
    fn http_port_prefers_http_port_over_port() {
        assert_eq!(read_http_port(Some("9090"), Some("8081")).unwrap(), 9090);
    }

    #[test]
    fn http_port_rejects_invalid_http_port_even_when_port_is_valid() {
        assert_eq!(
            read_http_port(Some("abc"), Some("8081")).unwrap_err(),
            invalid_env("HTTP_PORT", "a valid u16 TCP port", "abc")
        );
    }

    #[test]
    fn viewer_runtime_poll_ms_rejects_zero() {
        assert_eq!(
            read_viewer_runtime_poll_ms(Some("0")).unwrap_err(),
            invalid_env("VIEWER_RUNTIME_POLL_MS", "a positive integer", "0")
        );
    }

    #[test]
    fn standalone_accepts_false_literal() {
        assert!(!read_standalone(Some("false")).unwrap());
    }

    #[test]
    fn standalone_rejects_invalid_value() {
        assert_eq!(
            read_standalone(Some("sometimes")).unwrap_err(),
            invalid_env("STANDALONE", "`true`, `false`, `1`, or `0`", "sometimes")
        );
    }
}
