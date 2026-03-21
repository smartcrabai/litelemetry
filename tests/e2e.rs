//! E2E tests -- sends data using the real OpenTelemetry SDK
//!
//! Sends data from a real OTLP client (opentelemetry-otlp),
//! and verifies that the server receives and stores it in Redis.
//!
//! Requires Docker to run:
//!   cargo test --test e2e -- --include-ignored

use litelemetry::domain::telemetry::{NormalizedEntry, Signal};
use litelemetry::server::{build_app, build_app_with_services};
use litelemetry::storage::memory::{MemoryStreamStore, MemoryViewerStore};
use litelemetry::storage::redis::RedisStore;
use litelemetry::storage::{StreamStore, ViewerStore};
use litelemetry::viewer_runtime::runtime::ViewerRuntime;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger, LoggerProvider as _, Severity};
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{SpanKind, TraceContextExt, Tracer, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use std::sync::Arc;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;
use tokio::sync::Mutex;
use tokio::{
    net::{TcpListener, TcpStream},
    task,
    time::{Duration, sleep},
};

// --- Helpers -----------------------------------------------------------------

const E2E_INSTRUMENTATION_NAME: &str = "e2e-test";
const APP_CONNECT_ATTEMPTS: usize = 20;
const APP_CONNECT_INTERVAL: Duration = Duration::from_millis(50);
const REDIS_CONNECT_ATTEMPTS: usize = 20;
const REDIS_CONNECT_INTERVAL: Duration = Duration::from_millis(50);
const REDIS_POLL_ATTEMPTS: usize = 20;
const REDIS_POLL_INTERVAL: Duration = Duration::from_millis(50);

struct TestApp {
    _redis_container: ContainerAsync<Redis>,
    redis_port: u16,
    app_port: u16,
}

impl TestApp {
    async fn start() -> Self {
        let redis_container = Redis::default().start().await.unwrap();
        let redis_port = redis_container.get_host_port_ipv4(6379).await.unwrap();
        let app_port = start_app_server(redis_port).await;

        Self {
            _redis_container: redis_container,
            redis_port,
            app_port,
        }
    }

    async fn read_entries(&self, signal: Signal, count: usize) -> Vec<(String, NormalizedEntry)> {
        let mut redis = make_redis_store(self.redis_port).await;
        redis.read_entries_since(signal, None, count).await.unwrap()
    }

    async fn wait_for_entries(
        &self,
        signal: Signal,
        count: usize,
        min_entries: usize,
    ) -> Vec<(String, NormalizedEntry)> {
        let mut entries = Vec::new();

        for _ in 0..REDIS_POLL_ATTEMPTS {
            entries = self.read_entries(signal, count).await;
            if entries.len() >= min_entries {
                return entries;
            }
            sleep(REDIS_POLL_INTERVAL).await;
        }

        entries
    }

    async fn assert_signal_has_payload(&self, signal: Signal) {
        let entries = self.wait_for_entries(signal, 10, 1).await;

        assert!(
            !entries.is_empty(),
            "data should be stored in Redis {} stream",
            signal_name(signal)
        );
        assert_eq!(entries[0].1.signal, signal);
        assert!(
            !entries[0].1.payload.is_empty(),
            "{} payload should not be empty",
            signal_name(signal)
        );
    }
}

async fn make_redis_store(port: u16) -> RedisStore {
    let url = format!("redis://127.0.0.1:{port}");
    let mut last_error = None;

    for _ in 0..REDIS_CONNECT_ATTEMPTS {
        match RedisStore::new(&url).await {
            Ok(store) => return store,
            Err(err) => {
                last_error = Some(err);
                sleep(REDIS_CONNECT_INTERVAL).await;
            }
        }
    }

    panic!("Redis connection failed: {}", last_error.unwrap())
}

/// Starts litelemetry server on a random port and returns the port number.
async fn start_app_server(redis_port: u16) -> u16 {
    let redis = make_redis_store(redis_port).await;
    let app = build_app(StreamStore::Redis(redis));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    wait_for_app_server(port).await;
    port
}

async fn wait_for_app_server(port: u16) {
    let addr = format!("127.0.0.1:{port}");

    for _ in 0..APP_CONNECT_ATTEMPTS {
        match TcpStream::connect(&addr).await {
            Ok(stream) => {
                drop(stream);
                return;
            }
            Err(_) => sleep(APP_CONNECT_INTERVAL).await,
        }
    }

    panic!("app server failed to start: {addr}")
}

/// Builds a SdkTracerProvider using SimpleSpanProcessor.
///
/// SimpleSpanProcessor exports immediately on span end,
/// which is suitable for test verification.
fn build_tracer_provider(app_port: u16) -> SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("http://127.0.0.1:{app_port}/v1/traces"))
        .build()
        .expect("SpanExporter build failed");

    SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build()
}

/// Builds a SdkMeterProvider using PeriodicReader.
///
/// Final export is executed on shutdown().
fn build_meter_provider(app_port: u16) -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("http://127.0.0.1:{app_port}/v1/metrics"))
        .build()
        .expect("MetricExporter build failed");

    SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter).build())
        .build()
}

/// Builds a SdkLoggerProvider using SimpleLogProcessor.
fn build_logger_provider(app_port: u16) -> SdkLoggerProvider {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("http://127.0.0.1:{app_port}/v1/logs"))
        .build()
        .expect("LogExporter build failed");

    SdkLoggerProvider::builder()
        .with_simple_exporter(exporter)
        .build()
}

fn signal_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Traces => "traces",
        Signal::Metrics => "metrics",
        Signal::Logs => "logs",
    }
}

fn emit_log(
    provider: &SdkLoggerProvider,
    severity: Severity,
    severity_text: &'static str,
    body: &'static str,
) {
    let logger = provider.logger(E2E_INSTRUMENTATION_NAME);
    let mut record = logger.create_log_record();
    record.set_severity_number(severity);
    record.set_severity_text(severity_text);
    record.set_body(AnyValue::String(body.into()));
    logger.emit(record);
}

async fn run_blocking_otel<F, T>(operation: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    task::spawn_blocking(operation).await.unwrap()
}

// --- E2E: Traces -------------------------------------------------------------

/// E2E: Sends a span from the OTel SDK and verifies it is stored in Redis.
///
/// Scenario:
///   1. Start litelemetry server on a random port
///   2. Send a span using opentelemetry_sdk + opentelemetry-otlp
///   3. Verify one entry is stored in the Redis traces stream
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_trace_stored_in_redis() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_tracer_provider(app_port);
        {
            let tracer = provider.tracer(E2E_INSTRUMENTATION_NAME);
            let _span = tracer.start("test-operation");
            // dropping _span triggers immediate export via SimpleSpanProcessor
        }
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Traces).await;
}

/// E2E: Sends multiple spans and verifies each is stored separately in Redis.
///
/// SimpleSpanProcessor sends one HTTP request per span,
/// so 3 spans -> 3 Redis entries.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_multiple_spans_each_stored_separately() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_tracer_provider(app_port);
        {
            let tracer = provider.tracer(E2E_INSTRUMENTATION_NAME);
            for i in 0..3_u32 {
                let _span = tracer.start(format!("operation-{i}"));
            }
        }
        provider.shutdown().unwrap();
    })
    .await;

    let entries = app.wait_for_entries(Signal::Traces, 10, 3).await;

    assert_eq!(
        entries.len(),
        3,
        "SimpleSpanProcessor sends one request per span, so there should be 3 entries"
    );
}

/// E2E: Sends a trace with child spans and verifies each is stored.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_nested_spans_stored() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_tracer_provider(app_port);
        {
            let tracer = provider.tracer(E2E_INSTRUMENTATION_NAME);

            // create parent span
            let parent_span = tracer
                .span_builder("http-request")
                .with_kind(SpanKind::Server)
                .start(&tracer);

            // create child span (propagating parent context)
            {
                let cx = opentelemetry::Context::current_with_span(parent_span);
                let _child = tracer
                    .span_builder("db-query")
                    .with_kind(SpanKind::Client)
                    .start_with_context(&tracer, &cx);
                // dropping _child -> child span exported
                // dropping cx -> parent span exported
            }
        }
        provider.shutdown().unwrap();
    })
    .await;

    let entries = app.wait_for_entries(Signal::Traces, 10, 2).await;

    // SimpleSpanProcessor: child (ends first) + parent = 2 entries
    assert_eq!(
        entries.len(),
        2,
        "parent span and child span should each be stored (2 entries)"
    );
}

// --- E2E: Metrics ------------------------------------------------------------

/// E2E: Sends metrics from the OTel SDK and verifies they are stored in Redis.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_metrics_stored_in_redis() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_meter_provider(app_port);
        {
            let meter = provider.meter(E2E_INSTRUMENTATION_NAME);
            let counter = meter
                .u64_counter("http.server.requests")
                .with_description("HTTP request count")
                .build();
            counter.add(42, &[]);
        }
        // shutdown() guarantees the final export
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Metrics).await;
}

// --- E2E: Logs ---------------------------------------------------------------

/// E2E: Sends logs from the OTel SDK and verifies they are stored in Redis.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_logs_stored_in_redis() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_logger_provider(app_port);
        emit_log(&provider, Severity::Info, "INFO", "E2E test log message");
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Logs).await;
}

// --- E2E: All signals mixed --------------------------------------------------

/// E2E: Sends traces/metrics/logs simultaneously and verifies correct routing per signal.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_all_signals_routed_correctly() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        // -- Traces --
        let trace_provider = build_tracer_provider(app_port);
        {
            let tracer = trace_provider.tracer(E2E_INSTRUMENTATION_NAME);
            let _span = tracer.start("mixed-signal-test");
        }
        trace_provider.shutdown().unwrap();

        // -- Metrics --
        let meter_provider = build_meter_provider(app_port);
        {
            let meter = meter_provider.meter(E2E_INSTRUMENTATION_NAME);
            let counter = meter.u64_counter("test.count").build();
            counter.add(1, &[]);
        }
        meter_provider.shutdown().unwrap();

        // -- Logs --
        let logger_provider = build_logger_provider(app_port);
        emit_log(
            &logger_provider,
            Severity::Warn,
            "WARN",
            "mixed signal test",
        );
        logger_provider.shutdown().unwrap();
    })
    .await;

    // verify data is stored in each signal's Redis stream
    let traces = app.wait_for_entries(Signal::Traces, 10, 1).await;
    assert!(!traces.is_empty(), "traces stream should have data");

    let metrics = app.wait_for_entries(Signal::Metrics, 10, 1).await;
    assert!(!metrics.is_empty(), "metrics stream should have data");

    let logs = app.wait_for_entries(Signal::Logs, 10, 1).await;
    assert!(!logs.is_empty(), "logs stream should have data");
}

// --- Memory store (no Docker required) --------------------------------------

struct MemoryTestApp {
    stream_store: MemoryStreamStore,
    app_port: u16,
}

impl MemoryTestApp {
    async fn start() -> Self {
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
            Some(ViewerStore::Memory(viewer_store)),
            Some(runtime),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        wait_for_app_server(port).await;
        MemoryTestApp {
            stream_store,
            app_port: port,
        }
    }

    async fn read_entries(&self, signal: Signal, count: usize) -> Vec<(String, NormalizedEntry)> {
        let store = self.stream_store.clone();
        store.read_entries_since(signal, None, count).await.unwrap()
    }

    async fn wait_for_entries(
        &self,
        signal: Signal,
        count: usize,
        min_entries: usize,
    ) -> Vec<(String, NormalizedEntry)> {
        let mut entries = Vec::new();
        for _ in 0..REDIS_POLL_ATTEMPTS {
            entries = self.read_entries(signal, count).await;
            if entries.len() >= min_entries {
                return entries;
            }
            sleep(REDIS_POLL_INTERVAL).await;
        }
        entries
    }

    async fn assert_signal_has_payload(&self, signal: Signal) {
        let entries = self.wait_for_entries(signal, 10, 1).await;
        assert!(
            !entries.is_empty(),
            "data should be stored in memory {} stream",
            signal_name(signal)
        );
        assert_eq!(entries[0].1.signal, signal);
        assert!(
            !entries[0].1.payload.is_empty(),
            "{} payload should not be empty",
            signal_name(signal)
        );
    }
}

/// Memory: Sends a span from the OTel SDK and verifies it is stored in memory.
#[tokio::test]
async fn test_e2e_trace_stored_in_memory() {
    let app = MemoryTestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_tracer_provider(app_port);
        {
            let tracer = provider.tracer(E2E_INSTRUMENTATION_NAME);
            let _span = tracer.start("test-operation");
        }
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Traces).await;
}

/// Memory: Stores multiple spans in memory.
#[tokio::test]
async fn test_e2e_multiple_spans_each_stored_separately_memory() {
    let app = MemoryTestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_tracer_provider(app_port);
        {
            let tracer = provider.tracer(E2E_INSTRUMENTATION_NAME);
            for i in 0..3_u32 {
                let _span = tracer.start(format!("operation-{i}"));
            }
        }
        provider.shutdown().unwrap();
    })
    .await;

    let entries = app.wait_for_entries(Signal::Traces, 10, 3).await;
    assert_eq!(
        entries.len(),
        3,
        "SimpleSpanProcessor sends one request per span, so there should be 3 entries"
    );
}

/// Memory: Stores nested spans in memory.
#[tokio::test]
async fn test_e2e_nested_spans_stored_memory() {
    let app = MemoryTestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_tracer_provider(app_port);
        {
            let tracer = provider.tracer(E2E_INSTRUMENTATION_NAME);
            let parent_span = tracer
                .span_builder("http-request")
                .with_kind(SpanKind::Server)
                .start(&tracer);
            {
                let cx = opentelemetry::Context::current_with_span(parent_span);
                let _child = tracer
                    .span_builder("db-query")
                    .with_kind(SpanKind::Client)
                    .start_with_context(&tracer, &cx);
            }
        }
        provider.shutdown().unwrap();
    })
    .await;

    let entries = app.wait_for_entries(Signal::Traces, 10, 2).await;
    assert_eq!(
        entries.len(),
        2,
        "parent span and child span should each be stored (2 entries)"
    );
}

/// Memory: Stores metrics in memory.
#[tokio::test]
async fn test_e2e_metrics_stored_in_memory() {
    let app = MemoryTestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_meter_provider(app_port);
        {
            let meter = provider.meter(E2E_INSTRUMENTATION_NAME);
            let counter = meter
                .u64_counter("http.server.requests")
                .with_description("HTTP request count")
                .build();
            counter.add(42, &[]);
        }
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Metrics).await;
}

/// Memory: Stores logs in memory.
#[tokio::test]
async fn test_e2e_logs_stored_in_memory() {
    let app = MemoryTestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_logger_provider(app_port);
        emit_log(&provider, Severity::Info, "INFO", "memory test log message");
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Logs).await;
}

/// Memory: Sends traces/metrics/logs simultaneously and verifies routing per signal.
#[tokio::test]
async fn test_e2e_all_signals_routed_correctly_memory() {
    let app = MemoryTestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let trace_provider = build_tracer_provider(app_port);
        {
            let tracer = trace_provider.tracer(E2E_INSTRUMENTATION_NAME);
            let _span = tracer.start("mixed-signal-memory");
        }
        trace_provider.shutdown().unwrap();

        let meter_provider = build_meter_provider(app_port);
        {
            let meter = meter_provider.meter(E2E_INSTRUMENTATION_NAME);
            let counter = meter.u64_counter("test.count.memory").build();
            counter.add(1, &[]);
        }
        meter_provider.shutdown().unwrap();

        let logger_provider = build_logger_provider(app_port);
        emit_log(
            &logger_provider,
            Severity::Warn,
            "WARN",
            "mixed signal memory",
        );
        logger_provider.shutdown().unwrap();
    })
    .await;

    let traces = app.wait_for_entries(Signal::Traces, 10, 1).await;
    assert!(!traces.is_empty(), "traces stream should have data");

    let metrics = app.wait_for_entries(Signal::Metrics, 10, 1).await;
    assert!(!metrics.is_empty(), "metrics stream should have data");

    let logs = app.wait_for_entries(Signal::Logs, 10, 1).await;
    assert!(!logs.is_empty(), "logs stream should have data");
}
