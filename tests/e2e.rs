//! E2E テスト — 実際の OpenTelemetry SDK を使ってデータを送信する
//!
//! 実際の OTLP クライアント（opentelemetry-otlp）からデータを送り、
//! サーバーが受け取って Redis に格納することを検証する。
//!
//! 実行には Docker が必要:
//!   cargo test --test e2e -- --include-ignored

use litelemetry::domain::telemetry::{NormalizedEntry, Signal};
use litelemetry::server::build_app;
use litelemetry::storage::redis::RedisStore;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger, LoggerProvider as _, Severity};
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{SpanKind, TraceContextExt, Tracer, TracerProvider as _};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::redis::Redis;
use tokio::{
    net::{TcpListener, TcpStream},
    task,
    time::{Duration, sleep},
};

// ─── ヘルパー ────────────────────────────────────────────────────────────────

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
            "Redis の {} stream にデータが格納されること",
            signal_name(signal)
        );
        assert_eq!(entries[0].1.signal, signal);
        assert!(
            !entries[0].1.payload.is_empty(),
            "{} ペイロードが空でないこと",
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

    panic!("Redis接続失敗: {}", last_error.unwrap())
}

/// litelemetry サーバーをランダムポートで起動して、そのポート番号を返す
async fn start_app_server(redis_port: u16) -> u16 {
    let redis = make_redis_store(redis_port).await;
    let app = build_app(redis);
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

    panic!("アプリサーバー起動失敗: {addr}")
}

/// SimpleSpanProcessor を使う SdkTracerProvider を構築する
///
/// SimpleSpanProcessor は span 終了時に即時エクスポートするため、
/// テストでの動作確認に適している。
fn build_tracer_provider(app_port: u16) -> SdkTracerProvider {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(format!("http://127.0.0.1:{app_port}/v1/traces"))
        .build()
        .expect("SpanExporter 構築失敗");

    SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build()
}

/// PeriodicReader を使う SdkMeterProvider を構築する
///
/// shutdown() 時に最終エクスポートが実行される。
fn build_meter_provider(app_port: u16) -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(format!("http://127.0.0.1:{app_port}/v1/metrics"))
        .build()
        .expect("MetricExporter 構築失敗");

    SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(exporter).build())
        .build()
}

/// SimpleLogProcessor を使う SdkLoggerProvider を構築する
fn build_logger_provider(app_port: u16) -> SdkLoggerProvider {
    let exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_endpoint(format!("http://127.0.0.1:{app_port}/v1/logs"))
        .build()
        .expect("LogExporter 構築失敗");

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

// ─── E2E: Traces ─────────────────────────────────────────────────────────────

/// E2E: OTel SDK から span を送信して Redis に格納されることを確認
///
/// シナリオ:
///   1. litelemetry サーバーをランダムポートで起動
///   2. opentelemetry_sdk + opentelemetry-otlp で span を送信
///   3. Redis の traces stream に 1 件格納されることを確認
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
            // _span がドロップされると SimpleSpanProcessor が即時エクスポート
        }
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Traces).await;
}

/// E2E: 複数の span を送信して個別に Redis に格納されることを確認
///
/// SimpleSpanProcessor は span ごとに 1 HTTP リクエスト送るため、
/// 3 span → 3 Redis エントリとなる。
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
        "SimpleSpanProcessor は span 1 件ごとに 1 リクエスト送るため 3 エントリになること"
    );
}

/// E2E: 子 span を持つ trace を送信して、それぞれが格納されることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_nested_spans_stored() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        let provider = build_tracer_provider(app_port);
        {
            let tracer = provider.tracer(E2E_INSTRUMENTATION_NAME);

            // 親 span を生成
            let parent_span = tracer
                .span_builder("http-request")
                .with_kind(SpanKind::Server)
                .start(&tracer);

            // 子 span を生成（親コンテキストを伝播）
            {
                let cx = opentelemetry::Context::current_with_span(parent_span);
                let _child = tracer
                    .span_builder("db-query")
                    .with_kind(SpanKind::Client)
                    .start_with_context(&tracer, &cx);
                // _child ドロップ → 子 span エクスポート
                // cx ドロップ → 親 span エクスポート
            }
        }
        provider.shutdown().unwrap();
    })
    .await;

    let entries = app.wait_for_entries(Signal::Traces, 10, 2).await;

    // SimpleSpanProcessor: child (先に終了) + parent = 2 エントリ
    assert_eq!(
        entries.len(),
        2,
        "親 span と子 span の 2 件が格納されること"
    );
}

// ─── E2E: Metrics ────────────────────────────────────────────────────────────

/// E2E: OTel SDK からメトリクスを送信して Redis に格納されることを確認
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
                .with_description("HTTP リクエスト数")
                .build();
            counter.add(42, &[]);
        }
        // shutdown() が最終エクスポートを保証する
        provider.shutdown().unwrap();
    })
    .await;

    app.assert_signal_has_payload(Signal::Metrics).await;
}

// ─── E2E: Logs ───────────────────────────────────────────────────────────────

/// E2E: OTel SDK からログを送信して Redis に格納されることを確認
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

// ─── E2E: 全シグナル混在 ────────────────────────────────────────────────────

/// E2E: traces/metrics/logs を同時に送信してシグナルごとに正しく振り分けられることを確認
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_e2e_all_signals_routed_correctly() {
    let app = TestApp::start().await;
    let app_port = app.app_port;

    run_blocking_otel(move || {
        // ── Traces ──
        let trace_provider = build_tracer_provider(app_port);
        {
            let tracer = trace_provider.tracer(E2E_INSTRUMENTATION_NAME);
            let _span = tracer.start("mixed-signal-test");
        }
        trace_provider.shutdown().unwrap();

        // ── Metrics ──
        let meter_provider = build_meter_provider(app_port);
        {
            let meter = meter_provider.meter(E2E_INSTRUMENTATION_NAME);
            let counter = meter.u64_counter("test.count").build();
            counter.add(1, &[]);
        }
        meter_provider.shutdown().unwrap();

        // ── Logs ──
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

    // 各シグナルの Redis stream にデータが格納されていることを確認
    let traces = app.wait_for_entries(Signal::Traces, 10, 1).await;
    assert!(!traces.is_empty(), "traces stream にデータがあること");

    let metrics = app.wait_for_entries(Signal::Metrics, 10, 1).await;
    assert!(!metrics.is_empty(), "metrics stream にデータがあること");

    let logs = app.wait_for_entries(Signal::Logs, 10, 1).await;
    assert!(!logs.is_empty(), "logs stream にデータがあること");
}
