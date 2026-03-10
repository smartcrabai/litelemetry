use crate::domain::telemetry::Signal;
use crate::ingest::decode::DecodeError;
use crate::ingest::otlp_http::parse_ingest_request;
use crate::storage::redis::RedisStore;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::Html,
    routing::get,
    routing::post,
};

/// OTLP ペイロードの最大ボディサイズ (4 MiB)。
/// 単一バッチとして現実的なサイズ上限。
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

const LANDING_PAGE: &str = r###"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>litelemetry</title>
    <link rel="icon" href="data:," />
    <style>
      :root {
        --bg-top: #f7efe2;
        --bg-bottom: #d9e7e5;
        --panel: rgba(255, 252, 246, 0.84);
        --panel-strong: rgba(255, 249, 239, 0.96);
        --ink: #172033;
        --muted: #556070;
        --line: rgba(23, 32, 51, 0.12);
        --accent: #bb5a38;
        --accent-strong: #8e3f23;
        --accent-soft: #f6d7ca;
        --teal: #0e6b62;
        --teal-soft: #d3ece8;
        --shadow: 0 28px 70px rgba(24, 40, 55, 0.14);
      }

      * {
        box-sizing: border-box;
      }

      html {
        color-scheme: light;
      }

      body {
        margin: 0;
        min-height: 100vh;
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        color: var(--ink);
        background:
          radial-gradient(circle at top left, rgba(187, 90, 56, 0.18), transparent 28%),
          radial-gradient(circle at top right, rgba(14, 107, 98, 0.14), transparent 26%),
          linear-gradient(180deg, var(--bg-top), var(--bg-bottom));
      }

      body::before {
        content: "";
        position: fixed;
        inset: 0;
        pointer-events: none;
        background-image:
          linear-gradient(rgba(23, 32, 51, 0.03) 1px, transparent 1px),
          linear-gradient(90deg, rgba(23, 32, 51, 0.03) 1px, transparent 1px);
        background-size: 24px 24px;
        mask-image: radial-gradient(circle at center, black 35%, transparent 88%);
      }

      main {
        width: min(1100px, calc(100% - 32px));
        margin: 0 auto;
        padding: 40px 0 56px;
      }

      .hero,
      .card-grid,
      .details {
        display: grid;
        gap: 20px;
      }

      .hero {
        grid-template-columns: minmax(0, 1.4fr) minmax(320px, 0.9fr);
        align-items: stretch;
      }

      .panel {
        border: 1px solid var(--line);
        border-radius: 28px;
        background: var(--panel);
        box-shadow: var(--shadow);
        backdrop-filter: blur(16px);
      }

      .hero-copy,
      .hero-side,
      .card,
      .detail {
        padding: 28px;
      }

      .eyebrow,
      .label {
        margin: 0;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.18em;
        text-transform: uppercase;
      }

      .eyebrow {
        color: var(--accent-strong);
      }

      h1,
      h2,
      h3 {
        margin: 0;
        font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
        font-weight: 700;
        line-height: 1.05;
      }

      h1 {
        margin-top: 14px;
        font-size: clamp(2.4rem, 4vw, 4.8rem);
        max-width: 10ch;
      }

      h2 {
        font-size: clamp(1.6rem, 2.2vw, 2.2rem);
      }

      h3 {
        font-size: 1.3rem;
      }

      p {
        margin: 0;
        color: var(--muted);
        line-height: 1.65;
      }

      .hero-copy {
        display: grid;
        gap: 18px;
        background: linear-gradient(145deg, rgba(255, 251, 245, 0.96), rgba(255, 244, 232, 0.72));
      }

      .pill-row,
      .endpoint-list,
      .bullet-list,
      .actions {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
      }

      .pill {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 9px 14px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.72);
        border: 1px solid rgba(23, 32, 51, 0.08);
        color: var(--ink);
        font-size: 0.94rem;
      }

      .pill::before {
        content: "";
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: var(--teal);
        box-shadow: 0 0 0 6px rgba(14, 107, 98, 0.12);
      }

      .hero-side {
        display: grid;
        gap: 18px;
        background: linear-gradient(180deg, rgba(15, 44, 51, 0.92), rgba(14, 30, 41, 0.96));
        color: #f5f0e8;
      }

      .hero-side p,
      .hero-side code {
        color: rgba(245, 240, 232, 0.84);
      }

      code {
        font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
        font-size: 0.92rem;
      }

      .code-block {
        padding: 16px;
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.08);
        overflow-x: auto;
      }

      .card-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
        margin-top: 22px;
      }

      .card {
        display: grid;
        gap: 16px;
        background: var(--panel-strong);
      }

      .label {
        color: var(--teal);
      }

      .endpoint {
        color: var(--ink);
      }

      .actions {
        margin-top: 4px;
      }

      button {
        appearance: none;
        border: 0;
        border-radius: 999px;
        padding: 12px 18px;
        font: inherit;
        font-weight: 700;
        letter-spacing: 0.01em;
        cursor: pointer;
        transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
      }

      button:hover,
      button:focus-visible {
        transform: translateY(-1px);
        box-shadow: 0 14px 30px rgba(142, 63, 35, 0.18);
      }

      button:focus-visible {
        outline: 2px solid rgba(14, 107, 98, 0.45);
        outline-offset: 3px;
      }

      .primary {
        color: #fff7f2;
        background: linear-gradient(135deg, var(--accent), var(--accent-strong));
      }

      .secondary {
        color: var(--teal);
        background: var(--teal-soft);
      }

      .details {
        grid-template-columns: minmax(0, 1.2fr) minmax(0, 0.8fr);
        margin-top: 22px;
      }

      .detail {
        display: grid;
        gap: 16px;
        background: rgba(255, 254, 251, 0.9);
      }

      .status {
        padding: 18px;
        border-radius: 18px;
        background: #fffdf8;
        border: 1px solid var(--line);
        color: var(--ink);
      }

      .status[data-state="idle"] {
        background: #fff5ec;
      }

      .status[data-state="working"] {
        background: #fff7df;
      }

      .status[data-state="ok"] {
        background: #ebfbf5;
      }

      .status[data-state="error"] {
        background: #ffe9e4;
      }

      .endpoint-list,
      .bullet-list {
        padding: 0;
        margin: 0;
        list-style: none;
      }

      .endpoint-list li,
      .bullet-list li {
        padding: 10px 14px;
        border-radius: 16px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.72);
      }

      @media (max-width: 900px) {
        .hero,
        .card-grid,
        .details {
          grid-template-columns: 1fr;
        }
      }

      @media (max-width: 640px) {
        main {
          width: min(100% - 20px, 100%);
          padding: 20px 0 32px;
        }

        .hero-copy,
        .hero-side,
        .card,
        .detail {
          padding: 22px;
        }

        h1 {
          max-width: none;
        }

        button {
          width: 100%;
          justify-content: center;
        }

        .actions {
          display: grid;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <article class="panel hero-copy">
          <p class="eyebrow">OTLP / HTTP ingest</p>
          <h1>litelemetry is ready for local smoke tests.</h1>
          <p>
            The server is up, the ingest endpoints are wired, and this landing page gives you a
            browser-friendly checkpoint while the richer viewer screens are still taking shape.
          </p>
          <div class="pill-row">
            <span class="pill">Axum server online</span>
            <span class="pill">Redis-backed ingest</span>
            <span class="pill">Playwright-checkable</span>
          </div>
        </article>

        <aside class="panel hero-side">
          <p class="eyebrow">Quick start</p>
          <h2>Ship a sample payload in one click or use the OTLP endpoints directly.</h2>
          <div class="code-block">
            <code>curl -X POST http://127.0.0.1:8080/v1/logs -H 'content-type: application/json' -d '{}'</code>
          </div>
          <ul class="endpoint-list">
            <li><code>/v1/traces</code></li>
            <li><code>/v1/metrics</code></li>
            <li><code>/v1/logs</code></li>
            <li><code>/healthz</code></li>
          </ul>
        </aside>
      </section>

      <section class="card-grid" aria-label="Smoke tests">
        <article class="panel card">
          <p class="label">Trace ingest</p>
          <h3>Send a minimal trace sample.</h3>
          <p>Posts JSON to <span class="endpoint"><code>/v1/traces</code></span> and checks for a 200 response.</p>
          <div class="actions">
            <button class="primary" data-signal="traces">Send trace sample</button>
          </div>
        </article>

        <article class="panel card">
          <p class="label">Metric ingest</p>
          <h3>Ping the metrics route.</h3>
          <p>Useful for confirming the app is reachable from a browser session after Compose startup.</p>
          <div class="actions">
            <button class="primary" data-signal="metrics">Send metric sample</button>
          </div>
        </article>

        <article class="panel card">
          <p class="label">Log ingest</p>
          <h3>Write a small log payload.</h3>
          <p>Exercises the same OTLP/HTTP surface that SDK-based clients call during local testing.</p>
          <div class="actions">
            <button class="primary" data-signal="logs">Send log sample</button>
          </div>
        </article>
      </section>

      <section class="details">
        <article class="panel detail">
          <p class="eyebrow">Live result</p>
          <div class="status" data-state="idle" data-output aria-live="polite">
            No smoke test has run yet. Use one of the buttons above to send a sample request.
          </div>
          <div class="actions">
            <button class="secondary" onclick="window.location.href='/healthz'">Open health check</button>
          </div>
        </article>

        <article class="panel detail">
          <p class="eyebrow">What this page covers</p>
          <ul class="bullet-list">
            <li>Confirms the root route responds with HTML instead of a 404.</li>
            <li>Provides a browser-based smoke test for all OTLP ingest endpoints.</li>
            <li>Offers a simple health endpoint for local orchestration checks.</li>
          </ul>
        </article>
      </section>
    </main>

    <script>
      const output = document.querySelector('[data-output]');
      const sampleBody = signal => JSON.stringify({
        signal,
        sentAt: new Date().toISOString(),
        resourceSpans: [],
        resourceMetrics: [],
        resourceLogs: []
      });

      async function sendSample(signal) {
        const path = `/v1/${signal}`;
        output.dataset.state = 'working';
        output.textContent = `Sending sample payload to ${path}...`;

        try {
          const response = await fetch(path, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: sampleBody(signal)
          });

          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          output.dataset.state = 'ok';
          output.textContent = `Sample accepted by ${path} at ${new Date().toLocaleTimeString()}.`;
        } catch (error) {
          output.dataset.state = 'error';
          output.textContent = `Sample failed for ${path}: ${error.message}.`;
        }
      }

      for (const button of document.querySelectorAll('[data-signal]')) {
        button.addEventListener('click', () => sendSample(button.dataset.signal));
      }
    </script>
  </body>
</html>
"###;

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
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/v1/traces", post(ingest_traces))
        .route("/v1/metrics", post(ingest_metrics))
        .route("/v1/logs", post(ingest_logs))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(LANDING_PAGE)
}

async fn healthz() -> &'static str {
    "ok"
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_root_returns_landing_page() {
        let app = Router::new().route("/", get(index));

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("litelemetry"));
        assert!(html.contains("/v1/traces"));
        assert!(html.contains("Send trace sample"));
    }

    #[tokio::test]
    async fn test_healthz_returns_ok() {
        let app = Router::new().route("/healthz", get(healthz));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"ok");
    }
}
