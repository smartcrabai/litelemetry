use crate::domain::telemetry::{Signal, SignalMask};
use crate::domain::viewer::{ViewerDefinition, ViewerStatus};
use crate::ingest::decode::DecodeError;
use crate::ingest::otlp_http::parse_ingest_request;
use crate::storage::postgres::PostgresStore;
use crate::storage::redis::RedisStore;
use crate::viewer_runtime::compiler::CompiledViewer;
use crate::viewer_runtime::runtime::ViewerRuntime;
use crate::viewer_runtime::state::ViewerState;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    response::Html,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fmt::Write as _;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// OTLP ペイロードの最大ボディサイズ (4 MiB)。
/// 単一バッチとして現実的なサイズ上限。
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_VIEWER_LOOKBACK_MS: i64 = 5 * 60 * 1_000;
const DEFAULT_VIEWER_REFRESH_MS: u32 = 1_000;
const MAX_PAYLOAD_PREVIEW_CHARS: usize = 160;
const VIEWER_ENTRY_PREVIEW_LIMIT: usize = 50;

const VIEWER_PAGE: &str = r####"<!doctype html>
<html lang="ja">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>litelemetry viewer</title>
    <link rel="icon" href="data:," />
    <style>
      :root {
        --bg-1: #f5eddc;
        --bg-2: #d8e7e2;
        --panel: rgba(255, 251, 243, 0.84);
        --panel-strong: rgba(255, 249, 241, 0.95);
        --ink: #172033;
        --muted: #5d6778;
        --line: rgba(23, 32, 51, 0.12);
        --accent: #b75432;
        --accent-strong: #87381f;
        --accent-soft: rgba(183, 84, 50, 0.12);
        --teal: #0d6d62;
        --teal-soft: rgba(13, 109, 98, 0.12);
        --danger-soft: rgba(184, 64, 52, 0.14);
        --shadow: 0 28px 70px rgba(22, 35, 47, 0.12);
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
        color: var(--ink);
        font-family: "Avenir Next", "Segoe UI", sans-serif;
        background:
          radial-gradient(circle at top left, rgba(183, 84, 50, 0.18), transparent 28%),
          radial-gradient(circle at top right, rgba(13, 109, 98, 0.16), transparent 24%),
          linear-gradient(180deg, var(--bg-1), var(--bg-2));
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
        mask-image: radial-gradient(circle at center, black 42%, transparent 90%);
      }

      main {
        width: min(1200px, calc(100% - 32px));
        margin: 0 auto;
        padding: 32px 0 44px;
      }

      .stack,
      .hero,
      .workspace,
      .forms,
      .panel,
      .viewer-list,
      .table-wrap,
      .empty {
        display: grid;
        gap: 18px;
      }

      [hidden] {
        display: none !important;
      }

      .hero {
        grid-template-columns: minmax(0, 1.4fr) minmax(320px, 0.9fr);
        margin-bottom: 22px;
      }

      .workspace {
        grid-template-columns: minmax(320px, 360px) minmax(0, 1fr);
      }

      .panel {
        padding: 24px;
        border-radius: 28px;
        border: 1px solid var(--line);
        background: var(--panel);
        box-shadow: var(--shadow);
        backdrop-filter: blur(14px);
      }

      .panel-strong {
        background: var(--panel-strong);
      }

      .hero-copy {
        background: linear-gradient(145deg, rgba(255, 251, 245, 0.96), rgba(255, 243, 230, 0.72));
      }

      .hero-side {
        background: linear-gradient(180deg, rgba(16, 43, 52, 0.93), rgba(14, 30, 42, 0.96));
        color: #f5efe7;
      }

      .eyebrow,
      .label {
        margin: 0;
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.16em;
        text-transform: uppercase;
      }

      .eyebrow {
        color: var(--accent-strong);
      }

      .hero-side .eyebrow,
      .hero-side p,
      .hero-side li,
      .hero-side code {
        color: rgba(245, 239, 231, 0.86);
      }

      h1,
      h2,
      h3 {
        margin: 0;
        line-height: 1.08;
        font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
      }

      h1 {
        font-size: clamp(2.2rem, 4vw, 4.4rem);
        max-width: 11ch;
      }

      h2 {
        font-size: clamp(1.6rem, 2vw, 2.1rem);
      }

      h3 {
        font-size: 1.1rem;
      }

      p {
        margin: 0;
        line-height: 1.6;
        color: var(--muted);
      }

      code,
      pre,
      textarea,
      input,
      select,
      button {
        font: inherit;
      }

      .pill-row,
      .meta-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
      }

      .pill,
      .chip {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        border-radius: 999px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.72);
      }

      .pill::before {
        content: "";
        width: 10px;
        height: 10px;
        border-radius: 999px;
        background: var(--teal);
        box-shadow: 0 0 0 6px rgba(13, 109, 98, 0.12);
      }

      .hint-list {
        padding-left: 18px;
        margin: 0;
        display: grid;
        gap: 10px;
      }

      .forms {
        align-content: start;
      }

      .field {
        display: grid;
        gap: 8px;
      }

      .field label {
        font-size: 0.92rem;
        font-weight: 700;
      }

      .field small {
        color: var(--muted);
      }

      input,
      select {
        width: 100%;
        min-height: 48px;
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 12px 14px;
        color: var(--ink);
        background: rgba(255, 255, 255, 0.82);
      }

      input:focus-visible,
      select:focus-visible,
      button:focus-visible {
        outline: 2px solid rgba(13, 109, 98, 0.4);
        outline-offset: 3px;
      }

      .action-row {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
      }

      button {
        appearance: none;
        border: 0;
        border-radius: 999px;
        min-height: 46px;
        padding: 0 18px;
        font-weight: 700;
        cursor: pointer;
        transition: transform 120ms ease, box-shadow 120ms ease, opacity 120ms ease;
      }

      button:hover,
      button:focus-visible {
        transform: translateY(-1px);
      }

      button:disabled {
        cursor: not-allowed;
        opacity: 0.55;
        transform: none;
      }

      .primary {
        color: #fff7f2;
        background: linear-gradient(135deg, var(--accent), var(--accent-strong));
        box-shadow: 0 14px 28px rgba(135, 56, 31, 0.18);
      }

      .secondary {
        color: var(--teal);
        background: rgba(255, 255, 255, 0.9);
        border: 1px solid rgba(13, 109, 98, 0.16);
      }

      .status-box {
        padding: 16px 18px;
        border-radius: 18px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.78);
      }

      .status-box[data-state="working"] {
        background: #fff7df;
      }

      .status-box[data-state="ok"] {
        background: #ebfbf5;
      }

      .status-box[data-state="error"] {
        background: #ffe8e2;
      }

      .viewer-list {
        align-content: start;
      }

      .viewer-item {
        width: 100%;
        display: grid;
        gap: 12px;
        padding: 16px;
        border-radius: 20px;
        border: 1px solid var(--line);
        color: inherit;
        background: rgba(255, 255, 255, 0.72);
        text-align: left;
      }

      .viewer-item[data-active="true"] {
        border-color: rgba(13, 109, 98, 0.28);
        background: linear-gradient(135deg, rgba(13, 109, 98, 0.12), rgba(255, 255, 255, 0.84));
      }

      .viewer-item .title-row {
        display: flex;
        justify-content: space-between;
        gap: 12px;
        align-items: start;
      }

      .viewer-item .count {
        min-width: 56px;
        text-align: center;
        border-radius: 999px;
        padding: 6px 10px;
        background: var(--teal-soft);
        color: var(--teal);
      }

      .viewer-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
      }

      .viewer-meta span {
        padding: 6px 10px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.78);
        border: 1px solid var(--line);
        font-size: 0.88rem;
      }

      .table-wrap {
        overflow: hidden;
      }

      .table-scroll {
        overflow: auto;
        border-radius: 22px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.76);
      }

      table {
        width: 100%;
        border-collapse: collapse;
      }

      th,
      td {
        padding: 14px 16px;
        vertical-align: top;
        border-bottom: 1px solid var(--line);
      }

      th {
        position: sticky;
        top: 0;
        background: rgba(255, 250, 243, 0.96);
        text-align: left;
        font-size: 0.82rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      td {
        font-size: 0.95rem;
      }

      td code {
        display: inline-block;
        max-width: 42ch;
        white-space: pre-wrap;
        word-break: break-word;
        color: var(--ink);
      }

      tbody tr:last-child td {
        border-bottom: 0;
      }

      .empty {
        place-items: center;
        padding: 36px 18px;
        border-radius: 22px;
        border: 1px dashed rgba(23, 32, 51, 0.18);
        background: rgba(255, 255, 255, 0.55);
        text-align: center;
      }

      .error-inline {
        color: #9e2f25;
      }

      @media (max-width: 940px) {
        .hero,
        .workspace {
          grid-template-columns: 1fr;
        }
      }

      @media (max-width: 640px) {
        main {
          width: min(100% - 20px, 100%);
          padding: 20px 0 30px;
        }

        .panel {
          padding: 20px;
          border-radius: 24px;
        }

        .action-row {
          display: grid;
        }

        button {
          width: 100%;
        }
      }
    </style>
  </head>
  <body>
    <main>
      <section class="hero">
        <article class="panel panel-strong hero-copy stack">
          <p class="eyebrow">Viewer workspace</p>
          <h1>UI から viewer を作って telemetry を流し込む。</h1>
          <p>
            Compose 起動後、この画面だけで traces / metrics / logs の viewer 作成、サンプル送信、反映確認まで進められます。
            反映結果は右側のテーブルでそのまま確認できます。
          </p>
          <div class="pill-row">
            <span class="pill">Create viewer</span>
            <span class="pill">Send OTLP sample</span>
            <span class="pill">Table verification</span>
          </div>
        </article>

        <aside class="panel hero-side stack">
          <p class="eyebrow">Routes</p>
          <h2>Compose の smoke test を UI から完結させる。</h2>
          <ul class="hint-list">
            <li><code>POST /api/viewers</code> で signal ごとの viewer を作成</li>
            <li><code>POST /v1/{signal}</code> へ OTLP JSON を送信</li>
            <li><code>GET /api/viewers</code> で最新 state を取得</li>
            <li><code>GET /api/viewers/{id}</code> で単一 viewer の詳細を取得</li>
            <li><code>GET /healthz</code> で死活確認</li>
          </ul>
        </aside>
      </section>

      <section class="workspace">
        <aside class="forms">
          <article class="panel panel-strong stack">
            <p class="eyebrow">1. Create Viewer</p>
            <div class="field">
              <label for="viewer-signal-select">Signal</label>
              <select id="viewer-signal-select" data-testid="viewer-signal-select" name="viewer-signal">
                <option value="traces">traces</option>
                <option value="metrics">metrics</option>
                <option value="logs">logs</option>
              </select>
            </div>
            <div class="field">
              <label for="viewer-name-input">Viewer name</label>
              <input id="viewer-name-input" data-testid="viewer-name-input" name="viewer-name" placeholder="checkout traces" maxlength="80" />
              <small id="viewer-name-hint">選んだ signal の viewer を 5 分 lookback で作成します。</small>
            </div>
            <div class="action-row">
              <button id="create-viewer-button" data-testid="create-viewer-button" class="primary" type="button">
                Create viewer
              </button>
            </div>
          </article>

          <article class="panel stack">
            <p class="eyebrow">2. Send Sample</p>
            <div class="field">
              <label for="sample-service-input">Service name</label>
              <input id="sample-service-input" data-testid="sample-service-input" name="sample-service" value="checkout-ui" maxlength="80" />
            </div>
            <div class="field">
              <label id="sample-detail-label" for="sample-detail-input">Span name</label>
              <input id="sample-detail-input" data-testid="sample-detail-input" name="sample-detail" value="render-checkout" maxlength="120" />
              <small id="sample-signal-hint">現在選択中の viewer と同じ signal に送信します。</small>
            </div>
            <div class="action-row">
              <button id="send-sample-button" data-testid="send-sample-button" class="secondary" type="button" disabled>
                Send trace sample
              </button>
              <button id="refresh-viewers-button" class="secondary" type="button">
                Refresh table
              </button>
            </div>
          </article>

          <article class="panel stack">
            <p class="eyebrow">Status</p>
            <div id="status-box" data-testid="status-box" class="status-box" data-state="working">
              viewer 一覧を読み込んでいます。
            </div>
          </article>

          <article class="panel viewer-list">
            <div class="title-row">
              <div>
                <p class="eyebrow">Viewer List</p>
                <h3>Active viewers</h3>
              </div>
            </div>
            <div id="viewer-list" data-testid="viewer-list" class="viewer-list"></div>
          </article>
        </aside>

        <section class="panel panel-strong stack table-wrap">
          <div class="stack">
            <p id="active-viewer-eyebrow" class="eyebrow">3. Viewer</p>
            <h2 id="active-viewer-title" data-testid="active-viewer-title">Viewer を読み込み中</h2>
            <p id="active-viewer-subtitle">利用可能な viewer を取得しています。</p>
          </div>

          <div id="viewer-empty" data-testid="viewer-empty" class="empty">
            <div class="stack">
              <strong>viewer 読み込み中</strong>
              <p>viewer 一覧の取得が完了すると、ここに最新 telemetry が表示されます。</p>
            </div>
          </div>

          <div id="viewer-table-wrap" class="table-scroll" hidden>
            <table data-testid="viewer-table">
              <thead>
                <tr>
                  <th>Observed At</th>
                  <th>Signal</th>
                  <th>Service</th>
                  <th>Payload Preview</th>
                  <th>Bytes</th>
                </tr>
              </thead>
              <tbody id="viewer-table-body"></tbody>
            </table>
          </div>
        </section>
      </section>
    </main>

    <script>
      const statusBox = document.getElementById('status-box');
      const viewerList = document.getElementById('viewer-list');
      const viewerSignalSelect = document.getElementById('viewer-signal-select');
      const viewerNameInput = document.getElementById('viewer-name-input');
      const viewerNameHint = document.getElementById('viewer-name-hint');
      const sampleServiceInput = document.getElementById('sample-service-input');
      const sampleDetailLabel = document.getElementById('sample-detail-label');
      const sampleDetailInput = document.getElementById('sample-detail-input');
      const sampleSignalHint = document.getElementById('sample-signal-hint');
      const createViewerButton = document.getElementById('create-viewer-button');
      const sendSampleButton = document.getElementById('send-sample-button');
      const refreshViewersButton = document.getElementById('refresh-viewers-button');
      const activeViewerEyebrow = document.getElementById('active-viewer-eyebrow');
      const activeViewerTitle = document.getElementById('active-viewer-title');
      const activeViewerSubtitle = document.getElementById('active-viewer-subtitle');
      const viewerEmpty = document.getElementById('viewer-empty');
      const viewerEmptyTitle = viewerEmpty.querySelector('strong');
      const viewerEmptyBody = viewerEmpty.querySelector('p');
      const viewerTableWrap = document.getElementById('viewer-table-wrap');
      const viewerTableBody = document.getElementById('viewer-table-body');

      const SAMPLE_CONFIG = {
        traces: {
          viewerPlaceholder: 'checkout traces',
          detailLabel: 'Span name',
          defaultService: 'checkout-ui',
          defaultDetail: 'render-checkout',
          buttonLabel: 'Send trace sample'
        },
        metrics: {
          viewerPlaceholder: 'orders metrics',
          detailLabel: 'Metric name',
          defaultService: 'orders-api',
          defaultDetail: 'http.server.requests',
          buttonLabel: 'Send metrics sample'
        },
        logs: {
          viewerPlaceholder: 'billing logs',
          detailLabel: 'Log message',
          defaultService: 'worker-billing',
          defaultDetail: 'payment authorized',
          buttonLabel: 'Send logs sample'
        }
      };

      let activeViewerId = null;
      let latestViewers = [];
      let viewerLoadState = 'loading';
      let sampleSignal = null;

      function setStatus(kind, message) {
        statusBox.dataset.state = kind;
        statusBox.textContent = message;
      }

      function truncateId(id) {
        return id.length <= 12 ? id : `${id.slice(0, 8)}...${id.slice(-4)}`;
      }

      function makeTextElement(tagName, text, className) {
        const element = document.createElement(tagName);
        if (className) {
          element.className = className;
        }
        element.textContent = text;
        return element;
      }

      function makeViewerMetaPill(text) {
        return makeTextElement('span', text);
      }

      function formatLookbackMs(lookbackMs) {
        const seconds = Math.round(lookbackMs / 1000);
        if (seconds % 86400 === 0) {
          const days = seconds / 86400;
          return `${days}d`;
        }
        if (seconds % 3600 === 0) {
          const hours = seconds / 3600;
          return `${hours}h`;
        }
        if (seconds % 60 === 0) {
          const minutes = seconds / 60;
          return `${minutes}m`;
        }
        return `${seconds}s`;
      }

      function appendTableCell(row, text) {
        row.appendChild(makeTextElement('td', text));
      }

      function signalConfig(signal) {
        return SAMPLE_CONFIG[signal];
      }

      function titleCaseSignal(signal) {
        return signal.charAt(0).toUpperCase() + signal.slice(1);
      }

      function currentActiveViewer() {
        return latestViewers.find(viewer => viewer.id === activeViewerId) || null;
      }

      function currentViewerSignal(activeViewer) {
        return activeViewer && activeViewer.signals.length ? activeViewer.signals[0] : viewerSignalSelect.value;
      }

      function syncCreateForm() {
        const signal = viewerSignalSelect.value;
        const config = signalConfig(signal);
        viewerNameInput.placeholder = config.viewerPlaceholder;
        viewerNameHint.textContent = `選んだ ${signal} signal の viewer を 5 分 lookback で作成します。`;
      }

      function syncSampleForm(signal, options = {}) {
        const config = signalConfig(signal);
        const forceDefaults = options.forceDefaults ?? false;
        const signalChanged = sampleSignal !== signal;

        sampleSignal = signal;
        sampleDetailLabel.textContent = config.detailLabel;
        sampleServiceInput.placeholder = config.defaultService;
        sampleDetailInput.placeholder = config.defaultDetail;
        sampleSignalHint.textContent = latestViewers.length
          ? `現在選択中の viewer と同じ ${signal} signal に OTLP JSON を送信します。`
          : `viewer を作成すると ${signal} sample を送信できます。`;
        sendSampleButton.textContent = config.buttonLabel;

        if (forceDefaults || signalChanged) {
          sampleServiceInput.value = config.defaultService;
          sampleDetailInput.value = config.defaultDetail;
        }
      }

      function normalizeActiveViewer() {
        if (!latestViewers.length) {
          activeViewerId = null;
          return null;
        }

        const existing = latestViewers.find(viewer => viewer.id === activeViewerId);
        if (existing) {
          return existing;
        }

        activeViewerId = latestViewers[0].id;
        return latestViewers[0];
      }

      function renderViewerList() {
        viewerList.replaceChildren();

        if (viewerLoadState === 'loading') {
          viewerList.appendChild(makeTextElement('p', 'viewer 一覧を読み込んでいます。'));
          sendSampleButton.disabled = true;
          return;
        }

        if (viewerLoadState === 'error') {
          viewerList.appendChild(makeTextElement('p', 'viewer 一覧の取得に失敗しました。', 'error-inline'));
          sendSampleButton.disabled = true;
          return;
        }

        if (!latestViewers.length) {
          viewerList.appendChild(makeTextElement('p', 'viewer はまだありません。', 'error-inline'));
          sendSampleButton.disabled = true;
          return;
        }

        sendSampleButton.disabled = false;

        for (const viewer of latestViewers) {
          const button = document.createElement('button');
          button.type = 'button';
          button.className = 'viewer-item';
          button.dataset.viewerId = viewer.id;
          button.dataset.active = String(viewer.id === activeViewerId);

          const titleRow = document.createElement('div');
          titleRow.className = 'title-row';

          const titleStack = document.createElement('div');
          titleStack.className = 'stack';
          titleStack.appendChild(makeTextElement('strong', viewer.name));
          titleStack.appendChild(makeTextElement('small', truncateId(viewer.id)));

          const count = makeTextElement('span', String(viewer.entry_count), 'count');
          titleRow.append(titleStack, count);

          const meta = document.createElement('div');
          meta.className = 'viewer-meta';
          for (const signal of viewer.signals) {
            meta.appendChild(makeViewerMetaPill(signal));
          }
          meta.appendChild(makeViewerMetaPill(`lookback ${formatLookbackMs(viewer.lookback_ms)}`));

          button.append(titleRow, meta);
          button.addEventListener('click', () => {
            activeViewerId = viewer.id;
            render();
          });
          viewerList.appendChild(button);
        }
      }

      function renderTable(activeViewer) {
        if (viewerLoadState === 'loading') {
          activeViewerEyebrow.textContent = '3. Viewer';
          activeViewerTitle.textContent = 'Viewer を読み込み中';
          activeViewerSubtitle.textContent = '利用可能な viewer を取得しています。';
          viewerEmptyTitle.textContent = 'viewer 読み込み中';
          viewerEmptyBody.textContent = 'viewer 一覧の取得が完了すると、ここに最新 telemetry が表示されます。';
          viewerEmpty.hidden = false;
          viewerTableWrap.hidden = true;
          viewerTableBody.replaceChildren();
          return;
        }

        if (viewerLoadState === 'error') {
          activeViewerEyebrow.textContent = '3. Viewer';
          activeViewerTitle.textContent = 'Viewer の取得に失敗しました';
          activeViewerSubtitle.textContent = '接続が回復すると自動で再取得します。';
          viewerEmptyTitle.textContent = 'viewer 取得失敗';
          viewerEmptyBody.textContent = '少し待つか、Refresh table を押して再取得してください。';
          viewerEmpty.hidden = false;
          viewerTableWrap.hidden = true;
          viewerTableBody.replaceChildren();
          return;
        }

        if (!activeViewer) {
          const signal = viewerSignalSelect.value;
          activeViewerEyebrow.textContent = '3. Viewer';
          activeViewerTitle.textContent = 'Viewer がまだありません';
          activeViewerSubtitle.textContent = '左側で viewer を作成するか、seed 済み viewer を選ぶとここに最新 telemetry が表示されます。';
          viewerEmptyTitle.textContent = 'viewer 未作成';
          viewerEmptyBody.textContent = `まず ${signal} viewer を作成し、その後で sample を送信してください。`;
          viewerEmpty.hidden = false;
          viewerTableWrap.hidden = true;
          viewerTableBody.replaceChildren();
          return;
        }

        const signal = currentViewerSignal(activeViewer);
        activeViewerEyebrow.textContent = activeViewer.entries.length ? '3. Table' : '3. Viewer';
        activeViewerTitle.textContent = activeViewer.name;
        activeViewerSubtitle.textContent = `${activeViewer.entry_count} entries captured for ${signal}. Latest ${Math.min(activeViewer.entries.length, activeViewer.entry_count)} rows are shown below.`;

        if (!activeViewer.entries.length) {
          viewerEmpty.hidden = false;
          viewerTableWrap.hidden = true;
          viewerTableBody.replaceChildren();
          viewerEmptyTitle.textContent = `${signal} 未反映`;
          viewerEmptyBody.textContent = `Send sample を押すと、この ${signal} viewer に entries が追加されます。`;
          return;
        }

        viewerEmpty.hidden = true;
        viewerTableWrap.hidden = false;
        viewerTableBody.replaceChildren();

        for (const entry of activeViewer.entries) {
          const row = document.createElement('tr');
          appendTableCell(row, new Date(entry.observed_at).toLocaleString());
          appendTableCell(row, entry.signal);
          appendTableCell(row, entry.service_name || '-');

          const previewCell = document.createElement('td');
          previewCell.appendChild(makeTextElement('code', entry.payload_preview));
          row.appendChild(previewCell);

          appendTableCell(row, String(entry.payload_size_bytes));
          viewerTableBody.appendChild(row);
        }
      }

      function render() {
        const activeViewer = normalizeActiveViewer();
        renderViewerList();
        renderTable(activeViewer);
        syncSampleForm(currentViewerSignal(activeViewer));
      }

      async function refreshViewers(options = {}) {
        const silent = options.silent ?? false;
        const previousLoadState = viewerLoadState;
        const previousViewers = latestViewers;

        try {
          const response = await fetch('/api/viewers', {
            headers: { 'accept': 'application/json' }
          });

          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          const payload = await response.json();
          latestViewers = payload.viewers;
          viewerLoadState = 'ready';
          render();

          if (!silent) {
            setStatus('ok', `Viewer list refreshed at ${new Date().toLocaleTimeString()}.`);
          } else if (previousLoadState !== 'ready') {
            if (latestViewers.length) {
              setStatus('ok', `${latestViewers.length} viewer loaded.`);
            } else {
              setStatus('idle', 'Viewer はまだありません。左側から作成できます。');
            }
          }
        } catch (error) {
          if (previousLoadState === 'ready') {
            latestViewers = previousViewers;
            viewerLoadState = 'ready';
          } else {
            latestViewers = [];
            viewerLoadState = 'error';
          }

          render();

          if (previousLoadState === 'ready') {
            setStatus('error', `Viewer list refresh failed: ${error.message}. Showing the latest successful snapshot.`);
          } else {
            setStatus('error', `Viewer list refresh failed: ${error.message}`);
          }
        }
      }

      async function createViewer() {
        const name = viewerNameInput.value.trim();
        const signal = viewerSignalSelect.value;
        if (!name) {
          setStatus('error', 'Viewer name is required.');
          viewerNameInput.focus();
          return;
        }

        createViewerButton.disabled = true;
        setStatus('working', `Creating ${signal} viewer "${name}"...`);

        try {
          const response = await fetch('/api/viewers', {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              'accept': 'application/json'
            },
            body: JSON.stringify({ name, signal })
          });

          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          const payload = await response.json();
          activeViewerId = payload.id;
          await refreshViewers({ silent: true });
          setStatus('ok', `${titleCaseSignal(signal)} viewer "${name}" created.`);
        } catch (error) {
          setStatus('error', `Viewer creation failed: ${error.message}`);
        } finally {
          createViewerButton.disabled = false;
        }
      }

      function buildResourceAttributes(activeViewer, serviceName) {
        return [
          { key: 'service.name', value: { stringValue: serviceName } },
          { key: 'viewer.id', value: { stringValue: activeViewerId || '' } },
          { key: 'viewer.name', value: { stringValue: activeViewer ? activeViewer.name : '' } }
        ];
      }

      function buildTraceBody(activeViewer, serviceName, spanName) {
        const now = Date.now();
        const traceIdSuffix = `${now}`.padStart(32, '0').slice(-32);
        const spanIdSuffix = `${now}`.padStart(16, '0').slice(-16);

        return {
          resourceSpans: [
            {
              resource: {
                attributes: buildResourceAttributes(activeViewer, serviceName)
              },
              scopeSpans: [
                {
                  scope: { name: 'litelemetry.ui' },
                  spans: [
                    {
                      traceId: traceIdSuffix,
                      spanId: spanIdSuffix,
                      name: spanName,
                      kind: 1,
                      startTimeUnixNano: `${now}000000`,
                      endTimeUnixNano: `${now + 1}000000`
                    }
                  ]
                }
              ]
            }
          ]
        };
      }

      function buildMetricBody(activeViewer, serviceName, metricName) {
        const now = Date.now();
        const metricValue = String((now % 97) + 1);

        return {
          resourceMetrics: [
            {
              resource: {
                attributes: buildResourceAttributes(activeViewer, serviceName)
              },
              scopeMetrics: [
                {
                  scope: { name: 'litelemetry.ui' },
                  metrics: [
                    {
                      name: metricName,
                      description: 'litelemetry workspace sample metric',
                      unit: '1',
                      sum: {
                        aggregationTemporality: 2,
                        isMonotonic: true,
                        dataPoints: [
                          {
                            attributes: [
                              { key: 'viewer.id', value: { stringValue: activeViewerId || '' } }
                            ],
                            asInt: metricValue,
                            timeUnixNano: `${now}000000`
                          }
                        ]
                      }
                    }
                  ]
                }
              ]
            }
          ]
        };
      }

      function buildLogBody(activeViewer, serviceName, message) {
        const now = Date.now();

        return {
          resourceLogs: [
            {
              resource: {
                attributes: buildResourceAttributes(activeViewer, serviceName)
              },
              scopeLogs: [
                {
                  scope: { name: 'litelemetry.ui' },
                  logRecords: [
                    {
                      timeUnixNano: `${now}000000`,
                      observedTimeUnixNano: `${now}000000`,
                      severityNumber: 9,
                      severityText: 'INFO',
                      body: { stringValue: message },
                      attributes: [
                        { key: 'viewer.id', value: { stringValue: activeViewerId || '' } }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        };
      }

      function buildSampleBody(signal, activeViewer, serviceName, detailValue) {
        if (signal === 'metrics') {
          return buildMetricBody(activeViewer, serviceName, detailValue);
        }
        if (signal === 'logs') {
          return buildLogBody(activeViewer, serviceName, detailValue);
        }
        return buildTraceBody(activeViewer, serviceName, detailValue);
      }

      async function waitForEntries(previousCount) {
        for (let attempt = 0; attempt < 10; attempt += 1) {
          await new Promise(resolve => window.setTimeout(resolve, 300));
          await refreshViewers({ silent: true });
          const activeViewer = currentActiveViewer();
          if (activeViewer && activeViewer.entry_count > previousCount) {
            return true;
          }
        }
        return false;
      }

      async function sendSample() {
        if (!latestViewers.length) {
          setStatus('error', 'Create or select a viewer before sending a sample.');
          return;
        }

        const activeViewer = currentActiveViewer() || latestViewers[0];
        const signal = currentViewerSignal(activeViewer);
        const config = signalConfig(signal);
        const serviceName = sampleServiceInput.value.trim() || config.defaultService;
        const detailValue = sampleDetailInput.value.trim() || config.defaultDetail;
        const beforeCount = activeViewer ? activeViewer.entry_count : 0;

        sendSampleButton.disabled = true;
        setStatus('working', `Sending ${signal} sample for service "${serviceName}"...`);

        try {
          const response = await fetch(`/v1/${signal}`, {
            method: 'POST',
            headers: {
              'content-type': 'application/json'
            },
            body: JSON.stringify(buildSampleBody(signal, activeViewer, serviceName, detailValue))
          });

          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          const reflected = await waitForEntries(beforeCount);
          if (reflected) {
            setStatus('ok', `${titleCaseSignal(signal)} sample reflected in "${activeViewer.name}".`);
          } else {
            setStatus('error', `${titleCaseSignal(signal)} sample accepted, but the viewer table did not update in time.`);
          }
        } catch (error) {
          setStatus('error', `${titleCaseSignal(signal)} sample send failed: ${error.message}`);
        } finally {
          sendSampleButton.disabled = !latestViewers.length;
        }
      }

      createViewerButton.addEventListener('click', createViewer);
      sendSampleButton.addEventListener('click', sendSample);
      refreshViewersButton.addEventListener('click', () => refreshViewers());
      viewerSignalSelect.addEventListener('change', () => {
        syncCreateForm();
        if (!latestViewers.length) {
          syncSampleForm(viewerSignalSelect.value, { forceDefaults: true });
        }
      });
      viewerNameInput.addEventListener('keydown', event => {
        if (event.key === 'Enter') {
          event.preventDefault();
          createViewer();
        }
      });
      sampleDetailInput.addEventListener('keydown', event => {
        if (event.key === 'Enter' && !sendSampleButton.disabled) {
          event.preventDefault();
          sendSample();
        }
      });

      syncCreateForm();
      syncSampleForm(viewerSignalSelect.value, { forceDefaults: true });
      refreshViewers({ silent: true });
      window.setInterval(() => refreshViewers({ silent: true }), 1500);
    </script>
  </body>
</html>
"####;

/// Axum 共有 state
///
/// RedisStore は MultiplexedConnection をラップしており Clone で並行使用できるため、
/// Arc<Mutex<>> でラップする必要はない。Axum は各リクエストで State を clone する。
#[derive(Clone)]
pub struct AppState {
    pub redis: RedisStore,
    pub postgres: Option<PostgresStore>,
    pub viewer_runtime: Option<SharedViewerRuntime>,
}

pub type SharedViewerRuntime = Arc<Mutex<ViewerRuntime>>;

impl AppState {
    fn require_viewer_runtime(&self) -> Result<&SharedViewerRuntime, StatusCode> {
        self.viewer_runtime
            .as_ref()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }
}

#[derive(Debug, Deserialize)]
struct CreateViewerRequest {
    name: String,
    signal: String,
}

#[derive(Debug, Serialize)]
struct CreateViewerResponse {
    id: Uuid,
}

#[derive(Debug, Serialize)]
struct ViewerListResponse {
    viewers: Vec<ViewerSummary>,
}

#[derive(Debug, Serialize)]
struct ViewerSummary {
    id: Uuid,
    slug: String,
    name: String,
    signals: Vec<&'static str>,
    refresh_interval_ms: u32,
    lookback_ms: i64,
    entry_count: usize,
    status: ViewerStatus,
    entries: Vec<ViewerEntryRow>,
}

#[derive(Debug, Serialize)]
struct ViewerEntryRow {
    observed_at: DateTime<Utc>,
    signal: &'static str,
    service_name: Option<String>,
    payload_size_bytes: usize,
    payload_preview: String,
}

/// Axum app を構築して返す
pub fn build_app(redis: RedisStore) -> Router {
    build_app_with_services(redis, None, None)
}

pub fn build_app_with_services(
    redis: RedisStore,
    postgres: Option<PostgresStore>,
    viewer_runtime: Option<SharedViewerRuntime>,
) -> Router {
    let state = AppState {
        redis,
        postgres,
        viewer_runtime,
    };

    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/api/viewers", get(list_viewers).post(create_viewer))
        .route("/api/viewers/{id}", get(get_viewer))
        .route("/v1/traces", post(ingest_traces))
        .route("/v1/metrics", post(ingest_metrics))
        .route("/v1/logs", post(ingest_logs))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(VIEWER_PAGE)
}

async fn get_viewer(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<ViewerSummary>, StatusCode> {
    let runtime = state.require_viewer_runtime()?.lock().await;

    runtime
        .viewers()
        .iter()
        .find(|(viewer, _)| viewer.definition().id == id)
        .map(|(viewer, viewer_state)| Json(viewer_summary(viewer, viewer_state)))
        .ok_or(StatusCode::NOT_FOUND)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn list_viewers(
    State(state): State<AppState>,
) -> Result<Json<ViewerListResponse>, StatusCode> {
    let runtime = state.require_viewer_runtime()?.lock().await;

    let viewers = runtime
        .viewers()
        .iter()
        .map(|(viewer, viewer_state)| viewer_summary(viewer, viewer_state))
        .collect();

    Ok(Json(ViewerListResponse { viewers }))
}

async fn create_viewer(
    State(state): State<AppState>,
    Json(payload): Json<CreateViewerRequest>,
) -> Result<(StatusCode, Json<CreateViewerResponse>), StatusCode> {
    let postgres = state
        .postgres
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let runtime = state.require_viewer_runtime()?;

    let name = payload.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let signal = parse_signal_name(payload.signal.trim()).ok_or(StatusCode::BAD_REQUEST)?;

    let id = Uuid::new_v4();
    let definition = ViewerDefinition {
        id,
        slug: format!("viewer-{}", id.simple()),
        name: name.to_string(),
        refresh_interval_ms: DEFAULT_VIEWER_REFRESH_MS,
        lookback_ms: DEFAULT_VIEWER_LOOKBACK_MS,
        signal_mask: signal.into(),
        definition_json: json!({
            "kind": "table",
            "signal": signal_name(signal)
        }),
        layout_json: json!({
            "default_view": "table"
        }),
        revision: 1,
        enabled: true,
    };

    postgres
        .insert_viewer_definition(&definition)
        .await
        .map_err(|error| {
            tracing::error!("insert_viewer_definition failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    runtime
        .lock()
        .await
        .add_viewer(definition)
        .await
        .map_err(|error| {
            tracing::error!("viewer runtime add_viewer failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok((StatusCode::CREATED, Json(CreateViewerResponse { id })))
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
                Err(error) => {
                    tracing::error!("redis append_entry failed: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        Err(DecodeError::UnsupportedContentType(_)) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
    }
}

fn viewer_summary(viewer: &CompiledViewer, viewer_state: &ViewerState) -> ViewerSummary {
    let definition = viewer.definition();
    let entries = viewer_state
        .entries
        .iter()
        .rev()
        .take(VIEWER_ENTRY_PREVIEW_LIMIT)
        .map(|entry| ViewerEntryRow {
            observed_at: entry.observed_at,
            signal: signal_name(entry.signal),
            service_name: entry.service_name.clone(),
            payload_size_bytes: entry.payload.len(),
            payload_preview: payload_preview(entry.signal, &entry.payload),
        })
        .collect();

    ViewerSummary {
        id: definition.id,
        slug: definition.slug.clone(),
        name: definition.name.clone(),
        signals: signal_mask_labels(definition.signal_mask),
        refresh_interval_ms: definition.refresh_interval_ms,
        lookback_ms: definition.lookback_ms,
        entry_count: viewer_state.entries.len(),
        status: viewer_state.status.clone(),
        entries,
    }
}

fn signal_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Traces => "traces",
        Signal::Metrics => "metrics",
        Signal::Logs => "logs",
    }
}

fn parse_signal_name(value: &str) -> Option<Signal> {
    match value {
        "traces" => Some(Signal::Traces),
        "metrics" => Some(Signal::Metrics),
        "logs" => Some(Signal::Logs),
        _ => None,
    }
}

fn signal_mask_labels(mask: SignalMask) -> Vec<&'static str> {
    Signal::all()
        .into_iter()
        .filter(|signal| mask.contains(*signal))
        .map(signal_name)
        .collect()
}

fn payload_preview(signal: Signal, payload: &Bytes) -> String {
    if let Some(summary) = structured_payload_preview(signal, payload) {
        return summary;
    }

    match std::str::from_utf8(payload) {
        Ok(text) => {
            let compact = compact_whitespace(text);
            if compact.is_empty() {
                return "(empty payload)".to_string();
            }

            truncate_preview(&compact)
        }
        Err(_) => {
            let mut preview = String::new();
            for byte in payload.iter().take(24) {
                let _ = write!(preview, "{byte:02x}");
            }
            if payload.len() > 24 {
                preview.push_str("...");
            }
            preview
        }
    }
}

fn structured_payload_preview(signal: Signal, payload: &Bytes) -> Option<String> {
    let summary = match signal {
        Signal::Traces => structured_trace_preview(payload),
        Signal::Metrics => structured_metric_preview(payload),
        Signal::Logs => structured_log_preview(payload),
    }?;

    Some(truncate_preview(&summary))
}

fn compact_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn truncate_preview(text: &str) -> String {
    let preview: String = text.chars().take(MAX_PAYLOAD_PREVIEW_CHARS).collect();
    if text.chars().count() > MAX_PAYLOAD_PREVIEW_CHARS {
        format!("{preview}...")
    } else {
        preview
    }
}

fn extract_service_name_from_resource_blocks(
    signal: Signal,
    value: &serde_json::Value,
) -> Option<String> {
    let resource_blocks = match signal {
        Signal::Traces => value.get("resourceSpans")?.as_array()?,
        Signal::Metrics => value.get("resourceMetrics")?.as_array()?,
        Signal::Logs => value.get("resourceLogs")?.as_array()?,
    };

    for resource_block in resource_blocks {
        let Some(attributes) = resource_block
            .get("resource")
            .and_then(|resource| resource.get("attributes"))
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };

        if let Some(service_name) = attribute_string_value(attributes, "service.name") {
            return Some(service_name);
        }
    }

    None
}

fn attribute_string_value(attributes: &[serde_json::Value], key: &str) -> Option<String> {
    for attribute in attributes {
        if attribute.get("key").and_then(serde_json::Value::as_str) != Some(key) {
            continue;
        }

        if let Some(value) = attribute
            .get("value")
            .and_then(|value| value.get("stringValue"))
            .and_then(serde_json::Value::as_str)
        {
            return Some(value.to_string());
        }
    }

    None
}

fn structured_metric_preview(payload: &Bytes) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let resource_metrics = value.get("resourceMetrics")?.as_array()?;
    let service_name = extract_service_name_from_resource_blocks(Signal::Metrics, &value);
    let mut metric_name = None;
    let mut metric_value = None;

    for resource_metric in resource_metrics {
        let Some(scope_metrics) = resource_metric
            .get("scopeMetrics")
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };

        for scope_metric in scope_metrics {
            let Some(metrics) = scope_metric
                .get("metrics")
                .and_then(serde_json::Value::as_array)
            else {
                continue;
            };

            for metric in metrics {
                if metric_name.is_none() {
                    metric_name = metric
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                }

                if metric_value.is_none() {
                    metric_value = metric_first_value(metric);
                }

                if metric_name.is_some() && metric_value.is_some() {
                    break;
                }
            }

            if metric_name.is_some() && metric_value.is_some() {
                break;
            }
        }

        if metric_name.is_some() && metric_value.is_some() {
            break;
        }
    }

    match (service_name, metric_name, metric_value) {
        (Some(service_name), Some(metric_name), Some(metric_value)) => Some(format!(
            "service={service_name} | metric={metric_name} | value={metric_value} | otlp_json"
        )),
        (Some(service_name), Some(metric_name), None) => Some(format!(
            "service={service_name} | metric={metric_name} | otlp_json"
        )),
        (None, Some(metric_name), Some(metric_value)) => Some(format!(
            "metric={metric_name} | value={metric_value} | otlp_json"
        )),
        (None, Some(metric_name), None) => Some(format!("metric={metric_name} | otlp_json")),
        (Some(service_name), None, _) => Some(format!("service={service_name} | otlp_json")),
        (None, None, _) => None,
    }
}

fn metric_first_value(metric: &serde_json::Value) -> Option<String> {
    for metric_kind in ["sum", "gauge"] {
        let Some(points) = metric
            .get(metric_kind)
            .and_then(|kind| kind.get("dataPoints"))
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };

        for point in points {
            for field in ["asInt", "asDouble"] {
                let Some(raw_value) = point.get(field) else {
                    continue;
                };
                if let Some(value) = json_scalar_to_string(raw_value) {
                    return Some(value);
                }
            }
        }
    }

    None
}

fn json_scalar_to_string(value: &serde_json::Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    if let Some(number) = value.as_i64() {
        return Some(number.to_string());
    }
    if let Some(number) = value.as_u64() {
        return Some(number.to_string());
    }
    if let Some(number) = value.as_f64() {
        return Some(number.to_string());
    }
    None
}

fn structured_log_preview(payload: &Bytes) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let resource_logs = value.get("resourceLogs")?.as_array()?;
    let service_name = extract_service_name_from_resource_blocks(Signal::Logs, &value);
    let mut severity_text = None;
    let mut body_text = None;

    for resource_log in resource_logs {
        let Some(scope_logs) = resource_log
            .get("scopeLogs")
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };

        for scope_log in scope_logs {
            let Some(log_records) = scope_log
                .get("logRecords")
                .and_then(serde_json::Value::as_array)
            else {
                continue;
            };

            for log_record in log_records {
                if severity_text.is_none() {
                    severity_text = log_record
                        .get("severityText")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                }

                if body_text.is_none() {
                    body_text = log_record
                        .get("body")
                        .and_then(json_body_text)
                        .map(|text| compact_whitespace(&text));
                }

                if severity_text.is_some() && body_text.is_some() {
                    break;
                }
            }

            if severity_text.is_some() && body_text.is_some() {
                break;
            }
        }

        if severity_text.is_some() && body_text.is_some() {
            break;
        }
    }

    match (service_name, severity_text, body_text) {
        (Some(service_name), Some(severity_text), Some(body_text)) => Some(format!(
            "service={service_name} | severity={severity_text} | body={body_text} | otlp_json"
        )),
        (Some(service_name), None, Some(body_text)) => Some(format!(
            "service={service_name} | body={body_text} | otlp_json"
        )),
        (None, Some(severity_text), Some(body_text)) => Some(format!(
            "severity={severity_text} | body={body_text} | otlp_json"
        )),
        (None, None, Some(body_text)) => Some(format!("body={body_text} | otlp_json")),
        (Some(service_name), Some(severity_text), None) => Some(format!(
            "service={service_name} | severity={severity_text} | otlp_json"
        )),
        (Some(service_name), None, None) => Some(format!("service={service_name} | otlp_json")),
        (None, Some(severity_text), None) => Some(format!("severity={severity_text} | otlp_json")),
        (None, None, None) => None,
    }
}

fn json_body_text(value: &serde_json::Value) -> Option<String> {
    if let Some(string_value) = value.get("stringValue").and_then(serde_json::Value::as_str) {
        return Some(string_value.to_string());
    }

    json_scalar_to_string(value)
}

fn structured_trace_preview(payload: &Bytes) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let resource_spans = value.get("resourceSpans")?.as_array()?;
    let mut service_name = None;
    let mut span_name = None;

    for resource_span in resource_spans {
        if service_name.is_none() {
            let attributes = resource_span
                .get("resource")
                .and_then(|resource| resource.get("attributes"))
                .and_then(serde_json::Value::as_array);

            if let Some(attributes) = attributes {
                for attribute in attributes {
                    if attribute.get("key").and_then(serde_json::Value::as_str)
                        != Some("service.name")
                    {
                        continue;
                    }

                    service_name = attribute
                        .get("value")
                        .and_then(|value| value.get("stringValue"))
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string);
                    if service_name.is_some() {
                        break;
                    }
                }
            }
        }

        if span_name.is_none() {
            let scope_spans = resource_span
                .get("scopeSpans")
                .and_then(serde_json::Value::as_array);

            if let Some(scope_spans) = scope_spans {
                for scope_span in scope_spans {
                    let spans = scope_span
                        .get("spans")
                        .and_then(serde_json::Value::as_array);
                    if let Some(spans) = spans {
                        for span in spans {
                            span_name = span
                                .get("name")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_string);
                            if span_name.is_some() {
                                break;
                            }
                        }
                    }

                    if span_name.is_some() {
                        break;
                    }
                }
            }
        }

        if service_name.is_some() && span_name.is_some() {
            break;
        }
    }

    match (service_name, span_name) {
        (Some(service_name), Some(span_name)) => Some(format!(
            "service={service_name} | span={span_name} | otlp_json"
        )),
        (Some(service_name), None) => Some(format!("service={service_name} | otlp_json")),
        (None, Some(span_name)) => Some(format!("span={span_name} | otlp_json")),
        (None, None) => None,
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
    async fn test_root_returns_viewer_page() {
        let app = Router::new().route("/", get(index));

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();

        assert!(html.contains("litelemetry viewer"));
        assert!(html.contains("Create viewer"));
        assert!(html.contains("Send OTLP sample"));
        assert!(html.contains("viewer-signal-select"));
        assert!(html.contains("Send metrics sample"));
        assert!(html.contains("Send logs sample"));
        assert!(html.contains("Viewer を読み込み中"));
        assert!(html.contains("viewer 読み込み中"));
        assert!(html.contains("viewer-table"));
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

    #[test]
    fn test_payload_preview_extracts_metric_summary() {
        let payload = Bytes::from(
            serde_json::json!({
                "resourceMetrics": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "orders-api" }
                                }
                            ]
                        },
                        "scopeMetrics": [
                            {
                                "scope": { "name": "test" },
                                "metrics": [
                                    {
                                        "name": "http.server.requests",
                                        "sum": {
                                            "aggregationTemporality": 2,
                                            "isMonotonic": true,
                                            "dataPoints": [
                                                {
                                                    "asInt": "42",
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
        );

        let preview = payload_preview(Signal::Metrics, &payload);

        assert!(preview.contains("orders-api"));
        assert!(preview.contains("http.server.requests"));
        assert!(preview.contains("42"));
    }

    #[test]
    fn test_payload_preview_extracts_log_summary() {
        let payload = Bytes::from(
            serde_json::json!({
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "worker-billing" }
                                }
                            ]
                        },
                        "scopeLogs": [
                            {
                                "scope": { "name": "test" },
                                "logRecords": [
                                    {
                                        "severityText": "INFO",
                                        "body": { "stringValue": "payment authorized" }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            })
            .to_string(),
        );

        let preview = payload_preview(Signal::Logs, &payload);

        assert!(preview.contains("worker-billing"));
        assert!(preview.contains("INFO"));
        assert!(preview.contains("payment authorized"));
    }
}
