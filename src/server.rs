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
use std::collections::HashMap;
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
<html lang="en">
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
        display: grid;
        gap: 18px;
      }

      .stack,
      .panel,
      .table-wrap,
      .empty {
        display: grid;
        gap: 18px;
      }

      [hidden] {
        display: none !important;
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

      .toolbar {
        display: flex;
        flex-direction: column;
        gap: 14px;
      }

      .toolbar-row {
        display: flex;
        gap: 12px;
        align-items: center;
        flex-wrap: wrap;
      }

      .toolbar-row select {
        width: auto;
        min-width: 120px;
        flex-shrink: 0;
      }

      .toolbar-row input {
        flex: 1;
        min-width: 160px;
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

      @media (max-width: 640px) {
        main {
          width: min(100% - 20px, 100%);
          padding: 20px 0 30px;
        }

        .panel {
          padding: 20px;
          border-radius: 24px;
        }

        .toolbar-row {
          flex-direction: column;
          align-items: stretch;
        }

        .toolbar-row select,
        .toolbar-row input {
          width: 100%;
        }

        button {
          width: 100%;
        }
      }
      .chart-section {
        display: none;
      }

      .chart-section.visible {
        display: grid;
        gap: 18px;
      }

      .chart-section canvas {
        width: 100%;
        max-height: 400px;
      }

      .viewer-row {
        cursor: pointer;
      }

      .viewer-row:hover td {
        background: var(--accent-soft);
      }

      .viewer-row.selected td {
        background: var(--teal-soft);
      }

      .chart-type-select {
        min-height: 32px;
        padding: 4px 8px;
        border-radius: 10px;
        min-width: 100px;
      }

      .entries-table-wrap {
        overflow: auto;
        max-height: 400px;
        border-radius: 16px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.76);
      }

      .trace-detail-panel {
        display: grid;
        gap: 12px;
      }

      .trace-detail-header {
        display: flex;
        align-items: center;
        gap: 12px;
      }

      .trace-detail-header button {
        min-height: 36px;
        padding: 0 14px;
        font-size: 0.85rem;
      }

      .trace-detail-header h3 {
        font-size: 1rem;
        font-family: inherit;
      }

      .trace-timeline-wrap {
        overflow: auto;
        max-height: 600px;
        border-radius: 16px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.76);
      }

      .trace-timeline-header {
        display: grid;
        grid-template-columns: minmax(200px, 35%) 1fr;
        position: sticky;
        top: 0;
        background: rgba(255, 250, 243, 0.96);
        z-index: 1;
        border-bottom: 1px solid var(--line);
        font-size: 0.82rem;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      .trace-timeline-header > div {
        padding: 10px 12px;
      }

      .trace-axis {
        display: flex;
        justify-content: space-between;
        color: var(--muted);
        font-weight: 400;
        font-size: 0.75rem;
        text-transform: none;
        letter-spacing: 0;
      }

      .span-row {
        display: grid;
        grid-template-columns: minmax(200px, 35%) 1fr;
        min-height: 32px;
        border-bottom: 1px solid var(--line);
        align-items: center;
      }

      .span-row:last-child {
        border-bottom: 0;
      }

      .span-row:hover {
        background: var(--accent-soft);
      }

      .span-label {
        display: flex;
        align-items: center;
        gap: 4px;
        padding: 4px 10px;
        font-size: 0.82rem;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .span-toggle {
        width: 16px;
        height: 16px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        font-size: 0.7rem;
        color: var(--muted);
        flex-shrink: 0;
        user-select: none;
        border-radius: 3px;
      }

      .span-toggle:hover {
        background: var(--line);
      }

      .span-toggle.leaf {
        visibility: hidden;
      }

      .span-label .svc {
        font-weight: 700;
        flex-shrink: 0;
      }

      .span-label .op {
        color: var(--muted);
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .span-label .err-badge {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: #c0392b;
        flex-shrink: 0;
      }

      .span-indent {
        display: inline-block;
        flex-shrink: 0;
      }

      .span-bar-container {
        position: relative;
        height: 100%;
        min-height: 32px;
      }

      .span-bar {
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        height: 12px;
        border-radius: 3px;
        min-width: 2px;
        opacity: 0.85;
      }

      .span-bar-label {
        position: absolute;
        top: 50%;
        transform: translateY(-50%);
        font-size: 0.72rem;
        color: var(--muted);
        white-space: nowrap;
        padding-left: 4px;
      }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
  </head>
  <body>
    <main>
      <section class="toolbar panel panel-strong">
        <div class="toolbar-row">
          <select id="viewer-signal-select" data-testid="viewer-signal-select" name="viewer-signal">
            <option value="traces">traces</option>
            <option value="metrics">metrics</option>
            <option value="logs">logs</option>
          </select>
          <select id="viewer-chart-type-select" data-testid="viewer-chart-type-select" name="viewer-chart-type">
            <option value="table">Table</option>
            <option value="stacked_bar">Stacked Bar</option>
            <option value="line">Line</option>
          </select>
          <input id="viewer-name-input" data-testid="viewer-name-input" name="viewer-name" placeholder="checkout traces" maxlength="80" />
          <button id="create-viewer-button" data-testid="create-viewer-button" class="primary" type="button">+ Create viewer</button>
          <button id="refresh-viewers-button" class="secondary" type="button">Refresh</button>
        </div>
        <div id="status-box" data-testid="status-box" class="status-box" data-state="working">
          Loading viewers...
        </div>
      </section>

      <section class="panel panel-strong stack table-wrap">
        <div id="viewer-table-scroll" class="table-scroll" hidden>
          <table data-testid="viewer-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>ID</th>
                <th>Signal</th>
                <th>Chart</th>
                <th>Lookback</th>
                <th>Entries</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="viewer-table-body"></tbody>
          </table>
        </div>
        <div id="viewer-empty" data-testid="viewer-empty" class="empty">
          <div class="stack">
            <strong id="viewer-empty-title">Loading viewers</strong>
            <p id="viewer-empty-body">Fetching the viewer list...</p>
          </div>
        </div>
      </section>

      <section id="viewer-detail-section" class="panel panel-strong chart-section">
        <h3 id="viewer-detail-title">Viewer Detail</h3>
        <div id="viewer-chart-container">
          <canvas id="viewer-chart-canvas"></canvas>
        </div>
        <div id="viewer-entries-table" class="entries-table-wrap" hidden>
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Signal</th>
                <th>Service</th>
                <th>Preview</th>
              </tr>
            </thead>
            <tbody id="viewer-entries-body"></tbody>
          </table>
        </div>
        <div id="viewer-trace-list" class="entries-table-wrap" hidden>
          <table>
            <thead>
              <tr>
                <th>Trace ID</th>
                <th>Root Span</th>
                <th>Services</th>
                <th>Spans</th>
                <th>Duration</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="viewer-trace-list-body"></tbody>
          </table>
        </div>
        <div id="viewer-trace-detail" class="trace-detail-panel" hidden>
          <div class="trace-detail-header">
            <button id="trace-back-button" class="secondary" type="button">&larr; Back</button>
            <h3 id="trace-detail-title"></h3>
          </div>
          <div class="trace-timeline-wrap">
            <div class="trace-timeline-header">
              <div>Service &amp; Operation</div>
              <div class="trace-axis" id="trace-axis"></div>
            </div>
            <div id="trace-timeline"></div>
          </div>
        </div>
      </section>
    </main>

    <script>
      const statusBox = document.getElementById('status-box');
      const viewerSignalSelect = document.getElementById('viewer-signal-select');
      const viewerChartTypeSelect = document.getElementById('viewer-chart-type-select');
      const viewerNameInput = document.getElementById('viewer-name-input');
      const createViewerButton = document.getElementById('create-viewer-button');
      const refreshViewersButton = document.getElementById('refresh-viewers-button');
      const viewerEmpty = document.getElementById('viewer-empty');
      const viewerEmptyTitle = document.getElementById('viewer-empty-title');
      const viewerEmptyBody = document.getElementById('viewer-empty-body');
      const viewerTableScroll = document.getElementById('viewer-table-scroll');
      const viewerTableBody = document.getElementById('viewer-table-body');
      const viewerDetailSection = document.getElementById('viewer-detail-section');
      const viewerDetailTitle = document.getElementById('viewer-detail-title');
      const viewerChartContainer = document.getElementById('viewer-chart-container');
      const viewerChartCanvas = document.getElementById('viewer-chart-canvas');
      const viewerEntriesTable = document.getElementById('viewer-entries-table');
      const viewerEntriesBody = document.getElementById('viewer-entries-body');
      const viewerTraceList = document.getElementById('viewer-trace-list');
      const viewerTraceListBody = document.getElementById('viewer-trace-list-body');
      const viewerTraceDetail = document.getElementById('viewer-trace-detail');
      const traceBackButton = document.getElementById('trace-back-button');
      const traceDetailTitle = document.getElementById('trace-detail-title');
      const traceAxis = document.getElementById('trace-axis');
      const traceTimeline = document.getElementById('trace-timeline');

      let latestViewers = [];
      let viewerLoadState = 'loading';
      let selectedViewerId = null;
      let currentChart = null;

      function setStatus(kind, message) {
        statusBox.dataset.state = kind;
        statusBox.textContent = message;
      }

      function truncateId(id) {
        return id.length <= 12 ? id : `${id.slice(0, 8)}...${id.slice(-4)}`;
      }

      function formatDurationNs(ns) {
        const n = Number(ns);
        if (n < 1000) return n + 'ns';
        if (n < 1e6) return (n / 1e3).toFixed(1) + '\u00b5s';
        if (n < 1e9) return (n / 1e6).toFixed(2) + 'ms';
        return (n / 1e9).toFixed(2) + 's';
      }

      function formatNanoTimestamp(ns) {
        return new Date(Number(ns) / 1e6).toLocaleTimeString();
      }

      const _svcColorCache = {};
      function serviceColor(name) {
        if (_svcColorCache[name]) return _svcColorCache[name];
        let h = 0;
        for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) | 0;
        const hue = Math.abs(h) % 360;
        _svcColorCache[name] = 'hsl(' + hue + ', 55%, 50%)';
        return _svcColorCache[name];
      }

      function makeTextElement(tagName, text, className) {
        const element = document.createElement(tagName);
        if (className) {
          element.className = className;
        }
        element.textContent = text;
        return element;
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

      const VIEWER_PLACEHOLDERS = { traces: 'checkout traces', metrics: 'orders metrics', logs: 'billing logs' };
      const CHART_TYPE_LABELS = { table: 'Table', stacked_bar: 'Stacked Bar', line: 'Line' };

      function formatStatus(status) {
        if (!status) return JSON.stringify(status);
        if (status.type === 'ok') return 'ok';
        if (status.type === 'degraded') return `degraded: ${status.reason}`;
        return JSON.stringify(status);
      }

      function syncCreateForm() {
        const signal = viewerSignalSelect.value;
        viewerNameInput.placeholder = VIEWER_PLACEHOLDERS[signal] || signal;
        const isMetrics = signal === 'metrics';
        viewerChartTypeSelect.disabled = !isMetrics;
        if (!isMetrics) {
          viewerChartTypeSelect.value = 'table';
        }
      }

      function showEmpty(title, body) {
        viewerEmptyTitle.textContent = title;
        viewerEmptyBody.textContent = body;
        viewerEmpty.hidden = false;
        viewerTableScroll.hidden = true;
        viewerTableBody.replaceChildren();
      }

      async function patchViewerChartType(viewerId, chartType) {
        try {
          const response = await fetch(`/api/viewers/${viewerId}`, {
            method: 'PATCH',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ chart_type: chartType })
          });
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          await refreshViewers({ silent: true });
          if (selectedViewerId === viewerId) {
            await showViewerDetail(viewerId);
          }
          setStatus('ok', `Chart type updated to ${CHART_TYPE_LABELS[chartType] || chartType}.`);
        } catch (error) {
          setStatus('error', `Failed to update chart type: ${error.message}`);
        }
      }

      function renderViewerTable() {
        if (viewerLoadState === 'loading') {
          showEmpty('Loading viewers', 'Fetching the viewer list...');
          return;
        }

        if (viewerLoadState === 'error') {
          showEmpty('Failed to load viewers', 'Press Refresh to retry.');
          return;
        }

        if (!latestViewers.length) {
          showEmpty('No viewers yet', 'Use the form above to create a viewer.');
          return;
        }

        viewerEmpty.hidden = true;
        viewerTableScroll.hidden = false;
        viewerTableBody.replaceChildren();

        for (const viewer of latestViewers) {
          const row = document.createElement('tr');
          row.className = 'viewer-row';
          if (viewer.id === selectedViewerId) row.classList.add('selected');

          appendTableCell(row, viewer.name);
          appendTableCell(row, truncateId(viewer.id));
          appendTableCell(row, viewer.signals.join(', '));

          const chartCell = document.createElement('td');
          const isMetrics = viewer.signals.includes('metrics');
          if (isMetrics) {
            const sel = document.createElement('select');
            sel.className = 'chart-type-select';
            for (const [val, label] of Object.entries(CHART_TYPE_LABELS)) {
              const opt = document.createElement('option');
              opt.value = val;
              opt.textContent = label;
              if (val === (viewer.chart_type || 'table')) opt.selected = true;
              sel.appendChild(opt);
            }
            sel.addEventListener('change', (e) => {
              e.stopPropagation();
              patchViewerChartType(viewer.id, sel.value);
            });
            sel.addEventListener('click', (e) => e.stopPropagation());
            chartCell.appendChild(sel);
          } else {
            chartCell.textContent = 'table';
          }
          row.appendChild(chartCell);

          appendTableCell(row, formatLookbackMs(viewer.lookback_ms));
          appendTableCell(row, String(viewer.entry_count));
          appendTableCell(row, formatStatus(viewer.status));

          row.addEventListener('click', () => showViewerDetail(viewer.id));
          viewerTableBody.appendChild(row);
        }
      }

      function getBucketSizeMs(lookbackMs) {
        if (lookbackMs <= 5 * 60 * 1000) return 10 * 1000;
        if (lookbackMs <= 60 * 60 * 1000) return 60 * 1000;
        return 5 * 60 * 1000;
      }

      function bucketKey(dateStr, bucketMs) {
        const t = new Date(dateStr).getTime();
        return new Date(Math.floor(t / bucketMs) * bucketMs).toISOString();
      }

      function buildChartData(entries, lookbackMs) {
        const bucketMs = getBucketSizeMs(lookbackMs);
        const grouped = {};
        const allBuckets = new Set();

        for (const entry of entries) {
          const key = bucketKey(entry.observed_at, bucketMs);
          allBuckets.add(key);
          const series = `${entry.metric_name || 'unknown'} (${entry.service_name || 'unknown'})`;
          if (!grouped[series]) grouped[series] = {};
          grouped[series][key] = (grouped[series][key] || 0) + (entry.metric_value ?? 0);
        }

        const labels = [...allBuckets].sort();
        const datasets = Object.entries(grouped).map(([series, buckets]) => {
          const hue = Math.abs([...series].reduce((h, c) => h * 31 + c.charCodeAt(0), 0)) % 360;
          return {
            label: series,
            data: labels.map(l => buckets[l] || 0),
            backgroundColor: `hsla(${hue}, 60%, 55%, 0.7)`,
            borderColor: `hsl(${hue}, 60%, 45%)`,
            borderWidth: 1,
            fill: false,
          };
        });

        return {
          labels: labels.map(l => {
            const d = new Date(l);
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
          }),
          datasets,
        };
      }

      function renderChart(chartType, entries, lookbackMs) {
        if (currentChart) {
          currentChart.destroy();
          currentChart = null;
        }

        if (chartType === 'table' || !entries.length) {
          viewerChartContainer.hidden = true;
          return;
        }

        viewerChartContainer.hidden = false;
        const data = buildChartData(entries, lookbackMs);
        const isStacked = chartType === 'stacked_bar';
        const type = isStacked ? 'bar' : 'line';

        currentChart = new Chart(viewerChartCanvas, {
          type,
          data,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } },
            scales: {
              x: { stacked: isStacked },
              y: { stacked: isStacked, beginAtZero: true },
            },
          },
        });
      }

      function renderEntriesTable(entries) {
        viewerEntriesBody.replaceChildren();
        if (!entries.length) {
          viewerEntriesTable.hidden = true;
          return;
        }
        viewerEntriesTable.hidden = false;
        for (const entry of entries) {
          const row = document.createElement('tr');
          appendTableCell(row, new Date(entry.observed_at).toLocaleTimeString());
          appendTableCell(row, entry.signal);
          appendTableCell(row, entry.service_name || '-');
          const previewCell = document.createElement('td');
          const code = document.createElement('code');
          code.textContent = entry.payload_preview;
          previewCell.appendChild(code);
          row.appendChild(previewCell);
          viewerEntriesBody.appendChild(row);
        }
      }

      function hideAllDetailPanels() {
        viewerChartContainer.hidden = true;
        viewerEntriesTable.hidden = true;
        viewerTraceList.hidden = true;
        viewerTraceDetail.hidden = true;
      }

      function renderTraceList(traces) {
        viewerTraceListBody.replaceChildren();
        if (!traces || !traces.length) {
          viewerTraceList.hidden = true;
          return;
        }
        viewerTraceList.hidden = false;
        for (const trace of traces) {
          const row = document.createElement('tr');
          row.className = 'viewer-row';
          if (trace.has_error) row.style.background = 'var(--danger-soft)';
          appendTableCell(row, truncateId(trace.trace_id));
          appendTableCell(row, trace.root_span_name || '-');
          appendTableCell(row, trace.service_names.join(', '));
          appendTableCell(row, String(trace.span_count));
          appendTableCell(row, formatDurationNs(trace.duration_ns));
          const statusCell = document.createElement('td');
          statusCell.textContent = trace.has_error ? 'error' : 'ok';
          if (trace.has_error) statusCell.className = 'error-inline';
          row.appendChild(statusCell);
          row.addEventListener('click', () => showTraceDetail(trace));
          viewerTraceListBody.appendChild(row);
        }
      }

      function showTraceDetail(trace) {
        viewerTraceList.hidden = true;
        viewerTraceDetail.hidden = false;

        const spanMap = {};
        const childrenMap = {};
        for (const s of trace.spans) {
          spanMap[s.span_id] = s;
          const pid = s.parent_span_id || '';
          if (!childrenMap[pid]) childrenMap[pid] = [];
          childrenMap[pid].push(s);
        }

        const roots = trace.spans.filter(s => !s.parent_span_id || !spanMap[s.parent_span_id]);
        const rootSvc = roots.length ? roots[0].service_name : '';
        const titleOp = trace.root_span_name || 'unknown';
        traceDetailTitle.textContent = rootSvc + ': ' + titleOp + ' ' + truncateId(trace.trace_id);
        roots.sort((a, b) => Number(a.start_time_unix_nano) - Number(b.start_time_unix_nano));

        const traceStart = Math.min(...trace.spans.map(s => Number(s.start_time_unix_nano)));
        const traceEnd = Math.max(...trace.spans.map(s => Number(s.end_time_unix_nano)));
        const traceDuration = traceEnd - traceStart || 1;

        // render axis
        traceAxis.replaceChildren();
        const axisSteps = 5;
        for (let i = 0; i <= axisSteps; i++) {
          const tick = document.createElement('span');
          tick.textContent = formatDurationNs((traceDuration / axisSteps) * i);
          traceAxis.appendChild(tick);
        }

        // collect collapse state
        const collapseState = {};

        function hasChildren(spanId) {
          return childrenMap[spanId] && childrenMap[spanId].length > 0;
        }

        function renderTree() {
          const flatSpans = [];
          function dfs(span, depth) {
            flatSpans.push({ span, depth });
            if (collapseState[span.span_id]) return;
            const children = (childrenMap[span.span_id] || [])
              .sort((a, b) => Number(a.start_time_unix_nano) - Number(b.start_time_unix_nano));
            for (const child of children) dfs(child, depth + 1);
          }
          for (const root of roots) dfs(root, 0);

          traceTimeline.replaceChildren();
          for (const { span, depth } of flatSpans) {
            const row = document.createElement('div');
            row.className = 'span-row';

            // left: label
            const label = document.createElement('div');
            label.className = 'span-label';

            const indent = document.createElement('span');
            indent.className = 'span-indent';
            indent.style.width = (depth * 16) + 'px';
            label.appendChild(indent);

            const toggle = document.createElement('span');
            toggle.className = 'span-toggle';
            if (hasChildren(span.span_id)) {
              toggle.textContent = collapseState[span.span_id] ? '\u25b6' : '\u25bc';
              toggle.addEventListener('click', (e) => {
                e.stopPropagation();
                collapseState[span.span_id] = !collapseState[span.span_id];
                renderTree();
              });
            } else {
              toggle.classList.add('leaf');
            }
            label.appendChild(toggle);

            if (span.status_code === 2) {
              const badge = document.createElement('span');
              badge.className = 'err-badge';
              badge.title = 'Error';
              label.appendChild(badge);
            }

            const svc = document.createElement('span');
            svc.className = 'svc';
            svc.textContent = span.service_name || 'unknown';
            svc.style.color = serviceColor(span.service_name || 'unknown');
            label.appendChild(svc);

            const op = document.createElement('span');
            op.className = 'op';
            op.textContent = ' ' + span.name;
            label.appendChild(op);

            row.appendChild(label);

            // right: timeline bar
            const barContainer = document.createElement('div');
            barContainer.className = 'span-bar-container';

            const start = Number(span.start_time_unix_nano);
            const end = Number(span.end_time_unix_nano);
            const leftPct = ((start - traceStart) / traceDuration) * 100;
            const widthPct = Math.max(((end - start) / traceDuration) * 100, 0.3);

            const bar = document.createElement('div');
            bar.className = 'span-bar';
            bar.style.left = leftPct + '%';
            bar.style.width = widthPct + '%';
            const color = span.status_code === 2 ? '#c0392b' : serviceColor(span.service_name || 'unknown');
            bar.style.background = color;
            barContainer.appendChild(bar);

            const durLabel = document.createElement('span');
            durLabel.className = 'span-bar-label';
            durLabel.style.left = (leftPct + widthPct) + '%';
            durLabel.textContent = formatDurationNs(end - start);
            barContainer.appendChild(durLabel);

            row.appendChild(barContainer);
            traceTimeline.appendChild(row);
          }
        }

        renderTree();
      }

      traceBackButton.addEventListener('click', () => {
        viewerTraceDetail.hidden = true;
        viewerTraceList.hidden = false;
      });

      async function showViewerDetail(viewerId) {
        selectedViewerId = viewerId;
        renderViewerTable();

        try {
          const response = await fetch(`/api/viewers/${viewerId}`, {
            headers: { 'accept': 'application/json' }
          });
          if (!response.ok) throw new Error(`HTTP ${response.status}`);

          const viewer = await response.json();
          viewerDetailTitle.textContent = `${viewer.name} (${viewer.chart_type || 'table'})`;
          viewerDetailSection.classList.add('visible');
          hideAllDetailPanels();

          const chartType = viewer.chart_type || 'table';
          const isTraceViewer = viewer.signals.includes('traces');
          if (chartType !== 'table' && viewer.signals.includes('metrics')) {
            renderChart(chartType, viewer.entries, viewer.lookback_ms);
          } else if (isTraceViewer && viewer.traces && viewer.traces.length > 0) {
            renderTraceList(viewer.traces);
          } else {
            renderEntriesTable(viewer.entries);
          }
        } catch (error) {
          viewerDetailSection.classList.remove('visible');
          viewerDetailTitle.textContent = '';
          hideAllDetailPanels();
          if (currentChart) { currentChart.destroy(); currentChart = null; }
          viewerEntriesBody.replaceChildren();
          setStatus('error', `Failed to load viewer detail: ${error.message}`);
        }
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
          renderViewerTable();

          if (!silent) {
            setStatus('ok', `Viewer list refreshed at ${new Date().toLocaleTimeString()}.`);
          } else if (previousLoadState !== 'ready') {
            if (latestViewers.length) {
              setStatus('ok', `${latestViewers.length} viewer(s) loaded.`);
            } else {
              setStatus('idle', 'No viewers yet. Create one above.');
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

          renderViewerTable();

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
        const chart_type = viewerChartTypeSelect.value;
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
            body: JSON.stringify({ name, signal, chart_type })
          });

          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }

          await refreshViewers({ silent: true });
          setStatus('ok', `${signal.charAt(0).toUpperCase() + signal.slice(1)} viewer "${name}" created.`);
        } catch (error) {
          setStatus('error', `Viewer creation failed: ${error.message}`);
        } finally {
          createViewerButton.disabled = false;
        }
      }

      createViewerButton.addEventListener('click', createViewer);
      refreshViewersButton.addEventListener('click', () => refreshViewers());
      viewerSignalSelect.addEventListener('change', syncCreateForm);
      viewerNameInput.addEventListener('keydown', event => {
        if (event.key === 'Enter') {
          event.preventDefault();
          createViewer();
        }
      });

      syncCreateForm();
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

fn default_chart_type() -> String {
    "table".to_string()
}

fn is_valid_chart_type(chart_type: &str) -> bool {
    matches!(chart_type, "table" | "stacked_bar" | "line")
}

#[derive(Debug, Deserialize)]
struct CreateViewerRequest {
    name: String,
    signal: String,
    #[serde(default = "default_chart_type")]
    chart_type: String,
}

#[derive(Debug, Deserialize)]
struct PatchViewerRequest {
    chart_type: String,
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
    chart_type: String,
    refresh_interval_ms: u32,
    lookback_ms: i64,
    entry_count: usize,
    status: ViewerStatus,
    entries: Vec<ViewerEntryRow>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    traces: Vec<TraceSummary>,
}

#[derive(Debug, Serialize)]
struct ViewerEntryRow {
    observed_at: DateTime<Utc>,
    signal: &'static str,
    service_name: Option<String>,
    payload_size_bytes: usize,
    payload_preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    metric_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metric_value: Option<f64>,
}

#[derive(Debug, Serialize)]
struct SpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: String,
    service_name: String,
    name: String,
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    status_code: u64,
}

#[derive(Debug, Serialize)]
struct TraceSummary {
    trace_id: String,
    root_span_name: Option<String>,
    service_names: Vec<String>,
    span_count: usize,
    duration_ns: u64,
    started_at_ns: u64,
    has_error: bool,
    spans: Vec<SpanRow>,
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
        .route("/api/viewers/{id}", get(get_viewer).patch(patch_viewer))
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
        .map(|(viewer, viewer_state)| Json(viewer_summary(viewer, viewer_state, true)))
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
        .map(|(viewer, viewer_state)| viewer_summary(viewer, viewer_state, false))
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

    if !is_valid_chart_type(&payload.chart_type) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let id = Uuid::new_v4();
    let definition = ViewerDefinition {
        id,
        slug: format!("viewer-{}", id.simple()),
        name: name.to_string(),
        refresh_interval_ms: DEFAULT_VIEWER_REFRESH_MS,
        lookback_ms: DEFAULT_VIEWER_LOOKBACK_MS,
        signal_mask: signal.into(),
        definition_json: json!({
            "kind": payload.chart_type,
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

async fn patch_viewer(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<PatchViewerRequest>,
) -> Result<StatusCode, StatusCode> {
    let postgres = state
        .postgres
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let runtime = state.require_viewer_runtime()?;

    if !is_valid_chart_type(&payload.chart_type) {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Read current state under lock, then release before DB write
    let (definition_json, layout_json) = {
        let rt = runtime.lock().await;
        let (viewer, _) = rt
            .viewers()
            .iter()
            .find(|(viewer, _)| viewer.definition().id == id)
            .ok_or(StatusCode::NOT_FOUND)?;

        let current_kind = viewer
            .definition()
            .definition_json
            .get("kind")
            .and_then(|v| v.as_str())
            .unwrap_or("table");
        if current_kind == payload.chart_type {
            return Ok(StatusCode::OK);
        }

        let mut definition_json = viewer.definition().definition_json.clone();
        definition_json["kind"] = json!(payload.chart_type);
        let layout_json = viewer.definition().layout_json.clone();
        (definition_json, layout_json)
    }; // lock released here

    let updated = postgres
        .update_viewer_definition_json(id, &definition_json, &layout_json)
        .await
        .map_err(|error| {
            tracing::error!("update_viewer_definition_json failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if !updated {
        return Err(StatusCode::NOT_FOUND);
    }

    // Re-acquire lock to update in-memory state
    runtime
        .lock()
        .await
        .update_viewer_definition(id, definition_json, layout_json);

    Ok(StatusCode::OK)
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

fn viewer_summary(
    viewer: &CompiledViewer,
    viewer_state: &ViewerState,
    include_entries: bool,
) -> ViewerSummary {
    let definition = viewer.definition();
    let chart_type = definition
        .definition_json
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("table")
        .to_string();

    let entries = if include_entries {
        viewer_state
            .entries
            .iter()
            .rev()
            .take(VIEWER_ENTRY_PREVIEW_LIMIT)
            .map(|entry| {
                let (metric_name, metric_value, preview) = if entry.signal == Signal::Metrics {
                    let fields = extract_metric_fields(&entry.payload);
                    let name = fields.as_ref().and_then(|f| f.metric_name.clone());
                    let value = fields
                        .as_ref()
                        .and_then(|f| f.metric_value.as_ref().and_then(|s| s.parse::<f64>().ok()));
                    let preview = fields
                        .as_ref()
                        .and_then(format_metric_preview)
                        .map(|s| truncate_preview(&s))
                        .unwrap_or_else(|| raw_payload_preview(&entry.payload));
                    (name, value, preview)
                } else {
                    (None, None, payload_preview(entry.signal, &entry.payload))
                };
                ViewerEntryRow {
                    observed_at: entry.observed_at,
                    signal: signal_name(entry.signal),
                    service_name: entry.service_name.clone(),
                    payload_size_bytes: entry.payload.len(),
                    payload_preview: preview,
                    metric_name,
                    metric_value,
                }
            })
            .collect()
    } else {
        vec![]
    };

    ViewerSummary {
        id: definition.id,
        slug: definition.slug.clone(),
        name: definition.name.clone(),
        signals: signal_mask_labels(definition.signal_mask),
        chart_type,
        refresh_interval_ms: definition.refresh_interval_ms,
        lookback_ms: definition.lookback_ms,
        entry_count: viewer_state.entries.len(),
        status: viewer_state.status.clone(),
        entries,
        traces: if include_entries && definition.signal_mask.contains(Signal::Traces) {
            extract_traces_from_entries(&viewer_state.entries)
        } else {
            vec![]
        },
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
    raw_payload_preview(payload)
}

fn raw_payload_preview(payload: &Bytes) -> String {
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
    let mut chars = text.chars();
    let preview: String = chars.by_ref().take(MAX_PAYLOAD_PREVIEW_CHARS).collect();
    if chars.next().is_some() {
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

struct MetricFields {
    service_name: Option<String>,
    metric_name: Option<String>,
    metric_value: Option<String>,
}

fn extract_metric_fields(payload: &Bytes) -> Option<MetricFields> {
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

    Some(MetricFields {
        service_name,
        metric_name,
        metric_value,
    })
}

fn format_metric_preview(fields: &MetricFields) -> Option<String> {
    match (
        &fields.service_name,
        &fields.metric_name,
        &fields.metric_value,
    ) {
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

fn structured_metric_preview(payload: &Bytes) -> Option<String> {
    let fields = extract_metric_fields(payload)?;
    format_metric_preview(&fields)
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

fn extract_traces_from_entries(
    entries: &[crate::domain::telemetry::NormalizedEntry],
) -> Vec<TraceSummary> {
    let mut spans_by_trace: HashMap<String, Vec<SpanRow>> = HashMap::new();

    for entry in entries {
        if entry.signal != Signal::Traces {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(&entry.payload) else {
            continue;
        };
        let Some(resource_spans) = value.get("resourceSpans").and_then(|v| v.as_array()) else {
            continue;
        };

        for rs in resource_spans {
            let service_name = rs
                .get("resource")
                .and_then(|r| r.get("attributes"))
                .and_then(serde_json::Value::as_array)
                .and_then(|attrs| attribute_string_value(attrs, "service.name"))
                .unwrap_or_default();

            let Some(scope_spans) = rs.get("scopeSpans").and_then(|v| v.as_array()) else {
                continue;
            };

            for ss in scope_spans {
                let Some(spans) = ss.get("spans").and_then(|v| v.as_array()) else {
                    continue;
                };

                for span in spans {
                    let trace_id = span
                        .get("traceId")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    if trace_id.is_empty() {
                        continue;
                    }

                    let span_row = SpanRow {
                        trace_id: trace_id.clone(),
                        span_id: span
                            .get("spanId")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        parent_span_id: span
                            .get("parentSpanId")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        service_name: service_name.clone(),
                        name: span
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("")
                            .to_string(),
                        start_time_unix_nano: parse_nano(span.get("startTimeUnixNano")),
                        end_time_unix_nano: parse_nano(span.get("endTimeUnixNano")),
                        status_code: span
                            .get("status")
                            .and_then(|s| s.get("code"))
                            .and_then(|c| c.as_u64())
                            .unwrap_or(0),
                    };

                    spans_by_trace
                        .entry(span_row.trace_id.clone())
                        .or_default()
                        .push(span_row);
                }
            }
        }
    }

    let mut traces: Vec<TraceSummary> = spans_by_trace
        .into_iter()
        .map(|(trace_id, spans)| {
            let mut started_at_ns = u64::MAX;
            let mut ended_at_ns = 0u64;
            let mut root_span_name: Option<String> = None;
            let mut has_error = false;
            let mut svc_set = std::collections::HashSet::new();
            for s in &spans {
                if s.start_time_unix_nano < started_at_ns {
                    started_at_ns = s.start_time_unix_nano;
                }
                if s.end_time_unix_nano > ended_at_ns {
                    ended_at_ns = s.end_time_unix_nano;
                }
                if root_span_name.is_none() && s.parent_span_id.is_empty() {
                    root_span_name = Some(s.name.clone());
                }
                if s.status_code == 2 {
                    has_error = true;
                }
                if !s.service_name.is_empty() {
                    svc_set.insert(s.service_name.clone());
                }
            }
            if started_at_ns == u64::MAX {
                started_at_ns = 0;
            }
            let duration_ns = ended_at_ns.saturating_sub(started_at_ns);
            let mut service_names: Vec<String> = svc_set.into_iter().collect();
            service_names.sort();

            TraceSummary {
                trace_id,
                root_span_name,
                service_names,
                span_count: spans.len(),
                duration_ns,
                started_at_ns,
                has_error,
                spans,
            }
        })
        .collect();

    traces.sort_by(|a, b| b.started_at_ns.cmp(&a.started_at_ns));
    traces
}

fn parse_nano(value: Option<&serde_json::Value>) -> u64 {
    let Some(v) = value else { return 0 };
    if let Some(n) = v.as_u64() {
        return n;
    }
    if let Some(s) = v.as_str() {
        return s.parse::<u64>().unwrap_or(0);
    }
    0
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
        assert!(html.contains("viewer-signal-select"));
        assert!(html.contains("Loading viewers"));
        assert!(html.contains("status-box"));
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
