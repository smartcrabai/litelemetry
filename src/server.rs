use crate::domain::dashboard::{
    DashboardDefinition, PanelInput, build_layout_json, panel_inputs_from_viewer_ids,
};
use crate::domain::telemetry::{NormalizedEntry, Signal, SignalMask};
use crate::domain::viewer::{ViewerDefinition, ViewerStatus};
use crate::ingest::decode::DecodeError;
use crate::ingest::otlp_http::{
    attribute_string_value, extract_service_name_from_value, parse_ingest_request,
};
use crate::storage::{StreamStore, ViewerStore};
use crate::viewer_runtime::compiler::{
    CompiledViewer, compile, extract_searchable_payload_text, query_from_definition,
};
use crate::viewer_runtime::reducer::{apply_entry, prune_stale_buckets};
use crate::viewer_runtime::runtime::ViewerRuntime;
use crate::viewer_runtime::state::ViewerState;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Html,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// Maximum body size for OTLP payloads (4 MiB).
/// A realistic upper limit for a single batch.
const MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_VIEWER_LOOKBACK_MS: i64 = 5 * 60 * 1_000;
const DEFAULT_VIEWER_REFRESH_MS: u32 = 1_000;
const MAX_PAYLOAD_PREVIEW_CHARS: usize = 160;
const VIEWER_ENTRY_PREVIEW_LIMIT: usize = 50;

#[derive(Debug, Serialize)]
struct TemplateInfo {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    signal_types: Vec<&'static str>,
    viewer_count: usize,
    columns: u32,
}

struct TemplateViewerSpec {
    name: &'static str,
    signal: Signal,
    chart_type: &'static str,
    lookback_ms: i64,
    query: Option<&'static str>,
}

struct TemplatePanelSpec {
    viewer_index: usize,
    col_span: u32,
    row_span: u32,
}

struct TemplateDef {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    columns: u32,
    viewers: &'static [TemplateViewerSpec],
    panels: &'static [TemplatePanelSpec],
}

static ALL_TEMPLATES: &[TemplateDef] = &[
    TemplateDef {
        id: "service-overview",
        name: "Service Overview",
        description: "High-level metrics, error rate, latency, and recent traces for a service.",
        columns: 2,
        viewers: &[
            TemplateViewerSpec {
                name: "Request Count",
                signal: Signal::Metrics,
                chart_type: "billboard",
                lookback_ms: 300_000,
                query: None,
            },
            TemplateViewerSpec {
                name: "Error Rate",
                signal: Signal::Metrics,
                chart_type: "billboard",
                lookback_ms: 300_000,
                query: Some("error"),
            },
            TemplateViewerSpec {
                name: "Request Latency",
                signal: Signal::Metrics,
                chart_type: "line",
                lookback_ms: 900_000,
                query: None,
            },
            TemplateViewerSpec {
                name: "Recent Traces",
                signal: Signal::Traces,
                chart_type: "table",
                lookback_ms: 300_000,
                query: None,
            },
        ],
        panels: &[
            TemplatePanelSpec {
                viewer_index: 0,
                col_span: 1,
                row_span: 1,
            },
            TemplatePanelSpec {
                viewer_index: 1,
                col_span: 1,
                row_span: 1,
            },
            TemplatePanelSpec {
                viewer_index: 2,
                col_span: 2,
                row_span: 1,
            },
            TemplatePanelSpec {
                viewer_index: 3,
                col_span: 2,
                row_span: 1,
            },
        ],
    },
    TemplateDef {
        id: "trace-analysis",
        name: "Trace Analysis",
        description: "Detailed trace exploration: all traces, errors, and volume over time.",
        columns: 2,
        viewers: &[
            TemplateViewerSpec {
                name: "All Traces",
                signal: Signal::Traces,
                chart_type: "table",
                lookback_ms: 900_000,
                query: None,
            },
            TemplateViewerSpec {
                name: "Error Traces",
                signal: Signal::Traces,
                chart_type: "table",
                lookback_ms: 900_000,
                query: Some("error"),
            },
            TemplateViewerSpec {
                name: "Trace Volume",
                signal: Signal::Metrics,
                chart_type: "area",
                lookback_ms: 3_600_000,
                query: None,
            },
        ],
        panels: &[
            TemplatePanelSpec {
                viewer_index: 0,
                col_span: 2,
                row_span: 2,
            },
            TemplatePanelSpec {
                viewer_index: 1,
                col_span: 1,
                row_span: 1,
            },
            TemplatePanelSpec {
                viewer_index: 2,
                col_span: 1,
                row_span: 1,
            },
        ],
    },
    TemplateDef {
        id: "log-monitoring",
        name: "Log Monitoring",
        description: "Log exploration with error filtering and volume by severity.",
        columns: 2,
        viewers: &[
            TemplateViewerSpec {
                name: "All Logs",
                signal: Signal::Logs,
                chart_type: "table",
                lookback_ms: 900_000,
                query: None,
            },
            TemplateViewerSpec {
                name: "Error Logs",
                signal: Signal::Logs,
                chart_type: "table",
                lookback_ms: 900_000,
                query: Some("error"),
            },
            TemplateViewerSpec {
                name: "Log Volume by Severity",
                signal: Signal::Metrics,
                chart_type: "stacked_bar",
                lookback_ms: 3_600_000,
                query: None,
            },
        ],
        panels: &[
            TemplatePanelSpec {
                viewer_index: 0,
                col_span: 2,
                row_span: 1,
            },
            TemplatePanelSpec {
                viewer_index: 1,
                col_span: 1,
                row_span: 1,
            },
            TemplatePanelSpec {
                viewer_index: 2,
                col_span: 1,
                row_span: 1,
            },
        ],
    },
];

fn unique_signal_types(viewers: &[TemplateViewerSpec]) -> Vec<&'static str> {
    let mask = viewers
        .iter()
        .fold(SignalMask::NONE, |acc, v| acc | v.signal);
    signal_mask_labels(mask)
}

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
        --sidebar-width: 260px;
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
        width: min(1200px, calc(100% - 48px));
        margin: 0 auto;
        padding: 48px 0 56px;
      }

      #page-viewers,
      #page-dashboard {
        display: grid;
        gap: 24px;
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

      input[type="checkbox"],
      input[type="radio"] {
        width: auto;
        min-height: auto;
        padding: 0;
        border-radius: 4px;
        flex-shrink: 0;
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

      .btn-compact {
        min-height: auto;
        padding: 2px 8px;
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

      .filter-row {
        display: flex;
        gap: 8px;
        align-items: center;
        flex-wrap: wrap;
        width: 100%;
      }

      .filter-row select,
      .filter-row input[type="text"] {
        min-height: 38px;
        padding: 6px 10px;
        border-radius: 12px;
        width: auto;
      }

      .filter-row select {
        min-width: 110px;
        flex-shrink: 0;
      }

      .filter-row input[type="text"] {
        flex: 1;
        min-width: 120px;
      }

      .filter-mode-toggle {
        display: flex;
        border-radius: 999px;
        overflow: hidden;
        border: 1px solid var(--line);
        flex-shrink: 0;
      }

      .filter-mode-toggle button {
        min-height: 36px;
        padding: 0 14px;
        border-radius: 0;
        font-size: 0.82rem;
        background: rgba(255,255,255,0.85);
        color: var(--ink);
        font-weight: 600;
        border: 0;
      }

      .filter-mode-toggle button.active {
        background: var(--teal);
        color: #fff;
      }

      .filter-helper-text {
        font-size: 0.82rem;
        color: var(--muted);
        font-style: italic;
      }

      .filter-badges {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
      }

      .filter-badge {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        font-size: 0.78rem;
        background: var(--teal-soft);
        color: var(--teal);
        border: 1px solid rgba(13,109,98,0.2);
        border-radius: 999px;
        padding: 3px 10px;
        font-weight: 600;
      }

      .filter-badge-mode {
        background: var(--accent-soft);
        color: var(--accent);
        border-color: rgba(183,84,50,0.2);
      }

      .filter-section {
        display: grid;
        gap: 10px;
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
      /* --- Sidebar --- */
      .sidebar {
        position: fixed;
        top: 0;
        left: 0;
        width: var(--sidebar-width);
        height: 100vh;
        background: var(--panel-strong);
        border-right: 1px solid var(--line);
        backdrop-filter: blur(14px);
        display: flex;
        flex-direction: column;
        z-index: 100;
        overflow-y: auto;
      }

      .sidebar-brand {
        padding: 20px 20px 12px;
        font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
        font-size: 1.1rem;
        font-weight: 700;
        color: var(--accent);
        border-bottom: 1px solid var(--line);
      }

      .sidebar-nav {
        padding: 12px 0;
        flex: 1;
      }

      .sidebar-item {
        display: block;
        padding: 9px 20px;
        font-size: 0.9rem;
        color: var(--ink);
        text-decoration: none;
        border-radius: 0;
        cursor: pointer;
        transition: background 0.12s;
      }

      .sidebar-item:hover {
        background: var(--accent-soft);
      }

      .sidebar-item.active {
        background: var(--teal-soft);
        color: var(--teal);
        font-weight: 600;
      }

      .sidebar-section-label {
        padding: 12px 20px 4px;
        font-size: 0.72rem;
        font-weight: 700;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--muted);
      }

      .sidebar-dashboard-item {
        display: block;
        padding: 7px 20px 7px 28px;
        font-size: 0.88rem;
        color: var(--ink);
        text-decoration: none;
        cursor: pointer;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .sidebar-dashboard-item:hover {
        background: var(--accent-soft);
      }

      .sidebar-dashboard-item.active {
        background: var(--teal-soft);
        color: var(--teal);
        font-weight: 600;
      }

      .sidebar-new-btn {
        margin: 8px 20px;
        width: calc(100% - 40px);
        font-size: 0.85rem;
      }

      .sidebar-toggle {
        position: fixed;
        top: 12px;
        left: 12px;
        z-index: 200;
        display: none;
        min-height: 36px;
        padding: 0 12px;
        font-size: 1.1rem;
        line-height: 36px;
      }

      main.with-sidebar {
        margin-left: calc(var(--sidebar-width) + 24px);
        width: min(1200px, calc(100% - var(--sidebar-width) - 72px));
      }

      /* --- Dashboard page --- */
      .dashboard-page-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 16px;
        padding: 8px 0;
      }

      .dashboard-page-title-wrap {
        display: grid;
        gap: 6px;
        flex: 1;
        min-width: 220px;
      }

      .dashboard-page-header h2 {
        font-size: clamp(1.4rem, 2vw, 2rem);
        flex: 1;
        margin: 0;
      }

      .dashboard-refresh-controls {
        justify-content: flex-end;
      }

      .dashboard-refresh-controls label {
        display: grid;
        gap: 6px;
        color: var(--muted);
        font-size: 0.85rem;
      }

      .dashboard-refresh-controls select {
        width: auto;
        min-width: 92px;
      }

      .dashboard-last-updated {
        color: var(--muted);
        font-size: 0.85rem;
      }

      #dashboard-filter-bar {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        width: 100%;
      }

      #dashboard-service-filter {
        min-width: 140px;
        max-width: 240px;
        padding: 4px 8px;
        border-radius: 6px;
        border: 1px solid var(--line);
        background: var(--panel-strong);
        font-size: 0.85rem;
      }

      #dashboard-query-filter {
        flex: 1;
        min-width: 160px;
        padding: 4px 8px;
        border-radius: 6px;
        border: 1px solid var(--line);
        background: var(--panel-strong);
        font-size: 0.85rem;
      }

      #dashboard-filter-tags {
        display: flex;
        gap: 4px;
        flex-wrap: wrap;
      }

      .dashboard-filter-tag {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 2px 8px;
        border-radius: 12px;
        background: var(--accent);
        color: var(--bg-1);
        font-size: 0.78rem;
      }

      #dashboard-range-controls {
        display: flex;
        align-items: center;
        gap: 8px;
      }

      #dashboard-range-selector {
        padding: 4px 8px;
        border-radius: 6px;
        border: 1px solid var(--line);
        background: var(--panel-strong);
        font-size: 0.85rem;
      }

      #dashboard-range-label {
        font-size: 0.85rem;
        color: var(--muted);
        min-width: 50px;
      }

      #dashboard-range-custom-lookback {
        display: flex;
        align-items: center;
        gap: 8px;
      }

      #dashboard-range-custom-lookback input {
        padding: 4px 8px;
        border-radius: 6px;
        border: 1px solid var(--line);
        background: var(--panel-strong);
        font-size: 0.85rem;
      }

      .dashboard-page-actions {
        display: flex;
        align-items: center;
        gap: 8px;
      }

      .dashboard-panel-media {
        max-height: 220px;
      }

      .dashboard-panel-scroll {
        overflow: auto;
        max-height: 220px;
      }

      body.fullscreen #sidebar {
        display: none;
      }

      body.fullscreen main.with-sidebar {
        margin-left: 0;
        width: calc(100vw - 32px);
        max-width: none;
        padding: 0 16px 24px;
      }

      body.fullscreen .dashboard-page-header {
        padding: 8px 0;
        gap: 8px;
      }

      body.fullscreen .dashboard-grid {
        gap: 12px;
      }

      body.fullscreen .dashboard-panel-media,
      body.fullscreen .dashboard-panel-scroll {
        max-height: calc(100vh - 120px);
      }

      .dashboard-grid {
        display: grid;
        grid-template-columns: repeat(var(--dash-cols, 2), 1fr);
        gap: 18px;
        padding-bottom: 44px;
      }

      .dashboard-panel {
        padding: 20px;
        border-radius: 24px;
        border: 1px solid var(--line);
        background: var(--panel);
        box-shadow: var(--shadow);
        backdrop-filter: blur(14px);
        min-height: 200px;
        display: grid;
        gap: 12px;
        align-content: start;
      }

      .dashboard-panel-title {
        margin: 0;
        font-size: 0.95rem;
        font-weight: 700;
        color: var(--accent);
      }

      .dashboard-panel-empty {
        color: var(--muted);
        font-size: 0.85rem;
      }

      .dashboard-panel table {
        width: 100%;
        font-size: 0.82rem;
      }

      @media (max-width: 640px) {
        .sidebar {
          transform: translateX(-100%);
          transition: transform 0.22s ease;
        }

        .sidebar.open {
          transform: translateX(0);
        }

        .sidebar-toggle {
          display: block;
        }

        main.with-sidebar {
          margin-left: 0;
          width: min(1200px, calc(100% - 48px));
        }

        .dashboard-page-header {
          flex-direction: column;
          align-items: stretch;
        }

        .dashboard-refresh-controls {
          justify-content: flex-start;
        }

        .dashboard-grid {
          grid-template-columns: 1fr;
        }
      }

      /* --- Modal --- */
      .modal-overlay {
        position: fixed;
        inset: 0;
        background: rgba(22, 35, 47, 0.38);
        z-index: 300;
        display: flex;
        align-items: center;
        justify-content: center;
      }

      .modal-box {
        background: var(--panel-strong);
        border: 1px solid var(--line);
        border-radius: 20px;
        padding: 28px;
        width: min(480px, calc(100vw - 32px));
        display: grid;
        gap: 16px;
        box-shadow: var(--shadow);
      }

      .modal-box h3 {
        margin: 0;
        font-size: 1.2rem;
      }

      .modal-actions {
        display: flex;
        gap: 10px;
        justify-content: flex-end;
      }

      .viewer-checkbox-list {
        display: grid;
        gap: 6px;
        max-height: 260px;
        overflow-y: auto;
        padding: 4px;
      }

      .viewer-drag-handle {
        cursor: grab;
        color: var(--muted);
        user-select: none;
        font-size: 1rem;
        line-height: 1;
      }

      .viewer-sortable-item {
        display: flex;
        align-items: center;
        gap: 8px;
        font-size: 0.88rem;
        padding: 3px 0;
        border-radius: 6px;
      }

      .viewer-sortable-item.drag-over {
        outline: 2px solid var(--teal);
        background: var(--teal-soft);
      }

      .viewer-sortable-item.dragging {
        opacity: 0.4;
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

      .billboard-widget {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        text-align: center;
        gap: 6px;
      }

      .billboard-value {
        font-size: 48px;
        font-weight: 700;
        font-variant-numeric: tabular-nums;
        color: var(--teal);
      }

      .billboard-value.no-data {
        color: var(--muted);
      }

      .billboard-subtitle {
        font-size: 16px;
        color: var(--muted);
      }

      .billboard-change {
        font-size: 16px;
        font-variant-numeric: tabular-nums;
      }

      .billboard-change.up { color: var(--teal); }
      .billboard-change.down { color: var(--accent); }

      .billboard-widget.compact .billboard-value { font-size: 32px; }
      .billboard-widget.compact .billboard-subtitle { font-size: 12px; }
      .billboard-widget.compact .billboard-change { font-size: 12px; }

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

      .preview-panel {
        background: rgba(240, 246, 244, 0.92);
        gap: 14px;
      }

      .preview-panel-header {
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
      }

      .preview-summary {
        font-size: 0.85rem;
        color: var(--muted);
      }

      .preview-status {
        font-size: 0.8rem;
        color: var(--muted);
        margin-left: auto;
      }

      .preview-content {
        max-height: 300px;
        overflow-y: auto;
      }

      #preview-chart-container {
        max-height: 220px;
      }
    </style>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
  </head>
  <body>
    <button id="sidebar-toggle" class="sidebar-toggle secondary" type="button">&#9776;</button>
    <aside id="sidebar" class="sidebar">
      <div class="sidebar-brand">litelemetry</div>
      <nav class="sidebar-nav">
        <a id="nav-viewers" class="sidebar-item active" href="#viewers" data-page="viewers">Viewers</a>
        <div class="sidebar-section-label">Dashboards</div>
        <div id="dashboard-list"></div>
        <button id="new-dashboard-button" class="secondary sidebar-new-btn" type="button">+ New Dashboard</button>
        <button id="import-dashboard-button" class="secondary sidebar-new-btn" type="button">Import</button>
        <input type="file" id="import-dashboard-file" accept=".json" style="display:none" aria-hidden="true">
      </nav>
    </aside>
    <main class="with-sidebar">
      <div id="page-viewers">
      <section class="toolbar panel panel-strong">
        <div class="toolbar-row">
          <select id="viewer-signal-select" data-testid="viewer-signal-select" name="viewer-signal"
                  aria-label="Signal type" style="min-width:120px;max-width:160px;">
            <option value="traces">Traces</option>
            <option value="metrics">Metrics</option>
            <option value="logs">Logs</option>
          </select>
          <select id="viewer-chart-type-select" data-testid="viewer-chart-type-select" name="viewer-chart-type">
            <option value="table">Table</option>
            <option value="stacked_bar">Stacked Bar</option>
            <option value="line">Line</option>
            <option value="area">Area</option>
            <option value="pie">Pie</option>
            <option value="donut">Donut</option>
            <option value="billboard">Billboard</option>
          </select>
          <input id="viewer-name-input" data-testid="viewer-name-input" name="viewer-name" placeholder="checkout traces" maxlength="80" style="max-width: 200px;" list="viewer-name-datalist" aria-label="Viewer name" />
          <datalist id="viewer-name-datalist"></datalist>
          <button id="create-viewer-button" data-testid="create-viewer-button" class="primary" type="button">+ Create viewer</button>
          <button id="refresh-viewers-button" class="secondary" type="button">Refresh</button>
        </div>
        <div class="toolbar-row" id="filter-builder-row">
          <input id="viewer-query-input" data-testid="viewer-query-input" name="viewer-query"
                 type="search" placeholder="Filter by service name or keyword" maxlength="200"
                 aria-label="Text search query" style="flex:1;min-width:180px;" list="viewer-query-datalist" />
          <datalist id="viewer-query-datalist"></datalist>
          <div class="filter-mode-toggle" id="filter-mode-toggle">
            <button id="filter-mode-and" class="active" type="button" title="All filters must match" aria-pressed="true">AND</button>
            <button id="filter-mode-or" type="button" title="Any filter must match" aria-pressed="false">OR</button>
          </div>
          <div id="filter-rows-container" style="display:contents;"></div>
          <button id="add-filter-button" class="secondary" type="button" data-testid="add-filter-button">+ Add filter</button>
          <button id="preview-viewer-button" data-testid="preview-viewer-button"
                  class="secondary" type="button">Preview</button>
        </div>
        <div id="filter-helper-text" class="filter-helper-text" hidden>Advanced filters are active -- they take priority over the query field.</div>
        <div id="status-box" data-testid="status-box" class="status-box" data-state="working">
          Loading viewers...
        </div>
      </section>

      <section id="viewer-preview-panel" class="panel preview-panel" aria-labelledby="preview-label" hidden>
        <div class="preview-panel-header">
          <span id="preview-label" class="label">Preview</span>
          <span id="viewer-preview-count" class="preview-summary"></span>
          <span id="preview-status" class="preview-status" role="status" aria-live="polite"></span>
          <button id="viewer-preview-close" class="secondary btn-compact" type="button">x</button>
        </div>
        <div class="preview-content">
          <div id="preview-chart-container" hidden>
            <canvas id="preview-chart-canvas"></canvas>
          </div>
          <div id="viewer-preview-entries" class="entries-table-wrap" hidden>
            <table>
              <thead>
                <tr>
                  <th>Time</th><th>Signal</th><th>Service</th><th>Preview</th>
                </tr>
              </thead>
              <tbody id="viewer-preview-entries-body"></tbody>
            </table>
          </div>
          <div id="viewer-preview-traces" class="entries-table-wrap" hidden>
            <table>
              <thead>
                <tr>
                  <th>Trace ID</th><th>Root Span</th><th>Services</th>
                  <th>Spans</th><th>Duration</th><th>Status</th>
                </tr>
              </thead>
              <tbody id="viewer-preview-traces-body"></tbody>
            </table>
          </div>
          <p id="viewer-preview-empty" hidden style="text-align:center; color: var(--muted);">
            No matching entries.
          </p>
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
                <th>Query</th>
                <th>Lookback</th>
                <th>Entries</th>
                <th>Status</th>
                <th aria-label="Actions"></th>
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
        <div class="toolbar-row" style="margin-bottom: 4px;">
          <h3 id="viewer-detail-title">Viewer Detail</h3>
        </div>
        <div id="viewer-detail-query-row" class="toolbar-row" hidden>
          <input id="viewer-detail-query-input" placeholder="Query filter" maxlength="200"
                 aria-label="Query filter"
                 style="flex:1; min-width: 120px;" list="viewer-detail-query-datalist" />
          <datalist id="viewer-detail-query-datalist"></datalist>
          <button id="viewer-detail-query-update" class="secondary btn-compact" type="button">Update</button>
        </div>
        <div id="viewer-filter-section" class="filter-section" hidden>
          <div class="filter-badges" id="viewer-filter-badges"></div>
          <div id="viewer-filter-editor" hidden>
            <div class="toolbar-row" style="margin-top:6px;">
              <input id="detail-query-input" type="text" placeholder="Simple text search (optional)" aria-label="Text search query" style="flex:1;min-width:180px;" list="detail-query-datalist" />
              <datalist id="detail-query-datalist"></datalist>
              <div class="filter-mode-toggle" id="detail-filter-mode-toggle">
                <button id="detail-filter-mode-and" class="active" type="button" title="All filters must match" aria-pressed="true">AND</button>
                <button id="detail-filter-mode-or" type="button" title="Any filter must match" aria-pressed="false">OR</button>
              </div>
            </div>
            <div id="detail-filter-rows-container" style="display:grid;gap:6px;margin-top:8px;"></div>
            <div style="display:flex;gap:8px;margin-top:8px;">
              <button id="detail-add-filter-button" class="secondary" type="button">+ Add filter</button>
              <button id="detail-save-filters-button" class="primary" type="button">Save filters</button>
            </div>
          </div>
          <button id="edit-viewer-filters-button" class="secondary btn-compact" type="button" aria-expanded="false" aria-controls="viewer-filter-editor">Edit filters</button>
        </div>
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
      </div><!-- #page-viewers -->

      <div id="page-dashboard" hidden>
        <div class="dashboard-page-header">
          <div class="dashboard-page-title-wrap">
            <h2 id="dashboard-title"></h2>
            <span id="dashboard-last-updated" class="dashboard-last-updated">Last updated: --</span>
          </div>
          <div id="dashboard-range-controls">
            <select id="dashboard-range-selector">
              <option value="default">Default</option>
              <option value="5m">5m</option>
              <option value="15m">15m</option>
              <option value="1h">1h</option>
              <option value="6h">6h</option>
              <option value="24h">24h</option>
              <option value="custom">Custom</option>
            </select>
            <span id="dashboard-range-label"></span>
            <div id="dashboard-range-custom-lookback" hidden>
              <input type="number" id="dashboard-custom-lookback-minutes" min="1" step="1" inputmode="numeric">
              <span>min</span>
            </div>
          </div>
          <div id="dashboard-filter-bar">
            <select id="dashboard-service-filter" multiple size="1" aria-label="Filter by service">
              <option value="" selected>All services</option>
            </select>
            <div id="dashboard-filter-tags"></div>
            <input id="dashboard-query-filter" type="search" placeholder="Search payload..." maxlength="200" aria-label="Filter by payload content">
            <button id="dashboard-filter-clear" class="secondary btn-compact" type="button" hidden>Clear</button>
          </div>
          <div class="toolbar-row dashboard-refresh-controls">
            <label for="dashboard-refresh-interval">
              Auto refresh
              <select id="dashboard-refresh-interval">
                <option value="off">Off</option>
                <option value="5000">5s</option>
                <option value="10000">10s</option>
                <option value="30000" selected>30s</option>
                <option value="60000">60s</option>
              </select>
            </label>
            <button id="dashboard-refresh-toggle" class="secondary btn-compact" type="button">Pause</button>
            <button id="dashboard-refresh-button" class="secondary btn-compact" type="button">Refresh now</button>
          </div>
          <div id="dashboard-page-actions">
            <button id="dashboard-fullscreen-button" class="secondary" type="button" hidden aria-label="Enter fullscreen" title="Enter fullscreen">[ ]</button>
            <button id="dashboard-settings-button" class="secondary" type="button" hidden>Settings</button>
          </div>
        </div>
        <div id="dashboard-grid" class="dashboard-grid"></div>
      </div><!-- #page-dashboard -->
    </main>

    <script>
      const statusBox = document.getElementById('status-box');
      const viewerSignalSelect = document.getElementById('viewer-signal-select');
      const viewerChartTypeSelect = document.getElementById('viewer-chart-type-select');
      const viewerNameInput = document.getElementById('viewer-name-input');
      const viewerQueryInput = document.getElementById('viewer-query-input');
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
      const previewViewerButton = document.getElementById('preview-viewer-button');
      const viewerPreviewPanel = document.getElementById('viewer-preview-panel');
      const viewerPreviewCount = document.getElementById('viewer-preview-count');
      const viewerPreviewClose = document.getElementById('viewer-preview-close');
      const viewerPreviewEntries = document.getElementById('viewer-preview-entries');
      const viewerPreviewEntriesBody = document.getElementById('viewer-preview-entries-body');
      const viewerPreviewTraces = document.getElementById('viewer-preview-traces');
      const viewerPreviewTracesBody = document.getElementById('viewer-preview-traces-body');
      const viewerPreviewEmpty = document.getElementById('viewer-preview-empty');
      const previewStatusEl = document.getElementById('preview-status');
      const previewChartContainer = document.getElementById('preview-chart-container');
      const previewChartCanvas = document.getElementById('preview-chart-canvas');
      const viewerDetailQueryRow = document.getElementById('viewer-detail-query-row');
      const viewerDetailQueryInput = document.getElementById('viewer-detail-query-input');
      const viewerDetailQueryUpdate = document.getElementById('viewer-detail-query-update');

      // --- Filter builder (create form) ---
      const filterModeAndBtn = document.getElementById('filter-mode-and');
      const filterModeOrBtn = document.getElementById('filter-mode-or');
      const filterRowsContainer = document.getElementById('filter-rows-container');
      const addFilterButton = document.getElementById('add-filter-button');
      const filterHelperText = document.getElementById('filter-helper-text');

      // --- Filter section (viewer detail) ---
      const viewerFilterSection = document.getElementById('viewer-filter-section');
      const viewerFilterBadges = document.getElementById('viewer-filter-badges');
      const viewerFilterEditor = document.getElementById('viewer-filter-editor');
      const detailQueryInput = document.getElementById('detail-query-input');
      const detailFilterModeAndBtn = document.getElementById('detail-filter-mode-and');
      const detailFilterModeOrBtn = document.getElementById('detail-filter-mode-or');
      const detailFilterRowsContainer = document.getElementById('detail-filter-rows-container');
      const detailAddFilterButton = document.getElementById('detail-add-filter-button');
      const detailSaveFiltersButton = document.getElementById('detail-save-filters-button');
      const editViewerFiltersButton = document.getElementById('edit-viewer-filters-button');

      let latestViewers = [];
      let viewerLoadState = 'loading';
      let selectedViewerId = null;
      let currentChart = null;
      let previewChart = null;
      let previewAbortController = null;
      let previewDebounceTimer = null;
      let previewRequestSeq = 0;
      let latestPreviewPayload = null;

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

      function hashStringHue(s) {
        let h = 0;
        for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) | 0;
        return Math.abs(h) % 360;
      }

      const _svcColorCache = {};
      function serviceColor(name) {
        if (_svcColorCache[name]) return _svcColorCache[name];
        _svcColorCache[name] = 'hsl(' + hashStringHue(name) + ', 55%, 50%)';
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

      // --- Filter builder helpers ---

      function makeFilterRow(filter = null, onRemove = null, onChange = null) {
        const row = document.createElement('div');
        row.className = 'filter-row';
        const notifyChange = () => {
          if (onChange) onChange();
        };

        const fieldSel = document.createElement('select');
        fieldSel.setAttribute('aria-label', 'Filter field');
        for (const [val, label] of [['service_name', 'Service name'], ['payload', 'Payload']]) {
          const opt = document.createElement('option');
          opt.value = val;
          opt.textContent = label;
          if (filter && filter.field === val) opt.selected = true;
          fieldSel.appendChild(opt);
        }

        const opSel = document.createElement('select');
        opSel.setAttribute('aria-label', 'Filter operator');
        for (const [val, label] of [['contains', 'contains'], ['eq', 'equals'], ['regex', 'regex']]) {
          const opt = document.createElement('option');
          opt.value = val;
          opt.textContent = label;
          if (filter && filter.op === val) opt.selected = true;
          opSel.appendChild(opt);
        }

        const valueInput = document.createElement('input');
        valueInput.type = 'text';
        valueInput.placeholder = 'value';
        valueInput.setAttribute('aria-label', 'Filter value');
        if (filter && filter.value) valueInput.value = filter.value;

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'secondary btn-compact';
        removeBtn.textContent = '\u00d7';
        removeBtn.title = 'Remove filter';
        removeBtn.setAttribute('aria-label', 'Remove filter');
        removeBtn.addEventListener('click', () => {
          row.remove();
          if (onRemove) onRemove();
          notifyChange();
        });

        fieldSel.addEventListener('change', notifyChange);
        opSel.addEventListener('change', notifyChange);
        valueInput.addEventListener('input', notifyChange);

        row.appendChild(fieldSel);
        row.appendChild(opSel);
        row.appendChild(valueInput);
        attachServiceAutocomplete(valueInput, fieldSel);
        row.appendChild(removeBtn);
        return row;
      }

      function readFiltersFromBuilder(container) {
        const rows = container.querySelectorAll('.filter-row');
        if (rows.length === 0) return null;
        const filters = [];
        for (const row of rows) {
          const selects = row.querySelectorAll('select');
          const input = row.querySelector('input[type="text"]');
          if (!selects[0] || !selects[1] || !input) continue;
          const value = input.value.trim();
          if (!value) continue;
          filters.push({ field: selects[0].value, op: selects[1].value, value });
        }
        return filters.length > 0 ? filters : [];
      }

      function syncCreateFilterHelper() {
        const rows = filterRowsContainer.querySelectorAll('.filter-row');
        filterHelperText.hidden = rows.length === 0;
      }

      function setFilterModeToggle(andBtn, orBtn, mode) {
        andBtn.classList.toggle('active', mode === 'and');
        orBtn.classList.toggle('active', mode === 'or');
        andBtn.setAttribute('aria-pressed', String(mode === 'and'));
        orBtn.setAttribute('aria-pressed', String(mode === 'or'));
      }

      function readFilterMode(andBtn) {
        return andBtn.classList.contains('active') ? 'and' : 'or';
      }

      function wireFilterModeToggle(andBtn, orBtn) {
        andBtn.addEventListener('click', () => setFilterModeToggle(andBtn, orBtn, 'and'));
        orBtn.addEventListener('click', () => setFilterModeToggle(andBtn, orBtn, 'or'));
      }

      function renderFilterBadges(viewer) {
        viewerFilterBadges.replaceChildren();
        const filters = viewer.filters;
        const mode = viewer.filter_mode || 'and';
        const query = viewer.query;

        if ((!filters || filters.length === 0) && !query) {
          viewerFilterSection.hidden = true;
          return;
        }

        viewerFilterSection.hidden = false;

        if (filters && filters.length > 0) {
          const modeEl = document.createElement('span');
          modeEl.className = 'filter-badge filter-badge-mode';
          modeEl.textContent = mode.toUpperCase();
          viewerFilterBadges.appendChild(modeEl);

          for (const f of filters) {
            const badge = document.createElement('span');
            badge.className = 'filter-badge';
            badge.textContent = `${f.field} ${f.op} "${f.value}"`;
            viewerFilterBadges.appendChild(badge);
          }
        } else if (query) {
          const badge = document.createElement('span');
          badge.className = 'filter-badge';
          badge.textContent = `search: "${query}"`;
          viewerFilterBadges.appendChild(badge);
        }
      }

      function hydrateFilterEditor(viewer) {
        detailQueryInput.value = viewer.query || '';
        detailFilterRowsContainer.replaceChildren();

        const filters = viewer.filters;
        const mode = viewer.filter_mode || 'and';
        setFilterModeToggle(detailFilterModeAndBtn, detailFilterModeOrBtn, mode);

        if (Array.isArray(filters)) {
          for (const f of filters) {
            detailFilterRowsContainer.appendChild(makeFilterRow(f));
          }
        }
      }

      async function patchViewerFilters(viewerId) {
        const filters = readFiltersFromBuilder(detailFilterRowsContainer);
        const payload = {
          query: detailQueryInput.value.trim(),
          filters: filters ?? [],
          filter_mode: readFilterMode(detailFilterModeAndBtn),
        };
        setStatus('working', 'Saving filters...');
        try {
          const response = await fetch(`/api/viewers/${viewerId}`, {
            method: 'PATCH',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(payload),
          });
          if (!response.ok) {
            const text = await response.text().catch(() => '');
            throw new Error(`HTTP ${response.status}${text ? ': ' + text : ''}`);
          }
          await refreshViewers({ silent: true });
          await showViewerDetail(viewerId);
          setStatus('ok', 'Filters saved.');
        } catch (error) {
          setStatus('error', `Failed to save filters: ${error.message}`);
        }
      }

      // --- Filter builder event wiring (create form) ---
      wireFilterModeToggle(filterModeAndBtn, filterModeOrBtn);
      addFilterButton.addEventListener('click', () => {
        const row = makeFilterRow(
          null,
          () => {
            syncCreateFilterHelper();
            previewViewer();
          },
          () => previewViewer(),
        );
        filterRowsContainer.appendChild(row);
        syncCreateFilterHelper();
        previewViewer();
      });

      // --- Filter editor event wiring (viewer detail) ---
      wireFilterModeToggle(detailFilterModeAndBtn, detailFilterModeOrBtn);
      detailAddFilterButton.addEventListener('click', () => {
        detailFilterRowsContainer.appendChild(makeFilterRow());
      });
      detailSaveFiltersButton.addEventListener('click', () => {
        if (selectedViewerId) patchViewerFilters(selectedViewerId);
      });
      editViewerFiltersButton.addEventListener('click', () => {
        const hidden = viewerFilterEditor.hidden;
        viewerFilterEditor.hidden = !hidden;
        editViewerFiltersButton.textContent = hidden ? 'Cancel' : 'Edit filters';
        editViewerFiltersButton.setAttribute('aria-expanded', String(hidden));
      });

      function createViewerItem(v, checked) {
        const item = document.createElement('div');
        item.className = 'viewer-sortable-item';
        item.draggable = true;
        item.dataset.viewerId = v.id;
        const handle = document.createElement('span');
        handle.className = 'viewer-drag-handle';
        handle.textContent = '\u22EE';
        handle.setAttribute('aria-hidden', 'true');
        const cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.checked = checked;
        const label = document.createElement('span');
        label.textContent = v.name;
        item.appendChild(handle);
        item.appendChild(cb);
        item.appendChild(label);
        return item;
      }

      function attachSortableListeners(checkList) {
        let dragSrc = null;
        let prevTarget = null;
        checkList.addEventListener('dragstart', e => {
          const item = e.target.closest('.viewer-sortable-item');
          if (!item) return;
          if (e.target.tagName === 'INPUT') { e.preventDefault(); return; }
          dragSrc = item;
          item.classList.add('dragging');
          e.dataTransfer.effectAllowed = 'move';
        });
        checkList.addEventListener('dragend', e => {
          if (dragSrc) dragSrc.classList.remove('dragging');
          if (prevTarget) prevTarget.classList.remove('drag-over');
          dragSrc = null;
          prevTarget = null;
        });
        checkList.addEventListener('dragover', e => {
          e.preventDefault();
          e.dataTransfer.dropEffect = 'move';
          const target = e.target.closest('.viewer-sortable-item');
          if (!target || target === dragSrc || target === prevTarget) return;
          if (prevTarget) prevTarget.classList.remove('drag-over');
          target.classList.add('drag-over');
          prevTarget = target;
        });
        checkList.addEventListener('dragleave', e => {
          if (!checkList.contains(e.relatedTarget)) {
            if (prevTarget) prevTarget.classList.remove('drag-over');
            prevTarget = null;
          }
        });
        checkList.addEventListener('drop', e => {
          e.preventDefault();
          const target = e.target.closest('.viewer-sortable-item');
          if (!target || target === dragSrc || !dragSrc) return;
          checkList.insertBefore(dragSrc, target);
          target.classList.remove('drag-over');
        });
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
      const CHART_TYPE_LABELS = {
        table: 'Table',
        stacked_bar: 'Stacked Bar',
        line: 'Line',
        area: 'Area',
        pie: 'Pie',
        donut: 'Donut',
        billboard: 'Billboard',
      };
      const CIRCULAR_CHART_TYPES = { pie: 'pie', donut: 'doughnut' };
      const MIN_DASHBOARD_COLUMNS = 1;
      const MAX_DASHBOARD_COLUMNS = 4;

      function readViewerPlaceholder(signal) {
        const placeholder = VIEWER_PLACEHOLDERS[signal];
        if (typeof placeholder !== 'string') {
          throw new Error('Viewer create form is missing a valid signal.');
        }
        return placeholder;
      }

      function chartTypeLabel(chartType) {
        const label = CHART_TYPE_LABELS[chartType];
        if (typeof label !== 'string') {
          throw new Error('Unknown chart type.');
        }
        return label;
      }

      function readViewerChartType(viewer) {
        const chartType = viewer.chart_type;
        if (typeof chartType !== 'string' || !(chartType in CHART_TYPE_LABELS)) {
          throw new Error('Viewer payload is missing a valid chart type.');
        }
        return chartType;
      }

      function viewerSupportsMetricCharts(viewer) {
        return Array.isArray(viewer.signals) && viewer.signals.length === 1 && viewer.signals[0] === 'metrics';
      }

      function readDashboardColumns(value) {
        if (!Number.isInteger(value) || value < MIN_DASHBOARD_COLUMNS || value > MAX_DASHBOARD_COLUMNS) {
          throw new Error(`Dashboard columns must be between ${MIN_DASHBOARD_COLUMNS} and ${MAX_DASHBOARD_COLUMNS}.`);
        }
        return value;
      }

      function parseDashboardColumnsInput(input) {
        const value = Number(input.value);
        if (!Number.isInteger(value) || value < MIN_DASHBOARD_COLUMNS || value > MAX_DASHBOARD_COLUMNS) {
          return null;
        }
        return value;
      }

      function readDashboardRefreshInterval(value) {
        if (value === DASHBOARD_REFRESH_OFF_VALUE) {
          return null;
        }

        const interval = Number(value);
        if (!Number.isInteger(interval) || interval <= 0) {
          throw new Error('Dashboard refresh interval must be a positive integer or Off.');
        }
        return interval;
      }

      function formatDashboardRefreshTime(date) {
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
      }

      function setDashboardLastUpdated(dateOrNull) {
        dashboardLastUpdatedLabel.textContent = dateOrNull
          ? `Last updated: ${formatDashboardRefreshTime(dateOrNull)}`
          : 'Last updated: --';
      }

      function stopDashboardRefresh() {
        if (dashboardRefreshTimer !== null) {
          clearInterval(dashboardRefreshTimer);
          dashboardRefreshTimer = null;
        }
        clearTimeout(dashboardQueryDebounceTimer);
        dashboardQueryDebounceTimer = null;
      }

      function updateDashboardRefreshControls() {
        const hasDashboard = currentPage === 'dashboard' && !!currentDashboardId;
        const refreshOff = dashboardRefreshIntervalMs === null;

        dashboardRefreshIntervalSelect.disabled = !hasDashboard;
        dashboardManualRefreshButton.disabled = !hasDashboard;
        dashboardRefreshToggleButton.disabled = !hasDashboard || refreshOff;
        dashboardRefreshToggleButton.textContent = dashboardRefreshPaused ? 'Resume' : 'Pause';
        dashboardRefreshToggleButton.title = refreshOff
          ? 'Auto refresh is off.'
          : dashboardRefreshPaused
            ? 'Resume auto refresh.'
            : 'Pause auto refresh.';
        dashboardRefreshToggleButton.setAttribute('aria-pressed', String(!refreshOff && dashboardRefreshPaused));
      }

      async function refreshActiveDashboard() {
        if (dashboardRefreshRequest) {
          return dashboardRefreshRequest;
        }

        if (currentPage !== 'dashboard' || !currentDashboardId) {
          return false;
        }

        const dashboardId = currentDashboardId;
        dashboardRefreshRequest = (async () => {
          await refreshDashboardList();

          if (currentPage !== 'dashboard' || !currentDashboardId || currentDashboardId !== dashboardId) {
            return false;
          }

          return loadDashboard(currentDashboardId, { refresh: true });
        })();

        try {
          return await dashboardRefreshRequest;
        } finally {
          dashboardRefreshRequest = null;
        }
      }

      function restartDashboardRefresh({ immediate = false } = {}) {
        stopDashboardRefresh();
        updateDashboardRefreshControls();

        if (currentPage !== 'dashboard' || !currentDashboardId || dashboardRefreshPaused || dashboardRefreshIntervalMs === null) {
          return;
        }

        if (immediate) {
          refreshActiveDashboard();
        }

        dashboardRefreshTimer = window.setInterval(() => {
          refreshActiveDashboard();
        }, dashboardRefreshIntervalMs);
      }

      function formatStatus(status) {
        if (!status) return JSON.stringify(status);
        if (status.type === 'ok') return 'ok';
        if (status.type === 'degraded') return `degraded: ${status.reason}`;
        return JSON.stringify(status);
      }

      function getSignal() {
        return viewerSignalSelect.value;
      }

      function syncCreateForm() {
        const signal = getSignal();
        viewerNameInput.placeholder = readViewerPlaceholder(signal);
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

      async function patchViewer(viewerId, payload, successMsg) {
        try {
          const response = await fetch(`/api/viewers/${viewerId}`, {
            method: 'PATCH',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(payload)
          });
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          await refreshViewers({ silent: true });
          if (selectedViewerId === viewerId) {
            await showViewerDetail(viewerId);
          }
          setStatus('ok', successMsg);
        } catch (error) {
          setStatus('error', `Failed to update viewer: ${error.message}`);
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
          if (viewerSupportsMetricCharts(viewer)) {
            const currentChartType = readViewerChartType(viewer);
            const sel = document.createElement('select');
            sel.className = 'chart-type-select';
            for (const [val, label] of Object.entries(CHART_TYPE_LABELS)) {
              const opt = document.createElement('option');
              opt.value = val;
              opt.textContent = label;
              if (val === currentChartType) opt.selected = true;
              sel.appendChild(opt);
            }
            sel.addEventListener('change', (e) => {
              e.stopPropagation();
              patchViewer(viewer.id, { chart_type: sel.value }, `Chart type updated to ${chartTypeLabel(sel.value)}.`);
            });
            sel.addEventListener('click', (e) => e.stopPropagation());
            chartCell.appendChild(sel);
          } else {
            chartCell.textContent = 'table';
          }
          row.appendChild(chartCell);

          appendTableCell(row, viewer.query || '-');
          appendTableCell(row, formatLookbackMs(viewer.lookback_ms));
          appendTableCell(row, String(viewer.entry_count));
          appendTableCell(row, formatStatus(viewer.status));

          const delCell = document.createElement('td');
          const delBtn = document.createElement('button');
          delBtn.className = 'secondary btn-compact';
          delBtn.textContent = 'x';
          delBtn.title = 'Delete viewer';
          delBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            deleteViewer(viewer.id, viewer.name);
          });
          delCell.appendChild(delBtn);
          row.appendChild(delCell);

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

      function metricSeriesLabel(entry) {
        return `${entry.metric_name || 'unknown'} (${entry.service_name || 'unknown'})`;
      }

      function metricChartValue(entry) {
        return typeof entry.metric_value === 'number' && Number.isFinite(entry.metric_value)
          ? entry.metric_value
          : null;
      }

      function chartSeriesColors(series, backgroundAlpha = 0.7) {
        const hue = hashStringHue(series);
        return {
          backgroundColor: `hsla(${hue}, 60%, 55%, ${backgroundAlpha})`,
          borderColor: `hsl(${hue}, 60%, 45%)`,
        };
      }

      function buildChartData(entries, lookbackMs, chartType) {
        const bucketMs = getBucketSizeMs(lookbackMs);
        const grouped = {};
        const allBuckets = new Set();
        const isArea = chartType === 'area';

        for (const entry of entries) {
          const metricValue = metricChartValue(entry);
          if (metricValue === null) continue;
          const key = bucketKey(entry.observed_at, bucketMs);
          allBuckets.add(key);
          const series = metricSeriesLabel(entry);
          if (!grouped[series]) grouped[series] = {};
          grouped[series][key] = (grouped[series][key] || 0) + metricValue;
        }

        const labels = [...allBuckets].sort();
        const datasets = Object.entries(grouped).map(([series, buckets]) => {
          const colors = chartSeriesColors(series);
          return {
            label: series,
            data: labels.map(l => buckets[l] || 0),
            backgroundColor: chartSeriesColors(series, isArea ? 0.3 : 0.7).backgroundColor,
            borderColor: colors.borderColor,
            borderWidth: 1,
            fill: isArea,
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

      function renderBillboard(entries, lookbackMs, container, { compact = false } = {}) {
        const hasValues = entries.some(entry => metricChartValue(entry) !== null);

        let latestSum = 0;
        let previousSum = 0;
        let hasPrevious = false;

        if (hasValues) {
          const { datasets, labels } = buildChartData(entries, lookbackMs, 'line');
          const numBuckets = labels.length;
          hasPrevious = numBuckets >= 2;
          for (const dataset of datasets) {
            latestSum += dataset.data[numBuckets - 1];
            if (hasPrevious) previousSum += dataset.data[numBuckets - 2];
          }
        }

        const metricNames = [...new Set(entries.map(entry => entry.metric_name).filter(Boolean))];
        const subtitle = metricNames.length === 1 ? metricNames[0] : metricNames.length > 1 ? 'Multiple metrics' : '';

        let changePct = null;
        if (hasPrevious && previousSum !== 0) {
          changePct = ((latestSum - previousSum) / previousSum) * 100;
        }

        const widget = document.createElement('div');
        widget.classList.add('billboard-widget');
        if (compact) widget.classList.add('compact');
        widget.setAttribute('role', 'img');

        if (!hasValues) {
          widget.setAttribute('aria-label', subtitle ? `${subtitle}: No data` : 'No data');
        } else {
          const valuePart = subtitle ? `${subtitle}: ${latestSum.toLocaleString()}` : latestSum.toLocaleString();
          const changePart = changePct !== null
            ? `, ${changePct >= 0 ? 'up' : 'down'} ${Math.abs(changePct).toFixed(1)}%`
            : '';
          widget.setAttribute('aria-label', valuePart + changePart);
        }

        const valueEl = makeTextElement('div', hasValues ? latestSum.toLocaleString() : '--', hasValues ? 'billboard-value' : 'billboard-value no-data');
        valueEl.setAttribute('aria-hidden', 'true');
        widget.appendChild(valueEl);

        if (subtitle) {
          const subtitleEl = makeTextElement('div', subtitle, 'billboard-subtitle');
          subtitleEl.setAttribute('aria-hidden', 'true');
          widget.appendChild(subtitleEl);
        }

        if (changePct !== null) {
          const up = changePct >= 0;
          const pctStr = Math.abs(changePct).toFixed(1);
          const changeEl = makeTextElement('div', `${up ? '^' : 'v'} ${pctStr}%`, up ? 'billboard-change up' : 'billboard-change down');
          changeEl.setAttribute('aria-hidden', 'true');
          widget.appendChild(changeEl);
        }

        container.replaceChildren(widget);
      }
      function buildPieData(entries) {
        const grouped = {};

        for (const entry of entries) {
          const metricValue = metricChartValue(entry);
          if (metricValue === null) continue;
          const series = metricSeriesLabel(entry);
          grouped[series] = (grouped[series] || 0) + metricValue;
        }

        const labels = Object.keys(grouped).sort();
        const data = labels.map(label => grouped[label]);
        const total = data.reduce((s, v) => s + v, 0);
        const backgroundColors = [];
        const borderColors = [];
        for (const series of labels) {
          const c = chartSeriesColors(series);
          backgroundColors.push(c.backgroundColor);
          borderColors.push(c.borderColor);
        }

        return {
          labels,
          total,
          datasets: [{
            data,
            backgroundColor: backgroundColors,
            borderColor: borderColors,
            borderWidth: 1,
          }],
        };
      }

      function makeCircularTooltipLabel(total) {
        return function(context) {
          const value = Number(context.raw);
          const percentage = total > 0 ? (value / total) * 100 : 0;
          return `${context.label}: ${value} (${percentage.toFixed(1)}%)`;
        };
      }

      function buildCircularChartOptions(total, legendLabels) {
        const legend = { position: 'bottom' };
        if (legendLabels) {
          legend.labels = legendLabels;
        }

        return {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend,
            tooltip: {
              callbacks: {
                label: makeCircularTooltipLabel(total),
              },
            },
          },
        };
      }

      function isCircularChartType(chartType) {
        return chartType in CIRCULAR_CHART_TYPES;
      }

      function buildCircularChartConfig(chartType, entries, legendLabels) {
        if (!isCircularChartType(chartType)) return null;

        const data = buildPieData(entries);
        if (!data.labels.length) return null;

        return {
          type: CIRCULAR_CHART_TYPES[chartType],
          data,
          options: buildCircularChartOptions(data.total, legendLabels),
        };
      }

      function resolveChartConfig(chartType) {
        const isStacked = chartType === 'stacked_bar';
        return { type: isStacked ? 'bar' : 'line', isStacked };
      }
      function renderChart(chartType, entries, lookbackMs) {
        if (currentChart) {
          currentChart.destroy();
          currentChart = null;
        }

        if (chartType === 'table') {
          viewerChartContainer.hidden = true;
          return;
        }

        if (chartType === 'billboard') {
          viewerChartContainer.hidden = false;
          renderBillboard(entries, lookbackMs, viewerChartContainer);
          return;
        }

        if (!entries.length) {
          viewerChartContainer.hidden = true;
          return;
        }

        viewerChartContainer.replaceChildren(viewerChartCanvas);
        viewerChartContainer.hidden = false;
        if (isCircularChartType(chartType)) {
          const circularConfig = buildCircularChartConfig(chartType, entries);
          if (!circularConfig) {
            viewerChartContainer.hidden = true;
            return;
          }
          currentChart = new Chart(viewerChartCanvas, circularConfig);
          return;
        }

        const data = buildChartData(entries, lookbackMs, chartType);
        if (!data.datasets.length) {
          viewerChartContainer.hidden = true;
          return;
        }
        const { type, isStacked } = resolveChartConfig(chartType);

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

      function renderEntriesTable(entries, body = viewerEntriesBody, container = viewerEntriesTable) {
        body.replaceChildren();
        if (!entries.length) {
          container.hidden = true;
          return;
        }
        container.hidden = false;
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
          body.appendChild(row);
        }
      }

      function hideAllDetailPanels() {
        viewerChartContainer.hidden = true;
        viewerEntriesTable.hidden = true;
        viewerTraceList.hidden = true;
        viewerTraceDetail.hidden = true;
      }

      function renderTraceList(traces, body = viewerTraceListBody, container = viewerTraceList, clickable = true) {
        body.replaceChildren();
        if (!traces || !traces.length) {
          container.hidden = true;
          return;
        }
        container.hidden = false;
        for (const trace of traces) {
          const row = document.createElement('tr');
          if (clickable) row.className = 'viewer-row';
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
          if (clickable) row.addEventListener('click', () => showTraceDetail(trace));
          body.appendChild(row);
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
          const chartType = readViewerChartType(viewer);
          viewerDetailTitle.textContent = `${viewer.name} (${chartType})`;
          viewerDetailQueryInput.value = viewer.query || '';
          viewerDetailQueryRow.hidden = false;
          viewerDetailSection.classList.add('visible');
          hideAllDetailPanels();

          renderFilterBadges(viewer);
          hydrateFilterEditor(viewer);
          viewerFilterEditor.hidden = true;
          editViewerFiltersButton.textContent = 'Edit filters';
          editViewerFiltersButton.setAttribute('aria-expanded', 'false');

          const isTraceViewer = viewer.signals.includes('traces');
          if (chartType !== 'table' && viewerSupportsMetricCharts(viewer)) {
            renderChart(chartType, viewer.entries, viewer.lookback_ms);
          } else if (isTraceViewer && viewer.traces) {
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

      function clearPreviewDebounce() {
        if (previewDebounceTimer) { clearTimeout(previewDebounceTimer); previewDebounceTimer = null; }
      }

      function clearPreviewContent() {
        if (previewChart) { previewChart.destroy(); previewChart = null; }
        previewChartContainer.hidden = true;
        viewerPreviewEntries.hidden = true;
        viewerPreviewTraces.hidden = true;
        viewerPreviewEmpty.hidden = true;
        viewerPreviewEntriesBody.replaceChildren();
        viewerPreviewTracesBody.replaceChildren();
      }

      function showPreviewMessage(message) {
        clearPreviewContent();
        viewerPreviewEmpty.textContent = message;
        viewerPreviewEmpty.hidden = false;
      }

      function cancelPreview() {
        clearPreviewDebounce();
        if (previewAbortController) { previewAbortController.abort(); previewAbortController = null; }
        latestPreviewPayload = null;
        viewerPreviewCount.textContent = '';
        previewStatusEl.textContent = '';
        clearPreviewContent();
        viewerPreviewPanel.hidden = true;
        previewViewerButton.disabled = false;
      }

      function renderPreview(payload) {
        latestPreviewPayload = payload;
        const chartType = viewerChartTypeSelect.value;
        const signal = payload.signal;
        const entries = payload.entries || [];
        const traces = payload.traces || [];
        const filters = readFiltersFromBuilder(filterRowsContainer);
        const hasMatcher = !!viewerQueryInput.value.trim() || filters !== null;
        const shown = signal === 'traces' && traces.length ? traces.length : entries.length;

        viewerPreviewCount.textContent = hasMatcher
          ? `${payload.entry_count} matching entries (showing ${shown})`
          : `${payload.entry_count} entries (showing ${shown})`;
        previewStatusEl.textContent = '';

        clearPreviewContent();

        if (!entries.length && !traces.length) {
          showPreviewMessage('No matching entries.');
          viewerPreviewPanel.hidden = false;
          return;
        }

        if (signal === 'metrics' && chartType !== 'table' && entries.length) {
          previewChartContainer.hidden = false;
          previewChart = renderPanelChart(chartType, entries, 5 * 60 * 1000, previewChartCanvas);
          if (!previewChart) previewChartContainer.hidden = true;
        }
        if (signal === 'traces' && traces.length) {
          renderTraceList(traces, viewerPreviewTracesBody, viewerPreviewTraces, false);
        } else if (entries.length) {
          renderEntriesTable(entries, viewerPreviewEntriesBody, viewerPreviewEntries);
        } else if (previewChartContainer.hidden) {
          showPreviewMessage('No matching entries.');
        }

        viewerPreviewPanel.hidden = false;
      }

      async function fetchPreview() {
        const signalType = getSignal();
        const query = viewerQueryInput.value.trim() || null;
        const filters = readFiltersFromBuilder(filterRowsContainer);
        const seq = ++previewRequestSeq;

        previewAbortController = new AbortController();
        latestPreviewPayload = null;
        previewViewerButton.disabled = true;

        viewerPreviewCount.textContent = '';
        previewStatusEl.textContent = 'Loading...';
        clearPreviewContent();
        viewerPreviewPanel.hidden = false;

        try {
          const body = { signal: signalType };
          if (query) body.query = query;
          if (filters !== null) {
            body.filters = filters;
            body.filter_mode = readFilterMode(filterModeAndBtn);
          }
          const resp = await fetch('/api/viewers/preview', {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'accept': 'application/json' },
            body: JSON.stringify(body),
            signal: previewAbortController.signal,
          });
          if (seq !== previewRequestSeq) return;
          if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
          const payload = await resp.json();
          if (seq !== previewRequestSeq) return;
          renderPreview(payload);
        } catch (err) {
          if (err.name === 'AbortError') return;
          if (seq !== previewRequestSeq) return;
          latestPreviewPayload = null;
          viewerPreviewCount.textContent = '';
          previewStatusEl.textContent = `Error: ${err.message}`;
          showPreviewMessage('Preview unavailable.');
        } finally {
          if (seq === previewRequestSeq) previewAbortController = null;
          previewViewerButton.disabled = false;
        }
      }

      function previewViewer(immediate = false) {
        clearPreviewDebounce();
        if (previewAbortController) previewAbortController.abort();
        if (immediate) {
          fetchPreview();
        } else {
          previewDebounceTimer = setTimeout(fetchPreview, 500);
        }
      }

      async function createViewer() {
        const name = viewerNameInput.value.trim();
        const signal = getSignal();
        const chart_type = viewerChartTypeSelect.value;
        const query = viewerQueryInput.value.trim() || null;
        if (!name) {
          setStatus('error', 'Viewer name is required.');
          viewerNameInput.focus();
          return;
        }

        const filters = readFiltersFromBuilder(filterRowsContainer);

        createViewerButton.disabled = true;
        setStatus('working', `Creating ${signal} viewer "${name}"...`);

        try {
          const body = { name, signal, chart_type };
          if (query) body.query = query;
          if (filters !== null) { body.filters = filters; body.filter_mode = readFilterMode(filterModeAndBtn); }
          const response = await fetch('/api/viewers', {
            method: 'POST',
            headers: {
              'content-type': 'application/json',
              'accept': 'application/json'
            },
            body: JSON.stringify(body)
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

      async function deleteViewer(viewerId, viewerName) {
        if (!confirm(`Delete viewer "${viewerName}"?`)) return;
        setStatus('working', 'Deleting viewer...');
        try {
          const resp = await fetch(`/api/viewers/${viewerId}`, { method: 'DELETE' });
          if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
          if (selectedViewerId === viewerId) {
            selectedViewerId = null;
            viewerDetailSection.classList.remove('visible');
          }
          setStatus('ok', 'Viewer deleted');
          await refreshViewers();
        } catch (err) {
          setStatus('error', `Delete failed: ${err.message}`);
        }
      }

      createViewerButton.addEventListener('click', createViewer);
      previewViewerButton.addEventListener('click', () => previewViewer(true));
      viewerDetailQueryUpdate.addEventListener('click', () => {
        const query = viewerDetailQueryInput.value.trim();
        patchViewer(selectedViewerId, { query }, query ? 'Query filter updated.' : 'Query filter cleared.');
      });
      viewerPreviewClose.addEventListener('click', cancelPreview);
      refreshViewersButton.addEventListener('click', () => {
        refreshViewers();
        previewViewer(true);
      });
      viewerSignalSelect.addEventListener('change', () => {
        syncCreateForm();
        previewViewer();
      });
      viewerQueryInput.addEventListener('input', () => previewViewer());
      viewerQueryInput.addEventListener('keydown', event => {
        if (event.key === 'Enter') {
          event.preventDefault();
          previewViewer(true);
        }
      });
      viewerChartTypeSelect.addEventListener('change', () => {
        if (latestPreviewPayload) {
          renderPreview(latestPreviewPayload);
        } else {
          previewViewer(true);
        }
      });
      filterModeAndBtn.addEventListener('click', () => previewViewer());
      filterModeOrBtn.addEventListener('click', () => previewViewer());
      viewerNameInput.addEventListener('keydown', event => {
        if (event.key === 'Enter') {
          event.preventDefault();
          createViewer();
        }
      });

      syncCreateForm();
      previewViewer(true);
      refreshViewers({ silent: true });
      window.setInterval(() => refreshViewers({ silent: true }), 5000);
      loadServiceNamesForAutocomplete();

      // --- Sidebar & Navigation --------------------------------------------
      const sidebarToggle = document.getElementById('sidebar-toggle');
      const sidebar = document.getElementById('sidebar');
      const navViewers = document.getElementById('nav-viewers');
      const newDashboardButton = document.getElementById('new-dashboard-button');
      const dashboardListEl = document.getElementById('dashboard-list');
      const pageViewers = document.getElementById('page-viewers');
      const pageDashboard = document.getElementById('page-dashboard');
      const dashboardTitle = document.getElementById('dashboard-title');
      const dashboardGrid = document.getElementById('dashboard-grid');
      const dashboardRefreshIntervalSelect = document.getElementById('dashboard-refresh-interval');
      const dashboardRefreshToggleButton = document.getElementById('dashboard-refresh-toggle');
      const dashboardLastUpdatedLabel = document.getElementById('dashboard-last-updated');
      const dashboardManualRefreshButton = document.getElementById('dashboard-refresh-button');
      const dashboardSettingsButton = document.getElementById('dashboard-settings-button');
      const dashboardFullscreenButton = document.getElementById('dashboard-fullscreen-button');

      let currentPage = 'viewers';
      let currentDashboardId = null;
      let cachedServiceNames = [];
      let filterValDatalistSeq = 0;
      let dashboardPanelCharts = [];
      const DASHBOARD_REFRESH_OFF_VALUE = 'off';
      let dashboardRefreshIntervalMs = 30000;
      let dashboardRefreshRequest = null;
      let dashboardRefreshTimer = null;
      let dashboardRefreshPaused = false;
      let dashboardLoadRequestSeq = 0;

      const LOOKBACK_PRESETS = {
        'default': null,
        '5m': 5 * 60 * 1000,
        '15m': 15 * 60 * 1000,
        '1h': 60 * 60 * 1000,
        '6h': 6 * 60 * 60 * 1000,
        '24h': 24 * 60 * 60 * 1000,
        'custom': null,
      };
      let dashboardRangeMode = 'default';
      let dashboardLookbackMs = null;
      let dashboardMaxLookbackMs = null;
      let dashboardCustomLookbackMinutes = '';
      let dashboardServiceFilter = [];
      let dashboardQueryFilter = '';
      let dashboardQueryDebounceTimer = null;

      const dashboardRangeSelect = document.getElementById('dashboard-range-selector');
      const dashboardRangeLabel = document.getElementById('dashboard-range-label');
      const dashboardRangeCustomLookback = document.getElementById('dashboard-range-custom-lookback');
      const dashboardCustomLookbackInput = document.getElementById('dashboard-custom-lookback-minutes');
      const dashboardServiceFilterSelect = document.getElementById('dashboard-service-filter');
      const dashboardQueryFilterInput = document.getElementById('dashboard-query-filter');
      const dashboardFilterClearButton = document.getElementById('dashboard-filter-clear');
      const dashboardFilterTags = document.getElementById('dashboard-filter-tags');

      sidebarToggle.addEventListener('click', () => {
        sidebar.classList.toggle('open');
      });

      document.addEventListener('click', (e) => {
        if (sidebar.classList.contains('open') &&
            !sidebar.contains(e.target) &&
            e.target !== sidebarToggle) {
          sidebar.classList.remove('open');
        }
      });

      function buildDashboardHash(id, lookbackMs, serviceNames, query) {
        const params = new URLSearchParams();
        if (lookbackMs) params.set('lookback', String(lookbackMs));
        if (serviceNames && serviceNames.length) params.set('service', serviceNames.join(','));
        if (query) params.set('q', query);
        const qs = params.toString();
        return qs ? `#dashboard/${id}?${qs}` : `#dashboard/${id}`;
      }

      function parseDashboardHash() {
        const hash = window.location.hash;
        const match = hash.match(/^#dashboard\/([^/?]+)(\?.*)?$/);
        if (!match) return null;
        const id = match[1];
        const params = new URLSearchParams(match[2] ? match[2].slice(1) : '');
        const lookbackMs = params.has('lookback') ? (parseInt(params.get('lookback'), 10) || null) : null;
        const serviceNames = params.has('service') ? params.get('service').split(',').filter(Boolean) : [];
        const query = params.get('q') || '';
        return { id, lookbackMs, serviceNames, query };
      }

      function dashboardRangeModeFromLookbackMs(lookbackMs) {
        if (!lookbackMs) {
          return 'default';
        }
        for (const [mode, presetMs] of Object.entries(LOOKBACK_PRESETS)) {
          if (mode !== 'default' && mode !== 'custom' && presetMs === lookbackMs) {
            return mode;
          }
        }
        return 'custom';
      }

      function syncDashboardRangeControls() {
        for (const option of dashboardRangeSelect.options) {
          if (option.value === 'default' || option.value === 'custom') {
            option.hidden = false;
            option.disabled = false;
            continue;
          }
          const presetMs = LOOKBACK_PRESETS[option.value];
          const unavailable =
            dashboardMaxLookbackMs !== null && presetMs > dashboardMaxLookbackMs;
          option.hidden = unavailable;
          option.disabled = unavailable;
        }

        dashboardRangeSelect.value = dashboardRangeMode;

        if (dashboardRangeMode === 'custom') {
          dashboardRangeCustomLookback.hidden = false;
          if (dashboardCustomLookbackMinutes) {
            dashboardCustomLookbackInput.value = dashboardCustomLookbackMinutes;
          } else if (dashboardLookbackMs) {
            dashboardCustomLookbackInput.value = String(Math.ceil(dashboardLookbackMs / 60000));
          } else {
            dashboardCustomLookbackInput.value = '';
          }
        } else {
          dashboardRangeCustomLookback.hidden = true;
          dashboardCustomLookbackInput.value = '';
        }

        if (dashboardLookbackMs) {
          dashboardRangeLabel.textContent = formatLookbackMs(dashboardLookbackMs);
        } else {
          dashboardRangeLabel.textContent = '';
        }
      }

      function applyDashboardRangeSelection() {
        dashboardRangeMode = dashboardRangeSelect.value;
        let nextLookbackMs = null;
        let nextCustomLookbackMinutes = '';

        if (dashboardRangeMode === 'custom') {
          const candidateMinutes =
            dashboardCustomLookbackInput.value ||
            dashboardCustomLookbackMinutes ||
            (dashboardLookbackMs ? String(Math.ceil(dashboardLookbackMs / 60000)) : '');
          const minutes = Number.parseInt(candidateMinutes, 10);
          if (!Number.isFinite(minutes) || minutes <= 0) {
            setStatus('error', 'Enter a positive custom lookback in minutes.');
            dashboardCustomLookbackMinutes = candidateMinutes;
            syncDashboardRangeControls();
            return false;
          }
          nextLookbackMs = minutes * 60 * 1000;
          nextCustomLookbackMinutes = String(minutes);
        } else if (dashboardRangeMode !== 'default') {
          nextLookbackMs = LOOKBACK_PRESETS[dashboardRangeMode];
        }

        if (
          dashboardMaxLookbackMs !== null &&
          nextLookbackMs !== null &&
          nextLookbackMs > dashboardMaxLookbackMs
        ) {
          setStatus(
            'error',
            `Lookback exceeds dashboard limit (${formatLookbackMs(dashboardMaxLookbackMs)}).`,
          );
          return false;
        }

        dashboardLookbackMs = nextLookbackMs;
        dashboardCustomLookbackMinutes = nextCustomLookbackMinutes;
        syncDashboardRangeControls();
        return true;
      }

      function navigateTo(page, dashboardId) {
        stopDashboardRefresh();
        cancelPreview();
        currentPage = page;
        currentDashboardId = dashboardId || null;

        pageViewers.hidden = page !== 'viewers';
        pageDashboard.hidden = page !== 'dashboard';

        navViewers.classList.toggle('active', page === 'viewers');

        document.querySelectorAll('.sidebar-dashboard-item').forEach(el => {
          el.classList.toggle('active', el.dataset.id === dashboardId);
        });

        updateDashboardRefreshControls();

        if (page === 'dashboard' && dashboardId) {
          setDashboardLastUpdated(null);
          const hashData = parseDashboardHash();
          const hashMatch = hashData != null && hashData.id === dashboardId;
          dashboardLookbackMs = hashMatch ? hashData.lookbackMs : null;
          dashboardMaxLookbackMs = null;
          dashboardRangeMode = dashboardRangeModeFromLookbackMs(dashboardLookbackMs);
          dashboardCustomLookbackMinutes =
            dashboardRangeMode === 'custom' && dashboardLookbackMs
              ? String(Math.ceil(dashboardLookbackMs / 60000))
              : '';
          dashboardServiceFilter = hashMatch ? hashData.serviceNames : [];
          dashboardQueryFilter = hashMatch ? hashData.query : '';
          dashboardQueryFilterInput.value = dashboardQueryFilter;
          syncServiceFilterSelectToDOM();
          syncDashboardRangeControls();
          updateFilterTags();
          fetchAndPopulateServices();
          loadDashboard(dashboardId);
          restartDashboardRefresh();
        }
        if (page === 'viewers') {
          previewViewer(true);
        }
        sidebar.classList.remove('open');

        if (page !== 'dashboard' && document.body.classList.contains('fullscreen')) {
          exitDashboardFullscreen();
        }
      }

      function resizeDashboardPanelCharts() {
        for (const chart of dashboardPanelCharts) {
          try { chart.resize(); } catch (_) {}
        }
      }

      function applyFullscreenLayout(enabled) {
        document.body.classList.toggle('fullscreen', enabled);
        dashboardFullscreenButton.textContent = enabled ? 'X' : '[ ]';
        dashboardFullscreenButton.setAttribute('aria-label', enabled ? 'Exit fullscreen' : 'Enter fullscreen');
        if (dashboardPanelCharts.length) requestAnimationFrame(resizeDashboardPanelCharts);
      }

      function enterDashboardFullscreen() {
        document.documentElement.requestFullscreen().catch(() => {});
      }

      function exitDashboardFullscreen() {
        document.exitFullscreen().catch(() => {});
      }

      function toggleDashboardFullscreen() {
        document.fullscreenElement ? exitDashboardFullscreen() : enterDashboardFullscreen();
      }

      document.addEventListener('fullscreenchange', () => {
        applyFullscreenLayout(!!document.fullscreenElement);
      });

      dashboardFullscreenButton.addEventListener('click', toggleDashboardFullscreen);

      dashboardRefreshIntervalSelect.addEventListener('change', () => {
        try {
          dashboardRefreshIntervalMs = readDashboardRefreshInterval(dashboardRefreshIntervalSelect.value);
        } catch (err) {
          setStatus('error', `Invalid dashboard refresh interval: ${err.message}`);
          dashboardRefreshIntervalSelect.value = dashboardRefreshIntervalMs === null
            ? DASHBOARD_REFRESH_OFF_VALUE
            : String(dashboardRefreshIntervalMs);
          return;
        }

        dashboardRefreshPaused = false;
        updateDashboardRefreshControls();
        restartDashboardRefresh({ immediate: true });
      });

      navViewers.addEventListener('click', (e) => {
        e.preventDefault();
        navigateTo('viewers');
      });

      dashboardRefreshToggleButton.addEventListener('click', () => {
        if (dashboardRefreshIntervalMs === null) {
          return;
        }

        dashboardRefreshPaused = !dashboardRefreshPaused;
        updateDashboardRefreshControls();

        if (dashboardRefreshPaused) {
          stopDashboardRefresh();
        } else {
          restartDashboardRefresh({ immediate: true });
        }
      });

      dashboardManualRefreshButton.addEventListener('click', () => {
        refreshActiveDashboard();
      });


      // --- Dashboard list in sidebar ---------------------------------------

      async function refreshDashboardList() {
        try {
          const resp = await fetch('/api/dashboards', { headers: { accept: 'application/json' } });
          if (!resp.ok) return;
          const data = await resp.json();
          renderDashboardList(data.dashboards);
        } catch (_) { /* silent */ }
      }

      function renderDashboardList(dashboards) {
        dashboardListEl.replaceChildren();
        for (const d of dashboards) {
          const a = document.createElement('a');
          a.className = 'sidebar-dashboard-item';
          a.href = buildDashboardHash(d.id);
          a.dataset.id = d.id;
          a.textContent = d.name;
          if (d.id === currentDashboardId) a.classList.add('active');
          dashboardListEl.appendChild(a);
        }
      }

      // --- Dashboard detail ------------------------------------------------

      function destroyPanelCharts() {
        for (const c of dashboardPanelCharts) {
          try { c.destroy(); } catch (_) {}
        }
        dashboardPanelCharts = [];
      }

      async function loadDashboard(id, { refresh = false } = {}) {
        const seq = ++dashboardLoadRequestSeq;

        if (!refresh) {
          destroyPanelCharts();
          dashboardGrid.replaceChildren();
          dashboardTitle.textContent = 'Loading...';
          dashboardSettingsButton.hidden = true;
          dashboardFullscreenButton.hidden = true;
        }

        try {
          const url = new URL(`/api/dashboards/${id}`, window.location.origin);
          const params = new URLSearchParams();
          if (dashboardLookbackMs) params.set('lookback_ms', String(dashboardLookbackMs));
          if (dashboardServiceFilter.length) params.set('service_name', dashboardServiceFilter.join(','));
          if (dashboardQueryFilter) params.set('query', dashboardQueryFilter);
          url.search = params.toString();
          const resp = await fetch(url.toString(), { headers: { accept: 'application/json' } });
          if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
          const data = await resp.json();
          dashboardMaxLookbackMs = data.max_lookback_ms ?? null;
          syncDashboardRangeControls();

          if (seq !== dashboardLoadRequestSeq || currentDashboardId !== id) {
            return false;
          }

          const sorted = [...data.panels].sort((a, b) => a.position - b.position);
          const columns = readDashboardColumns(data.columns);

          destroyPanelCharts();
          dashboardGrid.replaceChildren();
          dashboardTitle.textContent = data.name;
          dashboardSettingsButton.hidden = false;
          dashboardFullscreenButton.hidden = false;
          dashboardGrid.style.setProperty('--dash-cols', String(columns));

          for (const panel of sorted) {
            dashboardGrid.appendChild(buildPanelEl(panel));
          }

          if (!sorted.length) {
            const empty = document.createElement('div');
            empty.className = 'dashboard-panel';
            empty.innerHTML = '<p class="dashboard-panel-empty">No viewers in this dashboard yet. Use Settings to add viewers.</p>';
            dashboardGrid.appendChild(empty);
          }

          setDashboardLastUpdated(new Date());
          updateDashboardRefreshControls();
          return true;
        } catch (err) {
          if (seq !== dashboardLoadRequestSeq || currentDashboardId !== id) {
            return false;
          }

          if (err.message === 'HTTP 400' && dashboardLookbackMs !== null) {
            dashboardRangeMode = 'default';
            dashboardLookbackMs = null;
            dashboardMaxLookbackMs = null;
            dashboardCustomLookbackMinutes = '';
            syncDashboardRangeControls();
            updateDashboardHash();
            loadDashboard(id, { refresh });
            return;
          }

          if (!refresh) {
            dashboardTitle.textContent = 'Error';
            setDashboardLastUpdated(null);
            const errEl = document.createElement('div');
            errEl.className = 'dashboard-panel';
            const msgEl = document.createElement('p');
            msgEl.className = 'dashboard-panel-empty';
            msgEl.textContent = `Failed to load: ${err.message}`;
            errEl.appendChild(msgEl);
            dashboardGrid.appendChild(errEl);
          }
        }

        return false;
      }

      function buildPanelEl(panel) {
        const div = document.createElement('div');
        div.className = 'dashboard-panel';

        const title = document.createElement('h4');
        title.className = 'dashboard-panel-title';

        if (!panel.viewer) {
          title.textContent = truncateId(panel.viewer_id);
          div.appendChild(title);
          const msg = document.createElement('p');
          msg.className = 'dashboard-panel-empty';
          msg.textContent = 'Viewer not found.';
          div.appendChild(msg);
          return div;
        }

        const v = panel.viewer;
        title.textContent = v.name;
        div.appendChild(title);

        const chartType = readViewerChartType(v);
        const isTraces = v.signals.includes('traces');
        const supportsMetricCharts = viewerSupportsMetricCharts(v);

        if (chartType === 'billboard' && supportsMetricCharts) {
          const host = document.createElement('div');
          host.classList.add('dashboard-panel-media');
          div.appendChild(host);
          renderBillboard(v.entries, v.lookback_ms, host, { compact: true });
        } else if (chartType !== 'table' && supportsMetricCharts && v.entries.length) {
          const canvas = document.createElement('canvas');
          canvas.classList.add('dashboard-panel-media');
          div.appendChild(canvas);
          const chart = renderPanelChart(chartType, v.entries, v.lookback_ms, canvas);
          if (chart) dashboardPanelCharts.push(chart);
        } else if (isTraces && v.traces && v.traces.length) {
          renderPanelTraceTable(div, v.traces);
        } else {
          renderPanelEntriesTable(div, v.entries);
        }

        return div;
      }

      function renderPanelChart(chartType, entries, lookbackMs, canvas) {
        if (isCircularChartType(chartType)) {
          const circularConfig = buildCircularChartConfig(chartType, entries, {
            boxWidth: 10,
            font: { size: 10 },
          });
          return circularConfig ? new Chart(canvas, circularConfig) : null;
        }

        const data = buildChartData(entries, lookbackMs, chartType);
        if (!data.datasets.length) return null;
        const { type, isStacked } = resolveChartConfig(chartType);
        return new Chart(canvas, {
          type,
          data,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
            scales: {
              x: { stacked: isStacked, ticks: { font: { size: 9 } } },
              y: { stacked: isStacked, beginAtZero: true, ticks: { font: { size: 9 } } },
            },
          },
        });
      }

      function buildScrollablePanelTable(container, headHtml, renderRows) {
        const wrap = document.createElement('div');
        wrap.classList.add('dashboard-panel-scroll');
        const table = document.createElement('table');
        table.style.width = '100%';
        table.style.fontSize = '0.8rem';
        const thead = document.createElement('thead');
        thead.innerHTML = headHtml;
        const tbody = document.createElement('tbody');
        renderRows(tbody);
        table.appendChild(thead);
        table.appendChild(tbody);
        wrap.appendChild(table);
        container.appendChild(wrap);
      }

      function renderPanelTraceTable(container, traces) {
        if (!traces.length) {
          const p = document.createElement('p');
          p.className = 'dashboard-panel-empty';
          p.textContent = 'No traces.';
          container.appendChild(p);
          return;
        }
        buildScrollablePanelTable(container,
          '<tr><th>Trace ID</th><th>Root Span</th><th>Spans</th><th>Status</th></tr>',
          (tbody) => {
            for (const t of traces.slice(0, 20)) {
              const row = document.createElement('tr');
              if (t.has_error) row.style.background = 'var(--danger-soft)';
              appendTableCell(row, truncateId(t.trace_id));
              appendTableCell(row, t.root_span_name || '-');
              appendTableCell(row, String(t.span_count));
              const sc = document.createElement('td');
              sc.textContent = t.has_error ? 'error' : 'ok';
              if (t.has_error) sc.className = 'error-inline';
              row.appendChild(sc);
              tbody.appendChild(row);
            }
          });
      }

      function renderPanelEntriesTable(container, entries) {
        if (!entries.length) {
          const p = document.createElement('p');
          p.className = 'dashboard-panel-empty';
          p.textContent = 'No entries.';
          container.appendChild(p);
          return;
        }
        buildScrollablePanelTable(container,
          '<tr><th>Time</th><th>Signal</th><th>Service</th><th>Preview</th></tr>',
          (tbody) => {
            for (const e of entries.slice(0, 20)) {
              const row = document.createElement('tr');
              appendTableCell(row, new Date(e.observed_at).toLocaleTimeString());
              appendTableCell(row, e.signal);
              appendTableCell(row, e.service_name || '-');
              const td = document.createElement('td');
              const code = document.createElement('code');
              code.style.fontSize = '0.75rem';
              code.textContent = e.payload_preview;
              td.appendChild(code);
              row.appendChild(td);
              tbody.appendChild(row);
            }
          });
      }

      // --- Dashboard settings modal ----------------------------------------

      dashboardSettingsButton.addEventListener('click', () => {
        if (!currentDashboardId) return;
        openDashboardSettings(currentDashboardId);
      });

      dashboardRangeSelect.addEventListener('change', () => {
        if (!applyDashboardRangeSelection()) {
          return;
        }
        if (currentPage === 'dashboard' && currentDashboardId) {
          updateDashboardHash();
          loadDashboard(currentDashboardId);
        }
      });

      function onCustomLookbackChange() {
        if (currentPage === 'dashboard' && currentDashboardId) {
          if (!applyDashboardRangeSelection()) {
            return;
          }
          updateDashboardHash();
          loadDashboard(currentDashboardId);
        }
      }
      dashboardCustomLookbackInput.addEventListener('change', onCustomLookbackChange);

      function updateDashboardHash() {
        const hash = buildDashboardHash(currentDashboardId, dashboardLookbackMs, dashboardServiceFilter, dashboardQueryFilter);
        window.history.replaceState(null, '', hash);
      }

      async function fetchServices() {
        try {
          const resp = await fetch('/api/services', { headers: { accept: 'application/json' } });
          if (!resp.ok) return null;
          const { services } = await resp.json();
          return services;
        } catch (_) { return null; /* best-effort: service list fetch failure is non-critical */ }
      }

      async function fetchAndPopulateServices() {
        const services = await fetchServices();
        if (!services) return;
        const existing = new Set(
          [...dashboardServiceFilterSelect.options].map(o => o.value).filter(Boolean)
        );
        for (const svc of services) {
          if (!existing.has(svc)) {
            const opt = document.createElement('option');
            opt.value = svc;
            opt.textContent = svc;
            opt.selected = dashboardServiceFilter.includes(svc);
            dashboardServiceFilterSelect.appendChild(opt);
          }
        }
      }

      async function loadServiceNamesForAutocomplete() {
        const services = await fetchServices();
        if (!services) return;
        cachedServiceNames = services;
        populateViewerNameDatalist();
        for (const id of ['viewer-query-datalist', 'detail-query-datalist', 'viewer-detail-query-datalist']) {
          fillDatalist(document.getElementById(id), cachedServiceNames);
        }
      }

      function fillDatalist(dl, values) {
        const frag = document.createDocumentFragment();
        for (const v of values) {
          const opt = document.createElement('option');
          opt.value = v;
          frag.appendChild(opt);
        }
        dl.replaceChildren(frag);
      }

      function populateViewerNameDatalist() {
        const dl = document.getElementById('viewer-name-datalist');
        const signals = ['traces', 'logs', 'metrics'];
        const values = cachedServiceNames.flatMap(svc => signals.map(sig => `${svc} ${sig}`));
        fillDatalist(dl, values);
      }

      function attachServiceAutocomplete(valueInput, fieldSel) {
        const datalistId = `filter-val-datalist-${filterValDatalistSeq++}`;
        const dl = document.createElement('datalist');
        dl.id = datalistId;
        valueInput.setAttribute('list', datalistId);
        valueInput.parentNode.appendChild(dl);

        function updateDatalist() {
          if (fieldSel.value === 'service_name') {
            fillDatalist(dl, cachedServiceNames);
          } else {
            dl.replaceChildren();
          }
        }
        updateDatalist();
        fieldSel.addEventListener('change', updateDatalist);
      }

      function syncServiceFilterSelectToDOM() {
        for (const opt of dashboardServiceFilterSelect.options) {
          opt.selected = dashboardServiceFilter.includes(opt.value);
        }
      }

      function updateFilterTags() {
        dashboardFilterTags.replaceChildren();
        for (const svc of dashboardServiceFilter) {
          const tag = document.createElement('span');
          tag.className = 'dashboard-filter-tag';
          tag.textContent = svc;
          dashboardFilterTags.appendChild(tag);
        }
        const hasFilter = dashboardServiceFilter.length > 0 || dashboardQueryFilter;
        dashboardFilterClearButton.hidden = !hasFilter;
      }

      function applyDashboardFilter() {
        updateFilterTags();
        updateDashboardHash();
        if (currentDashboardId) loadDashboard(currentDashboardId);
      }

      dashboardServiceFilterSelect.addEventListener('change', () => {
        dashboardServiceFilter = Array.from(dashboardServiceFilterSelect.selectedOptions)
          .map(o => o.value)
          .filter(Boolean);
        applyDashboardFilter();
      });

      dashboardQueryFilterInput.addEventListener('input', () => {
        clearTimeout(dashboardQueryDebounceTimer);
        dashboardQueryDebounceTimer = setTimeout(() => {
          dashboardQueryFilter = dashboardQueryFilterInput.value.trim();
          applyDashboardFilter();
        }, 500);
      });

      dashboardFilterClearButton.addEventListener('click', () => {
        dashboardServiceFilter = [];
        dashboardQueryFilter = '';
        dashboardQueryFilterInput.value = '';
        syncServiceFilterSelectToDOM();
        applyDashboardFilter();
      });

      async function exportDashboard(dashboardId, dashboardName) {
        try {
          const resp = await fetch(`/api/dashboards/${dashboardId}/export`, {
            headers: { accept: 'application/json' },
          });
          if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
          const data = await resp.json();
          const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url;
          a.download = `${dashboardName.replace(/[^a-z0-9]/gi, '-').toLowerCase()}.json`;
          document.body.appendChild(a);
          a.click();
          document.body.removeChild(a);
          URL.revokeObjectURL(url);
        } catch (err) {
          setStatus('error', `Export failed: ${err.message}`);
        }
      }

      async function openDashboardSettings(dashboardId) {
        // Fetch current dashboard and all viewers
        const [dashResp, viewersResp] = await Promise.all([
          fetch(`/api/dashboards/${dashboardId}`, { headers: { accept: 'application/json' } }),
          fetch('/api/viewers', { headers: { accept: 'application/json' } }),
        ]);
        if (!dashResp.ok || !viewersResp.ok) return;
        const dash = await dashResp.json();
        const { viewers } = await viewersResp.json();
        const currentViewerIds = new Set(dash.panels.map(p => p.viewer_id));

        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';

        const box = document.createElement('div');
        box.className = 'modal-box';
        box.innerHTML = `<h3>Dashboard Settings</h3>`;

        const nameLabel = document.createElement('label');
        nameLabel.textContent = 'Name';
        const nameInput = document.createElement('input');
        nameInput.value = dash.name;
        nameInput.maxLength = 80;
        nameLabel.appendChild(nameInput);
        box.appendChild(nameLabel);

        const colLabel = document.createElement('label');
        colLabel.textContent = 'Columns';
        const colInput = document.createElement('input');
        colInput.type = 'number';
        colInput.min = '1';
        colInput.max = '4';
        colInput.value = String(readDashboardColumns(dash.columns));
        colLabel.appendChild(colInput);
        box.appendChild(colLabel);

        const checkLabel = document.createElement('div');
        checkLabel.textContent = 'Viewers';
        box.appendChild(checkLabel);

        const checkList = document.createElement('div');
        checkList.className = 'viewer-checkbox-list';

        const orderedIds = [...dash.panels]
          .sort((a, b) => a.position - b.position)
          .map(p => p.viewer_id);
        const viewerMap = Object.fromEntries(viewers.map(v => [v.id, v]));
        const orderedViewers = [
          ...orderedIds.filter(id => viewerMap[id]).map(id => viewerMap[id]),
          ...viewers.filter(v => !currentViewerIds.has(v.id)),
        ];

        for (const v of orderedViewers) {
          checkList.appendChild(createViewerItem(v, currentViewerIds.has(v.id)));
        }
        attachSortableListeners(checkList);

        box.appendChild(checkList);

        const actions = document.createElement('div');
        actions.className = 'modal-actions';

        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'secondary';
        cancelBtn.type = 'button';
        cancelBtn.textContent = 'Cancel';
        cancelBtn.addEventListener('click', () => overlay.remove());

        const saveBtn = document.createElement('button');
        saveBtn.className = 'primary';
        saveBtn.type = 'button';
        saveBtn.textContent = 'Save';
        saveBtn.addEventListener('click', async () => {
          const name = nameInput.value.trim();
          const columns = parseDashboardColumnsInput(colInput);
          const viewer_ids = [...checkList.querySelectorAll('.viewer-sortable-item:has(input[type="checkbox"]:checked)')]
            .map(item => item.dataset.viewerId);
          if (!name) { nameInput.focus(); return; }
          if (columns === null) { colInput.focus(); return; }
          saveBtn.disabled = true;
          try {
            const r = await fetch(`/api/dashboards/${dashboardId}`, {
              method: 'PATCH',
              headers: { 'content-type': 'application/json', accept: 'application/json' },
              body: JSON.stringify({ name, columns, viewer_ids }),
            });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            overlay.remove();
            await refreshDashboardList();
            setDashboardLastUpdated(null);
            await loadDashboard(dashboardId);
            restartDashboardRefresh();
          } catch (err) {
            saveBtn.disabled = false;
          }
        });

        const exportBtn = document.createElement('button');
        exportBtn.className = 'secondary';
        exportBtn.type = 'button';
        exportBtn.textContent = 'Export';
        exportBtn.addEventListener('click', () => exportDashboard(dashboardId, dash.name));

        actions.appendChild(exportBtn);
        actions.appendChild(cancelBtn);
        actions.appendChild(saveBtn);
        box.appendChild(actions);
        overlay.appendChild(box);
        overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
        document.body.appendChild(overlay);
      }

      // --- New Dashboard modal ---------------------------------------------

      newDashboardButton.addEventListener('click', () => openNewDashboardModal());

      const importDashboardButton = document.getElementById('import-dashboard-button');
      const importDashboardFile = document.getElementById('import-dashboard-file');

      importDashboardButton.addEventListener('click', () => importDashboardFile.click());

      importDashboardFile.addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        importDashboardFile.value = '';
        try {
          const data = JSON.parse(await file.text());
          const resp = await fetch('/api/dashboards/import', {
            method: 'POST',
            headers: { 'content-type': 'application/json', accept: 'application/json' },
            body: JSON.stringify(data),
          });
          if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
          const { dashboard_id } = await resp.json();
          await refreshDashboardList();
          window.location.hash = buildDashboardHash(dashboard_id);
        } catch (err) {
          setStatus('error', `Import failed: ${err.message}`);
        }
      });

      async function openNewDashboardModal() {
        const viewersResp = await fetch('/api/viewers', { headers: { accept: 'application/json' } });
        const { viewers } = viewersResp.ok ? await viewersResp.json() : { viewers: [] };

        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';

        const box = document.createElement('div');
        box.className = 'modal-box';
        box.innerHTML = '<h3>New Dashboard</h3>';

        const nameLabel = document.createElement('label');
        nameLabel.textContent = 'Name';
        const nameInput = document.createElement('input');
        nameInput.placeholder = 'My Dashboard';
        nameInput.maxLength = 80;
        nameLabel.appendChild(nameInput);
        box.appendChild(nameLabel);

        const colLabel = document.createElement('label');
        colLabel.textContent = 'Columns';
        const colInput = document.createElement('input');
        colInput.type = 'number';
        colInput.min = '1';
        colInput.max = '4';
        colInput.value = '2';
        colLabel.appendChild(colInput);
        box.appendChild(colLabel);

        let checkList = null;
        if (viewers.length) {
          const checkLabel = document.createElement('div');
          checkLabel.textContent = 'Viewers (optional)';
          box.appendChild(checkLabel);

          checkList = document.createElement('div');
          checkList.className = 'viewer-checkbox-list';
          for (const v of viewers) {
            checkList.appendChild(createViewerItem(v, false));
          }
          attachSortableListeners(checkList);

          box.appendChild(checkList);
        }

        const actions = document.createElement('div');
        actions.className = 'modal-actions';

        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'secondary';
        cancelBtn.type = 'button';
        cancelBtn.textContent = 'Cancel';
        cancelBtn.addEventListener('click', () => overlay.remove());

        const createBtn = document.createElement('button');
        createBtn.className = 'primary';
        createBtn.type = 'button';
        createBtn.textContent = 'Create';
        createBtn.addEventListener('click', async () => {
          const name = nameInput.value.trim();
          const columns = parseDashboardColumnsInput(colInput);
          const viewer_ids = checkList ? [...checkList.querySelectorAll('.viewer-sortable-item:has(input[type="checkbox"]:checked)')].map(item => item.dataset.viewerId) : [];
          if (!name) { nameInput.focus(); return; }
          if (columns === null) { colInput.focus(); return; }
          createBtn.disabled = true;
          try {
            const r = await fetch('/api/dashboards', {
              method: 'POST',
              headers: { 'content-type': 'application/json', accept: 'application/json' },
              body: JSON.stringify({ name, columns, viewer_ids }),
            });
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            const { id } = await r.json();
            overlay.remove();
            await refreshDashboardList();
            window.location.hash = buildDashboardHash(id);
          } catch (err) {
            createBtn.disabled = false;
          }
        });

        actions.appendChild(cancelBtn);
        actions.appendChild(createBtn);
        box.appendChild(actions);

        overlay.appendChild(box);
        overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
        document.body.appendChild(overlay);
        nameInput.focus();
      }

      function routeFromHash() {
        const hashData = parseDashboardHash();
        if (hashData) {
          navigateTo('dashboard', hashData.id);
          return;
        }
        if (
          window.location.hash === '#viewers' ||
          window.location.hash === ''
        ) {
          navigateTo('viewers');
        }
      }

      window.addEventListener('hashchange', routeFromHash);

      // Handle initial hash on page load
      if (window.location.hash !== '') {
        routeFromHash();
      }

      // --- Polling ---------------------------------------------------------

      refreshDashboardList();
      setDashboardLastUpdated(null);
      updateDashboardRefreshControls();
    </script>
  </body>
</html>
"####;

/// Axum shared state
///
/// StreamStore is Clone-able, so it does not need to be wrapped in Arc<Mutex<>>.
/// Axum clones State for each request.
#[derive(Clone)]
pub struct AppState {
    pub stream_store: StreamStore,
    pub viewer_store: Option<ViewerStore>,
    pub viewer_runtime: Option<SharedViewerRuntime>,
}

pub type SharedViewerRuntime = Arc<Mutex<ViewerRuntime>>;

impl AppState {
    fn require_viewer_runtime(&self) -> Result<&SharedViewerRuntime, StatusCode> {
        self.viewer_runtime
            .as_ref()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }

    fn require_viewer_store(&self) -> Result<&crate::storage::ViewerStore, StatusCode> {
        self.viewer_store
            .as_ref()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }
}

fn default_chart_type() -> String {
    "table".to_string()
}

fn is_valid_chart_type(chart_type: &str) -> bool {
    matches!(
        chart_type,
        "table" | "stacked_bar" | "line" | "area" | "pie" | "donut" | "billboard"
    )
}

fn is_chart_type_supported_for_signal_mask(chart_type: &str, signal_mask: SignalMask) -> bool {
    match chart_type {
        "table" => true,
        "stacked_bar" | "line" | "area" | "pie" | "donut" | "billboard" => {
            signal_mask == Signal::Metrics.into()
        }
        _ => false,
    }
}

fn chart_type_from_definition(definition_json: &serde_json::Value) -> Result<&str, &'static str> {
    let Some(kind_value) = definition_json.get("kind") else {
        return Ok("table");
    };
    let chart_type = kind_value
        .as_str()
        .ok_or("viewer definition chart type is not a string")?;

    if !is_valid_chart_type(chart_type) {
        return Err("viewer definition contains invalid chart type");
    }

    Ok(chart_type)
}

const MIN_DASHBOARD_COLUMNS: u32 = 1;
const MAX_DASHBOARD_COLUMNS: u32 = 4;
const DEFAULT_DASHBOARD_COLUMNS: u32 = 2;
fn default_dashboard_columns() -> u32 {
    DEFAULT_DASHBOARD_COLUMNS
}

fn is_valid_dashboard_columns(columns: u32) -> bool {
    (MIN_DASHBOARD_COLUMNS..=MAX_DASHBOARD_COLUMNS).contains(&columns)
}

fn viewer_slug(id: Uuid) -> String {
    format!("viewer-{}", id.simple())
}

fn dashboard_slug(id: Uuid) -> String {
    format!("dashboard-{}", id.simple())
}

fn dashboard_columns_from_layout(layout_json: &serde_json::Value) -> Result<u32, &'static str> {
    let Some(columns_value) = layout_json.get("columns") else {
        return Ok(DEFAULT_DASHBOARD_COLUMNS);
    };
    let columns = columns_value
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .ok_or("dashboard layout columns are not a valid integer")?;

    if !is_valid_dashboard_columns(columns) {
        return Err("dashboard layout columns are out of range");
    }

    Ok(columns)
}

/// Raw filter condition sent by the client in create / patch / preview requests.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct ViewerFilterInput {
    field: String,
    op: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct CreateViewerRequest {
    name: String,
    signal: String,
    #[serde(default = "default_chart_type")]
    chart_type: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    filters: Option<Vec<ViewerFilterInput>>,
    #[serde(default)]
    filter_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PatchViewerRequest {
    chart_type: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    filters: Option<Vec<ViewerFilterInput>>,
    #[serde(default)]
    filter_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PreviewViewerRequest {
    signal: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    filters: Option<Vec<ViewerFilterInput>>,
    #[serde(default)]
    filter_mode: Option<String>,
}

#[derive(Debug, Serialize)]
struct ViewerPreviewResponse {
    signal: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_mode: Option<String>,
    entry_count: usize,
    entries: Vec<ViewerEntryRow>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    traces: Vec<TraceSummary>,
}

#[derive(Debug, Serialize)]
struct ServicesResponse {
    services: Vec<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_mode: Option<String>,
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

/// Builds and returns an Axum app (for ingest-only mode)
pub fn build_app(stream_store: StreamStore) -> Router {
    build_app_with_services(stream_store, None, None)
}

pub fn build_app_with_services(
    stream_store: StreamStore,
    viewer_store: Option<ViewerStore>,
    viewer_runtime: Option<SharedViewerRuntime>,
) -> Router {
    let state = AppState {
        stream_store,
        viewer_store,
        viewer_runtime,
    };

    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/api/viewers", get(list_viewers).post(create_viewer))
        .route("/api/viewers/preview", post(preview_viewer))
        .route(
            "/api/viewers/{id}",
            get(get_viewer).patch(patch_viewer).delete(delete_viewer),
        )
        .route("/api/services", get(list_services))
        .route(
            "/api/dashboards",
            get(list_dashboards).post(create_dashboard),
        )
        .route("/api/templates", get(list_templates))
        .route(
            "/api/dashboards/from-template",
            post(create_dashboard_from_template),
        )
        .route("/api/dashboards/import", post(import_dashboard))
        .route(
            "/api/dashboards/{id}",
            get(get_dashboard)
                .patch(patch_dashboard)
                .delete(delete_dashboard),
        )
        .route("/api/dashboards/{id}/export", get(export_dashboard))
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

    let (viewer, viewer_state) = runtime
        .viewers()
        .iter()
        .find(|(viewer, _)| viewer.definition().id == id)
        .ok_or(StatusCode::NOT_FOUND)?;
    let summary = viewer_summary(
        viewer,
        &viewer_state.entries,
        &viewer_state.status,
        viewer.lookback_ms(),
        true,
    )
    .map_err(|error| {
        tracing::error!("viewer {id}: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(summary))
}

async fn healthz() -> &'static str {
    "ok"
}

async fn list_services(
    State(state): State<AppState>,
) -> Result<Json<ServicesResponse>, StatusCode> {
    let runtime = state.require_viewer_runtime()?.lock().await;
    let services = runtime.collect_service_names();
    Ok(Json(ServicesResponse { services }))
}

async fn list_viewers(
    State(state): State<AppState>,
) -> Result<Json<ViewerListResponse>, StatusCode> {
    let runtime = state.require_viewer_runtime()?.lock().await;

    let viewers = runtime
        .viewers()
        .iter()
        .map(|(viewer, viewer_state)| {
            viewer_summary(
                viewer,
                &viewer_state.entries,
                &viewer_state.status,
                viewer.lookback_ms(),
                false,
            )
            .map_err(|error| {
                tracing::error!("viewer {}: {error}", viewer.definition().id);
                StatusCode::INTERNAL_SERVER_ERROR
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(ViewerListResponse { viewers }))
}

fn apply_query_to_definition(definition_json: &mut serde_json::Value, query: Option<&str>) {
    let Some(q) = query else { return };
    let trimmed = q.trim();
    if trimmed.is_empty() {
        if let Some(o) = definition_json.as_object_mut() {
            o.remove("query");
        }
    } else {
        definition_json["query"] = json!(trimmed);
    }
}

/// Applies filter conditions and mode to definition_json.
/// `filters: None` -> no change (keep existing).
/// `filters: Some([])` -> clear "filters" and "filter_mode".
/// `filters: Some([...])` -> set "filters"; apply filter_mode if provided.
fn apply_filters_to_definition(
    definition_json: &mut serde_json::Value,
    filters: Option<&[ViewerFilterInput]>,
    filter_mode: Option<&str>,
) {
    let Some(filters) = filters else { return };
    if filters.is_empty() {
        if let Some(o) = definition_json.as_object_mut() {
            o.remove("filters");
            o.remove("filter_mode");
        }
        return;
    }
    definition_json["filters"] = json!(filters);
    if let Some(mode) = filter_mode {
        let trimmed = mode.trim();
        if trimmed.is_empty() {
            if let Some(o) = definition_json.as_object_mut() {
                o.remove("filter_mode");
            }
        } else {
            definition_json["filter_mode"] = json!(trimmed);
        }
    }
}

fn default_viewer_layout() -> serde_json::Value {
    json!({ "default_view": "table" })
}

async fn provision_viewer(
    viewer_store: &crate::storage::ViewerStore,
    runtime: &SharedViewerRuntime,
    definition: ViewerDefinition,
) -> Result<(), StatusCode> {
    viewer_store
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

    Ok(())
}

async fn unprovision_viewer(
    viewer_store: &crate::storage::ViewerStore,
    runtime: &SharedViewerRuntime,
    id: Uuid,
) {
    if let Err(e) = viewer_store.delete_viewer(id).await {
        tracing::error!("unprovision_viewer: delete_viewer failed: {e}");
    } else {
        runtime.lock().await.remove_viewer(id);
    }
}

async fn create_viewer(
    State(state): State<AppState>,
    Json(payload): Json<CreateViewerRequest>,
) -> Result<(StatusCode, Json<CreateViewerResponse>), StatusCode> {
    let viewer_store = state
        .viewer_store
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let runtime = state.require_viewer_runtime()?;

    let name = payload.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let signal = parse_signal_name(payload.signal.trim()).ok_or(StatusCode::BAD_REQUEST)?;

    if !is_chart_type_supported_for_signal_mask(&payload.chart_type, signal.into()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let id = Uuid::new_v4();
    let mut definition_json = json!({
        "kind": payload.chart_type,
        "signal": signal_name(signal)
    });
    apply_query_to_definition(&mut definition_json, payload.query.as_deref());
    apply_filters_to_definition(
        &mut definition_json,
        payload.filters.as_deref(),
        payload.filter_mode.as_deref(),
    );
    let definition = ViewerDefinition {
        id,
        slug: viewer_slug(id),
        name: name.to_string(),
        refresh_interval_ms: DEFAULT_VIEWER_REFRESH_MS,
        lookback_ms: DEFAULT_VIEWER_LOOKBACK_MS,
        signal_mask: signal.into(),
        definition_json,
        layout_json: default_viewer_layout(),
        revision: 1,
        enabled: true,
    };

    compile(definition.clone()).map_err(|e| {
        tracing::error!("create_viewer: compile failed: {e}");
        StatusCode::BAD_REQUEST
    })?;

    provision_viewer(viewer_store, runtime, definition).await?;

    Ok((StatusCode::CREATED, Json(CreateViewerResponse { id })))
}

async fn patch_viewer(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<PatchViewerRequest>,
) -> Result<StatusCode, StatusCode> {
    let viewer_store = state
        .viewer_store
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let runtime = state.require_viewer_runtime()?;

    if let Some(ct) = &payload.chart_type
        && !is_valid_chart_type(ct)
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Read current state under lock, then release before DB write
    let (definition_json, layout_json, matcher_changed, signal_mask) = {
        let rt = runtime.lock().await;
        let (viewer, _) = rt
            .viewers()
            .iter()
            .find(|(viewer, _)| viewer.definition().id == id)
            .ok_or(StatusCode::NOT_FOUND)?;

        let current_kind = chart_type_from_definition(&viewer.definition().definition_json)
            .map_err(|error| {
                tracing::error!("viewer {id}: {error}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        let new_chart_type = payload.chart_type.as_deref().unwrap_or(current_kind);
        if !is_chart_type_supported_for_signal_mask(new_chart_type, viewer.definition().signal_mask)
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        let current_query = viewer.query().map(str::to_string);
        let old_def_json = &viewer.definition().definition_json;
        let mut definition_json = old_def_json.clone();
        definition_json["kind"] = json!(new_chart_type);
        apply_query_to_definition(&mut definition_json, payload.query.as_deref());
        apply_filters_to_definition(
            &mut definition_json,
            payload.filters.as_deref(),
            payload.filter_mode.as_deref(),
        );
        let query_changed = query_from_definition(&definition_json) != current_query;
        let filters_changed = definition_json.get("filters") != old_def_json.get("filters")
            || definition_json.get("filter_mode") != old_def_json.get("filter_mode");
        let matcher_changed = query_changed || filters_changed;
        if new_chart_type == current_kind && !matcher_changed {
            return Ok(StatusCode::OK);
        }
        let layout_json = viewer.definition().layout_json.clone();
        let signal_mask = viewer.definition().signal_mask;
        (definition_json, layout_json, matcher_changed, signal_mask)
    };

    if matcher_changed {
        let temp_def = ViewerDefinition {
            id,
            slug: String::new(),
            name: String::new(),
            refresh_interval_ms: DEFAULT_VIEWER_REFRESH_MS,
            lookback_ms: DEFAULT_VIEWER_LOOKBACK_MS,
            signal_mask,
            definition_json: definition_json.clone(),
            layout_json: serde_json::Value::Object(Default::default()),
            revision: 1,
            enabled: true,
        };
        compile(temp_def).map_err(|e| {
            tracing::error!("patch_viewer: compile failed: {e}");
            StatusCode::BAD_REQUEST
        })?;
    }

    let updated = viewer_store
        .update_viewer_definition_json(id, &definition_json, &layout_json)
        .await
        .map_err(|error| {
            tracing::error!("update_viewer_definition_json failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if !updated {
        return Err(StatusCode::NOT_FOUND);
    }

    let mut rt = runtime.lock().await;
    if matcher_changed {
        rt.rebuild_viewer(id, definition_json, layout_json)
            .await
            .map_err(|error| {
                tracing::error!("viewer {id}: rebuild_viewer failed: {error}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
    } else {
        rt.update_viewer_definition(id, definition_json, layout_json);
    }

    Ok(StatusCode::OK)
}

async fn preview_viewer(
    State(mut state): State<AppState>,
    Json(payload): Json<PreviewViewerRequest>,
) -> Result<Json<ViewerPreviewResponse>, StatusCode> {
    let signal = parse_signal_name(payload.signal.trim()).ok_or(StatusCode::BAD_REQUEST)?;

    let mut definition_json = json!({
        "kind": "table",
        "signal": signal_name(signal)
    });
    apply_query_to_definition(&mut definition_json, payload.query.as_deref());
    apply_filters_to_definition(
        &mut definition_json,
        payload.filters.as_deref(),
        payload.filter_mode.as_deref(),
    );

    let temp_def = ViewerDefinition {
        id: Uuid::new_v4(),
        slug: "preview".to_string(),
        name: "preview".to_string(),
        refresh_interval_ms: DEFAULT_VIEWER_REFRESH_MS,
        lookback_ms: DEFAULT_VIEWER_LOOKBACK_MS,
        signal_mask: signal.into(),
        definition_json,
        layout_json: json!({}),
        revision: 1,
        enabled: true,
    };

    let viewer = compile(temp_def).map_err(|e| {
        tracing::error!("preview_viewer: compile failed: {e}");
        StatusCode::BAD_REQUEST
    })?;

    let mut viewer_state = ViewerState::new(viewer.definition().id, 1);
    let now = Utc::now();

    let entries = state
        .stream_store
        .read_entries_since(signal, None, 100_000)
        .await
        .map_err(|error| {
            tracing::error!("preview_viewer: read_entries_since failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    for (_, entry) in entries {
        apply_entry(&mut viewer_state, &viewer, entry);
    }
    prune_stale_buckets(&mut viewer_state, viewer.lookback_ms(), now);

    let entry_rows = map_entries_to_rows(
        viewer_state
            .entries
            .iter()
            .rev()
            .take(VIEWER_ENTRY_PREVIEW_LIMIT),
    );

    let traces = if signal == Signal::Traces {
        extract_traces_from_entries(&viewer_state.entries)
    } else {
        vec![]
    };

    let def_json = &viewer.definition().definition_json;
    let query = def_json
        .get("query")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let filters = def_json.get("filters").cloned();
    let filter_mode = def_json
        .get("filter_mode")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    Ok(Json(ViewerPreviewResponse {
        signal: signal_name(signal).to_string(),
        query,
        filters,
        filter_mode,
        entry_count: viewer_state.entries.len(),
        entries: entry_rows,
        traces,
    }))
}

// --- Dashboard API ----------------------------------------------------------

#[derive(Debug)]
struct PanelEntry {
    viewer_id: Uuid,
    position: usize,
    col_span: u32,
    row_span: u32,
}

#[derive(Debug, Deserialize)]
struct PanelRequestItem {
    viewer_id: Uuid,
    #[serde(default = "default_span")]
    col_span: u32,
    #[serde(default = "default_span")]
    row_span: u32,
}

fn default_span() -> u32 {
    1
}

impl PanelRequestItem {
    fn into_panel_input(self, max_col: u32) -> PanelInput {
        PanelInput {
            viewer_id: self.viewer_id,
            col_span: self.col_span.clamp(1, max_col),
            row_span: self.row_span.max(1),
        }
    }
}

#[derive(Debug, Deserialize)]
struct CreateDashboardRequest {
    name: String,
    #[serde(default)]
    viewer_ids: Vec<Uuid>,
    #[serde(default)]
    panels: Vec<PanelRequestItem>,
    #[serde(default = "default_dashboard_columns")]
    columns: u32,
}

#[derive(Debug, Serialize)]
struct CreateDashboardResponse {
    id: Uuid,
}

#[derive(Debug, Serialize)]
struct DashboardListItem {
    id: Uuid,
    slug: String,
    name: String,
    panel_count: usize,
    viewer_ids: Vec<Uuid>,
}

#[derive(Debug, Serialize)]
struct DashboardListResponse {
    dashboards: Vec<DashboardListItem>,
}

#[derive(Debug, Serialize)]
struct TemplateListResponse {
    templates: Vec<TemplateInfo>,
}

#[derive(Debug, Serialize)]
struct DashboardPanel {
    viewer_id: Uuid,
    position: usize,
    col_span: u32,
    row_span: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    viewer: Option<ViewerSummary>,
}

#[derive(Debug, Serialize)]
struct DashboardDetailResponse {
    id: Uuid,
    slug: String,
    name: String,
    columns: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_lookback_ms: Option<i64>,
    panels: Vec<DashboardPanel>,
}

#[derive(Debug, Deserialize)]
struct PatchDashboardRequest {
    name: Option<String>,
    viewer_ids: Option<Vec<Uuid>>,
    panels: Option<Vec<PanelRequestItem>>,
    columns: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DashboardExportEnvelope {
    version: u32,
    dashboard: DashboardExportData,
    viewers: Vec<ViewerExportData>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DashboardExportData {
    name: String,
    columns: u32,
    panels: Vec<PanelInput>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ViewerExportData {
    id: Uuid,
    name: String,
    signal: String,
    chart_type: String,
    lookback_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<String>,
    #[serde(default)]
    filters: Vec<serde_json::Value>,
    #[serde(default = "default_filter_mode")]
    filter_mode: String,
}

const DEFAULT_FILTER_MODE: &str = "and";

fn default_filter_mode() -> String {
    DEFAULT_FILTER_MODE.to_string()
}

#[derive(Debug, Serialize)]
struct DashboardImportResponse {
    dashboard_id: Uuid,
}

fn dashboard_panels_from_layout(layout_json: &serde_json::Value) -> Vec<PanelEntry> {
    let Some(panels) = layout_json.get("panels").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    panels
        .iter()
        .filter_map(|p| {
            let viewer_id = p
                .get("viewer_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Uuid>().ok())?;
            let position = p
                .get("position")
                .and_then(|v| v.as_u64())
                .and_then(|v| usize::try_from(v).ok())?;
            let col_span = p
                .get("col_span")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(1);
            let row_span = p
                .get("row_span")
                .and_then(|v| v.as_u64())
                .and_then(|v| u32::try_from(v).ok())
                .unwrap_or(1);
            Some(PanelEntry {
                viewer_id,
                position,
                col_span,
                row_span,
            })
        })
        .collect()
}

async fn list_dashboards(
    State(state): State<AppState>,
) -> Result<Json<DashboardListResponse>, StatusCode> {
    let store = state.require_viewer_store()?;
    let dashboards = store.load_dashboards().await.map_err(|error| {
        tracing::error!("load_dashboards failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let items = dashboards
        .into_iter()
        .map(|d| {
            let panels = dashboard_panels_from_layout(&d.layout_json);
            DashboardListItem {
                id: d.id,
                slug: d.slug,
                name: d.name,
                panel_count: panels.len(),
                viewer_ids: panels.into_iter().map(|e| e.viewer_id).collect(),
            }
        })
        .collect();

    Ok(Json(DashboardListResponse { dashboards: items }))
}

async fn create_dashboard(
    State(state): State<AppState>,
    Json(payload): Json<CreateDashboardRequest>,
) -> Result<(StatusCode, Json<CreateDashboardResponse>), StatusCode> {
    let store = state.require_viewer_store()?;

    let name = payload.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !is_valid_dashboard_columns(payload.columns) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let id = Uuid::new_v4();
    let panel_inputs: Vec<PanelInput> = if !payload.panels.is_empty() {
        payload
            .panels
            .into_iter()
            .map(|p| p.into_panel_input(payload.columns))
            .collect()
    } else {
        panel_inputs_from_viewer_ids(&payload.viewer_ids)
    };
    let layout_json = build_layout_json(&panel_inputs, payload.columns);
    let dashboard = DashboardDefinition {
        id,
        slug: dashboard_slug(id),
        name: name.to_string(),
        layout_json,
        revision: 1,
        enabled: true,
    };

    store.insert_dashboard(&dashboard).await.map_err(|error| {
        tracing::error!("insert_dashboard failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok((StatusCode::CREATED, Json(CreateDashboardResponse { id })))
}

async fn list_templates() -> Json<TemplateListResponse> {
    let templates: Vec<TemplateInfo> = ALL_TEMPLATES
        .iter()
        .map(|t| TemplateInfo {
            id: t.id,
            name: t.name,
            description: t.description,
            signal_types: unique_signal_types(t.viewers),
            viewer_count: t.viewers.len(),
            columns: t.columns,
        })
        .collect();
    Json(TemplateListResponse { templates })
}

#[derive(Debug, Deserialize)]
struct CreateDashboardFromTemplateRequest {
    template_id: String,
    name: Option<String>,
}

#[derive(Debug, Serialize)]
struct CreateDashboardFromTemplateResponse {
    dashboard_id: Uuid,
    viewer_ids: Vec<Uuid>,
}

async fn create_dashboard_from_template(
    State(state): State<AppState>,
    Json(payload): Json<CreateDashboardFromTemplateRequest>,
) -> Result<(StatusCode, Json<CreateDashboardFromTemplateResponse>), StatusCode> {
    let viewer_store = state
        .viewer_store
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let runtime = state.require_viewer_runtime()?;

    let template = ALL_TEMPLATES
        .iter()
        .find(|t| t.id == payload.template_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    let dashboard_name = payload
        .name
        .as_deref()
        .map(str::trim)
        .unwrap_or(template.name)
        .to_string();
    if dashboard_name.is_empty() || dashboard_name.chars().count() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let mut viewer_ids: Vec<Uuid> = Vec::new();
    for spec in template.viewers {
        let id = Uuid::new_v4();
        let mut definition_json = json!({
            "kind": spec.chart_type,
            "signal": signal_name(spec.signal)
        });
        apply_query_to_definition(&mut definition_json, spec.query);
        let definition = ViewerDefinition {
            id,
            slug: format!("viewer-{}", id.simple()),
            name: spec.name.to_string(),
            refresh_interval_ms: DEFAULT_VIEWER_REFRESH_MS,
            lookback_ms: spec.lookback_ms,
            signal_mask: spec.signal.into(),
            definition_json,
            layout_json: default_viewer_layout(),
            revision: 1,
            enabled: true,
        };

        if let Err(e) = compile(definition.clone()).map_err(|e| {
            tracing::error!("create_dashboard_from_template: compile failed: {e}");
            StatusCode::BAD_REQUEST
        }) {
            for &prev_id in &viewer_ids {
                unprovision_viewer(viewer_store, runtime, prev_id).await;
            }
            return Err(e);
        }

        if let Err(e) = provision_viewer(viewer_store, runtime, definition).await {
            for &prev_id in &viewer_ids {
                unprovision_viewer(viewer_store, runtime, prev_id).await;
            }
            return Err(e);
        }

        viewer_ids.push(id);
    }

    let panel_inputs: Vec<PanelInput> = template
        .panels
        .iter()
        .map(|p| PanelInput {
            viewer_id: viewer_ids[p.viewer_index],
            col_span: p.col_span,
            row_span: p.row_span,
        })
        .collect();

    let layout_json = build_layout_json(&panel_inputs, template.columns);
    let dashboard_id = Uuid::new_v4();
    let dashboard = DashboardDefinition {
        id: dashboard_id,
        slug: format!("dashboard-{}", dashboard_id.simple()),
        name: dashboard_name,
        layout_json,
        revision: 1,
        enabled: true,
    };

    if let Err(e) = viewer_store
        .insert_dashboard(&dashboard)
        .await
        .map_err(|error| {
            tracing::error!("insert_dashboard failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
    {
        for &id in &viewer_ids {
            unprovision_viewer(viewer_store, runtime, id).await;
        }
        return Err(e);
    }

    Ok((
        StatusCode::CREATED,
        Json(CreateDashboardFromTemplateResponse {
            dashboard_id,
            viewer_ids,
        }),
    ))
}

#[derive(Debug, Deserialize)]
struct DashboardFilterQuery {
    lookback_ms: Option<i64>,
    service_name: Option<String>,
    query: Option<String>,
}

impl DashboardFilterQuery {
    fn parse_service_names(&self) -> Vec<String> {
        self.service_name
            .as_deref()
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn parse_query(&self) -> Option<String> {
        self.query
            .as_ref()
            .map(|q| q.trim().to_lowercase())
            .filter(|q| !q.is_empty())
    }
}

fn matches_dashboard_global_filter(
    entry: &NormalizedEntry,
    service_names: &[String],
    query: Option<&str>,
) -> bool {
    if !service_names.is_empty() {
        let svc = entry.service_name.as_deref().unwrap_or("");
        if !service_names.iter().any(|n| n.as_str() == svc) {
            return false;
        }
    }
    if let Some(q) = query {
        let svc_match = entry
            .service_name
            .as_deref()
            .map(|s| s.to_lowercase().contains(q))
            .unwrap_or(false);
        if !svc_match
            && !extract_searchable_payload_text(entry.signal, &entry.payload)
                .map(|t| t.to_lowercase().contains(q))
                .unwrap_or(false)
        {
            return false;
        }
    }
    true
}

async fn get_dashboard(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<DashboardFilterQuery>,
) -> Result<Json<DashboardDetailResponse>, StatusCode> {
    let lookback_override = if let Some(ms) = query.lookback_ms {
        if ms <= 0 {
            return Err(StatusCode::BAD_REQUEST);
        }
        Some(ms)
    } else {
        None
    };

    let global_service_names = query.parse_service_names();
    let global_query = query.parse_query();

    let store = state.require_viewer_store()?;

    let dashboard = store
        .load_dashboard(id)
        .await
        .map_err(|error| {
            tracing::error!("load_dashboard failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let columns = dashboard_columns_from_layout(&dashboard.layout_json).map_err(|error| {
        tracing::error!("dashboard {id}: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let panel_entries = dashboard_panels_from_layout(&dashboard.layout_json);

    // Fetch viewer data for panels from ViewerRuntime (empty if runtime is unavailable)
    let (panels, max_lookback_ms) = if let Some(runtime) = state.viewer_runtime.as_ref() {
        let rt = runtime.lock().await;
        let max_lookback_ms = panel_entries
            .iter()
            .filter_map(|entry| {
                rt.viewers()
                    .iter()
                    .find(|(viewer, _)| viewer.definition().id == entry.viewer_id)
                    .map(|(viewer, _)| viewer.lookback_ms())
            })
            .min();

        if let Some(ms) = lookback_override
            && max_lookback_ms.is_some_and(|limit| ms > limit)
        {
            return Err(StatusCode::BAD_REQUEST);
        }

        let now = chrono::Utc::now();
        let mut panels = Vec::with_capacity(panel_entries.len());

        for entry in panel_entries {
            let viewer = rt
                .get_dashboard_viewer(&entry.viewer_id, lookback_override, now)
                .map(|(viewer, entries, status, effective_lookback)| {
                    let filtered: Vec<NormalizedEntry>;
                    let effective_entries: &[NormalizedEntry] =
                        if global_service_names.is_empty() && global_query.is_none() {
                            entries
                        } else {
                            filtered = entries
                                .iter()
                                .filter(|e| {
                                    matches_dashboard_global_filter(
                                        e,
                                        &global_service_names,
                                        global_query.as_deref(),
                                    )
                                })
                                .cloned()
                                .collect();
                            &filtered
                        };
                    viewer_summary(viewer, effective_entries, status, effective_lookback, true)
                })
                .transpose()
                .map_err(|error| {
                    tracing::error!("viewer {}: {error}", entry.viewer_id);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            panels.push(DashboardPanel {
                viewer_id: entry.viewer_id,
                position: entry.position,
                col_span: entry.col_span,
                row_span: entry.row_span,
                viewer,
            });
        }

        (panels, max_lookback_ms)
    } else {
        (
            panel_entries
                .into_iter()
                .map(|e| DashboardPanel {
                    viewer_id: e.viewer_id,
                    position: e.position,
                    col_span: e.col_span,
                    row_span: e.row_span,
                    viewer: None,
                })
                .collect(),
            None,
        )
    };

    Ok(Json(DashboardDetailResponse {
        id: dashboard.id,
        slug: dashboard.slug,
        name: dashboard.name,
        columns,
        max_lookback_ms,
        panels,
    }))
}

async fn patch_dashboard(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<PatchDashboardRequest>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_viewer_store()?;

    let current = store
        .load_dashboard(id)
        .await
        .map_err(|error| {
            tracing::error!("load_dashboard failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let new_name = payload
        .name
        .as_deref()
        .map(str::trim)
        .unwrap_or(&current.name);
    if new_name.is_empty() || new_name.chars().count() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }
    if let Some(columns) = payload.columns
        && !is_valid_dashboard_columns(columns)
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    let new_layout =
        if payload.panels.is_some() || payload.viewer_ids.is_some() || payload.columns.is_some() {
            let current_columns =
                dashboard_columns_from_layout(&current.layout_json).map_err(|error| {
                    tracing::error!("dashboard {id}: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            let columns = payload.columns.unwrap_or(current_columns);

            let panel_inputs: Vec<PanelInput> = if let Some(panels) = payload.panels {
                panels
                    .into_iter()
                    .map(|p| p.into_panel_input(columns))
                    .collect()
            } else {
                // viewer_ids only: change order while preserving existing spans
                let current_panels = dashboard_panels_from_layout(&current.layout_json);
                let viewer_ids = payload
                    .viewer_ids
                    .unwrap_or_else(|| current_panels.iter().map(|e| e.viewer_id).collect());
                viewer_ids
                    .iter()
                    .map(|vid| {
                        let (col_span, row_span) = current_panels
                            .iter()
                            .find(|e| e.viewer_id == *vid)
                            .map(|e| (e.col_span, e.row_span))
                            .unwrap_or((1, 1));
                        PanelInput {
                            viewer_id: *vid,
                            col_span,
                            row_span,
                        }
                    })
                    .collect()
            };
            build_layout_json(&panel_inputs, columns)
        } else {
            current.layout_json.clone()
        };

    let updated = store
        .update_dashboard(id, new_name, &new_layout)
        .await
        .map_err(|error| {
            tracing::error!("update_dashboard failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if !updated {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(StatusCode::OK)
}

async fn delete_viewer(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_viewer_store()?;
    let runtime = state.require_viewer_runtime()?;

    let deleted = store.delete_viewer(id).await.map_err(|error| {
        tracing::error!("delete_viewer failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if !deleted {
        return Err(StatusCode::NOT_FOUND);
    }

    runtime.lock().await.remove_viewer(id);

    Ok(StatusCode::NO_CONTENT)
}

async fn delete_dashboard(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_viewer_store()?;

    let deleted = store.delete_dashboard(id).await.map_err(|error| {
        tracing::error!("delete_dashboard failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if !deleted {
        return Err(StatusCode::NOT_FOUND);
    }

    Ok(StatusCode::NO_CONTENT)
}

// --- Dashboard Export / Import ----------------------------------------------

async fn export_dashboard(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<DashboardExportEnvelope>, StatusCode> {
    let store = state.require_viewer_store()?;

    let (dashboard_result, all_defs_result) =
        tokio::join!(store.load_dashboard(id), store.load_viewer_definitions());

    let dashboard = dashboard_result
        .map_err(|e| {
            tracing::error!("load_dashboard: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let columns = dashboard_columns_from_layout(&dashboard.layout_json).map_err(|e| {
        tracing::error!("dashboard {id}: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let panels = dashboard_panels_from_layout(&dashboard.layout_json);

    let all_defs = all_defs_result.map_err(|e| {
        tracing::error!("load_viewer_definitions: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let defs_map: HashMap<Uuid, ViewerDefinition> =
        all_defs.into_iter().map(|d| (d.id, d)).collect();

    let panel_entries: Vec<PanelInput> = panels
        .iter()
        .map(|p| PanelInput {
            viewer_id: p.viewer_id,
            col_span: p.col_span,
            row_span: p.row_span,
        })
        .collect();

    let mut seen: HashSet<Uuid> = HashSet::new();
    let mut viewers: Vec<ViewerExportData> = Vec::new();
    for entry in &panel_entries {
        if !seen.insert(entry.viewer_id) {
            continue;
        }
        let Some(def) = defs_map.get(&entry.viewer_id) else {
            continue;
        };
        let signal = signal_mask_labels(def.signal_mask)
            .first()
            .ok_or_else(|| {
                tracing::error!("export_dashboard: viewer {} has empty signal mask", def.id);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .to_string();
        let chart_type = chart_type_from_definition(&def.definition_json)
            .map_err(|e| {
                tracing::error!("export_dashboard: viewer {}: {e}", def.id);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .to_string();
        viewers.push(ViewerExportData {
            id: def.id,
            name: def.name.clone(),
            signal,
            chart_type,
            lookback_ms: def.lookback_ms,
            query: def
                .definition_json
                .get("query")
                .and_then(|v| v.as_str())
                .map(String::from),
            filters: def
                .definition_json
                .get("filters")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            filter_mode: def
                .definition_json
                .get("filter_mode")
                .and_then(|v| v.as_str())
                .unwrap_or(DEFAULT_FILTER_MODE)
                .to_string(),
        });
    }

    Ok(Json(DashboardExportEnvelope {
        version: 1,
        dashboard: DashboardExportData {
            name: dashboard.name,
            columns,
            panels: panel_entries,
        },
        viewers,
    }))
}

async fn import_dashboard(
    State(state): State<AppState>,
    Json(payload): Json<DashboardExportEnvelope>,
) -> Result<(StatusCode, Json<DashboardImportResponse>), StatusCode> {
    if payload.version != 1 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let store = state.require_viewer_store()?;

    let name = payload.dashboard.name.trim().to_string();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !is_valid_dashboard_columns(payload.dashboard.columns) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let mut id_map: HashMap<Uuid, Uuid> = HashMap::new();

    for viewer_data in &payload.viewers {
        let viewer_name = viewer_data.name.trim();
        if viewer_name.is_empty() || viewer_name.chars().count() > 80 {
            return Err(StatusCode::BAD_REQUEST);
        }

        let signal = parse_signal_name(&viewer_data.signal).ok_or(StatusCode::BAD_REQUEST)?;

        if !is_chart_type_supported_for_signal_mask(&viewer_data.chart_type, signal.into()) {
            return Err(StatusCode::BAD_REQUEST);
        }

        let new_id = Uuid::new_v4();

        let mut definition_json = json!({
            "kind": viewer_data.chart_type,
            "signal": signal_name(signal),
        });
        apply_query_to_definition(&mut definition_json, viewer_data.query.as_deref());
        if !viewer_data.filters.is_empty() {
            definition_json["filters"] = json!(viewer_data.filters);
            definition_json["filter_mode"] = json!(viewer_data.filter_mode);
        }

        let def = ViewerDefinition {
            id: new_id,
            slug: viewer_slug(new_id),
            name: viewer_name.to_string(),
            refresh_interval_ms: DEFAULT_VIEWER_REFRESH_MS,
            lookback_ms: viewer_data.lookback_ms,
            signal_mask: signal.into(),
            definition_json,
            layout_json: json!({ "default_view": "table" }),
            revision: 1,
            enabled: true,
        };

        compile(def.clone()).map_err(|e| {
            tracing::error!("import_dashboard: compile failed: {e}");
            StatusCode::BAD_REQUEST
        })?;

        store.insert_viewer_definition(&def).await.map_err(|e| {
            tracing::error!("insert_viewer_definition: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        if let Some(runtime) = state.viewer_runtime.as_ref() {
            runtime.lock().await.add_viewer(def).await.map_err(|e| {
                tracing::error!("runtime.add_viewer: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        }

        id_map.insert(viewer_data.id, new_id);
    }

    let panel_inputs: Vec<PanelInput> = payload
        .dashboard
        .panels
        .iter()
        .filter_map(|p| {
            let new_viewer_id = id_map.get(&p.viewer_id)?;
            Some(PanelInput {
                viewer_id: *new_viewer_id,
                col_span: p.col_span,
                row_span: p.row_span,
            })
        })
        .collect();

    let layout_json = build_layout_json(&panel_inputs, payload.dashboard.columns);
    let dashboard_id = Uuid::new_v4();
    let dashboard = DashboardDefinition {
        id: dashboard_id,
        slug: dashboard_slug(dashboard_id),
        name,
        layout_json,
        revision: 1,
        enabled: true,
    };

    store.insert_dashboard(&dashboard).await.map_err(|e| {
        tracing::error!("insert_dashboard: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok((
        StatusCode::CREATED,
        Json(DashboardImportResponse { dashboard_id }),
    ))
}

// --- Ingest -----------------------------------------------------------------

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
            let mut stream_store = state.stream_store;
            match stream_store.append_entry(&entry).await {
                Ok(_) => StatusCode::OK,
                Err(error) => {
                    tracing::error!("stream append_entry failed: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        Err(DecodeError::UnsupportedContentType(_)) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
    }
}

fn map_entries_to_rows<'a>(
    entries: impl Iterator<Item = &'a NormalizedEntry>,
) -> Vec<ViewerEntryRow> {
    entries
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
}

fn viewer_summary(
    viewer: &CompiledViewer,
    entries: &[NormalizedEntry],
    status: &ViewerStatus,
    effective_lookback_ms: i64,
    include_entries: bool,
) -> Result<ViewerSummary, &'static str> {
    let definition = viewer.definition();
    let chart_type = chart_type_from_definition(&definition.definition_json)?.to_string();
    let query = definition
        .definition_json
        .get("query")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let filters = definition.definition_json.get("filters").cloned();
    let filter_mode = definition
        .definition_json
        .get("filter_mode")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let entries_for_response = if include_entries {
        map_entries_to_rows(entries.iter().rev().take(VIEWER_ENTRY_PREVIEW_LIMIT))
    } else {
        vec![]
    };

    Ok(ViewerSummary {
        id: definition.id,
        slug: definition.slug.clone(),
        name: definition.name.clone(),
        signals: signal_mask_labels(definition.signal_mask),
        chart_type,
        query,
        filters,
        filter_mode,
        refresh_interval_ms: definition.refresh_interval_ms,
        lookback_ms: effective_lookback_ms,
        entry_count: entries.len(),
        status: status.clone(),
        entries: entries_for_response,
        traces: if include_entries && definition.signal_mask.contains(Signal::Traces) {
            extract_traces_from_entries(entries)
        } else {
            vec![]
        },
    })
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

struct MetricFields {
    service_name: Option<String>,
    metric_name: Option<String>,
    metric_value: Option<String>,
}

fn extract_metric_fields(payload: &Bytes) -> Option<MetricFields> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let resource_metrics = value.get("resourceMetrics")?.as_array()?;
    let service_name = extract_service_name_from_value(Signal::Metrics, &value);
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
    let service_name = extract_service_name_from_value(Signal::Logs, &value);
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
    let service_name = extract_service_name_from_value(Signal::Traces, &value);
    let span_name = value
        .get("resourceSpans")
        .and_then(serde_json::Value::as_array)
        .and_then(|resource_spans| {
            resource_spans.iter().find_map(|rs| {
                rs.get("scopeSpans")
                    .and_then(serde_json::Value::as_array)?
                    .iter()
                    .find_map(|ss| {
                        ss.get("spans")
                            .and_then(serde_json::Value::as_array)?
                            .iter()
                            .find_map(|span| span.get("name").and_then(serde_json::Value::as_str))
                            .map(str::to_string)
                    })
            })
        });

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

    traces.sort_by_key(|b| std::cmp::Reverse(b.started_at_ns));
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

    #[test]
    fn test_chart_type_from_definition_defaults_to_table_when_kind_is_missing() {
        assert_eq!(
            chart_type_from_definition(&serde_json::json!({})).unwrap(),
            "table"
        );
    }

    #[test]
    fn test_chart_type_from_definition_requires_valid_kind_when_present() {
        assert_eq!(
            chart_type_from_definition(&serde_json::json!({ "kind": "line" })).unwrap(),
            "line"
        );
        assert_eq!(
            chart_type_from_definition(&serde_json::json!({ "kind": "area" })).unwrap(),
            "area"
        );
        assert!(chart_type_from_definition(&serde_json::json!({ "kind": 1 })).is_err());
        assert!(chart_type_from_definition(&serde_json::json!({ "kind": "heatmap" })).is_err());
    }

    #[test]
    fn test_chart_type_from_definition_accepts_pie_and_donut_when_present() {
        assert_eq!(
            chart_type_from_definition(&serde_json::json!({ "kind": "pie" })).unwrap(),
            "pie"
        );
        assert_eq!(
            chart_type_from_definition(&serde_json::json!({ "kind": "donut" })).unwrap(),
            "donut"
        );
    }

    #[test]
    fn test_chart_type_from_definition_accepts_billboard() {
        assert_eq!(
            chart_type_from_definition(&serde_json::json!({ "kind": "billboard" })).unwrap(),
            "billboard"
        );
    }

    #[test]
    fn test_is_valid_chart_type_accepts_billboard() {
        assert!(is_valid_chart_type("billboard"));
    }

    #[test]
    fn test_is_valid_chart_type_rejects_unknown_types() {
        assert!(!is_valid_chart_type("heatmap"));
        assert!(!is_valid_chart_type("scatter"));
        assert!(!is_valid_chart_type(""));
    }

    #[test]
    fn test_chart_type_support_is_metrics_only_for_non_table_views() {
        let metrics_only = Signal::Metrics.into();
        let traces_only = Signal::Traces.into();
        let logs_only = Signal::Logs.into();

        assert!(is_chart_type_supported_for_signal_mask(
            "table",
            metrics_only
        ));
        assert!(is_chart_type_supported_for_signal_mask(
            "table",
            traces_only
        ));
        assert!(is_chart_type_supported_for_signal_mask("table", logs_only));
        assert!(is_chart_type_supported_for_signal_mask(
            "line",
            metrics_only
        ));
        assert!(is_chart_type_supported_for_signal_mask(
            "stacked_bar",
            metrics_only
        ));
        assert!(is_chart_type_supported_for_signal_mask(
            "area",
            metrics_only
        ));
        assert!(is_chart_type_supported_for_signal_mask("pie", metrics_only));
        assert!(is_chart_type_supported_for_signal_mask(
            "donut",
            metrics_only
        ));
        assert!(is_chart_type_supported_for_signal_mask(
            "billboard",
            metrics_only
        ));
        assert!(!is_chart_type_supported_for_signal_mask(
            "line",
            traces_only
        ));
        assert!(!is_chart_type_supported_for_signal_mask(
            "stacked_bar",
            logs_only
        ));
        assert!(!is_chart_type_supported_for_signal_mask(
            "area",
            traces_only
        ));
        assert!(!is_chart_type_supported_for_signal_mask("pie", logs_only));
        assert!(!is_chart_type_supported_for_signal_mask(
            "donut",
            traces_only
        ));
        assert!(!is_chart_type_supported_for_signal_mask(
            "billboard",
            traces_only
        ));
        assert!(!is_chart_type_supported_for_signal_mask(
            "billboard",
            logs_only
        ));
    }

    #[test]
    fn test_dashboard_columns_from_layout_defaults_to_two_when_missing() {
        assert_eq!(
            dashboard_columns_from_layout(&serde_json::json!({})).unwrap(),
            DEFAULT_DASHBOARD_COLUMNS
        );
    }

    #[test]
    fn test_dashboard_columns_from_layout_requires_in_range_columns() {
        assert_eq!(
            dashboard_columns_from_layout(&serde_json::json!({ "columns": 3 })).unwrap(),
            3
        );
        assert!(dashboard_columns_from_layout(&serde_json::json!({ "columns": "2" })).is_err());
        assert!(dashboard_columns_from_layout(&serde_json::json!({ "columns": 0 })).is_err());
        assert!(dashboard_columns_from_layout(&serde_json::json!({ "columns": 5 })).is_err());
    }

    #[test]
    fn test_dashboard_panels_from_layout_skips_panels_without_position() {
        let panels = dashboard_panels_from_layout(&serde_json::json!({
            "panels": [
                { "viewer_id": Uuid::nil().to_string() },
                { "viewer_id": Uuid::max().to_string(), "position": 2 }
            ]
        }));

        assert_eq!(panels.len(), 1);
        assert_eq!(panels[0].viewer_id, Uuid::max());
        assert_eq!(panels[0].position, 2);
        assert_eq!(panels[0].col_span, 1); // default
        assert_eq!(panels[0].row_span, 1); // default
    }

    #[test]
    fn test_dashboard_panels_from_layout_defaults_missing_spans_to_one() {
        let id = Uuid::new_v4();
        let panels = dashboard_panels_from_layout(&serde_json::json!({
            "panels": [{ "viewer_id": id.to_string(), "position": 0 }]
        }));

        assert_eq!(panels.len(), 1);
        assert_eq!(panels[0].col_span, 1);
        assert_eq!(panels[0].row_span, 1);
    }

    #[test]
    fn test_dashboard_panels_from_layout_reads_explicit_spans() {
        let id = Uuid::new_v4();
        let panels = dashboard_panels_from_layout(&serde_json::json!({
            "panels": [{
                "viewer_id": id.to_string(),
                "position": 0,
                "col_span": 3,
                "row_span": 2
            }]
        }));

        assert_eq!(panels.len(), 1);
        assert_eq!(panels[0].col_span, 3);
        assert_eq!(panels[0].row_span, 2);
    }

    #[test]
    fn test_dashboard_panels_from_layout_returns_empty_when_no_panels_key() {
        let panels = dashboard_panels_from_layout(&serde_json::json!({ "columns": 2 }));

        assert!(panels.is_empty());
    }

    async fn index_html() -> (StatusCode, String) {
        let app = Router::new().route("/", get(index));
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.into()).unwrap())
    }

    #[tokio::test]
    async fn test_root_returns_viewer_page() {
        let (status, html) = index_html().await;

        assert_eq!(status, StatusCode::OK);

        assert!(html.contains("litelemetry viewer"));
        assert!(html.contains("Create viewer"));
        assert!(html.contains("viewer-signal-select"));
        assert!(html.contains("Loading viewers"));
        assert!(html.contains("status-box"));
        assert!(html.contains("viewer-table"));
    }

    #[tokio::test]
    async fn test_root_returns_dashboard_elements() {
        let (_, html) = index_html().await;

        assert!(html.contains("sidebar"));
        assert!(html.contains("nav-viewers"));
        assert!(html.contains("new-dashboard-button"));
        assert!(html.contains("page-dashboard"));
        assert!(html.contains("dashboard-grid"));
        assert!(html.contains("sidebar-toggle"));
        assert!(html.contains("viewer-sortable-item"));
        assert!(html.contains("viewer-drag-handle"));
    }

    #[tokio::test]
    async fn test_root_returns_dashboard_refresh_controls() {
        let (_, html) = index_html().await;

        assert!(html.contains("dashboard-refresh-interval"));
        assert!(html.contains("dashboard-refresh-toggle"));
        assert!(html.contains("dashboard-last-updated"));
        assert!(html.contains("dashboard-refresh-button"));
        assert!(html.contains("<option value=\"off\">Off</option>"));
        assert!(html.contains("<option value=\"30000\" selected>30s</option>"));
        assert!(html.contains("Last updated: --"));
        assert!(html.contains("Refresh now"));
    }

    #[tokio::test]
    async fn test_root_contains_dashboard_refresh_wiring() {
        let (_, html) = index_html().await;

        assert!(html.contains("let dashboardRefreshIntervalMs = 30000;"));
        assert!(html.contains("let dashboardRefreshRequest = null;"));
        assert!(html.contains("let dashboardRefreshTimer = null;"));
        assert!(html.contains("let dashboardRefreshPaused = false;"));
        assert!(html.contains("function stopDashboardRefresh() {"));
        assert!(html.contains("if (dashboardRefreshRequest) {"));
        assert!(html.contains("function restartDashboardRefresh({ immediate = false } = {}) {"));
        assert!(html.contains("dashboardRefreshIntervalSelect.addEventListener('change',"));
        assert!(html.contains("dashboardRefreshToggleButton.addEventListener('click',"));
        assert!(html.contains("dashboardManualRefreshButton.addEventListener('click',"));
        assert!(html.contains("stopDashboardRefresh();"));
        assert!(html.contains("loadDashboard(currentDashboardId, { refresh: true })"));
    }

    #[tokio::test]
    async fn test_dashboard_header_contains_lookback_selector() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("id=\"dashboard-range-selector\""),
            "dashboard header should contain lookback selector"
        );
        assert!(
            html.contains("id=\"dashboard-range-label\""),
            "dashboard header should contain lookback label"
        );
        assert!(
            html.contains("id=\"dashboard-custom-lookback-minutes\""),
            "dashboard header should contain custom lookback minutes input"
        );
    }

    #[tokio::test]
    async fn test_dashboard_lookback_presets_are_available() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("value=\"default\""),
            "dashboard range selector should have default preset"
        );
        assert!(
            html.contains("value=\"5m\""),
            "dashboard range selector should have 5m preset"
        );
        assert!(
            html.contains("value=\"15m\""),
            "dashboard range selector should have 15m preset"
        );
        assert!(
            html.contains("value=\"1h\""),
            "dashboard range selector should have 1h preset"
        );
        assert!(
            html.contains("value=\"6h\""),
            "dashboard range selector should have 6h preset"
        );
        assert!(
            html.contains("value=\"24h\""),
            "dashboard range selector should have 24h preset"
        );
        assert!(
            html.contains("value=\"custom\""),
            "dashboard range selector should have custom preset"
        );
    }

    #[tokio::test]
    async fn test_dashboard_js_contains_hash_sync_functions() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("function buildDashboardHash("),
            "dashboard JS should contain buildDashboardHash function"
        );
        assert!(
            html.contains("function parseDashboardHash("),
            "dashboard JS should contain parseDashboardHash function"
        );
        assert!(
            html.contains("function syncDashboardRangeControls("),
            "dashboard JS should contain syncDashboardRangeControls function"
        );
        assert!(
            html.contains("function applyDashboardRangeSelection("),
            "dashboard JS should contain applyDashboardRangeSelection function"
        );
        assert!(
            html.contains("function routeFromHash("),
            "dashboard JS should contain routeFromHash function"
        );
    }

    #[tokio::test]
    async fn test_dashboard_js_loads_with_lookback_query_param() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("function loadDashboard("),
            "dashboard JS should contain loadDashboard function"
        );
        assert!(
            html.contains("URLSearchParams"),
            "loadDashboard should use URLSearchParams for query building"
        );
    }

    #[tokio::test]
    async fn test_root_exposes_area_chart_type_in_metrics_ui() {
        let (_, html) = index_html().await;

        assert!(html.contains("<option value=\"area\">Area</option>"));
        assert!(html.contains("area: 'Area'"));
    }

    #[tokio::test]
    async fn test_root_exposes_pie_and_donut_chart_types_for_metrics_viewers() {
        let (_, html) = index_html().await;

        assert!(html.contains("<option value=\"pie\">Pie</option>"));
        assert!(html.contains("<option value=\"donut\">Donut</option>"));
        assert!(html.contains("pie: 'Pie'"));
        assert!(html.contains("donut: 'Donut'"));
    }

    #[tokio::test]
    async fn test_root_contains_area_chart_rendering_branch() {
        let (_, html) = index_html().await;
        assert!(html.contains("function buildChartData(entries, lookbackMs, chartType) {"));
        assert!(html.contains("const isArea = chartType === 'area';"));
        assert!(html.contains("fill: isArea"));
        assert!(html.contains("chartSeriesColors(series, isArea ? 0.3 : 0.7)"));
        assert!(html.contains("function resolveChartConfig(chartType) {"));
        assert!(html.contains("buildChartData(entries, lookbackMs, chartType)"));
        assert!(html.contains("resolveChartConfig(chartType)"));
        assert!(
            html.contains("function renderPanelChart(chartType, entries, lookbackMs, canvas) {")
        );
    }

    #[tokio::test]
    async fn test_root_contains_circular_chart_rendering_branch() {
        let (_, html) = index_html().await;

        assert!(html.contains("const CIRCULAR_CHART_TYPES = { pie: 'pie', donut: 'doughnut' };"));
        assert!(html.contains("function buildPieData(entries) {"));
        assert!(
            html.contains("function buildCircularChartConfig(chartType, entries, legendLabels) {")
        );
        assert!(html.contains("function makeCircularTooltipLabel(total) {"));
        assert!(html.contains("function isCircularChartType(chartType) {"));
    }

    #[tokio::test]
    async fn test_root_returns_query_input_and_preview_panel() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("viewer-query-input"),
            "query input element must exist"
        );
        assert!(
            html.contains("preview-panel"),
            "preview panel container must exist"
        );
        assert!(
            html.contains("preview-summary"),
            "preview summary element must exist"
        );
        assert!(html.contains("Preview"), "Preview label must appear");
        assert!(
            html.contains("if (page === 'viewers') {\n          previewViewer(true);\n        }"),
            "returning to viewers must restore the preview"
        );
    }

    #[tokio::test]
    async fn test_root_initializes_preview_on_first_load_and_refresh() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("syncCreateForm();\n      previewViewer(true);\n      refreshViewers({ silent: true });"),
            "initial page load must trigger the preview before the first viewer list refresh"
        );
        assert!(
            html.contains(
                "refreshViewersButton.addEventListener('click', () => {\n        refreshViewers();\n        previewViewer(true);\n      });"
            ),
            "manual refresh must also refresh the preview panel"
        );
    }

    #[tokio::test]
    async fn test_root_clears_stale_preview_state_during_reload_and_errors() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("function clearPreviewContent()"),
            "preview content reset helper must exist"
        );
        assert!(
            html.contains(
                "previewStatusEl.textContent = 'Loading...';\n        clearPreviewContent();"
            ),
            "loading a preview must clear stale content"
        );
        assert!(
            html.contains("showPreviewMessage('Preview unavailable.');"),
            "failed previews must replace stale content with an explicit empty state"
        );
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

    #[tokio::test]
    async fn test_main_with_sidebar_width_accounts_for_sidebar() {
        let (_, html) = index_html().await;
        assert!(
            html.contains("width: min(1200px, calc(100% - var(--sidebar-width) - 72px))"),
            "main.with-sidebar must constrain width to viewport minus sidebar, left gap (24px), and right gap (48px)"
        );
    }

    #[tokio::test]
    async fn test_root_does_not_hide_invalid_dashboard_or_chart_type_values_with_fallbacks() {
        let (_, html) = index_html().await;

        assert!(html.contains("readViewerChartType(viewer)"));
        assert!(html.contains("viewerSupportsMetricCharts(viewer)"));
        assert!(html.contains("metricChartValue(entry)"));
        assert!(html.contains("readViewerPlaceholder(signal)"));
        assert!(html.contains("chartTypeLabel(chartType)"));
        assert!(html.contains("readDashboardColumns(data.columns)"));
        assert!(html.contains("const value = Number(input.value);"));
        assert!(!html.contains("viewer.chart_type || 'table'"));
        assert!(!html.contains("viewer.signals.includes('metrics')"));
        assert!(!html.contains("v.chart_type || 'table'"));
        assert!(!html.contains("v.signals.includes('metrics')"));
        assert!(!html.contains("entry.metric_value ?? 0"));
        assert!(!html.contains("Number(v) || 0"));
        assert!(!html.contains("Number(context.raw) || 0"));
        assert!(!html.contains("tryCircularChart("));
        assert!(!html.contains("VIEWER_PLACEHOLDERS[signal] || signal"));
        assert!(!html.contains("CHART_TYPE_LABELS[chartType] || chartType"));
        assert!(!html.contains("data.columns || 2"));
        assert!(!html.contains("dash.columns || 2"));
        assert!(!html.contains("Number.parseInt(input.value, 10)"));
    }
    #[tokio::test]
    async fn test_root_contains_filter_ui_elements() {
        let (_, html) = index_html().await;

        // create form filter elements
        assert!(
            html.contains("filter-rows-container"),
            "create form filter rows container must be present"
        );
        assert!(
            html.contains("add-filter-button"),
            "create form add filter button must be present"
        );
        assert!(
            html.contains("filter-mode-toggle"),
            "create form filter mode toggle must be present"
        );
        assert!(
            html.contains("filter-mode-and"),
            "create form AND button must be present"
        );
        assert!(
            html.contains("filter-mode-or"),
            "create form OR button must be present"
        );

        // viewer detail filter elements
        assert!(
            html.contains("viewer-filter-badges"),
            "detail filter badges container must be present"
        );
        assert!(
            html.contains("viewer-filter-section"),
            "detail filter section must be present"
        );
        assert!(
            html.contains("detail-filter-mode-toggle"),
            "detail filter mode toggle must be present"
        );
        assert!(
            html.contains("detail-filter-rows-container"),
            "detail filter rows container must be present"
        );
        assert!(
            html.contains("detail-add-filter-button"),
            "detail add filter button must be present"
        );
        assert!(
            html.contains("detail-save-filters-button"),
            "detail save filters button must be present"
        );
    }

    #[test]
    fn test_apply_filters_to_definition_sets_filters_and_mode() {
        let mut def = json!({});
        let filters = vec![ViewerFilterInput {
            field: "service_name".into(),
            op: "eq".into(),
            value: "svc-a".into(),
        }];
        apply_filters_to_definition(&mut def, Some(&filters), Some("or"));
        assert_eq!(def["filters"][0]["field"], "service_name");
        assert_eq!(def["filters"][0]["op"], "eq");
        assert_eq!(def["filters"][0]["value"], "svc-a");
        assert_eq!(def["filter_mode"], "or");
    }

    #[test]
    fn test_apply_filters_to_definition_empty_mode_removes_filter_mode() {
        let mut def = json!({ "filter_mode": "or" });
        let filters = vec![ViewerFilterInput {
            field: "service_name".into(),
            op: "eq".into(),
            value: "x".into(),
        }];
        apply_filters_to_definition(&mut def, Some(&filters), Some(""));
        assert_eq!(def["filters"][0]["value"], "x");
        assert!(
            def.get("filter_mode").is_none(),
            "empty mode string should remove filter_mode"
        );
    }

    #[test]
    fn test_apply_filters_to_definition_empty_slice_removes_filters_and_mode() {
        let mut def = json!({ "filters": [{"field":"service_name","op":"eq","value":"x"}], "filter_mode": "and" });
        apply_filters_to_definition(&mut def, Some(&[]), Some("and"));
        assert!(
            def.get("filters").is_none(),
            "empty filters slice should remove filters key"
        );
        assert!(
            def.get("filter_mode").is_none(),
            "empty filters slice should remove filter_mode key"
        );
    }

    #[test]
    fn test_apply_filters_to_definition_none_does_not_touch_existing() {
        let mut def = json!({ "filters": [{"field":"service_name","op":"eq","value":"existing"}], "filter_mode": "or" });
        apply_filters_to_definition(&mut def, None, None);
        assert_eq!(
            def["filters"][0]["value"], "existing",
            "None filters should leave existing filters untouched"
        );
        assert_eq!(
            def["filter_mode"], "or",
            "None filters should leave existing filter_mode untouched"
        );
    }

    #[tokio::test]
    async fn test_root_returns_billboard_option_in_chart_type_select() {
        let (_, html) = index_html().await;

        assert!(
            html.contains(r#"<option value="billboard">Billboard</option>"#),
            "create viewer form must include billboard option"
        );
    }

    #[tokio::test]
    async fn test_root_returns_billboard_in_chart_type_labels() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("billboard: 'Billboard'"),
            "CHART_TYPE_LABELS must include billboard entry"
        );
    }

    #[tokio::test]
    async fn test_root_contains_dashboard_fullscreen() {
        let (_, html) = index_html().await;

        for (needle, label) in [
            ("dashboard-fullscreen-button", "fullscreen button"),
            ("dashboard-page-actions", "actions wrapper"),
            ("dashboard-panel-media", "panel media class"),
            ("dashboard-panel-scroll", "panel scroll class"),
            ("toggleDashboardFullscreen", "toggle function"),
            ("requestFullscreen", "enter fullscreen API"),
            ("exitFullscreen", "exit fullscreen API"),
            ("fullscreenchange", "fullscreen event listener"),
            ("chart.resize()", "chart resize on toggle"),
            ("exitDashboardFullscreen", "cleanup helper"),
        ] {
            assert!(html.contains(needle), "must contain {label}");
        }

        for css in [
            "body.fullscreen #sidebar",
            "body.fullscreen main.with-sidebar",
            "body.fullscreen .dashboard-page-header",
            "body.fullscreen .dashboard-grid",
            "body.fullscreen .dashboard-panel-media",
            "body.fullscreen .dashboard-panel-scroll",
        ] {
            assert!(
                html.contains(css),
                "must contain fullscreen CSS rule: {css}"
            );
        }
    }

    #[tokio::test]
    async fn test_root_contains_autocomplete_datalist_elements() {
        let (_, html) = index_html().await;

        assert!(
            html.contains(r#"id="viewer-name-datalist""#),
            "viewer-name-datalist must be present for name autocomplete"
        );
        assert!(
            html.contains(r#"id="viewer-query-datalist""#),
            "viewer-query-datalist must be present for query autocomplete"
        );
        assert!(
            html.contains(r#"id="viewer-detail-query-datalist""#),
            "viewer-detail-query-datalist must be present for viewer detail autocomplete"
        );
        assert!(
            html.contains(r#"id="detail-query-datalist""#),
            "detail-query-datalist must be present for filter editor autocomplete"
        );
        assert!(
            html.contains(r#"list="viewer-name-datalist""#),
            "viewer-name-input must reference viewer-name-datalist via list attribute"
        );
        assert!(
            html.contains(r#"list="viewer-query-datalist""#),
            "viewer-query-input must reference viewer-query-datalist via list attribute"
        );
        assert!(
            html.contains(r#"list="viewer-detail-query-datalist""#),
            "viewer-detail-query-input must reference viewer-detail-query-datalist via list attribute"
        );
        assert!(
            html.contains(r#"list="detail-query-datalist""#),
            "detail-query-input must reference detail-query-datalist via list attribute"
        );
    }

    #[tokio::test]
    async fn test_root_contains_autocomplete_js_wiring() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("async function loadServiceNamesForAutocomplete()"),
            "loadServiceNamesForAutocomplete function must exist"
        );
        assert!(
            html.contains("function populateViewerNameDatalist()"),
            "populateViewerNameDatalist function must exist"
        );
        assert!(
            html.contains("function attachServiceAutocomplete(valueInput, fieldSel)"),
            "attachServiceAutocomplete function must exist"
        );
        assert!(
            html.contains("let cachedServiceNames = []"),
            "cachedServiceNames global variable must be initialized as empty array"
        );
        assert!(
            html.contains("loadServiceNamesForAutocomplete()"),
            "loadServiceNamesForAutocomplete must be called during page initialization"
        );
        assert!(
            html.contains("attachServiceAutocomplete(valueInput, fieldSel)"),
            "makeFilterRow must call attachServiceAutocomplete for the value input"
        );
        assert!(
            html.contains("const signals = ['traces', 'logs', 'metrics']"),
            "viewer name autocomplete must enumerate traces/logs/metrics signal types"
        );
        assert!(
            html.contains("`${svc} ${sig}`"),
            "viewer name datalist options must be formatted as '<service> <signal>'"
        );
    }

    // --- Signal select tests ---

    #[tokio::test]
    async fn test_signal_select_html_structure() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("<select id=\"viewer-signal-select\""),
            "signal selector must be a <select> element"
        );
        assert!(
            html.contains("data-testid=\"viewer-signal-select\""),
            "signal select must preserve data-testid attribute"
        );
        assert!(
            !html.contains("list=\"viewer-signal-datalist\""),
            "signal select must not reference a datalist"
        );
    }

    #[tokio::test]
    async fn test_signal_select_options() {
        let (_, html) = index_html().await;

        assert!(
            !html.contains("<datalist id=\"viewer-signal-datalist\">"),
            "datalist element must not be present"
        );
        assert!(
            html.contains("<option value=\"traces\">Traces</option>"),
            "select must contain a Traces option"
        );
        assert!(
            html.contains("<option value=\"metrics\">Metrics</option>"),
            "select must contain a Metrics option"
        );
        assert!(
            html.contains("<option value=\"logs\">Logs</option>"),
            "select must contain a Logs option"
        );
    }

    #[tokio::test]
    async fn test_signal_js_simplified_get_signal() {
        let (_, html) = index_html().await;

        assert!(
            !html.contains("const VALID_SIGNALS"),
            "VALID_SIGNALS constant must not be present"
        );
        assert!(
            !html.contains("function normalizeSignal("),
            "normalizeSignal function must not be present"
        );
        assert!(
            html.contains("function getSignal()"),
            "getSignal helper must still be defined"
        );
        assert!(
            html.contains("return viewerSignalSelect.value"),
            "getSignal must return viewerSignalSelect.value directly"
        );
    }

    #[tokio::test]
    async fn test_signal_js_usage_in_functions() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("const signal = getSignal()"),
            "syncCreateForm should use getSignal()"
        );
        assert!(
            html.contains("const signalType = getSignal()"),
            "fetchPreview should use getSignal()"
        );
        assert!(
            !html.contains("Invalid signal type"),
            "invalid-signal error guard must not be present"
        );
        assert!(
            !html.contains("const signal = viewerSignalSelect.value"),
            "createViewer must not read viewerSignalSelect.value directly; use getSignal()"
        );
    }

    #[tokio::test]
    async fn test_signal_change_event_listener() {
        let (_, html) = index_html().await;

        assert!(
            html.contains("viewerSignalSelect.addEventListener('change'"),
            "signal select must listen to 'change' event"
        );
        assert!(
            !html.contains("viewerSignalSelect.addEventListener('input'"),
            "signal select must not listen to 'input' event"
        );
        assert!(
            html.contains("syncCreateForm()") && html.contains("previewViewer()"),
            "signal change handler must call syncCreateForm and previewViewer"
        );
    }
}
