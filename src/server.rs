use crate::api_auth::api_key_auth_middleware;
use crate::apm::error_groups::{ErrorGroup, ErrorGroupAggregator, extract_error_occurrences};
use crate::apm::parse_otlp_nano as parse_nano;
use crate::apm::service_map::{ServiceMap, build_service_map};
use crate::apm::waterfall::{TraceWaterfall, WaterfallError, build_waterfall};
use crate::domain::api_key::ApiKey;
use crate::domain::dashboard::{
    DashboardDefinition, PanelInput, build_layout_json, panel_inputs_from_viewer_ids,
};
use crate::domain::incident::{Incident, IncidentStatus, validate_transition};
use crate::domain::slo::{SloDefinition, SloFilterClause, SloFilterList};
use crate::domain::telemetry::{NormalizedEntry, Signal, SignalMask};
use crate::domain::viewer::{ViewerDefinition, ViewerStatus};
use crate::ingest::decode::{DecodeError, decompress_body, parse_content_encoding};
use crate::ingest::otlp_http::{
    attribute_string_value, extract_service_name_from_value, parse_ingest_request,
};
use crate::ingest::otlp_pb::payload_as_value;
use crate::notifications::{
    NotificationChannel, NotificationPayload, dispatch_to_channel, webhook::WebhookConfig,
};
use crate::query::executor::{Cell as QueryCell, execute as execute_query, source_to_signal};
use crate::query::parse as parse_query;
use crate::slo::calculator::{CompiledSlo, ErrorBudget};
use crate::storage::error_group_store::ErrorGroupStore;
use crate::storage::rollup::{Resolution, RollupStore};
use crate::storage::slo_store::SloStore;
use crate::storage::{ApiKeyStore, IncidentStore, StreamStore, ViewerStore};
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
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{delete, get, post},
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
      #page-dashboard,
      #page-alerts,
      #page-incidents,
      #page-notifications,
      #page-service-map {
        display: grid;
        gap: 24px;
      }

      .alerts-toolbar {
        display: flex;
        align-items: center;
        gap: 12px;
        flex-wrap: wrap;
      }

      #alert-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
      }
      #alert-table th,
      #alert-table td {
        padding: 10px 12px;
        text-align: left;
        border-bottom: 1px solid rgba(255, 255, 255, 0.08);
      }
      #alert-table tbody tr:hover {
        background: rgba(255, 255, 255, 0.03);
      }
      .alert-severity-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 600;
      }
      .alert-severity-warning {
        background: #c4810022;
        color: #f5b400;
      }
      .alert-severity-critical {
        background: #d2222222;
        color: #ff7373;
      }
      .modal-backdrop {
        position: fixed;
        inset: 0;
        background: rgba(0, 0, 0, 0.55);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 100;
      }
      .modal-backdrop[hidden] {
        display: none !important;
      }
      .modal-card {
        background: var(--panel-bg, #15171f);
        padding: 24px;
        border-radius: 16px;
        max-width: 480px;
        width: 92%;
        display: grid;
        gap: 12px;
      }
      .modal-card label {
        font-size: 12px;
        color: var(--muted, #9aa0aa);
        display: grid;
        gap: 4px;
      }
      .modal-card input,
      .modal-card select {
        padding: 8px 10px;
        border-radius: 8px;
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.12);
        color: inherit;
      }
      .modal-actions {
        display: flex;
        gap: 8px;
        justify-content: flex-end;
        margin-top: 8px;
      }

      .incident-status-pill {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
      .incident-status-pill.open {
        background: rgba(220, 38, 38, 0.15);
        color: #dc2626;
      }
      .incident-status-pill.acknowledged {
        background: rgba(217, 119, 6, 0.15);
        color: #d97706;
      }
      .incident-status-pill.resolved {
        background: rgba(22, 163, 74, 0.15);
        color: #16a34a;
      }
      .incident-detail-panel {
        background: var(--surface, #1f2937);
        border: 1px solid var(--border, rgba(148, 163, 184, 0.2));
        border-radius: 8px;
        padding: 16px;
      }
      .incident-detail-panel pre {
        background: rgba(15, 23, 42, 0.4);
        padding: 12px;
        border-radius: 6px;
        overflow: auto;
        max-height: 300px;
        font-size: 12px;
      }

      .service-map-svg {
        width: 100%;
        height: 420px;
        background: rgba(255, 251, 243, 0.5);
        border-radius: 18px;
        border: 1px solid var(--line);
      }

      .service-map-node {
        fill: var(--accent-soft);
        stroke: var(--accent);
        stroke-width: 1.5;
      }

      .service-map-node-label {
        font-size: 12px;
        fill: var(--ink);
        font-weight: 600;
        pointer-events: none;
      }

      .service-map-edge {
        stroke: var(--muted);
        stroke-width: 1.4;
        fill: none;
        marker-end: url(#service-map-arrow);
      }

      .service-map-edge-error {
        stroke: var(--accent-strong);
      }

      .service-map-edge-label {
        font-size: 10px;
        fill: var(--muted);
        pointer-events: none;
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

      .slo-budget-bar {
        position: relative;
        height: 16px;
        background: rgba(184, 64, 52, 0.18);
        border-radius: 6px;
        overflow: hidden;
      }
      .slo-budget-bar-fill {
        height: 100%;
        background: var(--teal);
        transition: width 0.3s ease;
      }
      .slo-budget-bar.warn .slo-budget-bar-fill {
        background: #c8932a;
      }
      .slo-budget-bar.crit .slo-budget-bar-fill {
        background: var(--accent);
      }
      .slo-budget-bar-label {
        position: absolute;
        inset: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 11px;
        color: var(--ink);
        font-variant-numeric: tabular-nums;
      }
      #slo-list-table th,
      #slo-list-table td {
        border-bottom: 1px solid var(--line);
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
        <a id="nav-waterfall" class="sidebar-item" href="#waterfall" data-page="waterfall">Trace Waterfall</a>
        <a id="nav-traces" class="sidebar-item" href="#traces" data-page="traces">Traces</a>
        <a id="nav-service-map" class="sidebar-item" href="#service-map" data-page="service-map">Service Map</a>
        <a id="nav-errors" class="sidebar-item" href="#errors" data-page="errors">Errors</a>
        <a id="nav-alerts" class="sidebar-item" href="#alerts" data-page="alerts">Alerts</a>
        <a id="nav-incidents" class="sidebar-item" href="#incidents" data-page="incidents">Incidents</a>
        <a id="nav-slos" class="sidebar-item" href="#slos" data-page="slos">SLOs</a>
        <a id="nav-query" class="sidebar-item" href="#query" data-page="query">Query</a>
        <a id="nav-anomaly" class="sidebar-item" href="#anomaly" data-page="anomaly">Anomaly</a>
        <a id="nav-db-insights" class="sidebar-item" href="#db-insights" data-page="db-insights">DB Insights</a>
        <div class="sidebar-section-label">Dashboards</div>
        <div id="dashboard-list"></div>
        <button id="new-dashboard-button" class="secondary sidebar-new-btn" type="button">+ New Dashboard</button>
        <button id="import-dashboard-button" class="secondary sidebar-new-btn" type="button">Import</button>
        <input type="file" id="import-dashboard-file" accept=".json" style="display:none" aria-hidden="true">
        <div class="sidebar-section-label">Settings</div>
        <a id="nav-notifications" class="sidebar-item" href="#notifications" data-page="notifications">Notifications</a>
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
            <option value="timeseries">Timeseries</option>
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
        <div id="viewer-exemplars" class="entries-table-wrap" hidden>
          <div class="toolbar-row" style="margin-bottom: 4px;">
            <strong>Exemplars (metric bucket -> sample trace_ids)</strong>
            <button id="viewer-exemplars-refresh" class="secondary btn-compact" type="button">Refresh</button>
          </div>
          <table>
            <thead>
              <tr>
                <th>Bucket Start</th>
                <th>Sample Trace IDs</th>
              </tr>
            </thead>
            <tbody id="viewer-exemplars-body"></tbody>
          </table>
          <p id="viewer-exemplars-empty" hidden style="text-align:center; color: var(--muted);">
            No matching trace samples in the current window.
          </p>
        </div>
      </section>
      </div><!-- #page-viewers -->

      <div id="page-query" hidden>
        <section class="panel panel-strong stack" aria-labelledby="query-title">
          <div class="toolbar-row" style="align-items:center;">
            <h2 id="query-title" style="margin:0;font-size:1.4rem;">Ad-hoc Query</h2>
            <span class="label" style="color:var(--accent-strong);">SQL DSL</span>
          </div>
          <p style="margin:0;color:var(--muted);font-size:0.9rem;">
            Run SQL-like queries over recent telemetry. Sources: <code>traces</code>, <code>metrics</code>, <code>logs</code>.
            Supported columns: <code>service_name</code>, <code>signal</code>, <code>observed_at_ms</code>, <code>duration_ms</code>, <code>payload</code>.
            Aggregates: <code>count</code>, <code>sum</code>, <code>avg</code>, <code>min</code>, <code>max</code>.
          </p>
          <textarea id="query-input" data-testid="query-input" rows="6"
                    style="width:100%;padding:14px 16px;font-family:'JetBrains Mono','Menlo',monospace;font-size:0.95rem;border-radius:14px;border:1px solid var(--line);background:var(--panel-strong);color:var(--ink);resize:vertical;"
                    placeholder="SELECT count(*), service_name FROM traces GROUP BY service_name LIMIT 50"></textarea>
          <div class="toolbar-row" style="align-items:center;gap:10px;">
            <label for="query-lookback" style="font-size:0.85rem;color:var(--muted);">Lookback (ms):</label>
            <input id="query-lookback" type="number" min="1000" step="1000" value="300000"
                   style="width:140px;padding:8px 10px;border-radius:10px;border:1px solid var(--line);background:var(--panel-strong);color:var(--ink);" />
            <button id="query-run-button" data-testid="query-run-button" class="primary" type="button">Run query</button>
            <span id="query-status" data-testid="query-status" class="preview-status" role="status" aria-live="polite"></span>
          </div>
          <div id="query-error" data-testid="query-error" class="status-box" data-state="error" hidden></div>
          <div id="query-result-summary" data-testid="query-result-summary" style="font-size:0.85rem;color:var(--muted);" hidden></div>
          <div class="entries-table-wrap" id="query-result-wrap" hidden>
            <table data-testid="query-result-table">
              <thead><tr id="query-result-header"></tr></thead>
              <tbody id="query-result-body"></tbody>
            </table>
          </div>
        </section>
      </div><!-- #page-query -->

      <div id="page-service-map" hidden>
        <section class="panel panel-strong stack">
          <div class="toolbar-row" style="align-items:center;gap:12px;">
            <h2 style="margin:0;">Service Map</h2>
            <label for="service-map-lookback" style="font-size:0.85rem;color:var(--muted);">Lookback</label>
            <select id="service-map-lookback" aria-label="Lookback window">
              <option value="60000">1m</option>
              <option value="300000">5m</option>
              <option value="600000" selected>10m</option>
              <option value="1800000">30m</option>
              <option value="3600000">1h</option>
            </select>
            <button id="service-map-refresh" class="secondary" type="button">Refresh</button>
            <span id="service-map-status" class="preview-status" role="status" aria-live="polite"></span>
          </div>
          <svg id="service-map-svg" class="service-map-svg"
               viewBox="0 0 800 420" preserveAspectRatio="xMidYMid meet"
               xmlns="http://www.w3.org/2000/svg" aria-label="Service map graph">
            <defs>
              <marker id="service-map-arrow" viewBox="0 0 10 10" refX="9" refY="5"
                      markerWidth="6" markerHeight="6" orient="auto-start-reverse">
                <path d="M 0 0 L 10 5 L 0 10 z" fill="var(--muted)" />
              </marker>
            </defs>
          </svg>
          <p id="service-map-empty" hidden style="text-align:center;color:var(--muted);">
            No services detected in this window. Send some OTLP traces with parent/child spans across services.
          </p>
        </section>
        <section class="panel stack table-wrap">
          <h3 style="margin:0;">Nodes</h3>
          <div class="table-scroll">
            <table data-testid="service-map-nodes-table">
              <thead>
                <tr>
                  <th>Service</th>
                  <th>Spans</th>
                  <th>Errors</th>
                  <th>p95 (ms)</th>
                </tr>
              </thead>
              <tbody id="service-map-nodes-body"></tbody>
            </table>
          </div>
        </section>
        <section class="panel stack table-wrap">
          <h3 style="margin:0;">Edges</h3>
          <div class="table-scroll">
            <table data-testid="service-map-edges-table">
              <thead>
                <tr>
                  <th>From</th>
                  <th>To</th>
                  <th>Calls</th>
                  <th>Error rate</th>
                </tr>
              </thead>
              <tbody id="service-map-edges-body"></tbody>
            </table>
          </div>
        </section>
      </div><!-- #page-service-map -->

      <div id="page-errors" hidden>
        <section class="toolbar panel panel-strong">
          <div class="toolbar-row">
            <h2 style="margin:0;flex:1;">Error Groups</h2>
            <select id="errors-lookback-select" aria-label="Lookback window">
              <option value="300000">5m</option>
              <option value="900000">15m</option>
              <option value="3600000" selected>1h</option>
              <option value="21600000">6h</option>
              <option value="86400000">24h</option>
            </select>
            <button id="errors-refresh-button" class="secondary" type="button">Refresh</button>
          </div>
          <div id="errors-status-box" class="status-box" data-state="working">Loading error groups...</div>
        </section>
        <section class="panel panel-strong stack table-wrap">
          <div id="errors-table-scroll" class="table-scroll" hidden>
            <table data-testid="errors-table">
              <thead>
                <tr>
                  <th>Signature</th>
                  <th>Fingerprint</th>
                  <th>Count</th>
                  <th>First seen</th>
                  <th>Last seen</th>
                </tr>
              </thead>
              <tbody id="errors-table-body"></tbody>
            </table>
          </div>
          <div id="errors-empty" class="empty">
            <div class="stack">
              <strong id="errors-empty-title">No errors yet</strong>
              <p id="errors-empty-body">Send traces with exception events or ERROR-severity logs to populate this view.</p>
            </div>
          </div>
        </section>
        <section id="error-detail-section" class="panel panel-strong" hidden>
          <div class="toolbar-row" style="margin-bottom: 4px;">
            <h3 id="error-detail-title">Error Detail</h3>
            <button id="error-detail-close" class="secondary btn-compact" type="button" style="margin-left:auto;">x</button>
          </div>
          <dl id="error-detail-meta" style="display:grid;grid-template-columns:max-content 1fr;gap:4px 16px;margin:0;"></dl>
          <h4 style="margin-top:12px;">Stacktrace</h4>
          <pre id="error-detail-stacktrace" style="background:rgba(0,0,0,0.04);padding:12px;border-radius:6px;overflow:auto;max-height:320px;white-space:pre-wrap;"></pre>
        </section>
      </div><!-- #page-errors -->

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

      <div id="page-waterfall" hidden>
        <section class="toolbar panel panel-strong">
          <div class="toolbar-row">
            <input id="waterfall-trace-id-input" data-testid="waterfall-trace-id-input"
                   name="waterfall-trace-id" placeholder="Enter trace_id (hex)" maxlength="64"
                   style="flex:1;min-width:280px;font-family:monospace;"
                   aria-label="Trace ID" />
            <button id="waterfall-load-button" data-testid="waterfall-load-button"
                    class="primary" type="button">Load Waterfall</button>
          </div>
          <div id="waterfall-status" data-testid="waterfall-status" class="status-box"
               data-state="idle">
            Enter a trace_id and click Load Waterfall to view the span timeline.
          </div>
        </section>
        <section class="panel panel-strong stack">
          <div id="waterfall-summary" class="waterfall-summary"
               style="font-size:12px;color:var(--muted);margin-bottom:6px;" hidden></div>
          <div id="waterfall-svg-container" data-testid="waterfall-svg-container"
               style="overflow-x:auto;width:100%;"></div>
        </section>
      </div><!-- #page-waterfall -->

      <div id="page-traces" hidden>
        <section class="toolbar panel panel-strong">
          <div class="toolbar-row">
            <input id="trace-search-trace-id" type="search" placeholder="Trace ID (exact match)" maxlength="128"
                   aria-label="Trace ID lookup" style="flex:1;min-width:200px;" />
            <button id="trace-lookup-button" class="primary" type="button">Lookup</button>
          </div>
          <div class="toolbar-row">
            <input id="trace-search-service" type="search" placeholder="service.name (e.g. frontend)"
                   aria-label="Service name filter" maxlength="120" style="flex:1;min-width:160px;" />
            <input id="trace-search-min-duration" type="number" min="0" step="1" inputmode="numeric"
                   placeholder="min duration (ms)" aria-label="Minimum trace duration in ms"
                   style="max-width:170px;" />
            <button id="trace-search-button" class="secondary" type="button">Search</button>
            <button id="trace-search-refresh-button" class="secondary" type="button">Refresh</button>
          </div>
          <div id="trace-search-status" class="status-box" data-state="idle" role="status" aria-live="polite">
            Enter a trace ID for an exact lookup, or filter the list below.
          </div>
        </section>

        <section id="trace-search-list-section" class="panel panel-strong stack table-wrap">
          <div id="trace-search-list-scroll" class="table-scroll">
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
              <tbody id="trace-search-list-body"></tbody>
            </table>
          </div>
          <div id="trace-search-empty" class="empty" hidden>
            <div class="stack">
              <strong>No traces yet</strong>
              <p>POST OTLP traces to <code>/v1/traces</code> and refresh.</p>
            </div>
          </div>
        </section>

        <section id="trace-search-detail-section" class="panel panel-strong" hidden>
          <div class="toolbar-row" style="align-items:center;">
            <button id="trace-search-back-button" class="secondary" type="button">&larr; Back</button>
            <h3 id="trace-search-detail-title" style="margin:0;"></h3>
          </div>
          <div id="trace-search-detail-meta" class="trace-detail-meta" style="margin:8px 0; color: var(--muted); font-size: 13px;"></div>
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Service</th>
                  <th>Span</th>
                  <th>Kind</th>
                  <th>Span ID</th>
                  <th>Parent</th>
                  <th>Duration</th>
                  <th>Status</th>
                  <th>Span attributes</th>
                </tr>
              </thead>
              <tbody id="trace-search-detail-body"></tbody>
            </table>
          </div>
        </section>
      </div><!-- #page-traces -->

      <div id="page-alerts" hidden>
        <section class="toolbar panel panel-strong">
          <div class="alerts-toolbar">
            <h2 style="margin:0;">Alerts</h2>
            <button id="new-alert-button" class="primary" type="button">+ New Alert</button>
            <button id="refresh-alerts-button" class="secondary" type="button">Refresh</button>
          </div>
          <div id="alerts-status" class="status-box" data-state="working">Loading alerts...</div>
        </section>
        <section class="panel panel-strong table-wrap">
          <table id="alert-table" data-testid="alert-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Viewer</th>
                <th>Condition</th>
                <th>Severity</th>
                <th>Interval</th>
                <th>Enabled</th>
                <th></th>
              </tr>
            </thead>
            <tbody id="alert-table-body"></tbody>
          </table>
          <p id="alert-empty" hidden style="text-align:center;color:var(--muted);">No alerts configured yet.</p>
        </section>
      </div><!-- #page-alerts -->

      <div id="alert-modal" class="modal-backdrop" hidden role="dialog" aria-modal="true">
        <div class="modal-card">
          <h3 style="margin:0;">Create Alert</h3>
          <label>Name
            <input id="alert-modal-name" type="text" maxlength="80" placeholder="High traffic" />
          </label>
          <label>Viewer
            <select id="alert-modal-viewer"></select>
          </label>
          <label>Condition operator
            <select id="alert-modal-op">
              <option value=">">&gt;</option>
              <option value=">=">&gt;=</option>
              <option value="<">&lt;</option>
              <option value="<=">&lt;=</option>
              <option value="==">==</option>
              <option value="!=">!=</option>
            </select>
          </label>
          <label>Threshold value
            <input id="alert-modal-value" type="number" step="any" value="100" />
          </label>
          <label>Metric
            <select id="alert-modal-metric">
              <option value="count">count</option>
            </select>
          </label>
          <label>Severity
            <select id="alert-modal-severity">
              <option value="warning">warning</option>
              <option value="critical">critical</option>
            </select>
          </label>
          <label>Evaluation interval (ms)
            <input id="alert-modal-interval" type="number" min="1000" step="1000" value="5000" />
          </label>
          <div id="alert-modal-error" class="status-box" data-state="error" hidden></div>
          <div class="modal-actions">
            <button id="alert-modal-cancel" class="secondary" type="button">Cancel</button>
            <button id="alert-modal-save" class="primary" type="button">Create</button>
          </div>
        </div>
      </div>

      <div id="page-incidents" hidden>
        <section class="toolbar panel panel-strong">
          <div class="toolbar-row">
            <h2 style="margin:0;">Incidents</h2>
            <select id="incident-status-filter" aria-label="Filter by status">
              <option value="">All</option>
              <option value="open" selected>Open</option>
              <option value="acknowledged">Acknowledged</option>
              <option value="resolved">Resolved</option>
            </select>
            <button id="incident-refresh-button" class="secondary" type="button">Refresh</button>
            <span style="flex:1;"></span>
            <input id="incident-create-severity" placeholder="severity (e.g. critical)" maxlength="32" style="max-width:200px;" />
            <button id="incident-create-button" class="primary" type="button" data-testid="incident-create-button">+ Create incident</button>
          </div>
          <div id="incident-status-box" class="status-box" data-state="working">Loading incidents...</div>
        </section>

        <section class="panel panel-strong stack table-wrap">
          <div id="incident-table-scroll" class="table-scroll" hidden>
            <table data-testid="incident-table">
              <thead>
                <tr>
                  <th>Status</th>
                  <th>Severity</th>
                  <th>Opened</th>
                  <th>Acknowledged</th>
                  <th>Resolved</th>
                  <th>ID</th>
                  <th aria-label="Actions"></th>
                </tr>
              </thead>
              <tbody id="incident-table-body"></tbody>
            </table>
          </div>
          <div id="incident-empty" class="empty" hidden>
            <div class="stack">
              <strong>No incidents</strong>
              <p>No incidents match the current filter.</p>
            </div>
          </div>
        </section>

        <section id="incident-detail-section" class="panel panel-strong stack" hidden>
          <h3 id="incident-detail-title">Incident Detail</h3>
          <div id="incident-detail-body" class="incident-detail-panel"></div>
        </section>
      </div><!-- #page-incidents -->

      <div id="page-notifications" hidden>
        <section class="toolbar panel panel-strong">
          <div class="toolbar-row">
            <h2 style="margin:0;font-size:1.1rem;flex:1;">Notification Channels</h2>
            <button id="new-notification-channel-button" class="primary" type="button">+ New Channel</button>
            <button id="refresh-notification-channels-button" class="secondary" type="button">Refresh</button>
          </div>
          <div id="notification-channels-status" class="status-box" data-state="idle">
            Notification channels deliver alerts to external destinations (e.g. Slack, custom webhooks).
          </div>
        </section>

        <section class="panel panel-strong stack table-wrap">
          <div class="table-scroll">
            <table data-testid="notification-channels-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Kind</th>
                  <th>Destination</th>
                  <th>Enabled</th>
                  <th></th>
                </tr>
              </thead>
              <tbody id="notification-channels-table-body"></tbody>
            </table>
          </div>
        </section>
      </div><!-- #page-notifications -->

      <div id="page-slos" hidden>
        <section class="toolbar panel panel-strong">
          <div class="toolbar-row">
            <h2 style="margin:0;font-size:18px;">SLOs &amp; Error Budgets</h2>
            <button id="refresh-slos-button" class="secondary btn-compact" type="button">Refresh</button>
            <button id="new-slo-button" class="primary" type="button">+ New SLO</button>
          </div>
          <div id="slo-status-box" class="status-box" data-state="working">Loading SLOs...</div>
        </section>

        <section class="panel" id="slo-list-panel">
          <div id="slo-empty" class="status-box" data-state="empty" hidden>
            No SLOs yet. Click <strong>+ New SLO</strong> to define one.
          </div>
          <table id="slo-list-table" style="width:100%;border-collapse:collapse;" hidden>
            <thead>
              <tr>
                <th style="text-align:left;padding:8px;">Name</th>
                <th style="text-align:left;padding:8px;">Target</th>
                <th style="text-align:left;padding:8px;">Window</th>
                <th style="text-align:left;padding:8px;">Current</th>
                <th style="text-align:left;padding:8px;width:30%;">Remaining budget</th>
                <th style="text-align:left;padding:8px;">Actions</th>
              </tr>
            </thead>
            <tbody id="slo-list-body"></tbody>
          </table>
        </section>
      </div><!-- #page-slos -->

      <div id="page-anomaly" hidden>
        <section class="toolbar panel panel-strong">
          <h2 style="margin:0 0 8px 0;">Anomaly Detection</h2>
          <div class="toolbar-row">
            <label for="anomaly-viewer-select">Viewer</label>
            <select id="anomaly-viewer-select" data-testid="anomaly-viewer-select" style="min-width:240px;">
              <option value="">Loading...</option>
            </select>
            <label for="anomaly-detector-type">Detector</label>
            <select id="anomaly-detector-type" data-testid="anomaly-detector-type">
              <option value="no_data">No Data</option>
              <option value="zscore">Z-Score</option>
            </select>
            <span id="anomaly-no-data-fields" class="toolbar-row" style="gap:6px;display:inline-flex;align-items:center;">
              <label for="anomaly-window-ms">Window (ms)</label>
              <input id="anomaly-window-ms" type="number" value="300000" min="1000" step="1000" style="max-width:120px;">
            </span>
            <span id="anomaly-zscore-fields" class="toolbar-row" hidden style="gap:6px;display:inline-flex;align-items:center;">
              <label for="anomaly-threshold">Threshold</label>
              <input id="anomaly-threshold" type="number" value="3" min="0" step="0.1" style="max-width:80px;">
              <label for="anomaly-bucket-ms">Bucket (ms)</label>
              <input id="anomaly-bucket-ms" type="number" value="60000" min="1000" step="1000" style="max-width:120px;">
            </span>
            <button id="anomaly-evaluate-button" data-testid="anomaly-evaluate-button" class="primary" type="button">Evaluate</button>
          </div>
          <div id="anomaly-status" class="status-box" data-state="idle">Choose a viewer and detector, then click Evaluate.</div>
        </section>
        <section class="panel panel-strong" id="anomaly-result-panel" hidden>
          <h3 style="margin:0 0 8px 0;">Result</h3>
          <table>
            <tbody>
              <tr><th style="text-align:left;">Breached</th><td id="anomaly-result-breached">--</td></tr>
              <tr><th style="text-align:left;">Value</th><td id="anomaly-result-value">--</td></tr>
              <tr><th style="text-align:left;">Expected</th><td id="anomaly-result-expected">--</td></tr>
              <tr><th style="text-align:left;">Observed at</th><td id="anomaly-result-observed">--</td></tr>
            </tbody>
          </table>
        </section>
      </div><!-- #page-anomaly -->

      <div id="page-db-insights" hidden>
        <section class="toolbar panel panel-strong">
          <div class="toolbar-row">
            <h2 style="margin: 0;">DB Insights</h2>
            <label for="db-insights-lookback">
              Lookback
              <select id="db-insights-lookback">
                <option value="60000">1m</option>
                <option value="300000">5m</option>
                <option value="600000" selected>10m</option>
                <option value="1800000">30m</option>
                <option value="3600000">1h</option>
              </select>
            </label>
            <label for="db-insights-min-p95">
              Min p95 (ms)
              <input id="db-insights-min-p95" type="number" min="0" step="1" value="10" style="width:80px;">
            </label>
            <label for="db-insights-min-repetitions">
              N+1 threshold
              <input id="db-insights-min-repetitions" type="number" min="2" step="1" value="5" style="width:80px;">
            </label>
            <button id="db-insights-refresh" class="primary" type="button">Refresh</button>
          </div>
          <div id="db-insights-status" class="status-box" data-state="idle">Ready.</div>
        </section>

        <section class="panel panel-strong stack table-wrap">
          <h3 style="margin: 0;">Slow Queries</h3>
          <div id="slow-queries-empty" class="empty">
            <p>No slow queries detected in the selected window.</p>
          </div>
          <div id="slow-queries-wrap" class="table-scroll" hidden>
            <table data-testid="slow-queries-table">
              <thead>
                <tr>
                  <th>Statement</th>
                  <th>System</th>
                  <th>Count</th>
                  <th>p50 (ms)</th>
                  <th>p95 (ms)</th>
                  <th>p99 (ms)</th>
                  <th>Max (ms)</th>
                </tr>
              </thead>
              <tbody id="slow-queries-body"></tbody>
            </table>
          </div>
        </section>

        <section class="panel panel-strong stack table-wrap">
          <h3 style="margin: 0;">N+1 Detections</h3>
          <div id="n-plus-one-empty" class="empty">
            <p>No N+1 patterns detected in the selected window.</p>
          </div>
          <div id="n-plus-one-wrap" class="table-scroll" hidden>
            <table data-testid="n-plus-one-table">
              <thead>
                <tr>
                  <th>Trace ID</th>
                  <th>Statement</th>
                  <th>System</th>
                  <th>Count</th>
                  <th>Services</th>
                </tr>
              </thead>
              <tbody id="n-plus-one-body"></tbody>
            </table>
          </div>
        </section>
      </div><!-- #page-db-insights -->
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
      const viewerExemplars = document.getElementById('viewer-exemplars');
      const viewerExemplarsBody = document.getElementById('viewer-exemplars-body');
      const viewerExemplarsEmpty = document.getElementById('viewer-exemplars-empty');
      const viewerExemplarsRefresh = document.getElementById('viewer-exemplars-refresh');
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
        timeseries: 'Timeseries',
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

      function viewerTargetsMetrics(viewer) {
        return Array.isArray(viewer.signals) && viewer.signals.indexOf('metrics') !== -1;
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
      function renderChart(chartType, entries, lookbackMs, aggregatedBuckets) {
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

        if (chartType === 'timeseries') {
          viewerChartContainer.replaceChildren(viewerChartCanvas);
          viewerChartContainer.hidden = false;
          renderTimeseriesCanvas(viewerChartCanvas, aggregatedBuckets || []);
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

      // Vanilla canvas-based line chart for timeseries aggregations.
      // Each `aggregatedBuckets` entry is `{ bucket_start_ms, group_keys, value }`.
      // Series are keyed by `group_keys.join(' / ')` (or 'all' if empty).
      function renderTimeseriesCanvas(canvas, buckets) {
        const ctx = canvas.getContext('2d');
        const cssWidth = canvas.clientWidth || 600;
        const cssHeight = canvas.clientHeight || 240;
        const dpr = window.devicePixelRatio || 1;
        canvas.width = cssWidth * dpr;
        canvas.height = cssHeight * dpr;
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, cssWidth, cssHeight);

        if (!buckets || !buckets.length) {
          ctx.fillStyle = '#5d6778';
          ctx.font = '13px "Avenir Next", sans-serif';
          ctx.textAlign = 'center';
          ctx.fillText('No aggregated data yet.', cssWidth / 2, cssHeight / 2);
          return;
        }

        const seriesMap = new Map();
        for (const b of buckets) {
          const key = (b.group_keys && b.group_keys.length) ? b.group_keys.join(' / ') : 'all';
          if (!seriesMap.has(key)) seriesMap.set(key, []);
          seriesMap.get(key).push({ t: b.bucket_start_ms, v: b.value });
        }
        for (const arr of seriesMap.values()) arr.sort((a, b) => a.t - b.t);

        const allTs = buckets.map(b => b.bucket_start_ms);
        const allVs = buckets.map(b => b.value);
        const minT = Math.min(...allTs);
        const maxT = Math.max(...allTs);
        const tSpan = Math.max(1, maxT - minT);
        const minV = 0;
        const maxV = Math.max(1, Math.max(...allVs));

        const padL = 48, padR = 16, padT = 16, padB = 28;
        const w = cssWidth - padL - padR;
        const h = cssHeight - padT - padB;

        // axes
        ctx.strokeStyle = 'rgba(23, 32, 51, 0.18)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(padL, padT);
        ctx.lineTo(padL, padT + h);
        ctx.lineTo(padL + w, padT + h);
        ctx.stroke();

        // y ticks
        ctx.fillStyle = '#5d6778';
        ctx.font = '11px "Avenir Next", sans-serif';
        ctx.textAlign = 'right';
        const yTicks = 4;
        for (let i = 0; i <= yTicks; i++) {
          const v = minV + (maxV - minV) * (i / yTicks);
          const y = padT + h - (h * i / yTicks);
          ctx.fillText(v.toFixed(2), padL - 4, y + 3);
          ctx.strokeStyle = 'rgba(23, 32, 51, 0.06)';
          ctx.beginPath();
          ctx.moveTo(padL, y);
          ctx.lineTo(padL + w, y);
          ctx.stroke();
        }

        // x ticks (start, mid, end)
        ctx.textAlign = 'center';
        const xTickTs = [minT, (minT + maxT) / 2, maxT];
        for (const t of xTickTs) {
          const x = padL + ((t - minT) / tSpan) * w;
          const label = new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
          ctx.fillText(label, x, padT + h + 16);
        }

        // series colours via simple hash
        function hueForKey(key) {
          let h = 0;
          for (let i = 0; i < key.length; i++) h = (h * 31 + key.charCodeAt(i)) >>> 0;
          return h % 360;
        }

        // draw each series
        let legendX = padL;
        for (const [key, points] of seriesMap.entries()) {
          const hue = hueForKey(key);
          const stroke = `hsl(${hue}, 60%, 45%)`;
          ctx.strokeStyle = stroke;
          ctx.lineWidth = 2;
          ctx.beginPath();
          points.forEach((p, idx) => {
            const x = padL + ((p.t - minT) / tSpan) * w;
            const y = padT + h - ((p.v - minV) / (maxV - minV || 1)) * h;
            if (idx === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
          });
          ctx.stroke();

          // dots
          ctx.fillStyle = stroke;
          for (const p of points) {
            const x = padL + ((p.t - minT) / tSpan) * w;
            const y = padT + h - ((p.v - minV) / (maxV - minV || 1)) * h;
            ctx.beginPath();
            ctx.arc(x, y, 2.5, 0, Math.PI * 2);
            ctx.fill();
          }

          // legend swatch
          ctx.fillStyle = stroke;
          ctx.fillRect(legendX, 2, 10, 10);
          ctx.fillStyle = '#172033';
          ctx.textAlign = 'left';
          ctx.fillText(key, legendX + 14, 11);
          legendX += 18 + ctx.measureText(key).width;
        }
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
        if (viewerExemplars) viewerExemplars.hidden = true;
      }

      function renderExemplarBuckets(buckets) {
        if (!viewerExemplars) return;
        viewerExemplarsBody.replaceChildren();
        if (!buckets || !buckets.length) {
          viewerExemplars.hidden = false;
          viewerExemplarsEmpty.hidden = false;
          return;
        }
        viewerExemplarsEmpty.hidden = true;
        viewerExemplars.hidden = false;
        for (const bucket of buckets) {
          const row = document.createElement('tr');
          appendTableCell(row, new Date(bucket.bucket_start_ms).toLocaleTimeString());
          const idsCell = document.createElement('td');
          for (const tid of bucket.sample_trace_ids) {
            const link = document.createElement('a');
            link.href = '#';
            link.className = 'exemplar-trace-link';
            link.textContent = truncateId(tid);
            link.title = tid;
            link.style.marginRight = '8px';
            link.addEventListener('click', e => {
              e.preventDefault();
              jumpToTrace(tid);
            });
            idsCell.appendChild(link);
          }
          row.appendChild(idsCell);
          viewerExemplarsBody.appendChild(row);
        }
      }

      async function jumpToTrace(traceId) {
        // Look for a trace viewer that has this trace_id in its current entries.
        try {
          const resp = await fetch('/api/viewers', { headers: { accept: 'application/json' } });
          if (!resp.ok) throw new Error('HTTP ' + resp.status);
          const payload = await resp.json();
          for (const v of payload.viewers || []) {
            if (!v.signals.includes('traces')) continue;
            const detailResp = await fetch('/api/viewers/' + v.id, { headers: { accept: 'application/json' } });
            if (!detailResp.ok) continue;
            const detail = await detailResp.json();
            const match = (detail.traces || []).find(t => t.trace_id === traceId);
            if (match) {
              showViewerDetail(v.id).then(() => {
                setTimeout(() => showTraceDetail(match), 50);
              });
              setStatus('ok', 'Opened trace ' + truncateId(traceId));
              return;
            }
          }
          setStatus('idle', 'Trace ' + truncateId(traceId) + ' not in any trace viewer; trace_id copied.');
          if (navigator.clipboard) navigator.clipboard.writeText(traceId).catch(() => {});
        } catch (error) {
          setStatus('error', 'Failed to look up trace: ' + error.message);
        }
      }

      async function loadExemplarsForViewer(viewerId) {
        if (!viewerExemplars) return;
        try {
          const url = '/api/exemplars?metric_viewer_id=' + encodeURIComponent(viewerId) + '&bucket_ms=60000';
          const resp = await fetch(url, { headers: { accept: 'application/json' } });
          if (!resp.ok) {
            viewerExemplars.hidden = true;
            return;
          }
          const payload = await resp.json();
          renderExemplarBuckets(payload.buckets || []);
        } catch (_e) {
          viewerExemplars.hidden = true;
        }
      }

      if (viewerExemplarsRefresh) {
        viewerExemplarsRefresh.addEventListener('click', () => {
          if (selectedViewerId) loadExemplarsForViewer(selectedViewerId);
        });
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
          const usesChart = chartType === 'timeseries'
            || (chartType !== 'table' && viewerSupportsMetricCharts(viewer));
          if (usesChart) {
            renderChart(chartType, viewer.entries, viewer.lookback_ms, viewer.aggregated_buckets);
          } else if (isTraceViewer && viewer.traces) {
            renderTraceList(viewer.traces);
          } else {
            renderEntriesTable(viewer.entries);
          }

          // Load exemplars for any viewer that targets metrics.
          if (viewerTargetsMetrics(viewer)) {
            loadExemplarsForViewer(viewerId);
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
      const navTraces = document.getElementById('nav-traces');
      const pageTraces = document.getElementById('page-traces');
      const navIncidents = document.getElementById('nav-incidents');
      const navNotifications = document.getElementById('nav-notifications');
      const navQuery = document.getElementById('nav-query');
      const navAnomaly = document.getElementById('nav-anomaly');
      const navErrors = document.getElementById('nav-errors');
      const navDbInsights = document.getElementById('nav-db-insights');
      const newDashboardButton = document.getElementById('new-dashboard-button');
      const dashboardListEl = document.getElementById('dashboard-list');
      const pageViewers = document.getElementById('page-viewers');
      const pageQuery = document.getElementById('page-query');
      const pageErrors = document.getElementById('page-errors');
      const pageDashboard = document.getElementById('page-dashboard');
      const pageIncidents = document.getElementById('page-incidents');
      const pageNotifications = document.getElementById('page-notifications');
      const pageSlos = document.getElementById('page-slos');
      const navSlos = document.getElementById('nav-slos');
      const sloStatusBox = document.getElementById('slo-status-box');
      const sloListTable = document.getElementById('slo-list-table');
      const sloListBody = document.getElementById('slo-list-body');
      const sloEmpty = document.getElementById('slo-empty');
      const newSloButton = document.getElementById('new-slo-button');
      const refreshSlosButton = document.getElementById('refresh-slos-button');
      const pageAnomaly = document.getElementById('page-anomaly');
      const pageServiceMap = document.getElementById('page-service-map');
      const navServiceMap = document.getElementById('nav-service-map');
      const errorsLookbackSelect = document.getElementById('errors-lookback-select');
      const errorsRefreshButton = document.getElementById('errors-refresh-button');
      const errorsStatusBox = document.getElementById('errors-status-box');
      const errorsTableScroll = document.getElementById('errors-table-scroll');
      const errorsTableBody = document.getElementById('errors-table-body');
      const errorsEmpty = document.getElementById('errors-empty');
      const errorDetailSection = document.getElementById('error-detail-section');
      const errorDetailTitle = document.getElementById('error-detail-title');
      const errorDetailMeta = document.getElementById('error-detail-meta');
      const errorDetailStacktrace = document.getElementById('error-detail-stacktrace');
      const errorDetailClose = document.getElementById('error-detail-close');
      const pageDbInsights = document.getElementById('page-db-insights');
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
        pageErrors.hidden = page !== 'errors';
        pageDashboard.hidden = page !== 'dashboard';
        const pageWaterfallEl = document.getElementById('page-waterfall');
        if (pageWaterfallEl) pageWaterfallEl.hidden = page !== 'waterfall';
        pageTraces.hidden = page !== 'traces';
        const pageAlertsEl = document.getElementById('page-alerts');
        if (pageAlertsEl) pageAlertsEl.hidden = page !== 'alerts';
        pageIncidents.hidden = page !== 'incidents';
        pageNotifications.hidden = page !== 'notifications';
        if (pageSlos) pageSlos.hidden = page !== 'slos';
        if (pageQuery) {
          pageQuery.hidden = page !== 'query';
        }
        if (pageAnomaly) pageAnomaly.hidden = page !== 'anomaly';
        pageServiceMap.hidden = page !== 'service-map';
        if (pageDbInsights) pageDbInsights.hidden = page !== 'db-insights';

        navViewers.classList.toggle('active', page === 'viewers');
        const navWaterfallEl = document.getElementById('nav-waterfall');
        if (navWaterfallEl) navWaterfallEl.classList.toggle('active', page === 'waterfall');
        navTraces.classList.toggle('active', page === 'traces');
        const navAlertsEl = document.getElementById('nav-alerts');
        if (navAlertsEl) navAlertsEl.classList.toggle('active', page === 'alerts');

        if (page === 'alerts') {
          refreshAlerts();
        }
        navIncidents.classList.toggle('active', page === 'incidents');
        if (navNotifications) {
          navNotifications.classList.toggle('active', page === 'notifications');
        }
        if (navSlos) navSlos.classList.toggle('active', page === 'slos');
        if (navQuery) {
          navQuery.classList.toggle('active', page === 'query');
        }
        if (navAnomaly) navAnomaly.classList.toggle('active', page === 'anomaly');
        navServiceMap.classList.toggle('active', page === 'service-map');
        navErrors.classList.toggle('active', page === 'errors');
        if (navDbInsights) navDbInsights.classList.toggle('active', page === 'db-insights');

        document.querySelectorAll('.sidebar-dashboard-item').forEach(el => {
          el.classList.toggle('active', el.dataset.id === dashboardId);
        });

        updateDashboardRefreshControls();

        if (page === 'slos') {
          refreshSlos();
        }

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
        if (page === 'traces') {
          loadTraceSearch();
        }
        if (page === 'incidents') {
          refreshIncidents();
        }
        if (page === 'notifications') {
          refreshNotificationChannels();
        }
        if (page === 'anomaly') {
          refreshAnomalyViewerOptions();
        }
        if (page === 'service-map') {
          refreshServiceMap();
        }
        if (page === 'errors') {
          loadErrorGroups();
        }
        if (page === 'db-insights') {
          refreshDbInsights();
        }
        sidebar.classList.remove('open');

        if (page !== 'dashboard' && document.body.classList.contains('fullscreen')) {
          exitDashboardFullscreen();
        }
      }

      let errorGroupsCache = [];

      function setErrorsStatus(state, text) {
        errorsStatusBox.dataset.state = state;
        errorsStatusBox.textContent = text;
      }

      function formatErrorTimestamp(ts) {
        if (!ts) return '--';
        try {
          return new Date(ts).toLocaleString();
        } catch (_) {
          return ts;
        }
      }

      async function loadErrorGroups() {
        const lookbackMs = errorsLookbackSelect.value;
        setErrorsStatus('working', 'Loading error groups...');
        try {
          const resp = await fetch(`/api/error-groups?lookback_ms=${encodeURIComponent(lookbackMs)}`);
          if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
          }
          const data = await resp.json();
          errorGroupsCache = Array.isArray(data.error_groups) ? data.error_groups : [];
          renderErrorGroups(errorGroupsCache);
          setErrorsStatus('ok', `Loaded ${errorGroupsCache.length} error group(s).`);
        } catch (err) {
          setErrorsStatus('error', `Failed to load error groups: ${err.message || err}`);
          errorGroupsCache = [];
          renderErrorGroups([]);
        }
      }

      function renderErrorGroups(groups) {
        errorsTableBody.innerHTML = '';
        if (!groups || groups.length === 0) {
          errorsEmpty.hidden = false;
          errorsTableScroll.hidden = true;
          return;
        }
        errorsEmpty.hidden = true;
        errorsTableScroll.hidden = false;
        for (const g of groups) {
          const tr = document.createElement('tr');
          tr.style.cursor = 'pointer';
          tr.dataset.fingerprint = g.fingerprint;
          tr.appendChild(buildCell('strong', g.signature || ''));
          tr.appendChild(buildCell('code', g.fingerprint));
          tr.appendChild(buildCell(null, String(g.count)));
          tr.appendChild(buildCell(null, formatErrorTimestamp(g.first_seen)));
          tr.appendChild(buildCell(null, formatErrorTimestamp(g.last_seen)));
          tr.addEventListener('click', () => showErrorDetail(g));
          errorsTableBody.appendChild(tr);
        }
      }

      function buildCell(wrapperTag, text) {
        const td = document.createElement('td');
        if (wrapperTag) {
          const wrap = document.createElement(wrapperTag);
          wrap.textContent = text;
          td.appendChild(wrap);
        } else {
          td.textContent = text;
        }
        return td;
      }

      function showErrorDetail(group) {
        errorDetailSection.hidden = false;
        errorDetailTitle.textContent = group.signature || group.fingerprint;
        const sample = group.sample_payload || {};
        const rows = [
          ['Fingerprint', group.fingerprint],
          ['Count', group.count],
          ['First seen', formatErrorTimestamp(group.first_seen)],
          ['Last seen', formatErrorTimestamp(group.last_seen)],
          ['Service', sample.service_name || '(unknown)'],
          ['Signal', sample.signal || '(unknown)'],
          ['Type', sample.exception_type || '(unknown)'],
          ['Message', sample.exception_message || '(unknown)'],
        ];
        errorDetailMeta.replaceChildren();
        for (const [key, value] of rows) {
          const dt = document.createElement('dt');
          dt.textContent = key;
          const dd = document.createElement('dd');
          dd.textContent = value === undefined || value === null ? '' : String(value);
          errorDetailMeta.appendChild(dt);
          errorDetailMeta.appendChild(dd);
        }
        errorDetailStacktrace.textContent = sample.stacktrace || '(no stacktrace captured)';
        errorDetailSection.scrollIntoView({behavior: 'smooth', block: 'nearest'});
      }

      errorsLookbackSelect.addEventListener('change', loadErrorGroups);
      errorsRefreshButton.addEventListener('click', loadErrorGroups);
      navErrors.addEventListener('click', (e) => {
        e.preventDefault();
        navigateTo('errors');
      });
      errorDetailClose.addEventListener('click', () => {
        errorDetailSection.hidden = true;
      });

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

      // --- Trace Waterfall page wiring ----------------------------------

      const navWaterfall = document.getElementById('nav-waterfall');
      const waterfallTraceIdInput = document.getElementById('waterfall-trace-id-input');
      const waterfallLoadButton = document.getElementById('waterfall-load-button');
      const waterfallStatus = document.getElementById('waterfall-status');
      const waterfallSummary = document.getElementById('waterfall-summary');
      const waterfallSvgContainer = document.getElementById('waterfall-svg-container');

      function setWaterfallStatus(state, message) {
        waterfallStatus.dataset.state = state;
        waterfallStatus.textContent = message;
      }

      function escapeXml(s) {
        return String(s)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&apos;');
      }

      function formatDurationMs(ms) {
        const v = Number(ms) || 0;
        if (v < 1) return v.toFixed(3) + 'ms';
        if (v < 1000) return v.toFixed(2) + 'ms';
        return (v / 1000).toFixed(2) + 's';
      }

      function renderWaterfallSvg(data) {
        waterfallSvgContainer.replaceChildren();
        const spans = data.spans || [];
        if (spans.length === 0) {
          const empty = document.createElement('p');
          empty.style.color = 'var(--muted)';
          empty.textContent = 'No spans for this trace.';
          waterfallSvgContainer.appendChild(empty);
          return;
        }

        const totalDurationMs = Math.max(
          ...spans.map(s => (Number(s.start_ms) || 0) + (Number(s.duration_ms) || 0))
        ) || 1;

        const labelWidth = 280;
        const barAreaWidth = 720;
        const rowHeight = 22;
        const headerHeight = 24;
        const padding = 8;
        const width = labelWidth + barAreaWidth + padding * 2;
        const height = headerHeight + spans.length * rowHeight + padding * 2;

        const svgNS = 'http://www.w3.org/2000/svg';
        const svg = document.createElementNS(svgNS, 'svg');
        svg.setAttribute('width', String(width));
        svg.setAttribute('height', String(height));
        svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
        svg.setAttribute('data-testid', 'waterfall-svg');
        svg.style.background = 'transparent';
        svg.style.fontFamily = 'system-ui, sans-serif';

        // Header / axis ticks
        const axisSteps = 5;
        for (let i = 0; i <= axisSteps; i++) {
          const x = padding + labelWidth + (barAreaWidth / axisSteps) * i;
          const tickLabel = document.createElementNS(svgNS, 'text');
          tickLabel.setAttribute('x', String(x));
          tickLabel.setAttribute('y', String(padding + 12));
          tickLabel.setAttribute('font-size', '10');
          tickLabel.setAttribute('fill', 'currentColor');
          tickLabel.setAttribute('text-anchor', i === 0 ? 'start' : (i === axisSteps ? 'end' : 'middle'));
          tickLabel.textContent = formatDurationMs((totalDurationMs / axisSteps) * i);
          svg.appendChild(tickLabel);

          const gridLine = document.createElementNS(svgNS, 'line');
          gridLine.setAttribute('x1', String(x));
          gridLine.setAttribute('x2', String(x));
          gridLine.setAttribute('y1', String(padding + headerHeight));
          gridLine.setAttribute('y2', String(height - padding));
          gridLine.setAttribute('stroke', 'currentColor');
          gridLine.setAttribute('stroke-opacity', '0.1');
          svg.appendChild(gridLine);
        }

        spans.forEach((span, idx) => {
          const yTop = padding + headerHeight + idx * rowHeight;
          const yMid = yTop + rowHeight / 2;

          // Row background
          if (idx % 2 === 0) {
            const bg = document.createElementNS(svgNS, 'rect');
            bg.setAttribute('x', String(padding));
            bg.setAttribute('y', String(yTop));
            bg.setAttribute('width', String(width - padding * 2));
            bg.setAttribute('height', String(rowHeight));
            bg.setAttribute('fill', 'currentColor');
            bg.setAttribute('fill-opacity', '0.04');
            svg.appendChild(bg);
          }

          // Label: name + service_name
          const label = document.createElementNS(svgNS, 'text');
          label.setAttribute('x', String(padding + 4));
          label.setAttribute('y', String(yMid + 4));
          label.setAttribute('font-size', '12');
          label.setAttribute('fill', 'currentColor');
          const svc = span.service_name || 'unknown';
          const name = span.name || '(anonymous)';
          const labelText = `${name}  -  ${svc}`;
          label.textContent = labelText.length > 44 ? labelText.slice(0, 41) + '...' : labelText;
          const titleEl = document.createElementNS(svgNS, 'title');
          titleEl.textContent = `${name} (${svc})\nspan_id: ${span.id}\nparent: ${span.parent_id || '(root)'}\nstart: ${formatDurationMs(span.start_ms)}\nduration: ${formatDurationMs(span.duration_ms)}\nstatus: ${span.status}`;
          label.appendChild(titleEl);
          svg.appendChild(label);

          // Bar
          const startMs = Number(span.start_ms) || 0;
          const durMs = Number(span.duration_ms) || 0;
          const xBar = padding + labelWidth + (startMs / totalDurationMs) * barAreaWidth;
          const wBar = Math.max((durMs / totalDurationMs) * barAreaWidth, 2);
          const bar = document.createElementNS(svgNS, 'rect');
          bar.setAttribute('x', String(xBar));
          bar.setAttribute('y', String(yTop + 4));
          bar.setAttribute('width', String(wBar));
          bar.setAttribute('height', String(rowHeight - 8));
          const isError = Number(span.status) === 2;
          bar.setAttribute('fill', isError ? '#c0392b' : serviceColor(svc));
          bar.setAttribute('rx', '2');
          bar.setAttribute('data-testid', 'waterfall-span-bar');
          const barTitle = document.createElementNS(svgNS, 'title');
          barTitle.textContent = `${name} (${formatDurationMs(durMs)})`;
          bar.appendChild(barTitle);
          svg.appendChild(bar);

          // Duration label after bar
          const durText = document.createElementNS(svgNS, 'text');
          durText.setAttribute('x', String(xBar + wBar + 4));
          durText.setAttribute('y', String(yMid + 4));
          durText.setAttribute('font-size', '10');
          durText.setAttribute('fill', 'currentColor');
          durText.setAttribute('fill-opacity', '0.7');
          durText.textContent = formatDurationMs(durMs);
          svg.appendChild(durText);
        });

        waterfallSvgContainer.appendChild(svg);
      }

      async function loadWaterfall() {
        const traceId = (waterfallTraceIdInput.value || '').trim();
        if (!traceId) {
          setWaterfallStatus('error', 'Please enter a trace_id.');
          return;
        }
        setWaterfallStatus('working', `Loading waterfall for ${traceId}...`);
        waterfallSummary.hidden = true;
        waterfallSvgContainer.replaceChildren();
        try {
          const resp = await fetch(`/api/traces/${encodeURIComponent(traceId)}/waterfall`, {
            headers: { accept: 'application/json' },
          });
          if (resp.status === 404) {
            setWaterfallStatus('error', `Trace ${traceId} not found.`);
            return;
          }
          if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
          const data = await resp.json();
          const total = (data.spans || []).length;
          waterfallSummary.textContent = `${total} span${total === 1 ? '' : 's'} in trace ${data.trace_id}`;
          waterfallSummary.hidden = false;
          renderWaterfallSvg(data);
          setWaterfallStatus('ok', `Loaded ${total} span${total === 1 ? '' : 's'}.`);
        } catch (err) {
          setWaterfallStatus('error', `Failed to load waterfall: ${err.message}`);
        }
      }

      if (navWaterfall) {
        navWaterfall.addEventListener('click', (e) => {
          e.preventDefault();
          navigateTo('waterfall');
        });
      }
      if (waterfallLoadButton) {
        waterfallLoadButton.addEventListener('click', loadWaterfall);
      }
      if (waterfallTraceIdInput) {
        waterfallTraceIdInput.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            loadWaterfall();
          }
        });
      }

      navTraces.addEventListener('click', (e) => {
        e.preventDefault();
        navigateTo('traces');
      });

      // --- Trace search page ----------------------------------------------
      const traceSearchTraceIdInput = document.getElementById('trace-search-trace-id');
      const traceSearchServiceInput = document.getElementById('trace-search-service');
      const traceSearchMinDurationInput = document.getElementById('trace-search-min-duration');
      const traceLookupButton = document.getElementById('trace-lookup-button');
      const traceSearchButton = document.getElementById('trace-search-button');
      const traceSearchRefreshButton = document.getElementById('trace-search-refresh-button');
      const traceSearchStatus = document.getElementById('trace-search-status');
      const traceSearchListSection = document.getElementById('trace-search-list-section');
      const traceSearchListBody = document.getElementById('trace-search-list-body');
      const traceSearchListScroll = document.getElementById('trace-search-list-scroll');
      const traceSearchEmpty = document.getElementById('trace-search-empty');
      const traceSearchDetailSection = document.getElementById('trace-search-detail-section');
      const traceSearchDetailTitle = document.getElementById('trace-search-detail-title');
      const traceSearchDetailMeta = document.getElementById('trace-search-detail-meta');
      const traceSearchDetailBody = document.getElementById('trace-search-detail-body');
      const traceSearchBackButton = document.getElementById('trace-search-back-button');

      const SPAN_KIND_LABELS = {
        0: 'UNSPECIFIED',
        1: 'INTERNAL',
        2: 'SERVER',
        3: 'CLIENT',
        4: 'PRODUCER',
        5: 'CONSUMER',
      };

      function spanKindLabel(k) {
        return SPAN_KIND_LABELS[k] || String(k);
      }

      function statusLabel(code) {
        if (code === 2) return 'ERROR';
        if (code === 1) return 'OK';
        return 'UNSET';
      }

      function formatDurationMs(durationNs) {
        const ms = Number(durationNs) / 1_000_000;
        if (ms >= 1000) return (ms / 1000).toFixed(2) + ' s';
        if (ms >= 10) return ms.toFixed(0) + ' ms';
        return ms.toFixed(2) + ' ms';
      }

      function setTraceSearchStatus(state, message) {
        traceSearchStatus.dataset.state = state;
        traceSearchStatus.textContent = message;
      }

      function showTraceList() {
        traceSearchListSection.hidden = false;
        traceSearchDetailSection.hidden = true;
      }

      function showTraceDetail() {
        traceSearchListSection.hidden = true;
        traceSearchDetailSection.hidden = false;
      }

      function attributeValueToString(value) {
        if (value == null || typeof value !== 'object') return String(value ?? '');
        if ('stringValue' in value) return String(value.stringValue);
        if ('intValue' in value) return String(value.intValue);
        if ('doubleValue' in value) return String(value.doubleValue);
        if ('boolValue' in value) return String(value.boolValue);
        if ('arrayValue' in value) {
          const arr = value.arrayValue && Array.isArray(value.arrayValue.values)
            ? value.arrayValue.values
            : [];
          return '[' + arr.map(attributeValueToString).join(', ') + ']';
        }
        return JSON.stringify(value);
      }

      function renderAttributesInline(attributes) {
        if (!Array.isArray(attributes) || attributes.length === 0) return '';
        return attributes
          .map((attr) => {
            const k = attr && attr.key ? String(attr.key) : '';
            const v = attr && attr.value ? attributeValueToString(attr.value) : '';
            return k + '=' + v;
          })
          .join(', ');
      }

      async function loadTraceSearch() {
        showTraceList();
        setTraceSearchStatus('working', 'Loading traces...');
        const params = new URLSearchParams();
        const service = traceSearchServiceInput.value.trim();
        if (service) params.set('service', service);
        const minDurationRaw = traceSearchMinDurationInput.value.trim();
        if (minDurationRaw) {
          const n = Number.parseInt(minDurationRaw, 10);
          if (Number.isFinite(n) && n >= 0) params.set('min_duration_ms', String(n));
        }
        const url = '/api/traces/search' + (params.toString() ? '?' + params.toString() : '');
        try {
          const resp = await fetch(url, { headers: { accept: 'application/json' } });
          if (!resp.ok) {
            setTraceSearchStatus('error', 'Search failed: HTTP ' + resp.status);
            renderTraceSearchList([]);
            return;
          }
          const data = await resp.json();
          renderTraceSearchList(data.traces || []);
          setTraceSearchStatus('ok', (data.traces || []).length + ' trace(s) found.');
        } catch (err) {
          setTraceSearchStatus('error', 'Search failed: ' + (err && err.message ? err.message : err));
          renderTraceSearchList([]);
        }
      }

      function renderTraceSearchList(traces) {
        traceSearchListBody.replaceChildren();
        if (!traces.length) {
          traceSearchListScroll.hidden = true;
          traceSearchEmpty.hidden = false;
          return;
        }
        traceSearchListScroll.hidden = false;
        traceSearchEmpty.hidden = true;
        for (const t of traces) {
          const tr = document.createElement('tr');

          const tdId = document.createElement('td');
          const link = document.createElement('a');
          link.href = '#';
          link.textContent = t.trace_id;
          link.style.color = 'var(--accent)';
          link.style.cursor = 'pointer';
          link.dataset.traceId = t.trace_id;
          link.addEventListener('click', (e) => {
            e.preventDefault();
            lookupTrace(t.trace_id);
          });
          tdId.appendChild(link);
          tr.appendChild(tdId);

          const tdRoot = document.createElement('td');
          tdRoot.textContent = t.root_span_name || '(no root span)';
          tr.appendChild(tdRoot);

          const tdServices = document.createElement('td');
          tdServices.textContent = (t.service_names || []).join(', ');
          tr.appendChild(tdServices);

          const tdSpans = document.createElement('td');
          tdSpans.textContent = String(t.span_count);
          tr.appendChild(tdSpans);

          const tdDuration = document.createElement('td');
          tdDuration.textContent = formatDurationMs(t.duration_ns || 0);
          tr.appendChild(tdDuration);

          const tdStatus = document.createElement('td');
          tdStatus.textContent = t.has_error ? 'ERROR' : 'OK';
          if (t.has_error) tdStatus.style.color = 'var(--accent-strong)';
          tr.appendChild(tdStatus);

          traceSearchListBody.appendChild(tr);
        }
      }

      async function lookupTrace(traceIdRaw) {
        const traceId = (traceIdRaw || '').trim();
        if (!traceId) {
          setTraceSearchStatus('error', 'Enter a trace ID to look up.');
          return;
        }
        setTraceSearchStatus('working', 'Loading trace ' + traceId + '...');
        try {
          const resp = await fetch('/api/traces?trace_id=' + encodeURIComponent(traceId), {
            headers: { accept: 'application/json' },
          });
          if (resp.status === 404) {
            setTraceSearchStatus('error', 'Trace not found: ' + traceId);
            return;
          }
          if (!resp.ok) {
            setTraceSearchStatus('error', 'Lookup failed: HTTP ' + resp.status);
            return;
          }
          const detail = await resp.json();
          renderTraceDetail(detail);
          showTraceDetail();
          setTraceSearchStatus('ok', 'Loaded trace ' + traceId);
        } catch (err) {
          setTraceSearchStatus('error', 'Lookup failed: ' + (err && err.message ? err.message : err));
        }
      }

      function renderTraceDetail(detail) {
        traceSearchDetailTitle.textContent = 'Trace ' + detail.trace_id;
        const services = (detail.service_names || []).join(', ') || '(none)';
        traceSearchDetailMeta.textContent =
          'Spans: ' + detail.span_count +
          '   |   Duration: ' + formatDurationMs(detail.duration_ns || 0) +
          '   |   Services: ' + services +
          '   |   Status: ' + (detail.has_error ? 'ERROR' : 'OK') +
          (detail.root_span_name ? '   |   Root: ' + detail.root_span_name : '');

        traceSearchDetailBody.replaceChildren();
        for (const span of detail.spans || []) {
          const tr = document.createElement('tr');

          const tdService = document.createElement('td');
          tdService.textContent = span.service_name || '(unknown)';
          tr.appendChild(tdService);

          const tdName = document.createElement('td');
          tdName.textContent = span.name || '';
          tr.appendChild(tdName);

          const tdKind = document.createElement('td');
          tdKind.textContent = spanKindLabel(span.kind);
          tr.appendChild(tdKind);

          const tdSpanId = document.createElement('td');
          tdSpanId.textContent = span.span_id || '';
          tdSpanId.style.fontFamily = 'monospace';
          tdSpanId.style.fontSize = '12px';
          tr.appendChild(tdSpanId);

          const tdParent = document.createElement('td');
          tdParent.textContent = span.parent_span_id || '(root)';
          tdParent.style.fontFamily = 'monospace';
          tdParent.style.fontSize = '12px';
          tr.appendChild(tdParent);

          const tdDur = document.createElement('td');
          tdDur.textContent = formatDurationMs(span.duration_ns || 0);
          tr.appendChild(tdDur);

          const tdStatus = document.createElement('td');
          tdStatus.textContent = statusLabel(span.status_code);
          if (span.status_code === 2) tdStatus.style.color = 'var(--accent-strong)';
          tr.appendChild(tdStatus);

          const tdAttrs = document.createElement('td');
          const attrs = renderAttributesInline(span.span_attributes);
          tdAttrs.textContent = attrs || '-';
          tdAttrs.style.fontSize = '12px';
          tdAttrs.style.color = 'var(--muted)';
          tr.appendChild(tdAttrs);

          traceSearchDetailBody.appendChild(tr);
        }
      }

      traceLookupButton.addEventListener('click', () => {
        lookupTrace(traceSearchTraceIdInput.value);
      });
      traceSearchButton.addEventListener('click', () => loadTraceSearch());
      traceSearchRefreshButton.addEventListener('click', () => loadTraceSearch());
      traceSearchBackButton.addEventListener('click', () => {
        showTraceList();
        setTraceSearchStatus('idle', 'Ready.');
      });
      traceSearchTraceIdInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          lookupTrace(traceSearchTraceIdInput.value);
        }
      });
      traceSearchServiceInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          loadTraceSearch();
        }
      });
      traceSearchMinDurationInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          e.preventDefault();
          loadTraceSearch();
        }
      });

      const navAlerts = document.getElementById('nav-alerts');
      if (navAlerts) {
        navAlerts.addEventListener('click', (e) => {
          e.preventDefault();
          navigateTo('alerts');
        });
      }

      // --- Alerts UI ------------------------------------------------------
      const alertModal = document.getElementById('alert-modal');
      const alertModalName = document.getElementById('alert-modal-name');
      const alertModalViewer = document.getElementById('alert-modal-viewer');
      const alertModalOp = document.getElementById('alert-modal-op');
      const alertModalValue = document.getElementById('alert-modal-value');
      const alertModalMetric = document.getElementById('alert-modal-metric');
      const alertModalSeverity = document.getElementById('alert-modal-severity');
      const alertModalInterval = document.getElementById('alert-modal-interval');
      const alertModalSave = document.getElementById('alert-modal-save');
      const alertModalCancel = document.getElementById('alert-modal-cancel');
      const alertModalError = document.getElementById('alert-modal-error');
      const alertTableBody = document.getElementById('alert-table-body');
      const alertEmpty = document.getElementById('alert-empty');
      const alertsStatus = document.getElementById('alerts-status');
      const newAlertButton = document.getElementById('new-alert-button');
      const refreshAlertsButton = document.getElementById('refresh-alerts-button');

      function setAlertsStatus(state, text) {
        if (!alertsStatus) return;
        alertsStatus.dataset.state = state;
        alertsStatus.textContent = text;
      }

      function escapeHtml(s) {
        return String(s)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;');
      }

      function renderAlerts(alerts, viewerNamesById) {
        if (!alertTableBody) return;
        alertTableBody.innerHTML = '';
        if (!alerts.length) {
          alertEmpty.hidden = false;
          return;
        }
        alertEmpty.hidden = true;
        for (const a of alerts) {
          const tr = document.createElement('tr');
          const cond = a.condition || {};
          const condText = `${cond.metric || ''} ${cond.op || ''} ${cond.value ?? ''}`;
          const viewerName = viewerNamesById.get(a.viewer_id) || a.viewer_id;
          const sevClass = a.severity === 'critical' ? 'alert-severity-critical' : 'alert-severity-warning';
          tr.innerHTML = `
            <td>${escapeHtml(a.name)}</td>
            <td>${escapeHtml(viewerName)}</td>
            <td><code>${escapeHtml(condText)}</code></td>
            <td><span class="alert-severity-badge ${sevClass}">${escapeHtml(a.severity)}</span></td>
            <td>${a.evaluation_interval_ms} ms</td>
            <td>${a.enabled ? 'yes' : 'no'}</td>
            <td><button class="secondary btn-compact" type="button" data-alert-id="${a.id}">Delete</button></td>
          `;
          tr.querySelector('button[data-alert-id]').addEventListener('click', async () => {
            if (!confirm(`Delete alert "${a.name}"?`)) return;
            try {
              const resp = await fetch(`/api/alerts/${a.id}`, { method: 'DELETE' });
              if (!resp.ok && resp.status !== 204) {
                setAlertsStatus('error', `Delete failed (${resp.status})`);
                return;
              }
              refreshAlerts();
            } catch (err) {
              setAlertsStatus('error', `Delete failed: ${err.message}`);
            }
          });
          alertTableBody.appendChild(tr);
        }
      }

      async function refreshAlerts() {
        if (!alertTableBody) return;
        setAlertsStatus('working', 'Loading alerts...');
        try {
          const [alertsResp, viewersResp] = await Promise.all([
            fetch('/api/alerts', { headers: { accept: 'application/json' } }),
            fetch('/api/viewers', { headers: { accept: 'application/json' } }),
          ]);
          if (!alertsResp.ok) {
            setAlertsStatus('error', `Failed to load alerts (${alertsResp.status})`);
            return;
          }
          const alertsBody = await alertsResp.json();
          const viewersBody = viewersResp.ok ? await viewersResp.json() : { viewers: [] };
          const viewerNamesById = new Map();
          for (const v of viewersBody.viewers || []) {
            viewerNamesById.set(v.id, v.name);
          }
          renderAlerts(alertsBody.alerts || [], viewerNamesById);
          setAlertsStatus('idle', `Loaded ${(alertsBody.alerts || []).length} alerts`);
        } catch (err) {
          setAlertsStatus('error', `Failed to load alerts: ${err.message}`);
        }
      }

      async function populateAlertViewers() {
        if (!alertModalViewer) return;
        try {
          const resp = await fetch('/api/viewers', { headers: { accept: 'application/json' } });
          if (!resp.ok) {
            alertModalViewer.innerHTML = '<option value="">(failed to load viewers)</option>';
            return;
          }
          const body = await resp.json();
          const viewers = body.viewers || [];
          alertModalViewer.innerHTML = viewers
            .map(v => `<option value="${v.id}">${escapeHtml(v.name)}</option>`)
            .join('');
        } catch (err) {
          alertModalViewer.innerHTML = `<option value="">(error: ${escapeHtml(err.message)})</option>`;
        }
      }

      function openAlertModal() {
        alertModalError.hidden = true;
        alertModalError.textContent = '';
        alertModalName.value = '';
        alertModalValue.value = '100';
        alertModalInterval.value = '5000';
        alertModalOp.value = '>';
        alertModalSeverity.value = 'warning';
        alertModalMetric.value = 'count';
        populateAlertViewers();
        alertModal.hidden = false;
      }

      function closeAlertModal() {
        alertModal.hidden = true;
      }

      async function saveAlert() {
        alertModalError.hidden = true;
        const name = alertModalName.value.trim();
        const viewerId = alertModalViewer.value;
        const value = Number(alertModalValue.value);
        const intervalMs = parseInt(alertModalInterval.value, 10);
        if (!name) {
          alertModalError.textContent = 'Name is required';
          alertModalError.hidden = false;
          return;
        }
        if (!viewerId) {
          alertModalError.textContent = 'Select a viewer';
          alertModalError.hidden = false;
          return;
        }
        if (!Number.isFinite(value)) {
          alertModalError.textContent = 'Threshold value must be a number';
          alertModalError.hidden = false;
          return;
        }
        if (!Number.isFinite(intervalMs) || intervalMs < 1000) {
          alertModalError.textContent = 'Evaluation interval must be >= 1000 ms';
          alertModalError.hidden = false;
          return;
        }
        const payload = {
          name,
          viewer_id: viewerId,
          condition: {
            type: 'threshold',
            op: alertModalOp.value,
            value,
            metric: alertModalMetric.value,
          },
          severity: alertModalSeverity.value,
          evaluation_interval_ms: intervalMs,
        };
        try {
          const resp = await fetch('/api/alerts', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify(payload),
          });
          if (!resp.ok) {
            const text = await resp.text();
            alertModalError.textContent = `Create failed (${resp.status}): ${text}`;
            alertModalError.hidden = false;
            return;
          }
          closeAlertModal();
          refreshAlerts();
        } catch (err) {
          alertModalError.textContent = `Create failed: ${err.message}`;
          alertModalError.hidden = false;
        }
      }

      if (newAlertButton) newAlertButton.addEventListener('click', openAlertModal);
      if (refreshAlertsButton) refreshAlertsButton.addEventListener('click', refreshAlerts);
      if (alertModalCancel) alertModalCancel.addEventListener('click', closeAlertModal);
      if (alertModalSave) alertModalSave.addEventListener('click', saveAlert);
      if (alertModal) {
        alertModal.addEventListener('click', (e) => {
          if (e.target === alertModal) closeAlertModal();
        });
      }

      navIncidents.addEventListener('click', (e) => {
        e.preventDefault();
        window.location.hash = '#incidents';
      });


      if (navNotifications) {
        navNotifications.addEventListener('click', (e) => {
          e.preventDefault();
          window.location.hash = '#notifications';
        });
      }

      if (navQuery) {
        navQuery.addEventListener('click', (e) => {
          e.preventDefault();
          navigateTo('query');
        });
      }

      // --- Ad-hoc query page -----------------------------------------------
      const queryInput = document.getElementById('query-input');
      const queryLookbackInput = document.getElementById('query-lookback');
      const queryRunButton = document.getElementById('query-run-button');
      const queryStatusEl = document.getElementById('query-status');
      const queryErrorEl = document.getElementById('query-error');
      const querySummaryEl = document.getElementById('query-result-summary');
      const queryResultWrap = document.getElementById('query-result-wrap');
      const queryResultHeader = document.getElementById('query-result-header');
      const queryResultBody = document.getElementById('query-result-body');

      function renderQueryResult(payload) {
        queryResultHeader.innerHTML = '';
        queryResultBody.innerHTML = '';
        const cols = Array.isArray(payload.columns) ? payload.columns : [];
        for (const col of cols) {
          const th = document.createElement('th');
          th.textContent = col;
          queryResultHeader.appendChild(th);
        }
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        for (const row of rows) {
          const tr = document.createElement('tr');
          for (const cell of row) {
            const td = document.createElement('td');
            if (cell === null || cell === undefined) {
              td.textContent = '';
              td.style.color = 'var(--muted)';
              td.style.fontStyle = 'italic';
              td.textContent = 'null';
            } else if (typeof cell === 'number') {
              td.textContent = Number.isInteger(cell) ? String(cell) : cell.toFixed(3);
              td.style.fontVariantNumeric = 'tabular-nums';
            } else {
              td.textContent = String(cell);
            }
            tr.appendChild(td);
          }
          queryResultBody.appendChild(tr);
        }
        queryResultWrap.hidden = cols.length === 0;
        const truncatedNote = payload.truncated ? ' (truncated)' : '';
        querySummaryEl.textContent =
          `scanned ${payload.scanned} / matched ${payload.matched} / returned ${payload.returned}${truncatedNote}`;
        querySummaryEl.hidden = false;
      }

      async function runQuery() {
        const sql = queryInput.value.trim();
        if (!sql) {
          queryErrorEl.textContent = 'SQL must not be empty.';
          queryErrorEl.hidden = false;
          return;
        }
        const lookback = parseInt(queryLookbackInput.value, 10);
        const lookbackMs = Number.isFinite(lookback) && lookback > 0 ? lookback : 300000;
        queryRunButton.disabled = true;
        queryStatusEl.textContent = 'Running...';
        queryErrorEl.hidden = true;
        queryErrorEl.textContent = '';
        try {
          const res = await fetch('/api/query', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ sql, lookback_ms: lookbackMs }),
          });
          const text = await res.text();
          let body = null;
          try { body = text ? JSON.parse(text) : null; } catch (_) { body = null; }
          if (!res.ok) {
            const message = (body && body.error) ? body.error : `HTTP ${res.status}`;
            queryErrorEl.textContent = message;
            queryErrorEl.hidden = false;
            queryResultWrap.hidden = true;
            querySummaryEl.hidden = true;
            queryStatusEl.textContent = '';
            return;
          }
          if (body) {
            renderQueryResult(body);
            queryStatusEl.textContent = 'OK';
          }
        } catch (err) {
          queryErrorEl.textContent = `request failed: ${err}`;
          queryErrorEl.hidden = false;
          queryResultWrap.hidden = true;
          querySummaryEl.hidden = true;
          queryStatusEl.textContent = '';
        } finally {
          queryRunButton.disabled = false;
        }
      }

      if (queryRunButton) {
        queryRunButton.addEventListener('click', runQuery);
      }
      if (queryInput) {
        queryInput.addEventListener('keydown', (e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault();
            runQuery();
          }
        });
      }

      navAnomaly.addEventListener('click', (e) => {
        e.preventDefault();
        navigateTo('anomaly');
      });

      // --- Anomaly tab ----------------------------------------------------
      const anomalyViewerSelect = document.getElementById('anomaly-viewer-select');
      const anomalyDetectorTypeSelect = document.getElementById('anomaly-detector-type');
      const anomalyNoDataFields = document.getElementById('anomaly-no-data-fields');
      const anomalyZScoreFields = document.getElementById('anomaly-zscore-fields');
      const anomalyWindowMsInput = document.getElementById('anomaly-window-ms');
      const anomalyThresholdInput = document.getElementById('anomaly-threshold');
      const anomalyBucketMsInput = document.getElementById('anomaly-bucket-ms');
      const anomalyEvaluateButton = document.getElementById('anomaly-evaluate-button');
      const anomalyStatus = document.getElementById('anomaly-status');
      const anomalyResultPanel = document.getElementById('anomaly-result-panel');
      const anomalyResultBreached = document.getElementById('anomaly-result-breached');
      const anomalyResultValue = document.getElementById('anomaly-result-value');
      const anomalyResultExpected = document.getElementById('anomaly-result-expected');
      const anomalyResultObserved = document.getElementById('anomaly-result-observed');

      function setAnomalyDetectorVisibility() {
        const t = anomalyDetectorTypeSelect.value;
        anomalyNoDataFields.hidden = t !== 'no_data';
        anomalyZScoreFields.hidden = t !== 'zscore';
      }

      anomalyDetectorTypeSelect.addEventListener('change', setAnomalyDetectorVisibility);
      setAnomalyDetectorVisibility();

      async function refreshAnomalyViewerOptions() {
        try {
          const resp = await fetch('/api/viewers', { headers: { accept: 'application/json' } });
          if (!resp.ok) {
            anomalyStatus.dataset.state = 'error';
            anomalyStatus.textContent = `Failed to load viewers: HTTP ${resp.status}`;
            return;
          }
          const data = await resp.json();
          const viewers = (data && data.viewers) || [];
          const previous = anomalyViewerSelect.value;
          anomalyViewerSelect.innerHTML = '';
          if (!viewers.length) {
            const opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'No viewers available';
            anomalyViewerSelect.appendChild(opt);
            anomalyStatus.dataset.state = 'idle';
            anomalyStatus.textContent = 'Create a viewer first.';
            return;
          }
          for (const v of viewers) {
            const opt = document.createElement('option');
            opt.value = v.id;
            opt.textContent = `${v.name} (${v.id.slice(0, 8)})`;
            anomalyViewerSelect.appendChild(opt);
          }
          if (previous && viewers.some(v => v.id === previous)) {
            anomalyViewerSelect.value = previous;
          }
          anomalyStatus.dataset.state = 'idle';
          anomalyStatus.textContent = 'Choose a viewer and detector, then click Evaluate.';
        } catch (err) {
          anomalyStatus.dataset.state = 'error';
          anomalyStatus.textContent = `Failed to load viewers: ${err.message}`;
        }
      }

      anomalyEvaluateButton.addEventListener('click', async () => {
        const viewerId = anomalyViewerSelect.value;
        if (!viewerId) {
          anomalyStatus.dataset.state = 'error';
          anomalyStatus.textContent = 'Select a viewer.';
          return;
        }
        const detectorType = anomalyDetectorTypeSelect.value;
        let detector;
        if (detectorType === 'no_data') {
          const windowMs = parseInt(anomalyWindowMsInput.value, 10);
          if (!Number.isFinite(windowMs) || windowMs <= 0) {
            anomalyStatus.dataset.state = 'error';
            anomalyStatus.textContent = 'Window (ms) must be a positive integer.';
            return;
          }
          detector = { type: 'no_data', window_ms: windowMs };
        } else {
          const threshold = parseFloat(anomalyThresholdInput.value);
          const bucketMs = parseInt(anomalyBucketMsInput.value, 10);
          if (!Number.isFinite(threshold) || threshold < 0) {
            anomalyStatus.dataset.state = 'error';
            anomalyStatus.textContent = 'Threshold must be non-negative.';
            return;
          }
          if (!Number.isFinite(bucketMs) || bucketMs <= 0) {
            anomalyStatus.dataset.state = 'error';
            anomalyStatus.textContent = 'Bucket (ms) must be a positive integer.';
            return;
          }
          detector = { type: 'zscore', threshold, bucket_ms: bucketMs };
        }
        anomalyStatus.dataset.state = 'working';
        anomalyStatus.textContent = 'Evaluating...';
        try {
          const resp = await fetch('/api/anomaly/evaluate', {
            method: 'POST',
            headers: { 'content-type': 'application/json', accept: 'application/json' },
            body: JSON.stringify({ viewer_id: viewerId, detector }),
          });
          if (!resp.ok) {
            anomalyStatus.dataset.state = 'error';
            anomalyStatus.textContent = `Evaluation failed: HTTP ${resp.status}`;
            return;
          }
          const result = await resp.json();
          anomalyResultBreached.textContent = result.breached ? 'YES' : 'no';
          anomalyResultValue.textContent = String(result.value);
          anomalyResultExpected.textContent = String(result.expected);
          anomalyResultObserved.textContent = result.observed_at;
          anomalyResultPanel.hidden = false;
          anomalyStatus.dataset.state = result.breached ? 'error' : 'ok';
          anomalyStatus.textContent = result.breached
            ? 'Anomaly detected (breached).'
            : 'No anomaly detected.';
        } catch (err) {
          anomalyStatus.dataset.state = 'error';
          anomalyStatus.textContent = `Evaluation failed: ${err.message}`;
        }
      });

      navDbInsights.addEventListener('click', (e) => {
        e.preventDefault();
        navigateTo('db-insights');
      });

      // --- DB Insights -----------------------------------------------------
      const dbInsightsLookbackEl = document.getElementById('db-insights-lookback');
      const dbInsightsMinP95El = document.getElementById('db-insights-min-p95');
      const dbInsightsMinRepetitionsEl = document.getElementById('db-insights-min-repetitions');
      const dbInsightsRefreshBtn = document.getElementById('db-insights-refresh');
      const dbInsightsStatus = document.getElementById('db-insights-status');
      const slowQueriesBody = document.getElementById('slow-queries-body');
      const slowQueriesEmpty = document.getElementById('slow-queries-empty');
      const slowQueriesWrap = document.getElementById('slow-queries-wrap');
      const nPlusOneBody = document.getElementById('n-plus-one-body');
      const nPlusOneEmpty = document.getElementById('n-plus-one-empty');
      const nPlusOneWrap = document.getElementById('n-plus-one-wrap');

      function setDbInsightsStatus(state, message) {
        dbInsightsStatus.dataset.state = state;
        dbInsightsStatus.textContent = message;
      }

      function renderSlowQueries(rows) {
        slowQueriesBody.innerHTML = '';
        if (!rows || rows.length === 0) {
          slowQueriesEmpty.hidden = false;
          slowQueriesWrap.hidden = true;
          return;
        }
        slowQueriesEmpty.hidden = true;
        slowQueriesWrap.hidden = false;
        for (const row of rows) {
          const tr = document.createElement('tr');
          const cells = [
            row.statement || '',
            row.system || '-',
            String(row.count),
            String(row.p50_ms),
            String(row.p95_ms),
            String(row.p99_ms),
            String(row.max_ms),
          ];
          for (const text of cells) {
            const td = document.createElement('td');
            td.textContent = text;
            tr.appendChild(td);
          }
          slowQueriesBody.appendChild(tr);
        }
      }

      function renderNPlusOne(rows) {
        nPlusOneBody.innerHTML = '';
        if (!rows || rows.length === 0) {
          nPlusOneEmpty.hidden = false;
          nPlusOneWrap.hidden = true;
          return;
        }
        nPlusOneEmpty.hidden = true;
        nPlusOneWrap.hidden = false;
        for (const row of rows) {
          const tr = document.createElement('tr');
          const services = (row.services || []).join(', ') || '-';
          const cells = [
            row.trace_id || '',
            row.statement || '',
            row.system || '-',
            String(row.count),
            services,
          ];
          for (const text of cells) {
            const td = document.createElement('td');
            td.textContent = text;
            tr.appendChild(td);
          }
          nPlusOneBody.appendChild(tr);
        }
      }

      async function refreshDbInsights() {
        const lookbackMs = parseInt(dbInsightsLookbackEl.value, 10) || 600000;
        const minP95 = parseInt(dbInsightsMinP95El.value, 10);
        const minRepetitions = parseInt(dbInsightsMinRepetitionsEl.value, 10);
        setDbInsightsStatus('working', 'Loading...');
        try {
          const slowParams = new URLSearchParams({ lookback_ms: String(lookbackMs) });
          if (Number.isFinite(minP95) && minP95 >= 0) {
            slowParams.set('min_p95_ms', String(minP95));
          }
          const npParams = new URLSearchParams({ lookback_ms: String(lookbackMs) });
          if (Number.isFinite(minRepetitions) && minRepetitions >= 1) {
            npParams.set('min_repetitions', String(minRepetitions));
          }
          const [slowResp, npResp] = await Promise.all([
            fetch(`/api/slow-queries?${slowParams}`, { headers: { accept: 'application/json' } }),
            fetch(`/api/n-plus-one?${npParams}`, { headers: { accept: 'application/json' } }),
          ]);
          if (!slowResp.ok) throw new Error(`slow-queries: HTTP ${slowResp.status}`);
          if (!npResp.ok) throw new Error(`n-plus-one: HTTP ${npResp.status}`);
          const slowRows = await slowResp.json();
          const npRows = await npResp.json();
          renderSlowQueries(slowRows);
          renderNPlusOne(npRows);
          setDbInsightsStatus('idle',
            `Slow queries: ${slowRows.length} | N+1: ${npRows.length}`);
        } catch (err) {
          setDbInsightsStatus('error', `Failed: ${err.message}`);
        }
      }

      dbInsightsRefreshBtn.addEventListener('click', refreshDbInsights);
      dbInsightsLookbackEl.addEventListener('change', refreshDbInsights);

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
        const attrValues = await fetchAttributeValuesForAutocomplete();
        const queryOptions = mergeUniqueSorted(cachedServiceNames, attrValues);
        for (const id of ['viewer-query-datalist', 'detail-query-datalist', 'viewer-detail-query-datalist']) {
          fillDatalist(document.getElementById(id), queryOptions);
        }
      }

      // Fetch a small slice of indexed attribute values to feed the query
      // autocomplete datalists. Returns [] when the attribute index is not
      // available (standalone mode -> /api/attributes returns 503).
      async function fetchAttributeValuesForAutocomplete() {
        try {
          const keysResp = await fetch('/api/attributes', { headers: { accept: 'application/json' } });
          if (!keysResp.ok) return [];
          const { keys } = await keysResp.json();
          if (!Array.isArray(keys) || keys.length === 0) return [];
          // Cap fan-out to keep page load snappy on large indices.
          const sliced = keys.slice(0, 10);
          const valueLists = await Promise.all(sliced.map(async (k) => {
            try {
              const r = await fetch(`/api/attributes/${encodeURIComponent(k)}/values`,
                { headers: { accept: 'application/json' } });
              if (!r.ok) return [];
              const { values } = await r.json();
              return Array.isArray(values) ? values : [];
            } catch (_) { return []; }
          }));
          return valueLists.flat();
        } catch (_) { return []; }
      }

      function mergeUniqueSorted(...lists) {
        const set = new Set();
        for (const l of lists) for (const v of l) if (v) set.add(v);
        return [...set].sort();
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

      // --- Service Map ----------------------------------------------------
      const serviceMapLookback = document.getElementById('service-map-lookback');
      const serviceMapRefreshButton = document.getElementById('service-map-refresh');
      const serviceMapStatus = document.getElementById('service-map-status');
      const serviceMapSvg = document.getElementById('service-map-svg');
      const serviceMapEmpty = document.getElementById('service-map-empty');
      const serviceMapNodesBody = document.getElementById('service-map-nodes-body');
      const serviceMapEdgesBody = document.getElementById('service-map-edges-body');
      const SVG_NS = 'http://www.w3.org/2000/svg';

      function setServiceMapStatus(text, kind) {
        serviceMapStatus.textContent = text || '';
        serviceMapStatus.dataset.state = kind || '';
      }

      async function refreshServiceMap() {
        const lookbackMs = parseInt(serviceMapLookback.value, 10) || 600000;
        setServiceMapStatus('Loading...', 'working');
        try {
          const resp = await fetch(`/api/service-map?lookback_ms=${lookbackMs}`);
          if (!resp.ok) {
            throw new Error(`status ${resp.status}`);
          }
          const data = await resp.json();
          renderServiceMap(data);
          setServiceMapStatus(
            `${data.nodes.length} services, ${data.edges.length} edges`,
            'ok',
          );
        } catch (err) {
          setServiceMapStatus(`Failed: ${err.message || err}`, 'error');
        }
      }

      function renderServiceMap(data) {
        const nodes = Array.isArray(data.nodes) ? data.nodes : [];
        const edges = Array.isArray(data.edges) ? data.edges : [];

        serviceMapNodesBody.innerHTML = '';
        for (const node of nodes) {
          const tr = document.createElement('tr');
          tr.innerHTML =
            `<td>${escapeHtml(node.name)}</td>` +
            `<td>${node.span_count}</td>` +
            `<td>${node.error_count}</td>` +
            `<td>${(node.p95_duration_ms || 0).toFixed(2)}</td>`;
          serviceMapNodesBody.appendChild(tr);
        }

        serviceMapEdgesBody.innerHTML = '';
        for (const edge of edges) {
          const tr = document.createElement('tr');
          const ratePct = ((edge.error_rate || 0) * 100).toFixed(1) + '%';
          tr.innerHTML =
            `<td>${escapeHtml(edge.from)}</td>` +
            `<td>${escapeHtml(edge.to)}</td>` +
            `<td>${edge.calls}</td>` +
            `<td>${ratePct}</td>`;
          serviceMapEdgesBody.appendChild(tr);
        }

        serviceMapEmpty.hidden = nodes.length > 0;
        renderServiceMapSvg(nodes, edges);
      }

      function renderServiceMapSvg(nodes, edges) {
        // Wipe existing rendered children but keep <defs>.
        for (const child of Array.from(serviceMapSvg.children)) {
          if (child.tagName.toLowerCase() !== 'defs') {
            serviceMapSvg.removeChild(child);
          }
        }
        if (!nodes.length) return;

        // Deterministic circular layout: positions depend only on node index.
        const width = 800;
        const height = 420;
        const cx = width / 2;
        const cy = height / 2;
        const radius = Math.min(width, height) / 2 - 80;
        const positions = new Map();
        nodes.forEach((node, i) => {
          const angle = (2 * Math.PI * i) / nodes.length - Math.PI / 2;
          positions.set(node.name, {
            x: cx + radius * Math.cos(angle),
            y: cy + radius * Math.sin(angle),
          });
        });

        // Edges first (so nodes overlay them).
        for (const edge of edges) {
          const from = positions.get(edge.from);
          const to = positions.get(edge.to);
          if (!from || !to) continue;
          // Shorten the line so the arrowhead stops at the node boundary.
          const dx = to.x - from.x;
          const dy = to.y - from.y;
          const dist = Math.sqrt(dx * dx + dy * dy) || 1;
          const nodeR = 26;
          const sx = from.x + (dx / dist) * nodeR;
          const sy = from.y + (dy / dist) * nodeR;
          const ex = to.x - (dx / dist) * nodeR;
          const ey = to.y - (dy / dist) * nodeR;
          const line = document.createElementNS(SVG_NS, 'line');
          line.setAttribute('x1', sx);
          line.setAttribute('y1', sy);
          line.setAttribute('x2', ex);
          line.setAttribute('y2', ey);
          line.setAttribute(
            'class',
            edge.error_rate > 0
              ? 'service-map-edge service-map-edge-error'
              : 'service-map-edge',
          );
          serviceMapSvg.appendChild(line);

          const label = document.createElementNS(SVG_NS, 'text');
          label.setAttribute('x', (sx + ex) / 2);
          label.setAttribute('y', (sy + ey) / 2 - 4);
          label.setAttribute('text-anchor', 'middle');
          label.setAttribute('class', 'service-map-edge-label');
          label.textContent = String(edge.calls);
          serviceMapSvg.appendChild(label);
        }

        for (const node of nodes) {
          const pos = positions.get(node.name);
          if (!pos) continue;
          const circle = document.createElementNS(SVG_NS, 'circle');
          circle.setAttribute('cx', pos.x);
          circle.setAttribute('cy', pos.y);
          circle.setAttribute('r', 26);
          circle.setAttribute('class', 'service-map-node');
          serviceMapSvg.appendChild(circle);

          const label = document.createElementNS(SVG_NS, 'text');
          label.setAttribute('x', pos.x);
          label.setAttribute('y', pos.y + 42);
          label.setAttribute('text-anchor', 'middle');
          label.setAttribute('class', 'service-map-node-label');
          label.textContent = node.name;
          serviceMapSvg.appendChild(label);
        }
      }

      function escapeHtml(input) {
        return String(input == null ? '' : input)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }

      serviceMapRefreshButton.addEventListener('click', refreshServiceMap);
      serviceMapLookback.addEventListener('change', refreshServiceMap);

      function routeFromHash() {
        const hashData = parseDashboardHash();
        if (hashData) {
          navigateTo('dashboard', hashData.id);
          return;
        }
        if (window.location.hash === '#traces') {
          navigateTo('traces');
          return;
        }
        if (window.location.hash === '#incidents') {
          navigateTo('incidents');
          return;
        }
        if (window.location.hash === '#notifications') {
          navigateTo('notifications');
          return;
        }
        if (window.location.hash === '#slos') {
          navigateTo('slos');
          return;
        }
        if (window.location.hash === '#service-map') {
          navigateTo('service-map');
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

      // --- Incidents -------------------------------------------------------
      const incidentStatusFilter = document.getElementById('incident-status-filter');
      const incidentRefreshButton = document.getElementById('incident-refresh-button');
      const incidentCreateButton = document.getElementById('incident-create-button');
      const incidentCreateSeverity = document.getElementById('incident-create-severity');
      const incidentTableScroll = document.getElementById('incident-table-scroll');
      const incidentTableBody = document.getElementById('incident-table-body');
      const incidentEmpty = document.getElementById('incident-empty');
      const incidentStatusBox = document.getElementById('incident-status-box');
      const incidentDetailSection = document.getElementById('incident-detail-section');
      const incidentDetailTitle = document.getElementById('incident-detail-title');
      const incidentDetailBody = document.getElementById('incident-detail-body');

      function setIncidentStatus(kind, message) {
        incidentStatusBox.dataset.state = kind;
        incidentStatusBox.textContent = message;
      }

      function formatIncidentTimestamp(ts) {
        if (!ts) return '-';
        try { return new Date(ts).toLocaleString(); } catch (_) { return String(ts); }
      }

      async function refreshIncidents() {
        const status = incidentStatusFilter.value;
        const url = new URL('/api/incidents', window.location.origin);
        if (status) url.searchParams.set('status', status);
        try {
          const resp = await fetch(url.toString(), { headers: { accept: 'application/json' } });
          if (!resp.ok) throw new Error('HTTP ' + resp.status);
          const data = await resp.json();
          renderIncidentList(data.incidents || []);
          setIncidentStatus('ok', `Loaded ${data.incidents.length} incident(s).`);
        } catch (err) {
          setIncidentStatus('error', `Failed to load incidents: ${err.message}`);
        }
      }

      function renderIncidentList(incidents) {
        incidentTableBody.replaceChildren();
        if (!incidents.length) {
          incidentTableScroll.hidden = true;
          incidentEmpty.hidden = false;
          incidentDetailSection.hidden = true;
          return;
        }
        incidentTableScroll.hidden = false;
        incidentEmpty.hidden = true;
        for (const incident of incidents) {
          const tr = document.createElement('tr');
          tr.dataset.incidentId = incident.id;

          const statusTd = document.createElement('td');
          const pill = document.createElement('span');
          pill.className = 'incident-status-pill ' + incident.status;
          pill.textContent = incident.status;
          statusTd.appendChild(pill);
          tr.appendChild(statusTd);

          tr.appendChild(makeTextElement('td', incident.severity));
          tr.appendChild(makeTextElement('td', formatIncidentTimestamp(incident.opened_at)));
          tr.appendChild(makeTextElement('td', formatIncidentTimestamp(incident.acknowledged_at)));
          tr.appendChild(makeTextElement('td', formatIncidentTimestamp(incident.resolved_at)));
          tr.appendChild(makeTextElement('td', truncateId(incident.id)));

          const actionsTd = document.createElement('td');
          if (incident.status === 'open') {
            const ackBtn = document.createElement('button');
            ackBtn.className = 'secondary btn-compact';
            ackBtn.type = 'button';
            ackBtn.dataset.testid = 'incident-ack';
            ackBtn.textContent = 'Ack';
            ackBtn.addEventListener('click', () => transitionIncident(incident.id, 'acknowledged'));
            actionsTd.appendChild(ackBtn);
          }
          if (incident.status !== 'resolved') {
            const resolveBtn = document.createElement('button');
            resolveBtn.className = 'primary btn-compact';
            resolveBtn.type = 'button';
            resolveBtn.dataset.testid = 'incident-resolve';
            resolveBtn.textContent = 'Resolve';
            resolveBtn.style.marginLeft = '6px';
            resolveBtn.addEventListener('click', () => transitionIncident(incident.id, 'resolved'));
            actionsTd.appendChild(resolveBtn);
          }
          const detailBtn = document.createElement('button');
          detailBtn.className = 'secondary btn-compact';
          detailBtn.type = 'button';
          detailBtn.textContent = 'View';
          detailBtn.style.marginLeft = '6px';
          detailBtn.addEventListener('click', () => showIncidentDetail(incident));
          actionsTd.appendChild(detailBtn);
          tr.appendChild(actionsTd);

          incidentTableBody.appendChild(tr);
        }
      }

      function showIncidentDetail(incident) {
        incidentDetailSection.hidden = false;
        incidentDetailTitle.textContent = `Incident ${incident.id}`;
        incidentDetailBody.replaceChildren();

        const meta = document.createElement('div');
        meta.style.marginBottom = '12px';
        meta.appendChild(makeTextElement('div', `Status: ${incident.status}`));
        meta.appendChild(makeTextElement('div', `Severity: ${incident.severity}`));
        meta.appendChild(makeTextElement('div', `Opened: ${formatIncidentTimestamp(incident.opened_at)}`));
        if (incident.acknowledged_at) {
          meta.appendChild(makeTextElement('div', `Acknowledged: ${formatIncidentTimestamp(incident.acknowledged_at)}`));
        }
        if (incident.resolved_at) {
          meta.appendChild(makeTextElement('div', `Resolved: ${formatIncidentTimestamp(incident.resolved_at)}`));
        }
        if (incident.alert_id) {
          meta.appendChild(makeTextElement('div', `Alert ID: ${incident.alert_id}`));
        }
        incidentDetailBody.appendChild(meta);

        const pre = document.createElement('pre');
        pre.textContent = JSON.stringify(incident.details_json || {}, null, 2);
        incidentDetailBody.appendChild(pre);
      }

      async function transitionIncident(id, status) {
        try {
          const resp = await fetch(`/api/incidents/${id}`, {
            method: 'PATCH',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ status }),
          });
          if (!resp.ok) {
            const text = await resp.text();
            throw new Error(`HTTP ${resp.status}: ${text}`);
          }
          await refreshIncidents();
        } catch (err) {
          setIncidentStatus('error', `Transition failed: ${err.message}`);
        }
      }

      async function createIncident() {
        const severity = (incidentCreateSeverity.value || '').trim();
        if (!severity) {
          setIncidentStatus('error', 'Severity is required to create an incident.');
          return;
        }
        try {
          const resp = await fetch('/api/incidents', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({
              severity,
              details_json: { message: 'Created via UI' },
            }),
          });
          if (!resp.ok) {
            const text = await resp.text();
            throw new Error(`HTTP ${resp.status}: ${text}`);
          }
          incidentCreateSeverity.value = '';
          await refreshIncidents();
        } catch (err) {
          setIncidentStatus('error', `Create failed: ${err.message}`);
        }
      }

      incidentRefreshButton.addEventListener('click', () => refreshIncidents());
      incidentStatusFilter.addEventListener('change', () => refreshIncidents());
      incidentCreateButton.addEventListener('click', () => createIncident());

      // --- Notification channels (Settings) --------------------------------
      const notificationChannelsTableBody = document.getElementById('notification-channels-table-body');
      const notificationChannelsStatus = document.getElementById('notification-channels-status');
      const newNotificationChannelButton = document.getElementById('new-notification-channel-button');
      const refreshNotificationChannelsButton = document.getElementById('refresh-notification-channels-button');

      function setNotificationChannelsStatus(state, text) {
        if (!notificationChannelsStatus) return;
        notificationChannelsStatus.dataset.state = state;
        notificationChannelsStatus.textContent = text;
      }

      function describeChannelDestination(channel) {
        if (channel.kind === 'webhook') {
          const url = channel.config && typeof channel.config.url === 'string' ? channel.config.url : '';
          return url || '(no url)';
        }
        try { return JSON.stringify(channel.config); }
        catch (_) { return ''; }
      }

      async function refreshNotificationChannels() {
        if (!notificationChannelsTableBody) return;
        try {
          const resp = await fetch('/api/notification-channels', { headers: { accept: 'application/json' } });
          if (!resp.ok) {
            setNotificationChannelsStatus('error', `Failed to load channels (HTTP ${resp.status}).`);
            return;
          }
          const { channels } = await resp.json();
          renderNotificationChannels(channels || []);
          setNotificationChannelsStatus('idle', channels && channels.length
            ? `${channels.length} channel${channels.length === 1 ? '' : 's'} configured.`
            : 'No channels yet. Click "+ New Channel" to add one.');
        } catch (err) {
          setNotificationChannelsStatus('error', `Failed to load channels: ${err.message}`);
        }
      }

      function renderNotificationChannels(channels) {
        notificationChannelsTableBody.replaceChildren();
        for (const ch of channels) {
          const tr = document.createElement('tr');
          tr.dataset.id = ch.id;
          appendTableCell(tr, ch.name);
          appendTableCell(tr, ch.kind);
          appendTableCell(tr, describeChannelDestination(ch));
          appendTableCell(tr, ch.enabled ? 'Yes' : 'No');

          const actionsTd = document.createElement('td');
          const testBtn = document.createElement('button');
          testBtn.className = 'secondary btn-compact';
          testBtn.type = 'button';
          testBtn.textContent = 'Test';
          testBtn.addEventListener('click', () => testNotificationChannel(ch));
          actionsTd.appendChild(testBtn);

          const deleteBtn = document.createElement('button');
          deleteBtn.className = 'secondary btn-compact';
          deleteBtn.type = 'button';
          deleteBtn.style.marginLeft = '6px';
          deleteBtn.textContent = 'Delete';
          deleteBtn.addEventListener('click', () => deleteNotificationChannel(ch));
          actionsTd.appendChild(deleteBtn);

          tr.appendChild(actionsTd);
          notificationChannelsTableBody.appendChild(tr);
        }
      }

      async function testNotificationChannel(channel) {
        setNotificationChannelsStatus('working', `Sending test to ${channel.name}...`);
        try {
          const resp = await fetch(`/api/notification-channels/${channel.id}/test`, { method: 'POST' });
          const data = await resp.json().catch(() => ({}));
          if (resp.ok && data && data.delivered) {
            setNotificationChannelsStatus('ok', `Test delivered to ${channel.name}.`);
          } else {
            const detail = (data && data.detail) || `HTTP ${resp.status}`;
            setNotificationChannelsStatus('error', `Test failed for ${channel.name}: ${detail}`);
          }
        } catch (err) {
          setNotificationChannelsStatus('error', `Test failed: ${err.message}`);
        }
      }

      async function deleteNotificationChannel(channel) {
        if (!confirm(`Delete channel "${channel.name}"?`)) return;
        try {
          const resp = await fetch(`/api/notification-channels/${channel.id}`, { method: 'DELETE' });
          if (resp.status !== 204 && !resp.ok) throw new Error(`HTTP ${resp.status}`);
          await refreshNotificationChannels();
        } catch (err) {
          setNotificationChannelsStatus('error', `Delete failed: ${err.message}`);
        }
      }

      function openNewNotificationChannelModal() {
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';

        const box = document.createElement('div');
        box.className = 'modal-box';
        box.innerHTML = '<h3>New Notification Channel</h3>';

        const nameLabel = document.createElement('label');
        nameLabel.textContent = 'Name';
        const nameInput = document.createElement('input');
        nameInput.placeholder = 'slack-team-alerts';
        nameInput.maxLength = 80;
        nameLabel.appendChild(nameInput);
        box.appendChild(nameLabel);

        const kindLabel = document.createElement('label');
        kindLabel.textContent = 'Kind';
        const kindSelect = document.createElement('select');
        const opt = document.createElement('option');
        opt.value = 'webhook';
        opt.textContent = 'Webhook';
        kindSelect.appendChild(opt);
        kindLabel.appendChild(kindSelect);
        box.appendChild(kindLabel);

        const urlLabel = document.createElement('label');
        urlLabel.textContent = 'Webhook URL';
        const urlInput = document.createElement('input');
        urlInput.type = 'url';
        urlInput.placeholder = 'https://hooks.example.com/services/...';
        urlInput.maxLength = 2048;
        urlLabel.appendChild(urlInput);
        box.appendChild(urlLabel);

        const headersLabel = document.createElement('label');
        headersLabel.textContent = 'Headers (JSON object, optional)';
        const headersInput = document.createElement('textarea');
        headersInput.rows = 3;
        headersInput.placeholder = '{"x-token":"abc"}';
        headersLabel.appendChild(headersInput);
        box.appendChild(headersLabel);

        const errorEl = document.createElement('div');
        errorEl.style.color = 'var(--coral, #d04545)';
        errorEl.style.fontSize = '0.85rem';
        errorEl.hidden = true;
        box.appendChild(errorEl);

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
          errorEl.hidden = true;
          const name = nameInput.value.trim();
          if (!name) { errorEl.hidden = false; errorEl.textContent = 'Name is required.'; nameInput.focus(); return; }
          const url = urlInput.value.trim();
          if (!url) { errorEl.hidden = false; errorEl.textContent = 'URL is required.'; urlInput.focus(); return; }
          const config = { url };
          const rawHeaders = headersInput.value.trim();
          if (rawHeaders) {
            try {
              const parsed = JSON.parse(rawHeaders);
              if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
                config.headers = parsed;
              } else {
                throw new Error('Headers must be a JSON object.');
              }
            } catch (err) {
              errorEl.hidden = false;
              errorEl.textContent = `Headers JSON parse error: ${err.message}`;
              return;
            }
          }
          createBtn.disabled = true;
          try {
            const resp = await fetch('/api/notification-channels', {
              method: 'POST',
              headers: { 'content-type': 'application/json', accept: 'application/json' },
              body: JSON.stringify({ name, kind: kindSelect.value, config }),
            });
            if (!resp.ok) {
              const text = await resp.text();
              throw new Error(text || `HTTP ${resp.status}`);
            }
            overlay.remove();
            await refreshNotificationChannels();
          } catch (err) {
            errorEl.hidden = false;
            errorEl.textContent = `Create failed: ${err.message}`;
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

      if (newNotificationChannelButton) {
        newNotificationChannelButton.addEventListener('click', openNewNotificationChannelModal);
      }
      if (refreshNotificationChannelsButton) {
        refreshNotificationChannelsButton.addEventListener('click', () => refreshNotificationChannels());
      }

      // --- SLO tab ---------------------------------------------------------

      function setSloStatus(state, message) {
        if (!sloStatusBox) return;
        sloStatusBox.dataset.state = state;
        sloStatusBox.textContent = message;
      }

      function formatWindowMs(ms) {
        if (!Number.isFinite(ms) || ms <= 0) return `${ms}ms`;
        const sec = Math.round(ms / 1000);
        if (sec < 60) return `${sec}s`;
        const min = Math.round(sec / 60);
        if (min < 60) return `${min}m`;
        const hr = Math.round(min / 60);
        if (hr < 24) return `${hr}h`;
        const day = Math.round(hr / 24);
        return `${day}d`;
      }

      function pctClass(remainingPct) {
        if (remainingPct < 20) return 'crit';
        if (remainingPct < 50) return 'warn';
        return '';
      }

      function renderBudgetCell(td, budget) {
        const remaining = budget && Number.isFinite(budget.remaining_pct) ? budget.remaining_pct : 100;
        const current = budget && Number.isFinite(budget.current_pct) ? budget.current_pct : 100;
        const wrap = document.createElement('div');
        wrap.className = `slo-budget-bar ${pctClass(remaining)}`.trim();
        const fill = document.createElement('div');
        fill.className = 'slo-budget-bar-fill';
        fill.style.width = `${Math.max(0, Math.min(100, remaining)).toFixed(1)}%`;
        const label = document.createElement('div');
        label.className = 'slo-budget-bar-label';
        label.textContent = `${remaining.toFixed(2)}% remaining (current ${current.toFixed(2)}%)`;
        wrap.appendChild(fill);
        wrap.appendChild(label);
        td.replaceChildren(wrap);
      }

      async function fetchBudget(id) {
        try {
          const r = await fetch(`/api/slos/${id}/budget`, { headers: { accept: 'application/json' } });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          return await r.json();
        } catch (err) {
          return null;
        }
      }

      async function refreshSlos() {
        setSloStatus('working', 'Loading SLOs...');
        let payload;
        try {
          const r = await fetch('/api/slos', { headers: { accept: 'application/json' } });
          if (!r.ok) throw new Error(`HTTP ${r.status}`);
          payload = await r.json();
        } catch (err) {
          setSloStatus('error', `Failed to load SLOs: ${err.message}`);
          return;
        }

        const slos = (payload && payload.slos) || [];
        sloListBody.replaceChildren();
        if (!slos.length) {
          sloListTable.hidden = true;
          sloEmpty.hidden = false;
          setSloStatus('ok', 'No SLOs configured.');
          return;
        }
        sloListTable.hidden = false;
        sloEmpty.hidden = true;

        for (const slo of slos) {
          const tr = document.createElement('tr');
          tr.dataset.id = slo.id;

          const nameTd = document.createElement('td');
          nameTd.style.padding = '8px';
          nameTd.textContent = slo.name;
          tr.appendChild(nameTd);

          const targetTd = document.createElement('td');
          targetTd.style.padding = '8px';
          targetTd.textContent = `${slo.target_pct}%`;
          tr.appendChild(targetTd);

          const windowTd = document.createElement('td');
          windowTd.style.padding = '8px';
          windowTd.textContent = formatWindowMs(slo.window_ms);
          tr.appendChild(windowTd);

          const currentTd = document.createElement('td');
          currentTd.style.padding = '8px';
          currentTd.textContent = '...';
          tr.appendChild(currentTd);

          const budgetTd = document.createElement('td');
          budgetTd.style.padding = '8px';
          budgetTd.textContent = 'Loading...';
          tr.appendChild(budgetTd);

          const actionsTd = document.createElement('td');
          actionsTd.style.padding = '8px';
          const delBtn = document.createElement('button');
          delBtn.className = 'secondary btn-compact';
          delBtn.type = 'button';
          delBtn.textContent = 'Delete';
          delBtn.addEventListener('click', async () => {
            if (!window.confirm(`Delete SLO "${slo.name}"?`)) return;
            try {
              const r = await fetch(`/api/slos/${slo.id}`, { method: 'DELETE' });
              if (!r.ok && r.status !== 204) throw new Error(`HTTP ${r.status}`);
              refreshSlos();
            } catch (err) {
              setSloStatus('error', `Delete failed: ${err.message}`);
            }
          });
          actionsTd.appendChild(delBtn);
          tr.appendChild(actionsTd);

          sloListBody.appendChild(tr);
        }

        setSloStatus('ok', `${slos.length} SLO${slos.length === 1 ? '' : 's'} configured.`);

        await Promise.all(slos.map(async (slo) => {
          const tr = sloListBody.querySelector(`tr[data-id="${slo.id}"]`);
          if (!tr) return;
          const budget = await fetchBudget(slo.id);
          const currentTd = tr.children[3];
          const budgetTd = tr.children[4];
          if (!budget) {
            currentTd.textContent = 'n/a';
            budgetTd.textContent = 'n/a';
            return;
          }
          currentTd.textContent = `${budget.current_pct.toFixed(2)}% (${budget.success_count}/${budget.total_count})`;
          renderBudgetCell(budgetTd, budget);
        }));
      }

      function openCreateSloModal() {
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';
        const box = document.createElement('div');
        box.className = 'modal-box';

        const title = document.createElement('h3');
        title.textContent = 'New SLO';
        title.style.marginTop = '0';
        box.appendChild(title);

        const fieldset = document.createElement('div');
        fieldset.style.display = 'grid';
        fieldset.style.gap = '8px';

        const nameInput = document.createElement('input');
        nameInput.placeholder = 'Name (e.g. checkout availability)';
        nameInput.maxLength = 120;
        fieldset.appendChild(nameInput);

        const targetInput = document.createElement('input');
        targetInput.type = 'number';
        targetInput.min = '0';
        targetInput.max = '100';
        targetInput.step = '0.01';
        targetInput.placeholder = 'Target % (e.g. 99.5)';
        fieldset.appendChild(targetInput);

        const windowInput = document.createElement('input');
        windowInput.type = 'number';
        windowInput.min = '1';
        windowInput.placeholder = 'Window minutes (e.g. 60)';
        fieldset.appendChild(windowInput);

        const viewerSelect = document.createElement('select');
        const noneOpt = document.createElement('option');
        noneOpt.value = '';
        noneOpt.textContent = '(no source viewer)';
        viewerSelect.appendChild(noneOpt);
        fetch('/api/viewers', { headers: { accept: 'application/json' } })
          .then(r => r.ok ? r.json() : { viewers: [] })
          .then(data => {
            for (const v of (data.viewers || [])) {
              const o = document.createElement('option');
              o.value = v.id;
              o.textContent = v.name;
              viewerSelect.appendChild(o);
            }
          })
          .catch(() => {});
        fieldset.appendChild(viewerSelect);

        const successInput = document.createElement('input');
        successInput.placeholder = 'Success service_name (optional, eq match)';
        fieldset.appendChild(successInput);

        const totalInput = document.createElement('input');
        totalInput.placeholder = 'Total service_name (optional, eq match)';
        fieldset.appendChild(totalInput);

        box.appendChild(fieldset);

        const errMsg = document.createElement('div');
        errMsg.style.color = 'var(--accent-strong)';
        errMsg.style.fontSize = '12px';
        errMsg.style.marginTop = '8px';
        box.appendChild(errMsg);

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
          const target_pct = Number.parseFloat(targetInput.value);
          const minutes = Number.parseInt(windowInput.value, 10);
          if (!name) { errMsg.textContent = 'Name is required.'; return; }
          if (!Number.isFinite(target_pct) || target_pct < 0 || target_pct > 100) {
            errMsg.textContent = 'Target must be between 0 and 100.'; return;
          }
          if (!Number.isFinite(minutes) || minutes <= 0) {
            errMsg.textContent = 'Window must be a positive integer (minutes).'; return;
          }
          const success_filters = successInput.value.trim()
            ? [{ field: 'service_name', op: 'eq', value: successInput.value.trim() }]
            : [];
          const total_filters = totalInput.value.trim()
            ? [{ field: 'service_name', op: 'eq', value: totalInput.value.trim() }]
            : [];
          const body = {
            name,
            target_pct,
            window_ms: minutes * 60_000,
            success_filters,
            total_filters,
          };
          if (viewerSelect.value) body.viewer_id = viewerSelect.value;
          createBtn.disabled = true;
          try {
            const r = await fetch('/api/slos', {
              method: 'POST',
              headers: { 'content-type': 'application/json', accept: 'application/json' },
              body: JSON.stringify(body),
            });
            if (!r.ok) {
              const txt = await r.text();
              throw new Error(`HTTP ${r.status}: ${txt}`);
            }
            overlay.remove();
            refreshSlos();
          } catch (err) {
            errMsg.textContent = `Create failed: ${err.message}`;
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

      if (newSloButton) newSloButton.addEventListener('click', openCreateSloModal);
      if (refreshSlosButton) refreshSlosButton.addEventListener('click', () => refreshSlos());
      if (navSlos) {
        navSlos.addEventListener('click', (e) => {
          e.preventDefault();
          window.location.hash = '#slos';
          if (window.location.hash === '#slos') navigateTo('slos');
        });
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
    pub rollup_store: Option<RollupStore>,
    pub alert_store: Option<crate::storage::alert_store::AlertStore>,
    /// Optional Postgres-backed inverted index for attributes.
    /// `None` in standalone (in-memory) mode -- index writes / lookups become no-ops.
    pub attr_index: Option<crate::storage::attr_index::AttrIndexStore>,
    pub incident_store: IncidentStore,
    pub slo_store: Option<SloStore>,
    pub error_group_store: Option<ErrorGroupStore>,
    pub api_key_store: Option<ApiKeyStore>,
    /// When false, the API key auth middleware is bypassed (useful for bootstrap or local dev).
    pub api_keys_enabled: bool,
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

    fn require_alert_store(&self) -> Result<&crate::storage::alert_store::AlertStore, StatusCode> {
        self.alert_store
            .as_ref()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }

    fn require_attr_index(
        &self,
    ) -> Result<&crate::storage::attr_index::AttrIndexStore, StatusCode> {
        self.attr_index
            .as_ref()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }

    fn require_slo_store(&self) -> Result<&SloStore, StatusCode> {
        self.slo_store
            .as_ref()
            .ok_or(StatusCode::SERVICE_UNAVAILABLE)
    }

    pub(crate) fn require_api_key_store(&self) -> Result<&ApiKeyStore, StatusCode> {
        self.api_key_store
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
        "table" | "stacked_bar" | "line" | "area" | "pie" | "donut" | "billboard" | "timeseries"
    )
}

fn is_chart_type_supported_for_signal_mask(chart_type: &str, signal_mask: SignalMask) -> bool {
    match chart_type {
        "table" => true,
        // Timeseries works for any signal because the aggregator can always
        // produce count/rate from `observed_at`.
        "timeseries" => !signal_mask.is_empty(),
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

const API_KEY_PREFIX: &str = "lt_";

#[derive(Debug, Deserialize)]
struct CreateApiKeyRequest {
    name: String,
}

#[derive(Debug, Serialize)]
struct CreateApiKeyResponse {
    id: Uuid,
    name: String,
    /// Raw key returned only once at creation time.
    key: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ApiKeyListItem {
    id: Uuid,
    name: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct ApiKeyListResponse {
    api_keys: Vec<ApiKeyListItem>,
}

/// Raw filter condition sent by the client in create / patch / preview requests.
#[derive(Debug, Deserialize, Serialize, Clone)]
struct ViewerFilterInput {
    field: String,
    op: String,
    value: String,
}

/// Raw aggregation input mirroring the `aggregation` block stored in
/// definition_json. Forwarded as-is to the compiler which validates the shape.
#[derive(Debug, Deserialize, Clone)]
struct AggregationInput {
    #[serde(rename = "fn")]
    func: String,
    bucket_ms: i64,
    #[serde(default)]
    group_by: Vec<String>,
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
    #[serde(default)]
    aggregation: Option<AggregationInput>,
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
    #[serde(default)]
    aggregation: Option<AggregationInput>,
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
struct AttributeKeysResponse {
    keys: Vec<String>,
}

#[derive(Debug, Serialize)]
struct AttributeValuesResponse {
    key: String,
    values: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct AttributeValuesQuery {
    prefix: Option<String>,
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
    /// Echoes the persisted aggregation block when one is configured.
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregation: Option<serde_json::Value>,
    refresh_interval_ms: u32,
    lookback_ms: i64,
    entry_count: usize,
    status: ViewerStatus,
    entries: Vec<ViewerEntryRow>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    traces: Vec<TraceSummary>,
    /// Time-bucketed aggregations -- empty when the viewer has no
    /// `aggregation` block.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    aggregated_buckets: Vec<crate::viewer_runtime::aggregator::Bucket>,
    /// Optional list of bucket -> sample trace_ids. Only populated for metric viewers when
    /// detail responses are requested and trace samples exist.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    exemplars: Vec<ExemplarPair>,
}

/// Flattened exemplar pair attached to a metric viewer summary.
#[derive(Debug, Serialize)]
struct ExemplarPair {
    bucket_start_ms: i64,
    trace_id: String,
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
    build_app_with_services(stream_store, None, None, None)
}

pub fn build_app_with_services(
    stream_store: StreamStore,
    viewer_store: Option<ViewerStore>,
    viewer_runtime: Option<SharedViewerRuntime>,
    alert_store: Option<crate::storage::alert_store::AlertStore>,
) -> Router {
    build_app_with_full_services(
        stream_store,
        viewer_store,
        viewer_runtime,
        None,
        alert_store,
    )
}

/// Same as [`build_app_with_services`] but also wires the rollup store used by
/// `/api/rollups`. Callers that don't care about rollups can keep using
/// [`build_app_with_services`] and the rollup endpoint will return 503.
pub fn build_app_with_full_services(
    stream_store: StreamStore,
    viewer_store: Option<ViewerStore>,
    viewer_runtime: Option<SharedViewerRuntime>,
    rollup_store: Option<RollupStore>,
    alert_store: Option<crate::storage::alert_store::AlertStore>,
) -> Router {
    build_app_with_services_full(
        stream_store,
        viewer_store,
        viewer_runtime,
        rollup_store,
        alert_store,
        None,
        None,
        None,
        None,
        None,
        false,
    )
}

/// Full builder that also accepts an optional attribute inverted index, an
/// optional incident store, and an optional SLO store.
///
/// `attr_index` is `None` in standalone (in-memory) mode; the `/api/attributes`
/// endpoints then return 503 and ingest skips index writes.
///
/// `incident_store` is `None` in standalone (in-memory) mode; an in-memory
/// incident store is used so the incident endpoints remain functional.
///
/// `slo_store` is `None` when no SLO storage backend is wired; the `/api/slos`
/// endpoints then return 503.
#[allow(clippy::too_many_arguments)]
pub fn build_app_with_services_full(
    stream_store: StreamStore,
    viewer_store: Option<ViewerStore>,
    viewer_runtime: Option<SharedViewerRuntime>,
    rollup_store: Option<RollupStore>,
    alert_store: Option<crate::storage::alert_store::AlertStore>,
    attr_index: Option<crate::storage::attr_index::AttrIndexStore>,
    incident_store: Option<IncidentStore>,
    slo_store: Option<SloStore>,
    error_group_store: Option<ErrorGroupStore>,
    api_key_store: Option<ApiKeyStore>,
    api_keys_enabled: bool,
) -> Router {
    let incident_store = incident_store.unwrap_or_else(|| {
        IncidentStore::Memory(crate::storage::memory::MemoryIncidentStore::new())
    });
    let state = AppState {
        stream_store,
        viewer_store,
        viewer_runtime,
        rollup_store,
        alert_store,
        attr_index,
        incident_store,
        slo_store,
        error_group_store,
        api_key_store,
        api_keys_enabled,
    };
    let connect = crate::grpc::build_connect_router(state.clone());

    let api_routes = Router::new()
        .route("/api/query", post(run_query))
        .route("/api/viewers", get(list_viewers).post(create_viewer))
        .route("/api/viewers/preview", post(preview_viewer))
        .route(
            "/api/viewers/{id}",
            get(get_viewer).patch(patch_viewer).delete(delete_viewer),
        )
        .route("/api/services", get(list_services))
        .route("/api/attributes", get(list_attribute_keys))
        .route("/api/attributes/{key}/values", get(list_attribute_values))
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
        .route("/api/traces/{trace_id}/waterfall", get(get_trace_waterfall))
        .route("/api/rollups", get(get_rollups))
        .route("/api/traces", get(get_trace_by_id))
        .route("/api/traces/search", get(search_traces_handler))
        .route("/api/alerts", get(list_alerts).post(create_alert))
        .route(
            "/api/alerts/{id}",
            get(get_alert).patch(patch_alert).delete(delete_alert),
        )
        .route("/api/incidents", get(list_incidents).post(create_incident))
        .route(
            "/api/incidents/{id}",
            get(get_incident).patch(patch_incident),
        )
        .route(
            "/api/notification-channels",
            get(list_notification_channels).post(create_notification_channel),
        )
        .route(
            "/api/notification-channels/{id}",
            delete(delete_notification_channel),
        )
        .route(
            "/api/notification-channels/{id}/test",
            post(test_notification_channel),
        )
        .route("/api/slos", get(list_slos).post(create_slo))
        .route("/api/slos/{id}", get(get_slo).delete(delete_slo))
        .route("/api/slos/{id}/budget", get(get_slo_budget))
        .route("/api/exemplars", get(list_exemplars))
        .route("/api/anomaly/evaluate", post(evaluate_anomaly))
        .route("/api/service-map", get(service_map))
        .route("/api/error-groups", get(list_error_groups))
        .route("/api/slow-queries", get(list_slow_queries))
        .route("/api/n-plus-one", get(list_n_plus_one))
        .route("/api/api-keys", get(list_api_keys).post(create_api_key))
        .route("/api/api-keys/{id}", delete(delete_api_key))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            api_key_auth_middleware,
        ));

    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/v1/traces", post(ingest_traces))
        .route("/v1/metrics", post(ingest_metrics))
        .route("/v1/logs", post(ingest_logs))
        .merge(api_routes)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state)
        .fallback_service(connect.into_axum_service())
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
    let mut summary = viewer_summary(
        viewer,
        &viewer_state.entries,
        &viewer_state.status,
        viewer.lookback_ms(),
        true,
        &viewer_state.aggregated_buckets,
    )
    .map_err(|error| {
        tracing::error!("viewer {id}: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // Opportunistically attach exemplars for metric viewers. Failures stay non-fatal --
    // the field stays empty.
    if viewer.definition().signal_mask.contains(Signal::Metrics) {
        match exemplar_buckets_for_metric_viewer(
            &runtime,
            viewer_state,
            viewer.lookback_ms(),
            DEFAULT_EXEMPLAR_BUCKET_MS,
            crate::apm::exemplars::DEFAULT_MAX_SAMPLES_PER_BUCKET,
        ) {
            Ok(buckets) => summary.exemplars = flatten_exemplar_buckets(buckets),
            Err(e) => tracing::warn!("get_viewer {id}: exemplars failed: {e}"),
        }
    }

    Ok(Json(summary))
}

/// Compute exemplar buckets for a metric viewer using trace entries observed by any trace
/// viewer currently in the runtime. Encapsulates the runtime traversal so handlers don't
/// repeat the boilerplate.
fn exemplar_buckets_for_metric_viewer(
    runtime: &ViewerRuntime,
    metric_state: &ViewerState,
    lookback_ms: i64,
    bucket_ms: i64,
    max_samples: usize,
) -> Result<Vec<crate::apm::exemplars::ExemplarBucket>, crate::apm::exemplars::ExemplarError> {
    let service_filter = single_service_name(&metric_state.entries);
    let trace_entries = collect_trace_entries(runtime);
    crate::apm::exemplars::compute_exemplars(
        &metric_state.entries,
        &trace_entries,
        service_filter.as_deref(),
        Utc::now(),
        lookback_ms,
        bucket_ms,
        max_samples,
    )
}

fn flatten_exemplar_buckets(
    buckets: Vec<crate::apm::exemplars::ExemplarBucket>,
) -> Vec<ExemplarPair> {
    buckets
        .into_iter()
        .flat_map(|b| {
            b.sample_trace_ids
                .into_iter()
                .map(move |trace_id| ExemplarPair {
                    bucket_start_ms: b.bucket_start_ms,
                    trace_id,
                })
        })
        .collect()
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

async fn list_attribute_keys(
    State(state): State<AppState>,
) -> Result<Json<AttributeKeysResponse>, StatusCode> {
    let store = state.require_attr_index()?;
    match store.list_keys().await {
        Ok(keys) => Ok(Json(AttributeKeysResponse { keys })),
        Err(error) => {
            tracing::error!("attr_index list_keys failed: {error}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn list_attribute_values(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Query(params): Query<AttributeValuesQuery>,
) -> Result<Json<AttributeValuesResponse>, StatusCode> {
    if key.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let store = state.require_attr_index()?;
    let prefix = params.prefix.as_deref();
    match store.list_values(&key, prefix).await {
        Ok(values) => Ok(Json(AttributeValuesResponse { key, values })),
        Err(error) => {
            tracing::error!("attr_index list_values failed: {error}");
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExemplarsQuery {
    metric_viewer_id: Uuid,
    #[serde(default)]
    lookback_ms: Option<i64>,
    #[serde(default)]
    bucket_ms: Option<i64>,
    #[serde(default)]
    max_samples: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ExemplarsResponse {
    metric_viewer_id: Uuid,
    bucket_ms: i64,
    lookback_ms: i64,
    buckets: Vec<crate::apm::exemplars::ExemplarBucket>,
}

const DEFAULT_EXEMPLAR_BUCKET_MS: i64 = 60_000;

async fn list_exemplars(
    State(state): State<AppState>,
    Query(params): Query<ExemplarsQuery>,
) -> Result<Json<ExemplarsResponse>, StatusCode> {
    let runtime = state.require_viewer_runtime()?.lock().await;

    // Locate the metric viewer.
    let (metric_viewer, metric_state) = runtime
        .viewers()
        .iter()
        .find(|(v, _)| v.definition().id == params.metric_viewer_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    if !metric_viewer
        .definition()
        .signal_mask
        .contains(Signal::Metrics)
    {
        return Err(StatusCode::BAD_REQUEST);
    }

    let bucket_ms = params.bucket_ms.unwrap_or(DEFAULT_EXEMPLAR_BUCKET_MS);
    let lookback_ms = params.lookback_ms.unwrap_or(metric_viewer.lookback_ms());
    let max_samples = params
        .max_samples
        .unwrap_or(crate::apm::exemplars::DEFAULT_MAX_SAMPLES_PER_BUCKET);

    let buckets = exemplar_buckets_for_metric_viewer(
        &runtime,
        metric_state,
        lookback_ms,
        bucket_ms,
        max_samples,
    )
    .map_err(|error| {
        tracing::warn!("exemplars: invalid request: {error}");
        StatusCode::BAD_REQUEST
    })?;

    Ok(Json(ExemplarsResponse {
        metric_viewer_id: params.metric_viewer_id,
        bucket_ms,
        lookback_ms,
        buckets,
    }))
}

/// Returns Some(name) when every entry shares the same `service_name`, else None.
/// Returns None if any entry lacks a `service_name` (so the caller falls back to no filter
/// rather than silently excluding unlabelled entries).
fn single_service_name(entries: &[NormalizedEntry]) -> Option<String> {
    let mut found: Option<&str> = None;
    for entry in entries {
        let name = entry.service_name.as_deref()?;
        match found {
            None => found = Some(name),
            Some(existing) if existing.eq_ignore_ascii_case(name) => {}
            Some(_) => return None,
        }
    }
    found.map(str::to_string)
}

/// Gathers trace entries observed by any viewer in the runtime, deduped by (observed_at, payload-ptr).
/// Cheap heuristic: clones each viewer's trace entries and concatenates them. The exemplar logic
/// dedupes trace_ids per bucket so duplicates across viewers do not inflate the result.
fn collect_trace_entries(runtime: &ViewerRuntime) -> Vec<NormalizedEntry> {
    let mut out = Vec::new();
    for (viewer, state) in runtime.viewers() {
        if !viewer.definition().signal_mask.contains(Signal::Traces) {
            continue;
        }
        for entry in &state.entries {
            if entry.signal == Signal::Traces {
                out.push(entry.clone());
            }
        }
    }
    out
}

#[derive(Debug, Deserialize)]
struct EvaluateAnomalyRequest {
    viewer_id: Uuid,
    detector: crate::anomaly::DetectorSpec,
}

async fn evaluate_anomaly(
    State(state): State<AppState>,
    Json(payload): Json<EvaluateAnomalyRequest>,
) -> Result<Json<crate::anomaly::DetectorResult>, StatusCode> {
    let runtime = state.require_viewer_runtime()?.lock().await;
    let (_, viewer_state) = runtime
        .viewers()
        .iter()
        .find(|(viewer, _)| viewer.definition().id == payload.viewer_id)
        .ok_or(StatusCode::NOT_FOUND)?;

    let now = Utc::now();
    let result = payload.detector.evaluate(&viewer_state.entries, now);
    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
struct ServiceMapQuery {
    lookback_ms: Option<i64>,
}

/// Default lookback window (10 minutes) for the service map API.
const DEFAULT_SERVICE_MAP_LOOKBACK_MS: i64 = 10 * 60 * 1_000;

async fn service_map(
    State(state): State<AppState>,
    Query(params): Query<ServiceMapQuery>,
) -> Result<Json<ServiceMap>, StatusCode> {
    let lookback_ms = params
        .lookback_ms
        .unwrap_or(DEFAULT_SERVICE_MAP_LOOKBACK_MS);
    let now = Utc::now();

    // Read traces directly from the stream so the endpoint works in
    // standalone mode (where viewer_runtime is also memory-backed) and in
    // the Redis/Postgres deployment without depending on user-configured
    // viewers retaining trace history.
    let mut stream_store = state.stream_store.clone();
    let entries = stream_store
        .read_entries_since(Signal::Traces, None, 100_000)
        .await
        .map_err(|error| {
            tracing::error!("service_map: read_entries_since failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let normalized: Vec<_> = entries.into_iter().map(|(_, entry)| entry).collect();
    let map = build_service_map(&normalized, now, lookback_ms);

    Ok(Json(map))
}

#[derive(Debug, Deserialize)]
struct ErrorGroupQuery {
    #[serde(default)]
    lookback_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ErrorGroupsResponse {
    error_groups: Vec<ErrorGroup>,
}

const DEFAULT_ERROR_GROUP_LOOKBACK_MS: i64 = 60 * 60 * 1_000;

async fn list_error_groups(
    State(state): State<AppState>,
    Query(query): Query<ErrorGroupQuery>,
) -> Result<Json<ErrorGroupsResponse>, StatusCode> {
    let store = state
        .error_group_store
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let lookback_ms = query
        .lookback_ms
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_ERROR_GROUP_LOOKBACK_MS);
    let since = Utc::now() - chrono::Duration::milliseconds(lookback_ms);
    let error_groups = store.list_recent(since).await.map_err(|error| {
        tracing::error!("error_group_store list_recent failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(ErrorGroupsResponse { error_groups }))
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
                &viewer_state.aggregated_buckets,
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

/// Applies (or clears) the `aggregation` block on `definition_json`.
/// `None` leaves any existing block untouched.
fn apply_aggregation_to_definition(
    definition_json: &mut serde_json::Value,
    aggregation: Option<&AggregationInput>,
) {
    let Some(agg) = aggregation else { return };
    definition_json["aggregation"] = json!({
        "fn": agg.func,
        "bucket_ms": agg.bucket_ms,
        "group_by": agg.group_by,
    });
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
    apply_aggregation_to_definition(&mut definition_json, payload.aggregation.as_ref());
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
    let (definition_json, layout_json, matcher_changed, aggregation_changed, signal_mask) = {
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
        apply_aggregation_to_definition(&mut definition_json, payload.aggregation.as_ref());
        let query_changed = query_from_definition(&definition_json) != current_query;
        let filters_changed = definition_json.get("filters") != old_def_json.get("filters")
            || definition_json.get("filter_mode") != old_def_json.get("filter_mode");
        let aggregation_changed =
            definition_json.get("aggregation") != old_def_json.get("aggregation");
        let matcher_changed = query_changed || filters_changed;
        if new_chart_type == current_kind && !matcher_changed && !aggregation_changed {
            return Ok(StatusCode::OK);
        }
        let layout_json = viewer.definition().layout_json.clone();
        let signal_mask = viewer.definition().signal_mask;
        (
            definition_json,
            layout_json,
            matcher_changed,
            aggregation_changed,
            signal_mask,
        )
    };

    if matcher_changed || aggregation_changed {
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

// --- Ad-hoc Query API -------------------------------------------------------

/// Default lookback window for ad-hoc queries (5 minutes).
const DEFAULT_QUERY_LOOKBACK_MS: i64 = 5 * 60 * 1_000;
/// Maximum lookback window for ad-hoc queries (24 hours).
const MAX_QUERY_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1_000;
/// Maximum number of entries scanned per signal for ad-hoc queries.
const MAX_QUERY_SCAN_ENTRIES: usize = 100_000;

#[derive(Debug, Deserialize)]
struct QueryRequest {
    sql: String,
    #[serde(default)]
    lookback_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    scanned: usize,
    matched: usize,
    returned: usize,
    truncated: bool,
}

#[derive(Debug, Serialize)]
struct QueryErrorBody {
    error: String,
}

fn cell_to_json(cell: QueryCell) -> serde_json::Value {
    match cell {
        QueryCell::Null => serde_json::Value::Null,
        QueryCell::Number(n) => serde_json::Number::from_f64(n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        QueryCell::String(s) => serde_json::Value::String(s),
    }
}

async fn run_query(
    State(mut state): State<AppState>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<QueryErrorBody>)> {
    let sql = payload.sql.trim();
    if sql.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(QueryErrorBody {
                error: "sql must not be empty".to_string(),
            }),
        ));
    }

    let stmt = parse_query(sql).map_err(|e| {
        tracing::debug!("run_query parse error: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(QueryErrorBody {
                error: format!("parse error: {e}"),
            }),
        )
    })?;

    let lookback_ms = match payload.lookback_ms {
        Some(v) if v > 0 => v.min(MAX_QUERY_LOOKBACK_MS),
        Some(_) | None => DEFAULT_QUERY_LOOKBACK_MS,
    };
    let signal = source_to_signal(stmt.from);
    let cutoff = Utc::now() - chrono::Duration::milliseconds(lookback_ms);

    let entries = state
        .stream_store
        .read_entries_since(signal, None, MAX_QUERY_SCAN_ENTRIES)
        .await
        .map_err(|error| {
            tracing::error!("run_query: read_entries_since failed: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(QueryErrorBody {
                    error: "failed to read entries".to_string(),
                }),
            )
        })?;

    // Apply the lookback window before evaluation. Entries are appended in
    // ascending time order; using a linear filter is sufficient.
    let filtered = entries
        .into_iter()
        .filter_map(|(_, entry)| (entry.observed_at > cutoff).then_some(entry));

    let result = execute_query(&stmt, filtered).map_err(|e| {
        tracing::debug!("run_query execute error: {e}");
        (
            StatusCode::BAD_REQUEST,
            Json(QueryErrorBody {
                error: format!("execute error: {e}"),
            }),
        )
    })?;

    let rows: Vec<Vec<serde_json::Value>> = result
        .rows
        .into_iter()
        .map(|row| row.into_iter().map(cell_to_json).collect())
        .collect();

    Ok(Json(QueryResponse {
        columns: result.columns,
        rows,
        scanned: result.scanned,
        matched: result.matched,
        returned: result.returned,
        truncated: result.truncated,
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
                .map(|slice| {
                    let filtered: Vec<NormalizedEntry>;
                    let effective_entries: &[NormalizedEntry] =
                        if global_service_names.is_empty() && global_query.is_none() {
                            slice.entries
                        } else {
                            filtered = slice
                                .entries
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
                    viewer_summary(
                        slice.viewer,
                        effective_entries,
                        slice.status,
                        slice.effective_lookback_ms,
                        true,
                        slice.aggregated_buckets,
                    )
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

// --- APM: Trace Waterfall ---------------------------------------------------

/// Builds a trace waterfall by reading recent trace entries directly from the
/// stream store. Independent of the viewer runtime so it works even when no
/// trace viewer has been created.
async fn get_trace_waterfall(
    State(mut state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<TraceWaterfall>, StatusCode> {
    let trace_id = trace_id.trim();
    if trace_id.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let entries = state
        .stream_store
        .read_entries_since(Signal::Traces, None, 100_000)
        .await
        .map_err(|error| {
            tracing::error!("get_trace_waterfall: read_entries_since failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let entries: Vec<NormalizedEntry> = entries.into_iter().map(|(_, entry)| entry).collect();

    match build_waterfall(&entries, trace_id) {
        Ok(waterfall) => Ok(Json(waterfall)),
        Err(WaterfallError::TraceNotFound(_)) => Err(StatusCode::NOT_FOUND),
    }
}

// --- Alerts API -------------------------------------------------------------

#[derive(Debug, Serialize)]
struct AlertListItem {
    id: Uuid,
    name: String,
    viewer_id: Uuid,
    condition: serde_json::Value,
    severity: String,
    evaluation_interval_ms: i32,
    enabled: bool,
    revision: i64,
}

#[derive(Debug, Serialize)]
struct AlertListResponse {
    alerts: Vec<AlertListItem>,
}

#[derive(Debug, Deserialize)]
struct CreateAlertRequest {
    name: String,
    viewer_id: Uuid,
    condition: serde_json::Value,
    severity: String,
    evaluation_interval_ms: i32,
    #[serde(default = "default_true")]
    enabled: bool,
}

#[derive(Debug, Serialize)]
struct CreateAlertResponse {
    id: Uuid,
}

#[derive(Debug, Deserialize)]
struct PatchAlertRequest {
    name: Option<String>,
    viewer_id: Option<Uuid>,
    condition: Option<serde_json::Value>,
    severity: Option<String>,
    evaluation_interval_ms: Option<i32>,
    enabled: Option<bool>,
}

const MIN_ALERT_EVAL_INTERVAL_MS: i32 = 1_000;
const MAX_ALERT_NAME_CHARS: usize = 80;

fn alert_to_item(def: &crate::domain::alert::AlertDefinition) -> AlertListItem {
    let condition = serde_json::to_value(&def.condition)
        .expect("AlertCondition serialization should never fail");
    AlertListItem {
        id: def.id,
        name: def.name.clone(),
        viewer_id: def.viewer_id,
        condition,
        severity: def.severity.as_str().to_string(),
        evaluation_interval_ms: def.evaluation_interval_ms,
        enabled: def.enabled,
        revision: def.revision,
    }
}

fn validate_alert_name(name: &str) -> Result<String, StatusCode> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.chars().count() > MAX_ALERT_NAME_CHARS {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(trimmed.to_string())
}

fn validate_alert_interval(value: i32) -> Result<i32, StatusCode> {
    if value < MIN_ALERT_EVAL_INTERVAL_MS {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(value)
}

fn parse_severity(value: &str) -> Result<crate::domain::alert::AlertSeverity, StatusCode> {
    crate::domain::alert::AlertSeverity::parse(value.trim()).ok_or(StatusCode::BAD_REQUEST)
}

fn parse_condition(
    value: &serde_json::Value,
) -> Result<crate::domain::alert::AlertCondition, StatusCode> {
    serde_json::from_value(value.clone()).map_err(|e| {
        tracing::warn!("invalid alert condition: {e}");
        StatusCode::BAD_REQUEST
    })
}

async fn list_alerts(State(state): State<AppState>) -> Result<Json<AlertListResponse>, StatusCode> {
    let store = state.require_alert_store()?;
    let defs = store.load_all().await.map_err(|e| {
        tracing::error!("list_alerts failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let alerts = defs.iter().map(alert_to_item).collect();
    Ok(Json(AlertListResponse { alerts }))
}

async fn get_alert(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<AlertListItem>, StatusCode> {
    let store = state.require_alert_store()?;
    let def = store
        .load(id)
        .await
        .map_err(|e| {
            tracing::error!("get_alert failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(alert_to_item(&def)))
}

async fn create_alert(
    State(state): State<AppState>,
    Json(payload): Json<CreateAlertRequest>,
) -> Result<(StatusCode, Json<CreateAlertResponse>), StatusCode> {
    let store = state.require_alert_store()?;
    let viewer_store = state.require_viewer_store()?;

    let name = validate_alert_name(&payload.name)?;
    let interval = validate_alert_interval(payload.evaluation_interval_ms)?;
    let severity = parse_severity(&payload.severity)?;
    let condition = parse_condition(&payload.condition)?;

    // Verify referenced viewer exists.
    let viewers = viewer_store.load_viewer_definitions().await.map_err(|e| {
        tracing::error!("create_alert: load_viewer_definitions failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if !viewers.iter().any(|v| v.id == payload.viewer_id) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let id = Uuid::new_v4();
    let def = crate::domain::alert::AlertDefinition {
        id,
        name,
        viewer_id: payload.viewer_id,
        condition,
        severity,
        evaluation_interval_ms: interval,
        enabled: payload.enabled,
        revision: 0,
    };
    store.insert(&def).await.map_err(|e| {
        tracing::error!("create_alert: insert failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok((StatusCode::CREATED, Json(CreateAlertResponse { id })))
}

async fn patch_alert(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<PatchAlertRequest>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_alert_store()?;
    let mut def = store
        .load(id)
        .await
        .map_err(|e| {
            tracing::error!("patch_alert: load failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    if let Some(name) = payload.name.as_deref() {
        def.name = validate_alert_name(name)?;
    }
    if let Some(viewer_id) = payload.viewer_id {
        def.viewer_id = viewer_id;
    }
    if let Some(condition) = payload.condition.as_ref() {
        def.condition = parse_condition(condition)?;
    }
    if let Some(severity) = payload.severity.as_deref() {
        def.severity = parse_severity(severity)?;
    }
    if let Some(interval) = payload.evaluation_interval_ms {
        def.evaluation_interval_ms = validate_alert_interval(interval)?;
    }
    if let Some(enabled) = payload.enabled {
        def.enabled = enabled;
    }

    let updated = store.update(&def).await.map_err(|e| {
        tracing::error!("patch_alert: update failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if !updated {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(StatusCode::OK)
}

async fn delete_alert(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_alert_store()?;
    let deleted = store.delete(id).await.map_err(|e| {
        tracing::error!("delete_alert failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if !deleted {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(StatusCode::NO_CONTENT)
}

// --- Notification channels --------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateNotificationChannelRequest {
    name: String,
    kind: String,
    config: serde_json::Value,
    #[serde(default = "default_true")]
    enabled: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize)]
struct NotificationChannelSummary {
    id: Uuid,
    name: String,
    kind: String,
    config: serde_json::Value,
    enabled: bool,
}

impl From<NotificationChannel> for NotificationChannelSummary {
    fn from(ch: NotificationChannel) -> Self {
        Self {
            id: ch.id,
            name: ch.name,
            kind: ch.kind,
            config: ch.config_json,
            enabled: ch.enabled,
        }
    }
}

#[derive(Debug, Serialize)]
struct NotificationChannelListResponse {
    channels: Vec<NotificationChannelSummary>,
}

#[derive(Debug, Serialize)]
struct CreateNotificationChannelResponse {
    id: Uuid,
}

#[derive(Debug, Serialize)]
struct TestNotificationChannelResponse {
    delivered: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<String>,
}

const VALID_NOTIFICATION_KINDS: &[&str] = &["webhook"];

async fn list_notification_channels(
    State(state): State<AppState>,
) -> Result<Json<NotificationChannelListResponse>, StatusCode> {
    let store = state.require_viewer_store()?;
    let channels = store.list_notification_channels().await.map_err(|e| {
        tracing::error!("list_notification_channels: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(NotificationChannelListResponse {
        channels: channels.into_iter().map(Into::into).collect(),
    }))
}

async fn create_notification_channel(
    State(state): State<AppState>,
    Json(payload): Json<CreateNotificationChannelRequest>,
) -> Result<(StatusCode, Json<CreateNotificationChannelResponse>), StatusCode> {
    let store = state.require_viewer_store()?;

    let name = payload.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let kind = payload.kind.trim();
    if !VALID_NOTIFICATION_KINDS.contains(&kind) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let channel = NotificationChannel {
        id: Uuid::new_v4(),
        name: name.to_string(),
        kind: kind.to_string(),
        config_json: payload.config,
        enabled: payload.enabled,
    };

    // Validate kind-specific config fields up front to fail fast on bad input.
    if kind == "webhook"
        && let Err(e) = WebhookConfig::from_channel(&channel)
    {
        tracing::warn!("create_notification_channel: invalid webhook config: {e}");
        return Err(StatusCode::BAD_REQUEST);
    }

    store
        .insert_notification_channel(&channel)
        .await
        .map_err(|e| {
            tracing::error!("insert_notification_channel: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok((
        StatusCode::CREATED,
        Json(CreateNotificationChannelResponse { id: channel.id }),
    ))
}

async fn delete_notification_channel(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_viewer_store()?;
    let removed = store.delete_notification_channel(id).await.map_err(|e| {
        tracing::error!("delete_notification_channel: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn test_notification_channel(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<TestNotificationChannelResponse>), StatusCode> {
    let store = state.require_viewer_store()?;
    let channel = store
        .load_notification_channel(id)
        .await
        .map_err(|e| {
            tracing::error!("load_notification_channel: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let payload = NotificationPayload::test_message(&channel.name);
    match dispatch_to_channel(&channel, &payload).await {
        Ok(()) => Ok((
            StatusCode::OK,
            Json(TestNotificationChannelResponse {
                delivered: true,
                detail: None,
            }),
        )),
        Err(e) => {
            tracing::warn!("test_notification_channel {id}: {e}");
            Ok((
                StatusCode::BAD_GATEWAY,
                Json(TestNotificationChannelResponse {
                    delivered: false,
                    detail: Some(e.to_string()),
                }),
            ))
        }
    }
}

// --- APM: slow queries / N+1 ------------------------------------------------

use crate::apm::slow_query::{
    NPlusOneStats, SlowQueryStats, aggregate_slow_queries, detect_n_plus_one, extract_db_spans,
};

const DEFAULT_APM_LOOKBACK_MS: i64 = 5 * 60 * 1_000;
const DEFAULT_SLOW_QUERY_MIN_P95_MS: u64 = 100;
const DEFAULT_N_PLUS_ONE_MIN_REPETITIONS: usize = 5;

#[derive(Debug, Deserialize)]
struct SlowQueriesQuery {
    #[serde(default)]
    lookback_ms: Option<i64>,
    #[serde(default)]
    min_p95_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct NPlusOneQuery {
    #[serde(default)]
    lookback_ms: Option<i64>,
    #[serde(default)]
    min_repetitions: Option<usize>,
}

/// Collects every traces [`NormalizedEntry`] currently held by the runtime,
/// deduplicating across viewers and trimming to the requested lookback window.
///
/// The viewer runtime fans the same entries out to every traces-watching
/// viewer, so we dedupe by `Bytes` pointer + observed_at.
async fn collect_traces_entries(
    runtime: &SharedViewerRuntime,
    lookback_ms: i64,
) -> Vec<NormalizedEntry> {
    let cutoff = Utc::now() - chrono::Duration::milliseconds(lookback_ms);
    let runtime = runtime.lock().await;

    let mut seen: HashSet<(*const u8, usize, i64)> = HashSet::new();
    let mut out: Vec<NormalizedEntry> = Vec::new();

    for (_, state) in runtime.viewers() {
        for entry in &state.entries {
            if entry.signal != Signal::Traces || entry.observed_at < cutoff {
                continue;
            }
            let key = (
                entry.payload.as_ptr(),
                entry.payload.len(),
                entry.observed_at.timestamp_nanos_opt().unwrap_or(0),
            );
            if seen.insert(key) {
                out.push(entry.clone());
            }
        }
    }

    out
}

async fn list_slow_queries(
    State(state): State<AppState>,
    Query(query): Query<SlowQueriesQuery>,
) -> Result<Json<Vec<SlowQueryStats>>, StatusCode> {
    let runtime = state.require_viewer_runtime()?;
    let lookback_ms = query.lookback_ms.unwrap_or(DEFAULT_APM_LOOKBACK_MS).max(0);
    let min_p95_ms = query.min_p95_ms.unwrap_or(DEFAULT_SLOW_QUERY_MIN_P95_MS);

    let entries = collect_traces_entries(runtime, lookback_ms).await;
    let facts = extract_db_spans(&entries);
    let stats = aggregate_slow_queries(&facts, min_p95_ms);
    Ok(Json(stats))
}

async fn list_n_plus_one(
    State(state): State<AppState>,
    Query(query): Query<NPlusOneQuery>,
) -> Result<Json<Vec<NPlusOneStats>>, StatusCode> {
    let runtime = state.require_viewer_runtime()?;
    let lookback_ms = query.lookback_ms.unwrap_or(DEFAULT_APM_LOOKBACK_MS).max(0);
    let min_repetitions = query
        .min_repetitions
        .unwrap_or(DEFAULT_N_PLUS_ONE_MIN_REPETITIONS)
        .max(1);

    let entries = collect_traces_entries(runtime, lookback_ms).await;
    let facts = extract_db_spans(&entries);
    Ok(Json(detect_n_plus_one(&facts, min_repetitions)))
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
    let content_encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok());

    let encoding = match parse_content_encoding(content_encoding) {
        Ok(encoding) => encoding,
        Err(DecodeError::UnsupportedContentEncoding(value)) => {
            tracing::warn!("unsupported Content-Encoding: {value}");
            return StatusCode::UNSUPPORTED_MEDIA_TYPE;
        }
        Err(other) => {
            tracing::error!("parse_content_encoding: {other}");
            return StatusCode::BAD_REQUEST;
        }
    };

    let body = match decompress_body(encoding, body) {
        Ok(decompressed) => decompressed,
        Err(error) => {
            tracing::warn!("decompress_body: {error}");
            return StatusCode::BAD_REQUEST;
        }
    };

    match parse_ingest_request(signal, content_type, body.clone()) {
        Ok(entry) => {
            let attr_index = state.attr_index.clone();
            let mut stream_store = state.stream_store;
            match stream_store.append_entry(&entry).await {
                Ok(stream_id) => {
                    if let Some(rollup) = state.rollup_store.as_ref()
                        && let Err(error) = rollup.record_entry(&entry).await
                    {
                        // Rollups are best-effort; ingest must not fail because of them.
                        tracing::warn!("rollup record_entry failed: {error}");
                    }
                    if let Some(attr_index) = attr_index {
                        index_entry_attributes(
                            &attr_index,
                            signal,
                            content_type,
                            &body,
                            &stream_id,
                            entry.observed_at,
                        )
                        .await;
                    }
                    record_error_groups_from_entry(state.error_group_store.as_ref(), &entry).await;
                    StatusCode::OK
                }
                Err(error) => {
                    tracing::error!("stream append_entry failed: {error}");
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            }
        }
        Err(DecodeError::UnsupportedContentType(_)) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
        Err(other) => {
            tracing::error!("parse_ingest_request: {other}");
            StatusCode::BAD_REQUEST
        }
    }
}

// --- Rollups ----------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct RollupQueryParams {
    signal: String,
    #[serde(default)]
    resolution: Option<String>,
    from: i64,
    to: i64,
}

#[derive(Debug, Serialize)]
struct RollupBucketRow {
    bucket_start_ms: i64,
    count: u64,
    by_service: std::collections::BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
struct RollupResponse {
    signal: &'static str,
    resolution: &'static str,
    from_ms: i64,
    to_ms: i64,
    buckets: Vec<RollupBucketRow>,
}

const DEFAULT_ROLLUP_RESOLUTION: &str = "1m";

async fn get_rollups(
    State(state): State<AppState>,
    Query(params): Query<RollupQueryParams>,
) -> Result<Json<RollupResponse>, StatusCode> {
    let signal = parse_signal_name(params.signal.trim()).ok_or(StatusCode::BAD_REQUEST)?;
    let resolution_str = params
        .resolution
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_ROLLUP_RESOLUTION);
    let resolution = Resolution::parse(resolution_str).ok_or(StatusCode::BAD_REQUEST)?;

    if params.from > params.to {
        return Err(StatusCode::BAD_REQUEST);
    }

    let store = state
        .rollup_store
        .as_ref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let buckets = store
        .fetch_range(signal, resolution, params.from, params.to)
        .await
        .map_err(|error| {
            tracing::error!("rollup fetch_range failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let buckets = buckets
        .into_iter()
        .map(|bucket| RollupBucketRow {
            bucket_start_ms: bucket.bucket_start_seconds.saturating_mul(1_000),
            count: bucket.count,
            by_service: bucket.by_service,
        })
        .collect();

    Ok(Json(RollupResponse {
        signal: signal_name(signal),
        resolution: resolution.as_str(),
        from_ms: params.from,
        to_ms: params.to,
        buckets,
    }))
}

/// Decode the OTLP payload again and write its (key, value) attribute pairs
/// to the inverted index. Failures here are logged but do not fail the
/// ingest, since the entry is already durably stored.
async fn index_entry_attributes(
    attr_index: &crate::storage::attr_index::AttrIndexStore,
    signal: Signal,
    content_type: Option<&str>,
    body: &Bytes,
    stream_id: &str,
    observed_at: chrono::DateTime<chrono::Utc>,
) {
    use crate::ingest::otlp_http::{decode_payload_value, extract_indexable_attributes_from_value};

    let Some(value) = decode_payload_value(signal, content_type, body) else {
        return;
    };
    let pairs = extract_indexable_attributes_from_value(signal, &value);
    if pairs.is_empty() {
        return;
    }
    if let Err(error) = attr_index
        .record_attributes(signal, stream_id, observed_at, pairs)
        .await
    {
        tracing::warn!("attr_index record_attributes failed: {error}");
    }
}

/// Extract error occurrences from `entry`, fold them by fingerprint, and upsert into the store.
///
/// Best-effort: errors during persistence are logged but do not fail ingest.
pub(crate) async fn record_error_groups_from_entry(
    store: Option<&ErrorGroupStore>,
    entry: &NormalizedEntry,
) {
    let Some(store) = store else { return };
    let occurrences = extract_error_occurrences(entry);
    if occurrences.is_empty() {
        return;
    }
    let mut aggregator = ErrorGroupAggregator::new();
    for occ in occurrences {
        aggregator.record(occ);
    }
    for group in aggregator.into_groups() {
        if let Err(e) = store.upsert(&group).await {
            tracing::warn!(
                "error_group upsert failed (fingerprint={}): {e}",
                group.fingerprint
            );
        }
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
    aggregated_buckets: &[crate::viewer_runtime::aggregator::Bucket],
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
    let aggregation = definition.definition_json.get("aggregation").cloned();

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
        aggregation,
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
        aggregated_buckets: aggregated_buckets.to_vec(),
        // populated by callers that have access to trace entries from other viewers
        exemplars: Vec::new(),
    })
}

fn signal_name(signal: Signal) -> &'static str {
    signal.as_str()
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
    let value = payload_as_value(Signal::Metrics, payload)?;
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
    let value = payload_as_value(Signal::Logs, payload)?;
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
    let value = payload_as_value(Signal::Traces, payload)?;
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

// --- APM trace search / lookup ---------------------------------------------

const TRACE_SEARCH_READ_LIMIT: usize = 100_000;

#[derive(Debug, Deserialize, Default)]
struct TraceLookupQuery {
    trace_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TraceSearchQuery {
    service: Option<String>,
    min_duration_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TraceSearchResponse {
    traces: Vec<crate::apm::trace_search::TraceListItem>,
}

async fn read_all_trace_entries(state: &AppState) -> Result<Vec<NormalizedEntry>, StatusCode> {
    let mut stream_store = state.stream_store.clone();
    let entries = stream_store
        .read_entries_since(Signal::Traces, None, TRACE_SEARCH_READ_LIMIT)
        .await
        .map_err(|error| {
            tracing::error!("trace search: read_entries_since failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(entries.into_iter().map(|(_, entry)| entry).collect())
}

async fn get_trace_by_id(
    State(state): State<AppState>,
    Query(params): Query<TraceLookupQuery>,
) -> Result<Json<crate::apm::trace_search::TraceDetail>, StatusCode> {
    let trace_id = params
        .trace_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or(StatusCode::BAD_REQUEST)?
        .to_string();

    let entries = read_all_trace_entries(&state).await?;
    let detail =
        crate::apm::trace_search::lookup_trace(&entries, &trace_id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(detail))
}

async fn search_traces_handler(
    State(state): State<AppState>,
    Query(params): Query<TraceSearchQuery>,
) -> Result<Json<TraceSearchResponse>, StatusCode> {
    let filter = crate::apm::trace_search::TraceSearchFilter {
        service: params
            .service
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        min_duration_ms: params.min_duration_ms,
    };

    let entries = read_all_trace_entries(&state).await?;
    let traces = crate::apm::trace_search::search_traces(&entries, &filter);
    Ok(Json(TraceSearchResponse { traces }))
}

fn extract_traces_from_entries(
    entries: &[crate::domain::telemetry::NormalizedEntry],
) -> Vec<TraceSummary> {
    let mut spans_by_trace: HashMap<String, Vec<SpanRow>> = HashMap::new();

    for entry in entries {
        if entry.signal != Signal::Traces {
            continue;
        }
        let Some(value) = payload_as_value(Signal::Traces, &entry.payload) else {
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

// --- Incident API ----------------------------------------------------------

const MAX_INCIDENT_SEVERITY_LEN: usize = 32;

#[derive(Debug, Deserialize)]
struct IncidentListQuery {
    status: Option<String>,
}

#[derive(Debug, Serialize)]
struct IncidentListResponse {
    incidents: Vec<Incident>,
}

#[derive(Debug, Deserialize)]
struct CreateIncidentRequest {
    #[serde(default)]
    alert_id: Option<Uuid>,
    severity: String,
    #[serde(default)]
    details_json: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct PatchIncidentRequest {
    status: String,
}

async fn list_incidents(
    State(state): State<AppState>,
    Query(query): Query<IncidentListQuery>,
) -> Result<Json<IncidentListResponse>, StatusCode> {
    let status_filter = match query.status.as_deref() {
        None | Some("") => None,
        Some(s) => Some(IncidentStatus::parse(s).ok_or(StatusCode::BAD_REQUEST)?),
    };
    let incidents = state
        .incident_store
        .list(status_filter)
        .await
        .map_err(|error| {
            tracing::error!("list_incidents failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(IncidentListResponse { incidents }))
}

async fn get_incident(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Incident>, StatusCode> {
    let incident = state
        .incident_store
        .get(id)
        .await
        .map_err(|error| {
            tracing::error!("get_incident failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(incident))
}

async fn create_incident(
    State(state): State<AppState>,
    Json(payload): Json<CreateIncidentRequest>,
) -> Result<(StatusCode, Json<Incident>), StatusCode> {
    let severity = payload.severity.trim().to_string();
    if severity.is_empty() || severity.chars().count() > MAX_INCIDENT_SEVERITY_LEN {
        return Err(StatusCode::BAD_REQUEST);
    }
    let details_json = payload
        .details_json
        .unwrap_or_else(|| serde_json::json!({}));
    let incident = Incident {
        id: Uuid::new_v4(),
        alert_id: payload.alert_id,
        status: IncidentStatus::Open,
        severity,
        opened_at: Utc::now(),
        acknowledged_at: None,
        resolved_at: None,
        details_json,
    };

    state
        .incident_store
        .insert(&incident)
        .await
        .map_err(|error| {
            tracing::error!("create_incident: insert failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok((StatusCode::CREATED, Json(incident)))
}

async fn patch_incident(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<PatchIncidentRequest>,
) -> Result<Json<Incident>, StatusCode> {
    let target = IncidentStatus::parse(payload.status.trim()).ok_or(StatusCode::BAD_REQUEST)?;

    let current = state
        .incident_store
        .get(id)
        .await
        .map_err(|error| {
            tracing::error!("patch_incident: get failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    if let Err(err) = validate_transition(current.status, target) {
        tracing::warn!("incident {id}: {err}");
        return Err(StatusCode::BAD_REQUEST);
    }

    let updated = state
        .incident_store
        .update_status(id, target, Utc::now())
        .await
        .map_err(|error| {
            tracing::error!("patch_incident: update failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(updated))
}

// --- SLO API ----------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CreateSloRequest {
    name: String,
    #[serde(default)]
    viewer_id: Option<Uuid>,
    target_pct: f64,
    window_ms: i64,
    #[serde(default)]
    success_filters: Vec<SloFilterClause>,
    #[serde(default)]
    total_filters: Vec<SloFilterClause>,
}

#[derive(Debug, Serialize)]
struct CreateSloResponse {
    id: Uuid,
}

#[derive(Debug, Serialize)]
struct SloSummary {
    id: Uuid,
    name: String,
    viewer_id: Option<Uuid>,
    target_pct: f64,
    window_ms: i64,
    success_filters: Vec<SloFilterClause>,
    total_filters: Vec<SloFilterClause>,
    enabled: bool,
}

impl From<SloDefinition> for SloSummary {
    fn from(def: SloDefinition) -> Self {
        Self {
            id: def.id,
            name: def.name,
            viewer_id: def.viewer_id,
            target_pct: def.target_pct,
            window_ms: def.window_ms,
            success_filters: def.success_filter.filters,
            total_filters: def.total_filter.filters,
            enabled: def.enabled,
        }
    }
}

#[derive(Debug, Serialize)]
struct SloListResponse {
    slos: Vec<SloSummary>,
}

async fn list_slos(State(state): State<AppState>) -> Result<Json<SloListResponse>, StatusCode> {
    let store = state.require_slo_store()?;
    let slos = store.list().await.map_err(|e| {
        tracing::error!("list_slos failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(SloListResponse {
        slos: slos.into_iter().map(SloSummary::from).collect(),
    }))
}

async fn create_slo(
    State(state): State<AppState>,
    Json(payload): Json<CreateSloRequest>,
) -> Result<(StatusCode, Json<CreateSloResponse>), StatusCode> {
    let store = state.require_slo_store()?;

    let name = payload.name.trim().to_string();
    if name.is_empty() || name.chars().count() > 120 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let definition = SloDefinition {
        id: Uuid::new_v4(),
        name,
        viewer_id: payload.viewer_id,
        target_pct: payload.target_pct,
        window_ms: payload.window_ms,
        success_filter: SloFilterList {
            filters: payload.success_filters,
        },
        total_filter: SloFilterList {
            filters: payload.total_filters,
        },
        enabled: true,
    };

    // Validate target/window/filter syntax up front so bad payloads get 400.
    CompiledSlo::compile(&definition).map_err(|e| {
        tracing::warn!("create_slo: compile failed: {e}");
        StatusCode::BAD_REQUEST
    })?;

    store.insert(&definition).await.map_err(|e| {
        tracing::error!("create_slo: insert failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok((
        StatusCode::CREATED,
        Json(CreateSloResponse { id: definition.id }),
    ))
}

async fn get_slo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SloSummary>, StatusCode> {
    let store = state.require_slo_store()?;
    let slo = store
        .get(id)
        .await
        .map_err(|e| {
            tracing::error!("get_slo failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(SloSummary::from(slo)))
}

async fn delete_slo(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_slo_store()?;
    let removed = store.delete(id).await.map_err(|e| {
        tracing::error!("delete_slo failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn get_slo_budget(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<ErrorBudget>, StatusCode> {
    let store = state.require_slo_store()?;
    let slo = store
        .get(id)
        .await
        .map_err(|e| {
            tracing::error!("get_slo_budget: storage error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    // A persisted SLO that no longer compiles is a server-side data corruption
    // bug, not a client error -- return 500 for any compile failure here.
    let compiled = CompiledSlo::compile(&slo).map_err(|e| {
        tracing::error!("get_slo_budget: compile failed: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let now = Utc::now();
    let entries = collect_slo_entries(&state, slo.viewer_id).await?;
    let budget = compiled.evaluate(&entries, now);
    Ok(Json(budget))
}

/// Collects entries from the viewer runtime to evaluate the SLO against.
///
/// When `viewer_id` is provided, uses that viewer's in-memory entries;
/// when None, returns an empty list (a not-yet-bound SLO has nothing to score).
async fn collect_slo_entries(
    state: &AppState,
    viewer_id: Option<Uuid>,
) -> Result<Vec<NormalizedEntry>, StatusCode> {
    let Some(viewer_id) = viewer_id else {
        return Ok(Vec::new());
    };
    let runtime = state.require_viewer_runtime()?.lock().await;
    let Some((_, viewer_state)) = runtime
        .viewers()
        .iter()
        .find(|(viewer, _)| viewer.definition().id == viewer_id)
    else {
        // Bound viewer was deleted: treat as no entries (budget = 100% / 0 total).
        return Ok(Vec::new());
    };
    Ok(viewer_state.entries.clone())
}

// --- API key management handlers ---

async fn create_api_key(
    State(state): State<AppState>,
    Json(payload): Json<CreateApiKeyRequest>,
) -> Result<Response, StatusCode> {
    let store = state.require_api_key_store()?;

    let name = payload.name.trim().to_string();
    if name.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let uuid1 = Uuid::new_v4().simple().to_string();
    let uuid2 = Uuid::new_v4().simple().to_string();
    let raw_key = format!("{API_KEY_PREFIX}{uuid1}{uuid2}");
    let key_hash = crate::domain::api_key::hash_api_key(&raw_key);
    let id = Uuid::new_v4();
    let now = Utc::now();

    store
        .insert(&ApiKey {
            id,
            name: name.clone(),
            key_hash,
            created_at: now,
        })
        .await
        .map_err(|error| {
            tracing::error!("api_key_store insert failed: {error}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut response = (
        StatusCode::CREATED,
        Json(CreateApiKeyResponse {
            id,
            name,
            key: raw_key,
            created_at: now,
        }),
    )
        .into_response();
    {
        let h = response.headers_mut();
        h.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-store, no-cache, must-revalidate, private"),
        );
        h.insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
        h.insert(header::EXPIRES, HeaderValue::from_static("0"));
    }

    Ok(response)
}

async fn list_api_keys(
    State(state): State<AppState>,
) -> Result<Json<ApiKeyListResponse>, StatusCode> {
    let store = state.require_api_key_store()?;

    let keys = store.list().await.map_err(|error| {
        tracing::error!("api_key_store list failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(ApiKeyListResponse {
        api_keys: keys
            .into_iter()
            .map(|k| ApiKeyListItem {
                id: k.id,
                name: k.name,
                created_at: k.created_at,
            })
            .collect(),
    }))
}

async fn delete_api_key(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, StatusCode> {
    let store = state.require_api_key_store()?;

    let deleted = store.delete(id).await.map_err(|error| {
        tracing::error!("api_key_store delete failed: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(StatusCode::NOT_FOUND)
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

    use crate::domain::api_key::hash_api_key;
    use crate::storage::api_key_store::MemoryApiKeyStore;
    use crate::storage::memory::MemoryStreamStore;
    use crate::storage::memory::MemoryViewerStore;

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

    fn make_auth_test_app(api_key_store: Option<ApiKeyStore>, api_keys_enabled: bool) -> Router {
        build_app_with_services_full(
            StreamStore::Memory(MemoryStreamStore::new(100)),
            Some(ViewerStore::Memory(MemoryViewerStore::new())),
            None, // viewer_runtime
            None, // rollup_store
            None, // alert_store
            None, // attr_index
            None, // incident_store (defaults to Memory)
            None, // slo_store
            None, // error_group_store
            api_key_store,
            api_keys_enabled,
        )
    }

    // --- API key auth middleware integration tests ---

    #[tokio::test]
    async fn auth_without_token_when_enabled_returns_401() {
        // Given: auth enabled with an empty api_key_store, no Authorization header
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), true);

        // When: requesting /api/api-keys without an Authorization header
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 401 Unauthorized
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_with_invalid_token_when_enabled_returns_401() {
        // Given: auth enabled, a key is in the store but we use a different token
        let store = MemoryApiKeyStore::new();
        let raw_key = "lt_valid_key_in_store";
        let key_hash_val = hash_api_key(raw_key);
        let key = ApiKey {
            id: Uuid::new_v4(),
            name: "test-key".to_string(),
            key_hash: key_hash_val,
            created_at: Utc::now(),
        };
        store.insert(&key).await.unwrap();
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(store)), true);

        // When: using a wrong token (not matching any hash)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .header("Authorization", "Bearer lt_wrong_token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 401 Unauthorized
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_with_valid_token_when_enabled_succeeds() {
        // Given: auth enabled, the store contains a key whose hash matches the token
        let store = MemoryApiKeyStore::new();
        let raw_key = "lt_my_valid_token_42";
        let key_hash_val = hash_api_key(raw_key);
        let key = ApiKey {
            id: Uuid::new_v4(),
            name: "test-key".to_string(),
            key_hash: key_hash_val,
            created_at: Utc::now(),
        };
        store.insert(&key).await.unwrap();
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(store)), true);

        // When: using the correct token to list api-keys
        let auth_header = format!("Bearer {}", raw_key);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .header("Authorization", auth_header.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 200 OK
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_when_disabled_bypasses_check() {
        // Given: auth disabled (api_keys_enabled=false), no token
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), false);

        // When: requesting /api/api-keys without Authorization header
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 200 OK (auth is bypassed, api_key_store responds)
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_protects_dashboards_endpoint() {
        // Given: auth enabled, no token
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), true);

        // When: requesting /api/dashboards
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/dashboards")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 401 Unauthorized
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_does_not_affect_healthz() {
        // Given: auth enabled, no token
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), true);

        // When: hitting /healthz (which is outside /api/* scope)
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 200 OK (healthz never requires auth)
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"ok");
    }

    #[tokio::test]
    async fn auth_does_not_affect_root_page() {
        // Given: auth enabled, no token
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), true);

        // When: hitting / (which is outside /api/* scope)
        let response = app
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        // Then: 200 OK (even with auth enabled)
        assert_eq!(response.status(), StatusCode::OK);
    }

    // --- API key management handler integration tests ---

    #[tokio::test]
    async fn create_api_key_returns_created() {
        // Given: auth disabled, empty store
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), false);

        // When: creating a new API key with valid name
        let payload = serde_json::json!({ "name": "test-key" });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/api-keys")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 201 Created with key info (raw key starts with lt_)
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "test-key");
        assert!(json["key"].as_str().unwrap().starts_with("lt_"));
        assert!(json["id"].as_str().is_some());
        assert!(json["created_at"].as_str().is_some());
    }

    #[tokio::test]
    async fn create_api_key_rejects_empty_name() {
        // Given: auth disabled
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), false);

        // When: creating a key with whitespace-only name
        let payload = serde_json::json!({ "name": "   " });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/api-keys")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 400 Bad Request
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_api_key_requires_auth_when_enabled() {
        // Given: auth enabled, store with an existing key (so bootstrap is not triggered), no auth header
        let store = MemoryApiKeyStore::new();
        store
            .insert(&ApiKey {
                id: Uuid::new_v4(),
                name: "existing".to_string(),
                key_hash: "abc123".to_string(),
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(store)), true);

        // When: trying to create a key without authentication
        let payload = serde_json::json!({ "name": "should-fail" });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/api-keys")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 401 Unauthorized
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn list_api_keys_returns_empty_list() {
        // Given: auth disabled, empty store
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), false);

        // When: listing keys
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 200 OK with empty array
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["api_keys"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_api_keys_includes_created_key_without_hash() {
        // Given: a key exists in the store, auth disabled
        let store = MemoryApiKeyStore::new();
        let key = ApiKey {
            id: Uuid::new_v4(),
            name: "my-key".to_string(),
            key_hash: "abc123".to_string(),
            created_at: Utc::now(),
        };
        store.insert(&key).await.unwrap();
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(store)), false);

        // When: listing keys
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: key appears in list but key_hash is NOT exposed
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let keys = json["api_keys"].as_array().unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0]["name"], "my-key");
        assert!(keys[0].get("key_hash").is_none());
        assert!(keys[0].get("key").is_none());
    }

    #[tokio::test]
    async fn delete_existing_api_key_returns_no_content() {
        // Given: a key exists in the store, auth disabled
        let store = MemoryApiKeyStore::new();
        let key = ApiKey {
            id: Uuid::new_v4(),
            name: "delete-me".to_string(),
            key_hash: "del-hash".to_string(),
            created_at: Utc::now(),
        };
        store.insert(&key).await.unwrap();
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(store)), false);

        // When: deleting the key
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/api-keys/{}", key.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 204 No Content
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn delete_nonexistent_api_key_returns_not_found() {
        // Given: empty store, auth disabled
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(MemoryApiKeyStore::new())), false);

        // When: deleting a random UUID
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/api-keys/{}", Uuid::new_v4()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 404 Not Found
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_api_key_requires_auth_when_enabled() {
        // Given: auth enabled, a key exists
        let store = MemoryApiKeyStore::new();
        let key = ApiKey {
            id: Uuid::new_v4(),
            name: "protected".to_string(),
            key_hash: "prot-hash".to_string(),
            created_at: Utc::now(),
        };
        store.insert(&key).await.unwrap();
        let app = make_auth_test_app(Some(ApiKeyStore::Memory(store)), true);

        // When: trying to delete without authentication
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/api-keys/{}", key.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 401 Unauthorized
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    // --- End-to-end: create key then use it for auth ---

    #[tokio::test]
    async fn created_api_key_can_be_used_for_authenticated_requests() {
        // Given: auth disabled (for initial key creation)
        let store = MemoryApiKeyStore::new();
        let store_for_enabled = store.clone();
        let app_disabled = make_auth_test_app(Some(ApiKeyStore::Memory(store)), false);

        // When: creating a key via the API
        let payload = serde_json::json!({ "name": "e2e-key" });
        let response = app_disabled
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/api-keys")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let raw_key = json["key"].as_str().unwrap().to_string();

        // Then: build a new app with auth enabled (same underlying store)
        let app_enabled = make_auth_test_app(Some(ApiKeyStore::Memory(store_for_enabled)), true);

        // When: using the created key to authenticate
        let auth_header = format!("Bearer {}", raw_key);
        let response = app_enabled
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .header("Authorization", auth_header.as_str())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 200 OK — the key authenticates successfully
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn created_api_key_cannot_use_wrong_key_for_auth() {
        // Given: create a key with auth disabled
        let store = MemoryApiKeyStore::new();
        let store_for_enabled = store.clone();
        let app_disabled = make_auth_test_app(Some(ApiKeyStore::Memory(store)), false);

        let payload = serde_json::json!({ "name": "e2e-key2" });
        let response = app_disabled
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/api-keys")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let _body = to_bytes(response.into_body(), usize::MAX).await.unwrap();

        // Then: build app with auth enabled (same store)
        let app_enabled = make_auth_test_app(Some(ApiKeyStore::Memory(store_for_enabled)), true);

        // When: using a DIFFERENT (wrong) key
        let response = app_enabled
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/api-keys")
                    .header("Authorization", "Bearer lt_completely_different_key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Then: 401 Unauthorized — wrong key is rejected
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
        assert!(html.contains("nav-errors"));
        assert!(html.contains("page-errors"));
        assert!(html.contains("errors-table"));
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
