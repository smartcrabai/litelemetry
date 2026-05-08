//! APM (Application Performance Monitoring) helpers.
//!
//! This module aggregates trace-centric utilities used by the HTTP API to
//! search and inspect distributed traces. Today it exposes a `trace_search`
//! decoder that groups OTLP spans by `trace_id` and surfaces the
//! attributes / status / kind required by the Traces tab in the viewer UI,
//! plus a `waterfall` renderer for visualizing per-trace span timelines.
//!
//! Additional features:
//! - `exemplars`: link metric viewer buckets to sample trace_ids observed in the same time window.
//! - `service_map`: extract service-to-service edges from OTLP spans for the Service Map tab.

pub mod exemplars;
pub mod service_map;
pub mod trace_search;
pub mod waterfall;
