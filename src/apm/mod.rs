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
//! - `error_groups`: aggregate exception events from traces and ERROR-severity logs into
//!   fingerprinted issue groups.
//! - `slow_query`: detect slow database queries (p95) and N+1 patterns by aggregating
//!   spans carrying `db.statement` / `db.system` attributes.

pub mod error_groups;
pub mod exemplars;
pub mod service_map;
pub mod slow_query;
pub mod trace_search;
pub mod waterfall;

/// OTLP can encode nano timestamps as a JSON number, string, or float.
/// Returns 0 when the value is missing or unparseable (best-effort telemetry processing).
pub fn parse_otlp_nano(value: Option<&serde_json::Value>) -> u64 {
    let Some(v) = value else { return 0 };
    if let Some(n) = v.as_u64() {
        return n;
    }
    if let Some(s) = v.as_str() {
        return s.parse::<u64>().unwrap_or(0);
    }
    if let Some(f) = v.as_f64()
        && f >= 0.0
        && f.is_finite()
    {
        if f >= u64::MAX as f64 {
            return u64::MAX;
        }
        #[allow(clippy::cast_possible_truncation)]
        return f as u64;
    }
    0
}

/// Aligns a timestamp (epoch ms) down to the nearest `bucket_ms` boundary.
/// Uses div_euclid which is correct for negative timestamps.
///
/// # Panics (debug)
///
/// Panics in debug builds if `bucket_ms` is 0. All callers validate this at
/// construction time, so the check is omitted in release builds.
pub fn bucket_start(ts_ms: i64, bucket_ms: i64) -> i64 {
    debug_assert!(bucket_ms > 0, "bucket_ms must be positive, got {bucket_ms}");
    ts_ms.div_euclid(bucket_ms) * bucket_ms
}
