//! Metric-to-trace exemplar linking.
//!
//! Given a metric viewer's bucketed time series, find sample trace_ids observed in the
//! same bucket window (and matching the same service_name when present) so that users
//! can jump from a bucket on a chart to the relevant trace detail view.
//!
//! The linking is done in-memory at request time -- no DDL or background job needed.
//!
//! Inputs:
//!   - metric entries (drive bucket boundaries)
//!   - trace entries (provide candidate trace_ids)
//!   - bucket size (`bucket_ms`)
//!
//! Outputs:
//!   - one entry per bucket that has at least one matching trace_id (capped per bucket).

use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_pb::payload_as_value;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use thiserror::Error;

/// Default cap on the number of trace_ids reported per bucket. Keeps response payloads small.
pub const DEFAULT_MAX_SAMPLES_PER_BUCKET: usize = 5;

/// Minimum/maximum allowed bucket size (ms). Avoids degenerate cases like 0 or extremely small
/// buckets that would explode the response cardinality.
pub const MIN_BUCKET_MS: i64 = 1_000;
pub const MAX_BUCKET_MS: i64 = 24 * 60 * 60 * 1_000;

#[derive(Debug, Error)]
pub enum ExemplarError {
    #[error("bucket_ms must be in [{MIN_BUCKET_MS}, {MAX_BUCKET_MS}], got {0}")]
    InvalidBucket(i64),
}

/// One bucket of exemplars: a bucket start timestamp (ms) plus sample trace_ids observed in it.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExemplarBucket {
    /// Bucket start time in epoch milliseconds.
    pub bucket_start_ms: i64,
    /// Sample trace_ids associated with this bucket. Bounded by `max_samples`.
    pub sample_trace_ids: Vec<String>,
}

/// Compute exemplar buckets from metric + trace entries.
///
/// Algorithm:
/// 1. For each metric entry within `[now - lookback_ms, now]`, mark its bucket as "active".
///    A bucket is `floor(observed_at_ms / bucket_ms) * bucket_ms`.
///    If `service_filter` is set, only metric entries whose `service_name` equals it count.
/// 2. For each trace entry within the same window (and matching `service_filter` when set),
///    extract the unique trace_ids present in its payload and assign them to the trace's bucket.
/// 3. Emit a bucket only when its set of trace_ids is non-empty AND the metric viewer also had
///    activity in that bucket. Buckets are sorted ascending by `bucket_start_ms`.
///
/// `max_samples` caps trace_ids per bucket; insertion-ordered (first-seen wins).
pub fn compute_exemplars(
    metric_entries: &[NormalizedEntry],
    trace_entries: &[NormalizedEntry],
    service_filter: Option<&str>,
    now: DateTime<Utc>,
    lookback_ms: i64,
    bucket_ms: i64,
    max_samples: usize,
) -> Result<Vec<ExemplarBucket>, ExemplarError> {
    if !(MIN_BUCKET_MS..=MAX_BUCKET_MS).contains(&bucket_ms) {
        return Err(ExemplarError::InvalidBucket(bucket_ms));
    }
    let max_samples = max_samples.max(1);
    let cutoff_ms = now.timestamp_millis().saturating_sub(lookback_ms.max(0));
    let now_ms = now.timestamp_millis();

    // 1. Buckets that have metric activity.
    let mut active_buckets: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
    for entry in metric_entries {
        if entry.signal != Signal::Metrics {
            continue;
        }
        let ts = entry.observed_at.timestamp_millis();
        if ts < cutoff_ms || ts > now_ms {
            continue;
        }
        if !service_matches(service_filter, entry.service_name.as_deref()) {
            continue;
        }
        active_buckets.insert(bucket_start(ts, bucket_ms));
    }

    if active_buckets.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Map bucket -> ordered unique trace_ids.
    let mut bucket_trace_ids: BTreeMap<i64, Vec<String>> = BTreeMap::new();
    for entry in trace_entries {
        if entry.signal != Signal::Traces {
            continue;
        }
        let ts = entry.observed_at.timestamp_millis();
        if ts < cutoff_ms || ts > now_ms {
            continue;
        }
        if !service_matches(service_filter, entry.service_name.as_deref()) {
            continue;
        }
        let bucket = bucket_start(ts, bucket_ms);
        if !active_buckets.contains(&bucket) {
            continue;
        }
        let slot = bucket_trace_ids.entry(bucket).or_default();
        if slot.len() >= max_samples {
            continue;
        }
        for trace_id in extract_trace_ids(&entry.payload) {
            if slot.len() >= max_samples {
                break;
            }
            if !slot.iter().any(|existing| existing == &trace_id) {
                slot.push(trace_id);
            }
        }
    }

    // 3. Emit buckets ordered by start time. Drop empties.
    Ok(bucket_trace_ids
        .into_iter()
        .filter(|(_, ids)| !ids.is_empty())
        .map(|(bucket_start_ms, sample_trace_ids)| ExemplarBucket {
            bucket_start_ms,
            sample_trace_ids,
        })
        .collect())
}

fn service_matches(filter: Option<&str>, candidate: Option<&str>) -> bool {
    match filter {
        None => true,
        Some(want) => candidate
            .map(|s| s.eq_ignore_ascii_case(want))
            .unwrap_or(false),
    }
}

fn bucket_start(ts_ms: i64, bucket_ms: i64) -> i64 {
    // Floor division that works for negative timestamps too (i.e. before 1970).
    // bucket_ms is validated > 0 above.
    let q = ts_ms.div_euclid(bucket_ms);
    q * bucket_ms
}

/// Extract trace_ids from an OTLP trace payload (JSON or protobuf). Empty IDs are skipped.
pub fn extract_trace_ids(payload: &bytes::Bytes) -> Vec<String> {
    let Some(value) = payload_as_value(Signal::Traces, payload) else {
        return Vec::new();
    };
    let Some(resource_spans) = value.get("resourceSpans").and_then(|v| v.as_array()) else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    for rs in resource_spans {
        let Some(scope_spans) = rs.get("scopeSpans").and_then(|v| v.as_array()) else {
            continue;
        };
        for ss in scope_spans {
            let Some(spans) = ss.get("spans").and_then(|v| v.as_array()) else {
                continue;
            };
            for span in spans {
                if let Some(id) = span.get("traceId").and_then(serde_json::Value::as_str)
                    && !id.is_empty()
                    && !ids.iter().any(|existing: &String| existing == id)
                {
                    ids.push(id.to_string());
                }
            }
        }
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Duration;
    use serde_json::json;

    fn metric_entry(service: Option<&str>, age_ms: i64, now: DateTime<Utc>) -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Metrics,
            observed_at: now - Duration::milliseconds(age_ms),
            service_name: service.map(str::to_string),
            payload: Bytes::from_static(b"{}"),
        }
    }

    fn trace_entry(
        service: Option<&str>,
        age_ms: i64,
        trace_ids: &[&str],
        now: DateTime<Utc>,
    ) -> NormalizedEntry {
        let spans: Vec<serde_json::Value> = trace_ids
            .iter()
            .map(|id| {
                json!({
                    "traceId": id,
                    "spanId": "0000000000000001",
                    "name": "op"
                })
            })
            .collect();
        let payload = json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": service.map(|s| vec![json!({
                        "key": "service.name",
                        "value": { "stringValue": s }
                    })]).unwrap_or_default()
                },
                "scopeSpans": [{
                    "spans": spans
                }]
            }]
        })
        .to_string();
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at: now - Duration::milliseconds(age_ms),
            service_name: service.map(str::to_string),
            payload: Bytes::from(payload),
        }
    }

    #[test]
    fn rejects_bucket_below_min() {
        let now = Utc::now();
        let result = compute_exemplars(&[], &[], None, now, 60_000, 0, 5);
        assert!(matches!(result, Err(ExemplarError::InvalidBucket(0))));
    }

    #[test]
    fn rejects_bucket_above_max() {
        let now = Utc::now();
        let result = compute_exemplars(&[], &[], None, now, 60_000, MAX_BUCKET_MS + 1, 5);
        assert!(matches!(result, Err(ExemplarError::InvalidBucket(_))));
    }

    #[test]
    fn empty_inputs_yield_empty_buckets() {
        let now = Utc::now();
        let buckets = compute_exemplars(&[], &[], None, now, 600_000, 60_000, 5).unwrap();
        assert!(buckets.is_empty());
    }

    #[test]
    fn no_metric_buckets_means_no_exemplars_even_with_traces() {
        // Given: trace entries exist but no metric entries
        let now = Utc::now();
        let traces = vec![trace_entry(Some("svc"), 30_000, &["aa"], now)];
        let buckets = compute_exemplars(&[], &traces, None, now, 600_000, 60_000, 5).unwrap();
        // Then: no buckets emitted (a bucket only counts when metric activity exists in it)
        assert!(buckets.is_empty());
    }

    #[test]
    fn matches_trace_id_to_metric_bucket_in_same_window() {
        let now = Utc::now();
        // Bucket = 60s. Metric and trace both observed 30s ago -> same bucket.
        let metrics = vec![metric_entry(Some("svc"), 30_000, now)];
        let traces = vec![trace_entry(Some("svc"), 30_000, &["aabbccdd"], now)];
        let buckets = compute_exemplars(&metrics, &traces, None, now, 600_000, 60_000, 5).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].sample_trace_ids, vec!["aabbccdd".to_string()]);
    }

    #[test]
    fn skips_entries_outside_lookback_window() {
        let now = Utc::now();
        let metrics = vec![metric_entry(Some("svc"), 30_000, now)];
        // Trace is older than the 60s lookback window.
        let traces = vec![trace_entry(Some("svc"), 120_000, &["old"], now)];
        let buckets = compute_exemplars(&metrics, &traces, None, now, 60_000, 60_000, 5).unwrap();
        // The metric bucket exists but no in-window trace_ids -> no bucket emitted.
        assert!(buckets.is_empty());
    }

    #[test]
    fn caps_trace_ids_per_bucket_at_max_samples() {
        let now = Utc::now();
        let metrics = vec![metric_entry(Some("svc"), 5_000, now)];
        let mut traces = Vec::new();
        for i in 0..10u32 {
            traces.push(trace_entry(
                Some("svc"),
                5_000,
                &[&format!("{i:032x}")],
                now,
            ));
        }
        let buckets = compute_exemplars(&metrics, &traces, None, now, 600_000, 60_000, 3).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].sample_trace_ids.len(), 3);
    }

    #[test]
    fn dedupes_trace_ids_within_same_bucket() {
        // Use the same age for all entries so that bucket alignment relative to `now` cannot
        // accidentally split them into different buckets due to wall-clock boundary effects.
        let now = Utc::now();
        let age = 10_000;
        let metrics = vec![metric_entry(Some("svc"), age, now)];
        let traces = vec![
            trace_entry(Some("svc"), age, &["dup"], now),
            trace_entry(Some("svc"), age, &["dup"], now),
            trace_entry(Some("svc"), age, &["other"], now),
        ];
        let buckets = compute_exemplars(&metrics, &traces, None, now, 600_000, 60_000, 5).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(
            buckets[0].sample_trace_ids,
            vec!["dup".to_string(), "other".to_string()]
        );
    }

    #[test]
    fn buckets_sorted_ascending_by_start_ms() {
        let now = Utc::now();
        // Two metric activity buckets: 5s ago and 130s ago (60s buckets -> different buckets).
        let metrics = vec![
            metric_entry(Some("svc"), 5_000, now),
            metric_entry(Some("svc"), 130_000, now),
        ];
        let traces = vec![
            trace_entry(Some("svc"), 5_000, &["recent"], now),
            trace_entry(Some("svc"), 130_000, &["older"], now),
        ];
        let buckets = compute_exemplars(&metrics, &traces, None, now, 600_000, 60_000, 5).unwrap();
        assert_eq!(buckets.len(), 2);
        assert!(buckets[0].bucket_start_ms < buckets[1].bucket_start_ms);
    }

    #[test]
    fn service_filter_excludes_other_services() {
        let now = Utc::now();
        let metrics = vec![metric_entry(Some("orders"), 5_000, now)];
        let traces = vec![
            trace_entry(Some("orders"), 5_000, &["ok"], now),
            trace_entry(Some("billing"), 5_000, &["nope"], now),
        ];
        let buckets =
            compute_exemplars(&metrics, &traces, Some("orders"), now, 600_000, 60_000, 5).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].sample_trace_ids, vec!["ok".to_string()]);
    }

    #[test]
    fn service_filter_is_case_insensitive() {
        let now = Utc::now();
        let metrics = vec![metric_entry(Some("Orders"), 5_000, now)];
        let traces = vec![trace_entry(Some("orders"), 5_000, &["ok"], now)];
        let buckets =
            compute_exemplars(&metrics, &traces, Some("ORDERS"), now, 600_000, 60_000, 5).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].sample_trace_ids, vec!["ok".to_string()]);
    }

    #[test]
    fn no_service_filter_includes_all_services() {
        let now = Utc::now();
        let metrics = vec![metric_entry(Some("a"), 5_000, now)];
        let traces = vec![
            trace_entry(Some("a"), 5_000, &["t1"], now),
            trace_entry(Some("b"), 5_000, &["t2"], now),
        ];
        let buckets = compute_exemplars(&metrics, &traces, None, now, 600_000, 60_000, 5).unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].sample_trace_ids.len(), 2);
    }

    #[test]
    fn extract_trace_ids_from_json_payload_returns_unique_ids() {
        let payload = Bytes::from(
            json!({
                "resourceSpans": [{
                    "scopeSpans": [{
                        "spans": [
                            { "traceId": "aa", "spanId": "01", "name": "x" },
                            { "traceId": "aa", "spanId": "02", "name": "y" },
                            { "traceId": "bb", "spanId": "03", "name": "z" }
                        ]
                    }]
                }]
            })
            .to_string(),
        );
        let ids = extract_trace_ids(&payload);
        assert_eq!(ids, vec!["aa".to_string(), "bb".to_string()]);
    }

    #[test]
    fn extract_trace_ids_from_invalid_payload_returns_empty() {
        let payload = Bytes::from_static(b"not-otlp");
        assert!(extract_trace_ids(&payload).is_empty());
    }
}
