//! Time-bucket aggregation engine for viewers.
//!
//! Parses an `aggregation` block from `definition_json` and computes time-bucketed
//! aggregated values (`count`, `sum`, `avg`, `p50`, `p95`, `p99`, `rate`) over a
//! viewer's `NormalizedEntry` stream.
//!
//! Aggregation is deliberately self-contained: it consumes the same entries that
//! the table view shows and groups them by `bucket_start_ms` (a UTC epoch
//! milliseconds boundary aligned to `bucket_ms`) and an optional set of group keys
//! derived from the entry. For now, the only supported group_by key is
//! `service_name`; unknown keys are ignored at compile time.
//!
//! `count` and `rate` work for any signal because they only need
//! `NormalizedEntry::observed_at`. `sum`, `avg`, and the percentile functions
//! attempt to extract a numeric value from the payload (currently the first
//! data point of the first metric for `Signal::Metrics`); when no value is
//! available the entry is ignored.

use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_pb::payload_as_value;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AggregationError {
    #[error("aggregation must be a JSON object")]
    NotAnObject,
    #[error("aggregation 'fn' is required and must be a string")]
    MissingFn,
    #[error("unknown aggregation fn '{0}': must be count|sum|avg|p50|p95|p99|rate")]
    UnknownFn(String),
    #[error("aggregation 'bucket_ms' must be a positive integer, got {0}")]
    InvalidBucketMs(i64),
    #[error("aggregation 'group_by' must be an array of strings")]
    InvalidGroupBy,
}

/// Aggregation function applied within each bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Sum,
    Avg,
    P50,
    P95,
    P99,
    /// Counts per bucket, scaled to events per second.
    Rate,
}

impl AggFn {
    fn as_str(self) -> &'static str {
        match self {
            AggFn::Count => "count",
            AggFn::Sum => "sum",
            AggFn::Avg => "avg",
            AggFn::P50 => "p50",
            AggFn::P95 => "p95",
            AggFn::P99 => "p99",
            AggFn::Rate => "rate",
        }
    }

    fn parse(s: &str) -> Result<Self, AggregationError> {
        match s {
            "count" => Ok(AggFn::Count),
            "sum" => Ok(AggFn::Sum),
            "avg" => Ok(AggFn::Avg),
            "p50" => Ok(AggFn::P50),
            "p95" => Ok(AggFn::P95),
            "p99" => Ok(AggFn::P99),
            "rate" => Ok(AggFn::Rate),
            other => Err(AggregationError::UnknownFn(other.to_string())),
        }
    }

    /// Whether this aggregation needs a numeric value extracted from the payload.
    fn requires_numeric(self) -> bool {
        matches!(
            self,
            AggFn::Sum | AggFn::Avg | AggFn::P50 | AggFn::P95 | AggFn::P99
        )
    }
}

/// Group-by key recognised by the aggregator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupKey {
    ServiceName,
}

impl GroupKey {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "service_name" => Some(GroupKey::ServiceName),
            _ => None,
        }
    }

    fn extract(self, entry: &NormalizedEntry) -> String {
        match self {
            GroupKey::ServiceName => entry
                .service_name
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
        }
    }
}

/// Compiled aggregation spec parsed from `definition_json["aggregation"]`.
#[derive(Debug, Clone)]
pub struct CompiledAggregation {
    pub func: AggFn,
    pub bucket_ms: i64,
    pub group_by: Vec<GroupKey>,
}

/// One produced bucket -- a time slice with optional group keys and the
/// aggregated value.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Bucket {
    pub bucket_start_ms: i64,
    /// Empty when `group_by` was empty.
    pub group_keys: Vec<String>,
    pub value: f64,
}

/// Parses the optional `aggregation` block from `definition_json`.
///
/// Returns `Ok(None)` when the field is absent. Returns `Err` for malformed
/// values so that callers can surface a 400 / log a warning.
pub fn parse_aggregation(
    definition_json: &Value,
) -> Result<Option<CompiledAggregation>, AggregationError> {
    let Some(raw) = definition_json.get("aggregation") else {
        return Ok(None);
    };
    let obj = raw.as_object().ok_or(AggregationError::NotAnObject)?;

    let fn_str = obj
        .get("fn")
        .and_then(|v| v.as_str())
        .ok_or(AggregationError::MissingFn)?;
    let func = AggFn::parse(fn_str)?;

    let bucket_ms_raw = obj.get("bucket_ms").and_then(|v| v.as_i64()).unwrap_or(0);
    if bucket_ms_raw <= 0 {
        return Err(AggregationError::InvalidBucketMs(bucket_ms_raw));
    }

    let group_by = match obj.get("group_by") {
        None => Vec::new(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().and_then(GroupKey::parse))
            .collect(),
        Some(_) => return Err(AggregationError::InvalidGroupBy),
    };

    Ok(Some(CompiledAggregation {
        func,
        bucket_ms: bucket_ms_raw,
        group_by,
    }))
}

/// Computes aggregated buckets over the supplied entries.
///
/// Entries are assumed to be in ascending time order, but the aggregator is
/// resilient to mild reordering -- buckets are keyed by epoch ms so order does
/// not affect correctness, only insertion ordering of the output.
pub fn aggregate_entries(spec: &CompiledAggregation, entries: &[NormalizedEntry]) -> Vec<Bucket> {
    // BTreeMap to keep buckets sorted by (bucket_start_ms, group_keys).
    let mut acc: BTreeMap<(i64, Vec<String>), Accumulator> = BTreeMap::new();

    for entry in entries {
        let bucket_start_ms = bucket_start(entry.observed_at.timestamp_millis(), spec.bucket_ms);
        let group_keys: Vec<String> = spec.group_by.iter().map(|gk| gk.extract(entry)).collect();

        let value_opt = if spec.func.requires_numeric() {
            extract_numeric_value(entry)
        } else {
            None
        };

        // For sum/avg/percentiles, ignore entries without a numeric value.
        if spec.func.requires_numeric() && value_opt.is_none() {
            continue;
        }

        let slot = acc
            .entry((bucket_start_ms, group_keys))
            .or_insert_with(|| Accumulator::new(spec.func));
        slot.add(value_opt);
    }

    acc.into_iter()
        .map(|((bucket_start_ms, group_keys), slot)| Bucket {
            bucket_start_ms,
            group_keys,
            value: slot.finalize(spec),
        })
        .collect()
}

/// Aligns a timestamp (epoch ms) down to the nearest `bucket_ms` boundary.
fn bucket_start(ts_ms: i64, bucket_ms: i64) -> i64 {
    // div_euclid is correct for negative ts as well, although in practice
    // observed_at is always positive.
    ts_ms.div_euclid(bucket_ms) * bucket_ms
}

/// Per-bucket accumulator state.
#[derive(Debug)]
struct Accumulator {
    count: u64,
    sum: f64,
    /// Only collected for percentile aggregations.
    samples: Vec<f64>,
    needs_samples: bool,
}

impl Accumulator {
    fn new(func: AggFn) -> Self {
        Self {
            count: 0,
            sum: 0.0,
            samples: Vec::new(),
            needs_samples: matches!(func, AggFn::P50 | AggFn::P95 | AggFn::P99),
        }
    }

    fn add(&mut self, value: Option<f64>) {
        self.count += 1;
        if let Some(v) = value {
            self.sum += v;
            if self.needs_samples {
                self.samples.push(v);
            }
        }
    }

    fn finalize(mut self, spec: &CompiledAggregation) -> f64 {
        match spec.func {
            AggFn::Count => self.count as f64,
            AggFn::Sum => self.sum,
            AggFn::Avg => {
                if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                }
            }
            AggFn::P50 => percentile(&mut self.samples, 0.50),
            AggFn::P95 => percentile(&mut self.samples, 0.95),
            AggFn::P99 => percentile(&mut self.samples, 0.99),
            AggFn::Rate => {
                let bucket_seconds = spec.bucket_ms as f64 / 1_000.0;
                if bucket_seconds <= 0.0 {
                    0.0
                } else {
                    self.count as f64 / bucket_seconds
                }
            }
        }
    }
}

/// Nearest-rank percentile on an unsorted f64 slice. Sorts in place and
/// returns 0.0 for empty inputs.
fn percentile(samples: &mut [f64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    // Nearest-rank method: rank = ceil(p * n), 1-indexed.
    let n = samples.len() as f64;
    let rank = (p * n).ceil().max(1.0) as usize;
    let idx = rank.saturating_sub(1).min(samples.len() - 1);
    samples[idx]
}

/// Best-effort numeric value extractor for an entry.
///
/// Currently only inspects metric payloads, returning the first
/// `asInt`/`asDouble` data point of the first metric. Returns None for
/// traces / logs / unparseable metric payloads.
fn extract_numeric_value(entry: &NormalizedEntry) -> Option<f64> {
    if entry.signal != Signal::Metrics {
        return None;
    }
    let value = payload_as_value(Signal::Metrics, &entry.payload)?;
    let resource_metrics = value.get("resourceMetrics")?.as_array()?;
    for rm in resource_metrics {
        let scope_metrics = rm.get("scopeMetrics").and_then(|v| v.as_array())?;
        for sm in scope_metrics {
            let metrics = sm.get("metrics").and_then(|v| v.as_array())?;
            for m in metrics {
                if let Some(v) = first_data_point_value(m) {
                    return Some(v);
                }
            }
        }
    }
    None
}

fn first_data_point_value(metric: &Value) -> Option<f64> {
    for kind in ["sum", "gauge", "histogram"] {
        let Some(points) = metric
            .get(kind)
            .and_then(|k| k.get("dataPoints"))
            .and_then(|v| v.as_array())
        else {
            continue;
        };
        for p in points {
            if let Some(v) = p.get("asDouble").and_then(|v| v.as_f64()) {
                return Some(v);
            }
            if let Some(s) = p.get("asInt").and_then(|v| v.as_str())
                && let Ok(i) = s.parse::<i64>()
            {
                return Some(i as f64);
            }
            if let Some(i) = p.get("asInt").and_then(|v| v.as_i64()) {
                return Some(i as f64);
            }
            if let Some(s) = p.get("sum").and_then(|v| v.as_f64()) {
                return Some(s);
            }
        }
    }
    None
}

/// Serializes the aggregation spec to its JSON shape, used for round-tripping
/// in API responses.
pub fn aggregation_to_json(spec: &CompiledAggregation) -> Value {
    let group_by: Vec<&'static str> = spec
        .group_by
        .iter()
        .map(|gk| match gk {
            GroupKey::ServiceName => "service_name",
        })
        .collect();
    serde_json::json!({
        "fn": spec.func.as_str(),
        "bucket_ms": spec.bucket_ms,
        "group_by": group_by,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    fn entry(signal: Signal, ts_ms: i64, service: Option<&str>, payload: Bytes) -> NormalizedEntry {
        NormalizedEntry {
            signal,
            observed_at: Utc.timestamp_millis_opt(ts_ms).unwrap(),
            service_name: service.map(str::to_string),
            payload,
        }
    }

    fn metric_payload(value: u64) -> Bytes {
        Bytes::from(
            json!({
                "resourceMetrics": [{
                    "scopeMetrics": [{
                        "metrics": [{
                            "name": "test",
                            "sum": {
                                "dataPoints": [{ "asInt": value.to_string() }]
                            }
                        }]
                    }]
                }]
            })
            .to_string(),
        )
    }

    #[test]
    fn parse_aggregation_returns_none_when_missing() {
        let result = parse_aggregation(&json!({})).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_aggregation_basic_count() {
        let spec = parse_aggregation(&json!({
            "aggregation": { "fn": "count", "bucket_ms": 60_000 }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(spec.func, AggFn::Count);
        assert_eq!(spec.bucket_ms, 60_000);
        assert!(spec.group_by.is_empty());
    }

    #[test]
    fn parse_aggregation_with_group_by() {
        let spec = parse_aggregation(&json!({
            "aggregation": {
                "fn": "rate",
                "bucket_ms": 1_000,
                "group_by": ["service_name", "unknown_key"]
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(spec.func, AggFn::Rate);
        // Unknown keys silently dropped.
        assert_eq!(spec.group_by, vec![GroupKey::ServiceName]);
    }

    #[test]
    fn parse_aggregation_rejects_zero_bucket() {
        let err = parse_aggregation(&json!({
            "aggregation": { "fn": "count", "bucket_ms": 0 }
        }))
        .unwrap_err();
        assert!(matches!(err, AggregationError::InvalidBucketMs(0)));
    }

    #[test]
    fn parse_aggregation_rejects_unknown_fn() {
        let err = parse_aggregation(&json!({
            "aggregation": { "fn": "median", "bucket_ms": 1_000 }
        }))
        .unwrap_err();
        assert!(matches!(err, AggregationError::UnknownFn(_)));
    }

    #[test]
    fn aggregate_count_groups_entries_into_buckets() {
        let spec = CompiledAggregation {
            func: AggFn::Count,
            bucket_ms: 60_000,
            group_by: vec![],
        };
        let entries = vec![
            entry(Signal::Traces, 60_000, None, Bytes::new()),
            entry(Signal::Traces, 75_000, None, Bytes::new()),
            entry(Signal::Traces, 120_000, None, Bytes::new()),
        ];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].bucket_start_ms, 60_000);
        assert_eq!(buckets[0].value, 2.0);
        assert_eq!(buckets[1].bucket_start_ms, 120_000);
        assert_eq!(buckets[1].value, 1.0);
    }

    #[test]
    fn aggregate_rate_scales_to_events_per_second() {
        let spec = CompiledAggregation {
            func: AggFn::Rate,
            bucket_ms: 10_000,
            group_by: vec![],
        };
        let entries = vec![
            entry(Signal::Traces, 0, None, Bytes::new()),
            entry(Signal::Traces, 1_000, None, Bytes::new()),
            entry(Signal::Traces, 9_000, None, Bytes::new()),
        ];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        // 3 events / 10 seconds = 0.3
        assert!((buckets[0].value - 0.3).abs() < 1e-9);
    }

    #[test]
    fn aggregate_count_with_group_by_service_name() {
        let spec = CompiledAggregation {
            func: AggFn::Count,
            bucket_ms: 60_000,
            group_by: vec![GroupKey::ServiceName],
        };
        let entries = vec![
            entry(Signal::Traces, 60_000, Some("a"), Bytes::new()),
            entry(Signal::Traces, 65_000, Some("b"), Bytes::new()),
            entry(Signal::Traces, 70_000, Some("a"), Bytes::new()),
        ];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 2);
        // BTreeMap sorts group_keys lexicographically; "a" comes first.
        assert_eq!(buckets[0].group_keys, vec!["a".to_string()]);
        assert_eq!(buckets[0].value, 2.0);
        assert_eq!(buckets[1].group_keys, vec!["b".to_string()]);
        assert_eq!(buckets[1].value, 1.0);
    }

    #[test]
    fn aggregate_sum_uses_metric_value() {
        let spec = CompiledAggregation {
            func: AggFn::Sum,
            bucket_ms: 60_000,
            group_by: vec![],
        };
        let entries = vec![
            entry(Signal::Metrics, 60_000, None, metric_payload(10)),
            entry(Signal::Metrics, 65_000, None, metric_payload(20)),
        ];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].value, 30.0);
    }

    #[test]
    fn aggregate_avg_uses_metric_value() {
        let spec = CompiledAggregation {
            func: AggFn::Avg,
            bucket_ms: 60_000,
            group_by: vec![],
        };
        let entries = vec![
            entry(Signal::Metrics, 60_000, None, metric_payload(10)),
            entry(Signal::Metrics, 65_000, None, metric_payload(30)),
        ];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets[0].value, 20.0);
    }

    #[test]
    fn aggregate_percentiles_uses_nearest_rank() {
        let spec_p50 = CompiledAggregation {
            func: AggFn::P50,
            bucket_ms: 60_000,
            group_by: vec![],
        };
        let spec_p95 = CompiledAggregation {
            func: AggFn::P95,
            bucket_ms: 60_000,
            group_by: vec![],
        };
        let entries: Vec<_> = (1..=10)
            .map(|i| entry(Signal::Metrics, 60_000, None, metric_payload(i)))
            .collect();

        let p50 = aggregate_entries(&spec_p50, &entries);
        let p95 = aggregate_entries(&spec_p95, &entries);

        // nearest-rank p50 of [1..=10] = 5; p95 = 10.
        assert_eq!(p50[0].value, 5.0);
        assert_eq!(p95[0].value, 10.0);
    }

    #[test]
    fn aggregate_skips_metric_entries_without_value_for_sum() {
        let spec = CompiledAggregation {
            func: AggFn::Sum,
            bucket_ms: 60_000,
            group_by: vec![],
        };
        let entries = vec![
            entry(Signal::Traces, 60_000, None, Bytes::from_static(b"{}")),
            entry(Signal::Metrics, 65_000, None, metric_payload(7)),
        ];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].value, 7.0);
    }

    #[test]
    fn aggregation_to_json_round_trip() {
        let spec = CompiledAggregation {
            func: AggFn::P95,
            bucket_ms: 30_000,
            group_by: vec![GroupKey::ServiceName],
        };
        let value = aggregation_to_json(&spec);
        assert_eq!(
            value,
            json!({ "fn": "p95", "bucket_ms": 30_000, "group_by": ["service_name"] })
        );
    }
}
