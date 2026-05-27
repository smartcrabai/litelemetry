//! Time-bucket aggregation engine for viewers.
//!
//! Parses an `aggregation` block from `definition_json` and computes time-bucketed
//! aggregated values (`count`, `sum`, `avg`, `p50`, `p95`, `p99`, `rate`) over a
//! viewer's `NormalizedEntry` stream.
//!
//! Aggregation is deliberately self-contained: it consumes the same entries that
//! the table view shows and groups them by `bucket_start_ms` (a UTC epoch
//! milliseconds boundary aligned to `bucket_ms`) and an optional set of group keys
//! derived from the entry. Supported group_by keys are `service_name`,
//! `k8s.node.name`, `k8s.cluster.name`, `k8s.namespace.name`, `k8s.pod.name`,
//! and `host.name`; unknown keys are silently ignored.
//!
//! `count` and `rate` work for any signal because they only need
//! `NormalizedEntry::observed_at`. `sum`, `avg`, and the percentile functions
//! attempt to extract a numeric value from the payload (currently the first
//! data point of the first metric for `Signal::Metrics`); when no value is
//! available the entry is ignored.

use crate::apm::bucket_start;
use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_http::attribute_string_value;
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
    K8sNodeName,
    K8sClusterName,
    K8sNamespace,
    K8sPodName,
    HostName,
}

impl GroupKey {
    pub(crate) fn parse(s: &str) -> Option<Self> {
        match s {
            "service_name" => Some(GroupKey::ServiceName),
            "k8s.node.name" => Some(GroupKey::K8sNodeName),
            "k8s.cluster.name" => Some(GroupKey::K8sClusterName),
            "k8s.namespace.name" => Some(GroupKey::K8sNamespace),
            "k8s.pod.name" => Some(GroupKey::K8sPodName),
            "host.name" => Some(GroupKey::HostName),
            _ => None,
        }
    }

    fn extract(self, entry: &NormalizedEntry) -> String {
        match self {
            GroupKey::ServiceName => entry
                .service_name
                .clone()
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sNodeName => extract_resource_attribute(entry, "k8s.node.name")
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sClusterName => extract_resource_attribute(entry, "k8s.cluster.name")
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sNamespace => extract_resource_attribute(entry, "k8s.namespace.name")
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sPodName => extract_resource_attribute(entry, "k8s.pod.name")
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::HostName => extract_resource_attribute(entry, "host.name")
                .unwrap_or_else(|| "unknown".to_string()),
        }
    }

    /// Like [`extract`](Self::extract) but reads from an already-decoded metrics
    /// payload so the caller can decode once and reuse it across all group keys.
    /// `decoded` is `None` for non-metric or undecodable entries. `service_name`
    /// comes from the entry (matching `extract`); every other key is looked up in
    /// the payload's resource attributes -- so the result is identical to
    /// `extract` for the same entry.
    fn extract_from_decoded(self, decoded: Option<&Value>, entry: &NormalizedEntry) -> String {
        if let GroupKey::ServiceName = self {
            return entry
                .service_name
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
        }
        let key = match self {
            GroupKey::ServiceName => unreachable!("handled above"),
            GroupKey::K8sNodeName => "k8s.node.name",
            GroupKey::K8sClusterName => "k8s.cluster.name",
            GroupKey::K8sNamespace => "k8s.namespace.name",
            GroupKey::K8sPodName => "k8s.pod.name",
            GroupKey::HostName => "host.name",
        };
        decoded
            .and_then(|v| resource_attribute_from_metrics_value(v, key))
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Extracts the group value from a specific `ResourceMetrics` block's
    /// `resource.attributes`, falling back to the entry-level value when the
    /// resource has no matching attribute.
    fn extract_with_resource(self, attrs: Option<&[Value]>, entry: &NormalizedEntry) -> String {
        let lookup = |key: &str| -> Option<String> {
            attrs
                .filter(|slice| !slice.is_empty())
                .and_then(|slice| attribute_string_value(slice, key))
        };
        match self {
            GroupKey::ServiceName => lookup("service.name")
                .or_else(|| entry.service_name.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sNodeName => lookup("k8s.node.name")
                .or_else(|| extract_resource_attribute(entry, "k8s.node.name"))
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sClusterName => lookup("k8s.cluster.name")
                .or_else(|| extract_resource_attribute(entry, "k8s.cluster.name"))
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sNamespace => lookup("k8s.namespace.name")
                .or_else(|| extract_resource_attribute(entry, "k8s.namespace.name"))
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::K8sPodName => lookup("k8s.pod.name")
                .or_else(|| extract_resource_attribute(entry, "k8s.pod.name"))
                .unwrap_or_else(|| "unknown".to_string()),
            GroupKey::HostName => lookup("host.name")
                .or_else(|| extract_resource_attribute(entry, "host.name"))
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
    pub metric_name: Option<String>,
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

    let metric_name = obj
        .get("metric_name")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(Some(CompiledAggregation {
        func,
        bucket_ms: bucket_ms_raw,
        group_by,
        metric_name,
    }))
}

/// Computes aggregated buckets over the supplied entries in a single pass.
///
/// The runtime hot path no longer calls this -- it maintains an
/// [`AggregationState`] incrementally instead. This full-recompute form is
/// retained as the reference implementation that the incremental path is tested
/// against (see `assert_incremental_matches_full`); keep the two in sync.
///
/// Entries are assumed to be in ascending time order, but the aggregator is
/// resilient to mild reordering -- buckets are keyed by epoch ms so order does
/// not affect correctness, only insertion ordering of the output.
///
/// When the spec carries a `metric_name` filter, each entry's payload is
/// expanded into one input per matching data point so that batches bundling
/// many resources (e.g. a single kubeletstats push carrying `k8s.pod.cpu.usage`
/// for every pod) contribute every matching pod's value rather than only the
/// first one. Group keys are then extracted from that data point's owning
/// `ResourceMetrics` block so series stay aligned with the actual resource.
pub fn aggregate_entries(spec: &CompiledAggregation, entries: &[NormalizedEntry]) -> Vec<Bucket> {
    // BTreeMap to keep buckets sorted by (bucket_start_ms, group_keys).
    let mut acc: BTreeMap<(i64, Vec<String>), Accumulator> = BTreeMap::new();

    let needs_numeric = spec.func.requires_numeric();

    for entry in entries {
        let bucket_start_ms = bucket_start(entry.observed_at.timestamp_millis(), spec.bucket_ms);

        let inputs = collect_aggregation_inputs(entry, spec);
        for (group_keys, value_opt) in inputs {
            // For sum/avg/percentiles, ignore inputs without a numeric value.
            if needs_numeric && value_opt.is_none() {
                continue;
            }

            let slot = acc
                .entry((bucket_start_ms, group_keys))
                .or_insert_with(|| Accumulator::new(spec.func));
            slot.add(value_opt);
        }
    }

    acc.into_iter()
        .map(|((bucket_start_ms, group_keys), slot)| Bucket {
            bucket_start_ms,
            group_keys,
            value: slot.finalize(spec),
        })
        .collect()
}

/// Produces the (group_keys, value) inputs an entry contributes to the
/// aggregator. Most entries produce one input; metric payloads filtered by
/// `metric_name` produce one per matching data point so each resource in the
/// batch lands in its own series.
fn collect_aggregation_inputs(
    entry: &NormalizedEntry,
    spec: &CompiledAggregation,
) -> Vec<(Vec<String>, Option<f64>)> {
    let needs_numeric = spec.func.requires_numeric();
    let entry_level_keys =
        || -> Vec<String> { spec.group_by.iter().map(|gk| gk.extract(entry)).collect() };

    let Some(filter) = spec.metric_name.as_deref() else {
        // No metric_name filter: the entry contributes exactly one input. Decode
        // the metric payload at most once and reuse it for both the numeric value
        // and any resource-derived group keys (service_name comes from the entry,
        // not the payload). This replaces the previous code path that decoded once
        // per numeric value plus once per non-service group_by key.
        let needs_decode = entry.signal == Signal::Metrics
            && (needs_numeric || spec.group_by.iter().any(|gk| *gk != GroupKey::ServiceName));
        let decoded = if needs_decode {
            payload_as_value(Signal::Metrics, &entry.payload)
        } else {
            None
        };
        let group_keys: Vec<String> = spec
            .group_by
            .iter()
            .map(|gk| gk.extract_from_decoded(decoded.as_ref(), entry))
            .collect();
        let value = if needs_numeric {
            decoded.as_ref().and_then(numeric_value_from_metrics_value)
        } else {
            None
        };
        return vec![(group_keys, value)];
    };

    if entry.signal != Signal::Metrics {
        return vec![(entry_level_keys(), None)];
    }

    let Some(payload) = payload_as_value(Signal::Metrics, &entry.payload) else {
        return vec![(entry_level_keys(), None)];
    };
    let Some(resource_metrics) = payload.get("resourceMetrics").and_then(|v| v.as_array()) else {
        return vec![(entry_level_keys(), None)];
    };

    let mut inputs: Vec<(Vec<String>, Option<f64>)> = Vec::new();
    for rm in resource_metrics {
        let resource_attrs = rm
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(|v| v.as_array());

        let Some(scope_metrics) = rm.get("scopeMetrics").and_then(|v| v.as_array()) else {
            continue;
        };
        for sm in scope_metrics {
            let Some(metrics) = sm.get("metrics").and_then(|v| v.as_array()) else {
                continue;
            };
            for m in metrics {
                if m.get("name").and_then(|n| n.as_str()) != Some(filter) {
                    continue;
                }
                let group_keys: Vec<String> = spec
                    .group_by
                    .iter()
                    .map(|gk| gk.extract_with_resource(resource_attrs.map(|v| v.as_slice()), entry))
                    .collect();
                let value = if needs_numeric {
                    first_data_point_value(m)
                } else {
                    None
                };
                inputs.push((group_keys, value));
            }
        }
    }

    if inputs.is_empty() {
        return vec![(entry_level_keys(), None)];
    }
    inputs
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

// ---------------------------------------------------------------------------
// Incremental aggregation
//
// `aggregate_entries` above rebuilds every bucket from scratch. On the polling
// hot path that meant re-decoding *every* entry's OTLP payload on *every* tick,
// which dominated tick latency in production. `AggregationState` instead keeps
// a running per-(bucket, group) accumulator: an entry's contribution is decoded
// when it is ingested (`add_entry`) and subtracted again when it falls out of
// the lookback window (`remove_oldest`). `finalize` then just reads the
// accumulators, with no payload decoding. This turns per-tick decode cost from
// O(all live entries) into O(entries ingested this tick).
//
// `add_entry` delegates to `collect_aggregation_inputs`, which decodes an
// entry's payload at most once (the numeric value and every resource-derived
// group_by key share a single decode). Combined with the per-tick decode now
// scaling with newly-ingested entries rather than accumulated history, this is
// what keeps tick latency flat.
//
// The output matches `aggregate_entries` for the same set of live entries
// (verified by the equivalence tests below), with two caveats that do not arise
// for real dashboard data: (a) `sum`/`avg` can differ by a last-ULP amount from
// repeated add/subtract, and (b) percentile sample ordering uses `f64::total_cmp`
// rather than the slice version's `partial_cmp`, so NaN samples would order
// differently -- but OTLP numeric values decoded from JSON are never NaN.
// ---------------------------------------------------------------------------

/// Total-order wrapper around `f64` so samples can be kept in a `BTreeMap`.
/// Uses `f64::total_cmp`, which orders NaN deterministically.
#[derive(Debug, Clone, Copy, PartialEq)]
struct TotalF64(f64);

impl Eq for TotalF64 {}

impl PartialOrd for TotalF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TotalF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Multiset of samples supporting add, remove, and nearest-rank percentile.
/// Backs the percentile accumulators so a pruned entry's sample can be removed
/// without an O(n) scan, while reproducing `percentile()`'s nearest-rank result.
#[derive(Debug, Clone, Default)]
struct SampleMultiset {
    counts: BTreeMap<TotalF64, u64>,
    total: u64,
}

impl SampleMultiset {
    fn insert(&mut self, v: f64) {
        *self.counts.entry(TotalF64(v)).or_insert(0) += 1;
        self.total += 1;
    }

    fn remove(&mut self, v: f64) {
        // Callers only remove values they previously inserted (each removal is
        // paired with an `EntryContribution` recorded at insert time), so the
        // key must be present and `total` must stay equal to the sum of counts.
        // The debug_assert guards that invariant in tests without costing release
        // builds; saturating_sub keeps a broken invariant from underflowing.
        debug_assert!(
            self.counts.contains_key(&TotalF64(v)),
            "SampleMultiset::remove of an absent sample {v} -- add/remove are unbalanced"
        );
        if let Some(c) = self.counts.get_mut(&TotalF64(v)) {
            *c -= 1;
            if *c == 0 {
                self.counts.remove(&TotalF64(v));
            }
            self.total = self.total.saturating_sub(1);
        }
    }

    /// Nearest-rank percentile, matching [`percentile`] on the equivalent
    /// sorted sample slice: rank = ceil(p * total), 1-indexed.
    fn percentile(&self, p: f64) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let rank = (p * self.total as f64).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;
        for (val, cnt) in &self.counts {
            cumulative += *cnt;
            if cumulative >= rank {
                return val.0;
            }
        }
        // Unreachable when total > 0, but fall back to the largest sample.
        self.counts.keys().next_back().map_or(0.0, |k| k.0)
    }
}

/// Running accumulator for one (bucket, group) slot that supports removal.
#[derive(Debug, Clone)]
struct IncrementalAccumulator {
    func: AggFn,
    count: u64,
    sum: f64,
    /// `Some` only for percentile functions.
    samples: Option<SampleMultiset>,
}

impl IncrementalAccumulator {
    fn new(func: AggFn) -> Self {
        Self {
            func,
            count: 0,
            sum: 0.0,
            samples: matches!(func, AggFn::P50 | AggFn::P95 | AggFn::P99)
                .then(SampleMultiset::default),
        }
    }

    fn add(&mut self, value: Option<f64>) {
        self.count += 1;
        if let Some(v) = value {
            self.sum += v;
            if let Some(s) = &mut self.samples {
                s.insert(v);
            }
        }
    }

    fn remove(&mut self, value: Option<f64>) {
        self.count = self.count.saturating_sub(1);
        if let Some(v) = value {
            self.sum -= v;
            if let Some(s) = &mut self.samples {
                s.remove(v);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn finalize(&self, spec: &CompiledAggregation) -> f64 {
        match self.func {
            AggFn::Count => self.count as f64,
            AggFn::Sum => self.sum,
            AggFn::Avg => {
                if self.count == 0 {
                    0.0
                } else {
                    self.sum / self.count as f64
                }
            }
            AggFn::P50 => self.samples.as_ref().map_or(0.0, |s| s.percentile(0.50)),
            AggFn::P95 => self.samples.as_ref().map_or(0.0, |s| s.percentile(0.95)),
            AggFn::P99 => self.samples.as_ref().map_or(0.0, |s| s.percentile(0.99)),
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

/// One entry's recorded contribution to the running accumulators, kept so it
/// can be subtracted exactly when the entry is pruned.
#[derive(Debug, Clone)]
struct EntryContribution {
    bucket_start_ms: i64,
    /// The (group_keys, value) pairs this entry actually added (after the
    /// `needs_numeric` filter), in the same form fed to `IncrementalAccumulator`.
    added: Vec<(Vec<String>, Option<f64>)>,
}

/// Incrementally-maintained aggregation for one viewer.
///
/// Invariant: `per_entry` is parallel to the live `ViewerState::entries`
/// (oldest first) and `buckets` reflects exactly the contributions recorded in
/// `per_entry`. Callers must mirror every `entries` mutation: push -> [`add_entry`],
/// front-drain of `n` -> [`remove_oldest(n)`](Self::remove_oldest).
#[derive(Debug, Clone)]
pub struct AggregationState {
    spec: CompiledAggregation,
    per_entry: std::collections::VecDeque<EntryContribution>,
    buckets: BTreeMap<(i64, Vec<String>), IncrementalAccumulator>,
}

impl AggregationState {
    pub fn new(spec: CompiledAggregation) -> Self {
        Self {
            spec,
            per_entry: std::collections::VecDeque::new(),
            buckets: BTreeMap::new(),
        }
    }

    /// Builds state by folding `entries` in order. Used when there is no
    /// running state to maintain incrementally (initial load with pre-existing
    /// entries, or after a spec change).
    pub fn from_entries(spec: CompiledAggregation, entries: &[NormalizedEntry]) -> Self {
        let mut state = Self::new(spec);
        for entry in entries {
            state.add_entry(entry);
        }
        state
    }

    /// Decodes the entry once, records its contribution, and folds it into the
    /// running accumulators.
    pub fn add_entry(&mut self, entry: &NormalizedEntry) {
        let bucket_start_ms =
            bucket_start(entry.observed_at.timestamp_millis(), self.spec.bucket_ms);
        let needs_numeric = self.spec.func.requires_numeric();
        let inputs = collect_aggregation_inputs(entry, &self.spec);

        let mut added = Vec::with_capacity(inputs.len());
        for (group_keys, value) in inputs {
            if needs_numeric && value.is_none() {
                continue;
            }
            self.buckets
                .entry((bucket_start_ms, group_keys.clone()))
                .or_insert_with(|| IncrementalAccumulator::new(self.spec.func))
                .add(value);
            added.push((group_keys, value));
        }
        self.per_entry.push_back(EntryContribution {
            bucket_start_ms,
            added,
        });
    }

    /// Subtracts the contributions of the `count` oldest entries, removing any
    /// bucket that becomes empty. Mirrors a front-drain of `ViewerState::entries`.
    pub fn remove_oldest(&mut self, count: usize) {
        for _ in 0..count {
            let Some(contrib) = self.per_entry.pop_front() else {
                break;
            };
            for (group_keys, value) in contrib.added {
                let key = (contrib.bucket_start_ms, group_keys);
                if let Some(slot) = self.buckets.get_mut(&key) {
                    slot.remove(value);
                    if slot.is_empty() {
                        self.buckets.remove(&key);
                    }
                }
            }
        }
    }

    /// Materializes the current buckets. `BTreeMap` iteration yields them sorted
    /// by `(bucket_start_ms, group_keys)`, matching `aggregate_entries`.
    pub fn finalize(&self) -> Vec<Bucket> {
        self.buckets
            .iter()
            .map(|((bucket_start_ms, group_keys), acc)| Bucket {
                bucket_start_ms: *bucket_start_ms,
                group_keys: group_keys.clone(),
                value: acc.finalize(&self.spec),
            })
            .collect()
    }
}

/// Best-effort numeric value over an already-decoded metrics payload: the first
/// `asInt`/`asDouble` data point of the first metric, or `None` when there is no
/// usable data point. The `metric_name`-filtered aggregation path extracts its
/// own values via `first_data_point_value`, so this helper is only used by the
/// unfiltered path and takes no name filter. Callers decode the payload once
/// (see `collect_aggregation_inputs`) and reuse it across the value and group
/// keys. Preserves the original early-return when the first `ResourceMetrics`
/// block lacks `scopeMetrics`/`metrics`.
fn numeric_value_from_metrics_value(value: &Value) -> Option<f64> {
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

fn extract_resource_attribute(entry: &NormalizedEntry, key: &str) -> Option<String> {
    if entry.signal != Signal::Metrics {
        return None;
    }
    let value = payload_as_value(Signal::Metrics, &entry.payload)?;
    resource_attribute_from_metrics_value(&value, key)
}

/// Resource-attribute lookup over an already-decoded metrics payload. Split out
/// from [`extract_resource_attribute`] so a single decode can be shared across
/// the numeric value and every group_by key. Scans all `ResourceMetrics` blocks
/// and returns the first block that carries the requested attribute, identical
/// to the previous inline version.
fn resource_attribute_from_metrics_value(value: &Value, key: &str) -> Option<String> {
    let resource_metrics = value.get("resourceMetrics")?.as_array()?;
    for rm in resource_metrics {
        let Some(attrs) = rm
            .get("resource")
            .and_then(|v| v.get("attributes"))
            .and_then(|v| v.as_array())
        else {
            continue;
        };
        if let Some(sv) = attribute_string_value(attrs, key) {
            return Some(sv);
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
            GroupKey::K8sNodeName => "k8s.node.name",
            GroupKey::K8sClusterName => "k8s.cluster.name",
            GroupKey::K8sNamespace => "k8s.namespace.name",
            GroupKey::K8sPodName => "k8s.pod.name",
            GroupKey::HostName => "host.name",
        })
        .collect();
    let mut obj = serde_json::json!({
        "fn": spec.func.as_str(),
        "bucket_ms": spec.bucket_ms,
        "group_by": group_by,
    });
    if let Some(ref name) = spec.metric_name {
        obj["metric_name"] = serde_json::json!(name);
    }
    obj
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
            metric_name: None,
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
            metric_name: None,
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
            metric_name: None,
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
            metric_name: None,
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
            metric_name: None,
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
            metric_name: None,
        };
        let spec_p95 = CompiledAggregation {
            func: AggFn::P95,
            bucket_ms: 60_000,
            group_by: vec![],
            metric_name: None,
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
            metric_name: None,
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
            metric_name: None,
        };
        let value = aggregation_to_json(&spec);
        assert_eq!(
            value,
            json!({ "fn": "p95", "bucket_ms": 30_000, "group_by": ["service_name"] })
        );
    }

    fn metric_payload_with_resource_attrs(attrs: Vec<(&str, &str)>, value: u64) -> Bytes {
        let attributes: Vec<Value> = attrs
            .into_iter()
            .map(|(key, val)| json!({"key": key, "value": {"stringValue": val}}))
            .collect();
        Bytes::from(
            json!({
                "resourceMetrics": [{
                    "resource": { "attributes": attributes },
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

    fn multi_metric_payload() -> Bytes {
        Bytes::from(
            json!({
                "resourceMetrics": [{
                    "scopeMetrics": [{
                        "metrics": [
                            {
                                "name": "system.cpu.utilization",
                                "gauge": {
                                    "dataPoints": [{ "asDouble": 0.5 }]
                                }
                            },
                            {
                                "name": "system.memory.utilization",
                                "gauge": {
                                    "dataPoints": [{ "asDouble": 0.8 }]
                                }
                            }
                        ]
                    }]
                }]
            })
            .to_string(),
        )
    }

    #[test]
    fn parse_aggregation_parses_metric_name_field() {
        let spec = parse_aggregation(&json!({
            "aggregation": {
                "fn": "sum",
                "bucket_ms": 60_000,
                "metric_name": "system.cpu.utilization"
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(spec.metric_name, Some("system.cpu.utilization".to_string()));
    }

    #[test]
    fn aggregate_count_with_group_by_resource_attributes() {
        struct Case {
            group_key: GroupKey,
            attr_key: &'static str,
            inputs: &'static [(&'static str, u64)],
            expected: &'static [(&'static str, f64)],
        }
        let cases = &[
            Case {
                group_key: GroupKey::K8sNodeName,
                attr_key: "k8s.node.name",
                inputs: &[("node-a", 1), ("node-b", 2), ("node-a", 3)],
                expected: &[("node-a", 2.0), ("node-b", 1.0)],
            },
            Case {
                group_key: GroupKey::K8sClusterName,
                attr_key: "k8s.cluster.name",
                inputs: &[("prod", 1), ("staging", 2), ("prod", 3)],
                expected: &[("prod", 2.0), ("staging", 1.0)],
            },
            Case {
                group_key: GroupKey::K8sNamespace,
                attr_key: "k8s.namespace.name",
                inputs: &[("default", 1), ("kube-system", 2)],
                expected: &[("default", 1.0), ("kube-system", 1.0)],
            },
            Case {
                group_key: GroupKey::K8sPodName,
                attr_key: "k8s.pod.name",
                inputs: &[("pod-1", 1), ("pod-2", 2), ("pod-1", 3)],
                expected: &[("pod-1", 2.0), ("pod-2", 1.0)],
            },
            Case {
                group_key: GroupKey::HostName,
                attr_key: "host.name",
                inputs: &[("host-1", 1), ("host-2", 2)],
                expected: &[("host-1", 1.0), ("host-2", 1.0)],
            },
        ];
        for case in cases {
            let entries: Vec<_> = case
                .inputs
                .iter()
                .enumerate()
                .map(|(i, (name, val))| {
                    entry(
                        Signal::Metrics,
                        60_000 + (i as i64 * 5_000),
                        None,
                        metric_payload_with_resource_attrs(vec![(case.attr_key, name)], *val),
                    )
                })
                .collect();
            let spec = CompiledAggregation {
                func: AggFn::Count,
                bucket_ms: 60_000,
                group_by: vec![case.group_key],
                metric_name: None,
            };
            let buckets = aggregate_entries(&spec, &entries);
            assert_eq!(
                buckets.len(),
                case.expected.len(),
                "bucket count mismatch for {}",
                case.attr_key
            );
            for (i, (exp_name, exp_val)) in case.expected.iter().enumerate() {
                assert_eq!(
                    buckets[i].group_keys,
                    vec![(*exp_name).to_string()],
                    "group key mismatch for {}",
                    case.attr_key
                );
                assert_eq!(
                    buckets[i].value, *exp_val,
                    "value mismatch for {}",
                    case.attr_key
                );
            }
        }
    }

    #[test]
    fn extract_resource_attribute_returns_unknown_for_missing_key() {
        let entry = entry(
            Signal::Metrics,
            60_000,
            None,
            metric_payload_with_resource_attrs(vec![("k8s.node.name", "node-a")], 1),
        );
        let result = GroupKey::K8sClusterName.extract(&entry);
        assert_eq!(result, "unknown");
    }

    #[test]
    fn extract_resource_attribute_returns_unknown_for_non_metric_signal() {
        let entry = entry(Signal::Traces, 60_000, None, Bytes::new());
        let result = GroupKey::K8sNodeName.extract(&entry);
        assert_eq!(result, "unknown");
    }

    #[test]
    fn service_name_group_key_returns_unknown_when_not_set() {
        let spec = CompiledAggregation {
            func: AggFn::Count,
            bucket_ms: 60_000,
            group_by: vec![GroupKey::ServiceName],
            metric_name: None,
        };
        let entries = vec![entry(Signal::Traces, 60_000, None, Bytes::new())];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].group_keys, vec!["unknown".to_string()]);
    }

    #[test]
    fn aggregate_filters_by_metric_name() {
        let spec = CompiledAggregation {
            func: AggFn::Sum,
            bucket_ms: 60_000,
            group_by: vec![],
            metric_name: Some("system.cpu.utilization".to_string()),
        };
        let entries = vec![entry(Signal::Metrics, 60_000, None, multi_metric_payload())];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        assert!((buckets[0].value - 0.5).abs() < 1e-9);
    }

    #[test]
    fn aggregate_no_metric_name_filter_returns_first_metric_when_none() {
        let spec = CompiledAggregation {
            func: AggFn::Sum,
            bucket_ms: 60_000,
            group_by: vec![],
            metric_name: None,
        };
        let entries = vec![entry(Signal::Metrics, 60_000, None, multi_metric_payload())];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        // Without filter, returns first metric value (0.5)
        assert!((buckets[0].value - 0.5).abs() < 1e-9);
    }

    fn multi_resource_pod_cpu_payload(pods: &[(&str, f64)]) -> Bytes {
        let resource_metrics: Vec<Value> = pods
            .iter()
            .map(|(pod, value)| {
                json!({
                    "resource": {
                        "attributes": [
                            { "key": "k8s.pod.name", "value": { "stringValue": pod } },
                            { "key": "k8s.namespace.name", "value": { "stringValue": "default" } }
                        ]
                    },
                    "scopeMetrics": [{
                        "metrics": [
                            {
                                "name": "k8s.pod.cpu.usage",
                                "gauge": { "dataPoints": [{ "asDouble": value }] }
                            },
                            {
                                "name": "k8s.pod.memory.usage",
                                "gauge": { "dataPoints": [{ "asDouble": value * 1000.0 }] }
                            }
                        ]
                    }]
                })
            })
            .collect();
        Bytes::from(json!({ "resourceMetrics": resource_metrics }).to_string())
    }

    #[test]
    fn aggregate_emits_one_series_per_matching_resource_metric() {
        let spec = CompiledAggregation {
            func: AggFn::Avg,
            bucket_ms: 60_000,
            group_by: vec![GroupKey::K8sPodName],
            metric_name: Some("k8s.pod.cpu.usage".to_string()),
        };
        let payload =
            multi_resource_pod_cpu_payload(&[("pod-a", 0.10), ("pod-b", 0.50), ("pod-c", 1.20)]);
        let entries = vec![entry(Signal::Metrics, 60_000, None, payload)];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 3, "one bucket per pod");

        let mut by_pod: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
        for b in buckets {
            assert_eq!(b.group_keys.len(), 1);
            by_pod.insert(b.group_keys[0].clone(), b.value);
        }
        assert!((by_pod["pod-a"] - 0.10).abs() < 1e-9);
        assert!((by_pod["pod-b"] - 0.50).abs() < 1e-9);
        assert!((by_pod["pod-c"] - 1.20).abs() < 1e-9);
    }

    #[test]
    fn aggregate_sum_metric_name_filter_totals_all_matching_resources() {
        let spec = CompiledAggregation {
            func: AggFn::Sum,
            bucket_ms: 60_000,
            group_by: vec![],
            metric_name: Some("k8s.pod.cpu.usage".to_string()),
        };
        let payload =
            multi_resource_pod_cpu_payload(&[("pod-a", 0.10), ("pod-b", 0.50), ("pod-c", 1.40)]);
        let entries = vec![entry(Signal::Metrics, 60_000, None, payload)];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        assert!((buckets[0].value - 2.0).abs() < 1e-9);
    }

    #[test]
    fn aggregate_count_metric_name_filter_counts_data_points() {
        let spec = CompiledAggregation {
            func: AggFn::Count,
            bucket_ms: 60_000,
            group_by: vec![],
            metric_name: Some("k8s.pod.cpu.usage".to_string()),
        };
        let payload =
            multi_resource_pod_cpu_payload(&[("pod-a", 0.10), ("pod-b", 0.20), ("pod-c", 0.30)]);
        let entries = vec![entry(Signal::Metrics, 60_000, None, payload)];

        let buckets = aggregate_entries(&spec, &entries);
        assert_eq!(buckets.len(), 1);
        assert!((buckets[0].value - 3.0).abs() < 1e-9);
    }

    #[test]
    fn aggregate_metric_name_filter_skips_when_no_match() {
        let spec = CompiledAggregation {
            func: AggFn::Sum,
            bucket_ms: 60_000,
            group_by: vec![],
            metric_name: Some("nonexistent.metric".to_string()),
        };
        let entries = vec![entry(Signal::Metrics, 60_000, None, multi_metric_payload())];

        let buckets = aggregate_entries(&spec, &entries);
        // No matching metric → no numeric value → entry skipped.
        assert!(buckets.is_empty());
    }

    #[test]
    fn aggregation_to_json_emits_metric_name_when_set() {
        let spec = CompiledAggregation {
            func: AggFn::Avg,
            bucket_ms: 30_000,
            group_by: vec![],
            metric_name: Some("system.cpu.utilization".to_string()),
        };
        let value = aggregation_to_json(&spec);
        assert_eq!(
            value,
            json!({
                "fn": "avg",
                "bucket_ms": 30_000,
                "group_by": [],
                "metric_name": "system.cpu.utilization"
            })
        );
    }

    #[test]
    fn aggregation_to_json_omits_metric_name_when_none() {
        let spec = CompiledAggregation {
            func: AggFn::Avg,
            bucket_ms: 30_000,
            group_by: vec![],
            metric_name: None,
        };
        let value = aggregation_to_json(&spec);
        assert_eq!(
            value,
            json!({ "fn": "avg", "bucket_ms": 30_000, "group_by": [] })
        );
    }

    #[test]
    fn parse_aggregation_parses_k8s_group_by_keys() {
        let spec = parse_aggregation(&json!({
            "aggregation": {
                "fn": "count",
                "bucket_ms": 60_000,
                "group_by": ["k8s.node.name", "k8s.cluster.name", "k8s.namespace.name", "k8s.pod.name", "host.name", "unknown_key"]
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            spec.group_by,
            vec![
                GroupKey::K8sNodeName,
                GroupKey::K8sClusterName,
                GroupKey::K8sNamespace,
                GroupKey::K8sPodName,
                GroupKey::HostName,
            ]
        );
    }

    #[test]
    fn aggregation_to_json_round_trips_new_group_keys() {
        let spec = CompiledAggregation {
            func: AggFn::Count,
            bucket_ms: 60_000,
            group_by: vec![GroupKey::K8sNodeName, GroupKey::HostName],
            metric_name: None,
        };
        let value = aggregation_to_json(&spec);
        assert_eq!(
            value,
            json!({
                "fn": "count",
                "bucket_ms": 60_000,
                "group_by": ["k8s.node.name", "host.name"]
            })
        );
    }

    // --- incremental aggregation ------------------------------------------
    // The incremental path must produce byte-for-byte identical buckets to the
    // full `aggregate_entries` recompute for the same set of live entries. All
    // metric values here are integers (exact as f64) so sum/avg/percentile
    // comparisons are exact despite incremental add/subtract.

    /// Asserts that folding all `entries` then pruning the oldest `prune` of
    /// them yields the same buckets as a full recompute over the survivors.
    fn assert_incremental_matches_full(
        spec: &CompiledAggregation,
        entries: &[NormalizedEntry],
        prune: usize,
    ) {
        let full = aggregate_entries(spec, &entries[prune..]);

        let mut state = AggregationState::from_entries(spec.clone(), entries);
        state.remove_oldest(prune);
        let incremental = state.finalize();

        assert_eq!(
            incremental, full,
            "incremental != full for fn={:?} prune={prune}",
            spec.func
        );
    }

    /// Entries spanning three buckets (bucket_ms=60_000), with the middle bucket
    /// straddling the prune boundary so partial-bucket pruning is exercised.
    fn cross_bucket_metric_entries() -> Vec<NormalizedEntry> {
        vec![
            entry(Signal::Metrics, 10_000, Some("a"), metric_payload(5)), // bucket 0
            entry(Signal::Metrics, 20_000, Some("b"), metric_payload(15)), // bucket 0
            entry(Signal::Metrics, 70_000, Some("a"), metric_payload(25)), // bucket 60_000
            entry(Signal::Metrics, 80_000, Some("b"), metric_payload(35)), // bucket 60_000
            entry(Signal::Metrics, 130_000, Some("a"), metric_payload(45)), // bucket 120_000
        ]
    }

    fn spec(func: AggFn, group_by: Vec<GroupKey>) -> CompiledAggregation {
        CompiledAggregation {
            func,
            bucket_ms: 60_000,
            group_by,
            metric_name: None,
        }
    }

    #[test]
    fn incremental_matches_full_all_funcs_no_prune() {
        let entries = cross_bucket_metric_entries();
        for func in [
            AggFn::Count,
            AggFn::Sum,
            AggFn::Avg,
            AggFn::P50,
            AggFn::P95,
            AggFn::P99,
            AggFn::Rate,
        ] {
            assert_incremental_matches_full(&spec(func, vec![]), &entries, 0);
        }
    }

    #[test]
    fn incremental_matches_full_all_funcs_with_partial_bucket_prune() {
        let entries = cross_bucket_metric_entries();
        // prune=3 drops both bucket-0 entries and the first bucket-60_000 entry,
        // leaving the second bucket-60_000 entry and the bucket-120_000 entry.
        for func in [
            AggFn::Count,
            AggFn::Sum,
            AggFn::Avg,
            AggFn::P50,
            AggFn::P95,
            AggFn::P99,
            AggFn::Rate,
        ] {
            assert_incremental_matches_full(&spec(func, vec![]), &entries, 3);
        }
    }

    #[test]
    fn incremental_matches_full_with_group_by_and_prune() {
        let entries = vec![
            entry(
                Signal::Metrics,
                10_000,
                None,
                metric_payload_with_resource_attrs(vec![("service.name", "svc-a")], 5),
            ),
            entry(
                Signal::Metrics,
                20_000,
                None,
                metric_payload_with_resource_attrs(vec![("service.name", "svc-b")], 15),
            ),
            entry(
                Signal::Metrics,
                70_000,
                None,
                metric_payload_with_resource_attrs(vec![("service.name", "svc-a")], 25),
            ),
            entry(
                Signal::Metrics,
                80_000,
                None,
                metric_payload_with_resource_attrs(vec![("service.name", "svc-a")], 35),
            ),
        ];
        for prune in 0..=entries.len() {
            assert_incremental_matches_full(
                &spec(AggFn::Sum, vec![GroupKey::ServiceName]),
                &entries,
                prune,
            );
        }
    }

    #[test]
    fn incremental_percentile_multiset_handles_duplicates_and_removal() {
        // Many duplicate values within one bucket, then prune some off the front;
        // the multiset must reproduce nearest-rank on the surviving samples.
        let values = [3, 3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5];
        let entries: Vec<_> = values
            .iter()
            .map(|&v| entry(Signal::Metrics, 60_000, None, metric_payload(v)))
            .collect();
        for func in [AggFn::P50, AggFn::P95, AggFn::P99] {
            for prune in 0..=entries.len() {
                assert_incremental_matches_full(&spec(func, vec![]), &entries, prune);
            }
        }
    }

    #[test]
    fn incremental_remove_all_empties_buckets() {
        let entries = cross_bucket_metric_entries();
        let mut state = AggregationState::from_entries(spec(AggFn::Sum, vec![]), &entries);
        state.remove_oldest(entries.len());
        assert!(
            state.finalize().is_empty(),
            "removing every entry should leave no buckets"
        );
    }

    #[test]
    fn incremental_remove_more_than_present_is_saturating() {
        let entries = cross_bucket_metric_entries();
        let mut state = AggregationState::from_entries(spec(AggFn::Count, vec![]), &entries);
        // Asking to remove more than were added must not panic and must empty out.
        state.remove_oldest(entries.len() + 10);
        assert!(state.finalize().is_empty());
    }

    // --- decode-once / multi-resource semantics ---------------------------
    // `collect_aggregation_inputs` now decodes a payload at most once per entry
    // and reuses it for the numeric value and every group_by key. These tests
    // pin the exact traversal semantics of the no-`metric_name` path against
    // multi-`ResourceMetrics` payloads, so the decode-once refactor cannot
    // silently change which block a value or group key comes from.

    /// Two `ResourceMetrics` blocks; the metric "test" sits in `sum.dataPoints`.
    /// `attrs` sets each block's `resource.attributes`. A block with `value:
    /// None` omits `scopeMetrics` entirely (used to exercise the numeric
    /// early-return quirk).
    fn two_block_metrics_payload(
        block_a: (Vec<(&str, &str)>, Option<u64>),
        block_b: (Vec<(&str, &str)>, Option<u64>),
    ) -> Bytes {
        let block = |attrs: Vec<(&str, &str)>, value: Option<u64>| -> Value {
            let attributes: Vec<Value> = attrs
                .into_iter()
                .map(|(k, v)| json!({"key": k, "value": {"stringValue": v}}))
                .collect();
            let mut rm = json!({ "resource": { "attributes": attributes } });
            if let Some(v) = value {
                rm["scopeMetrics"] = json!([{
                    "metrics": [{ "name": "test", "sum": { "dataPoints": [{ "asInt": v.to_string() }] } }]
                }]);
            }
            rm
        };
        Bytes::from(
            json!({ "resourceMetrics": [block(block_a.0, block_a.1), block(block_b.0, block_b.1)] })
                .to_string(),
        )
    }

    #[test]
    fn non_filter_numeric_takes_first_block_value() {
        // sum, no group_by, no metric_name: the single input's value is the
        // first metric's first data point (block A = 10), not block B's 20.
        let payload = two_block_metrics_payload(
            (vec![("service.name", "svc-a")], Some(10)),
            (vec![("service.name", "svc-b")], Some(20)),
        );
        let entries = vec![entry(Signal::Metrics, 60_000, Some("svc-a"), payload)];
        let buckets = aggregate_entries(&spec(AggFn::Sum, vec![]), &entries);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].value, 10.0);
    }

    #[test]
    fn non_filter_numeric_preserves_first_block_early_return() {
        // Pinned pre-existing quirk: if the FIRST ResourceMetrics block has no
        // scopeMetrics, numeric extraction returns None even though block B
        // carries a value -- so the entry contributes nothing to a sum.
        let payload = two_block_metrics_payload(
            (vec![("service.name", "svc-a")], None), // no scopeMetrics
            (vec![("service.name", "svc-b")], Some(20)),
        );
        let entries = vec![entry(Signal::Metrics, 60_000, Some("svc-a"), payload)];
        let buckets = aggregate_entries(&spec(AggFn::Sum, vec![]), &entries);
        assert!(
            buckets.is_empty(),
            "first-block early-return must yield no value"
        );
    }

    #[test]
    fn non_filter_group_key_scans_all_blocks_for_attribute() {
        // count, group_by [k8s.pod.name], no metric_name: the group key scans
        // every block and uses the first one carrying the attribute (block B),
        // even though block A comes first.
        let payload = two_block_metrics_payload(
            (vec![("service.name", "svc-a")], Some(10)),
            (vec![("k8s.pod.name", "pod-b")], Some(20)),
        );
        let entries = vec![entry(Signal::Metrics, 60_000, Some("svc-a"), payload)];
        let buckets = aggregate_entries(&spec(AggFn::Count, vec![GroupKey::K8sPodName]), &entries);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].group_keys, vec!["pod-b".to_string()]);
        assert_eq!(buckets[0].value, 1.0);
    }

    #[test]
    fn non_filter_single_decode_serves_value_and_multiple_group_keys() {
        // sum, group_by [service_name, k8s.pod.name]: service_name comes from the
        // entry, k8s.pod.name from the (singly-decoded) payload, value from the
        // payload -- all from one decode. Single resource block here.
        let payload = metric_payload_with_resource_attrs(vec![("k8s.pod.name", "pod-1")], 42);
        let entries = vec![entry(Signal::Metrics, 60_000, Some("svc-1"), payload)];
        let buckets = aggregate_entries(
            &spec(
                AggFn::Sum,
                vec![GroupKey::ServiceName, GroupKey::K8sPodName],
            ),
            &entries,
        );
        assert_eq!(buckets.len(), 1);
        assert_eq!(
            buckets[0].group_keys,
            vec!["svc-1".to_string(), "pod-1".to_string()]
        );
        assert_eq!(buckets[0].value, 42.0);
    }

    #[test]
    fn non_filter_no_decode_needed_for_count_by_service_name() {
        // count + group_by [service_name] needs neither the numeric value nor a
        // payload-derived key, so service_name must still resolve from the entry
        // (a non-metric payload here proves no decode is required).
        let entries = vec![entry(
            Signal::Traces,
            60_000,
            Some("svc-x"),
            Bytes::from_static(b"not-otlp"),
        )];
        let buckets = aggregate_entries(&spec(AggFn::Count, vec![GroupKey::ServiceName]), &entries);
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].group_keys, vec!["svc-x".to_string()]);
        assert_eq!(buckets[0].value, 1.0);
    }
}
