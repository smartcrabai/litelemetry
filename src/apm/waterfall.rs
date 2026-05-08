//! Trace waterfall builder.
//!
//! Given a list of [`NormalizedEntry`] values for the `Traces` signal and a
//! target `trace_id`, this module produces a [`TraceWaterfall`] suitable for
//! rendering as a gantt-style chart.

use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_http::attribute_string_value;
use crate::ingest::otlp_pb::payload_as_value;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WaterfallError {
    /// No spans were found for the requested trace_id.
    #[error("trace {0} not found")]
    TraceNotFound(String),
}

/// One row in the waterfall, representing a single OTLP span.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct WaterfallSpan {
    pub id: String,
    pub parent_id: String,
    pub name: String,
    pub service_name: String,
    /// Span start, in milliseconds offset from the trace's earliest start.
    pub start_ms: f64,
    /// Span duration, in milliseconds. Always >= 0.
    pub duration_ms: f64,
    /// OTLP status code: 0=unset, 1=ok, 2=error.
    pub status: u64,
    /// Span attributes as `{key: value}` map (string-coerced for non-string types).
    pub attributes: serde_json::Map<String, Value>,
}

/// Output payload for `GET /api/traces/{trace_id}/waterfall`.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TraceWaterfall {
    pub trace_id: String,
    pub spans: Vec<WaterfallSpan>,
}

/// Builds a [`TraceWaterfall`] for `trace_id` by scanning `entries`.
///
/// Spans are returned in pre-order DFS by parent/child relation; siblings are
/// sorted by `start_ms`. Roots (spans with empty or unknown parent_id) are
/// emitted first. `start_ms` is normalized so the trace's earliest start is 0.
///
/// Returns [`WaterfallError::TraceNotFound`] if no spans match the trace_id.
pub fn build_waterfall(
    entries: &[NormalizedEntry],
    trace_id: &str,
) -> Result<TraceWaterfall, WaterfallError> {
    let raw_spans = collect_raw_spans(entries, trace_id);
    if raw_spans.is_empty() {
        return Err(WaterfallError::TraceNotFound(trace_id.to_string()));
    }

    let earliest_start_ns = raw_spans
        .iter()
        .map(|s| s.start_ns)
        .min()
        .unwrap_or_default();

    let normalized: Vec<WaterfallSpan> = raw_spans
        .into_iter()
        .map(|s| WaterfallSpan {
            id: s.id,
            parent_id: s.parent_id,
            name: s.name,
            service_name: s.service_name,
            start_ms: ns_to_ms(s.start_ns.saturating_sub(earliest_start_ns)),
            duration_ms: ns_to_ms(s.end_ns.saturating_sub(s.start_ns)),
            status: s.status,
            attributes: s.attributes,
        })
        .collect();

    let ordered = sort_spans_dfs(normalized);

    Ok(TraceWaterfall {
        trace_id: trace_id.to_string(),
        spans: ordered,
    })
}

struct RawSpan {
    id: String,
    parent_id: String,
    name: String,
    service_name: String,
    start_ns: u64,
    end_ns: u64,
    status: u64,
    attributes: serde_json::Map<String, Value>,
}

fn collect_raw_spans(entries: &[NormalizedEntry], trace_id: &str) -> Vec<RawSpan> {
    let mut spans = Vec::new();
    for entry in entries {
        if entry.signal != Signal::Traces {
            continue;
        }
        let Some(value) = payload_as_value(Signal::Traces, &entry.payload) else {
            continue;
        };
        let Some(resource_spans) = value.get("resourceSpans").and_then(Value::as_array) else {
            continue;
        };

        for rs in resource_spans {
            let service_name = rs
                .get("resource")
                .and_then(|r| r.get("attributes"))
                .and_then(Value::as_array)
                .and_then(|attrs| attribute_string_value(attrs, "service.name"))
                .unwrap_or_default();

            let Some(scope_spans) = rs.get("scopeSpans").and_then(Value::as_array) else {
                continue;
            };
            for ss in scope_spans {
                let Some(span_list) = ss.get("spans").and_then(Value::as_array) else {
                    continue;
                };
                for span in span_list {
                    let span_trace_id = span
                        .get("traceId")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    if span_trace_id != trace_id {
                        continue;
                    }
                    spans.push(RawSpan {
                        id: span
                            .get("spanId")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        parent_id: span
                            .get("parentSpanId")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        name: span
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        service_name: service_name.clone(),
                        start_ns: parse_nano(span.get("startTimeUnixNano")),
                        end_ns: parse_nano(span.get("endTimeUnixNano")),
                        status: span
                            .get("status")
                            .and_then(|s| s.get("code"))
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        attributes: extract_attributes(span.get("attributes")),
                    });
                }
            }
        }
    }
    spans
}

/// OTLP can encode nano timestamps as a JSON number or string.
fn parse_nano(value: Option<&Value>) -> u64 {
    let Some(v) = value else { return 0 };
    if let Some(n) = v.as_u64() {
        return n;
    }
    if let Some(s) = v.as_str() {
        return s.parse::<u64>().unwrap_or(0);
    }
    0
}

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

/// Flattens an OTLP attribute list into a JSON object. Non-string AnyValue types
/// are coerced to their natural JSON representation (numbers, bools) or stringified.
fn extract_attributes(value: Option<&Value>) -> serde_json::Map<String, Value> {
    let mut map = serde_json::Map::new();
    let Some(arr) = value.and_then(Value::as_array) else {
        return map;
    };
    for kv in arr {
        let Some(key) = kv.get("key").and_then(Value::as_str) else {
            continue;
        };
        let Some(any_value) = kv.get("value") else {
            continue;
        };
        if let Some(v) = any_value_to_json(any_value) {
            map.insert(key.to_string(), v);
        }
    }
    map
}

fn any_value_to_json(value: &Value) -> Option<Value> {
    if let Some(v) = value.get("stringValue") {
        return Some(v.clone());
    }
    if let Some(v) = value.get("boolValue") {
        return Some(v.clone());
    }
    if let Some(v) = value.get("intValue") {
        // OTLP often serializes 64-bit integers as strings. Try numeric first.
        if v.is_number() {
            return Some(v.clone());
        }
        if let Some(s) = v.as_str()
            && let Ok(n) = s.parse::<i64>()
        {
            return Some(Value::from(n));
        }
        return Some(v.clone());
    }
    if let Some(v) = value.get("doubleValue") {
        return Some(v.clone());
    }
    if let Some(v) = value.get("bytesValue") {
        return Some(v.clone());
    }
    if let Some(v) = value.get("arrayValue") {
        return Some(v.clone());
    }
    if let Some(v) = value.get("kvlistValue") {
        return Some(v.clone());
    }
    None
}

/// Sort spans into a DFS pre-order so the waterfall renders as a hierarchy.
/// Siblings (and roots) are ordered by `start_ms` ascending. Spans whose
/// declared parent is missing in `spans` are treated as additional roots so
/// no span is silently dropped.
fn sort_spans_dfs(spans: Vec<WaterfallSpan>) -> Vec<WaterfallSpan> {
    let known_ids: std::collections::HashSet<&str> = spans.iter().map(|s| s.id.as_str()).collect();

    let mut by_parent: HashMap<String, Vec<usize>> = HashMap::new();
    let mut root_indices: Vec<usize> = Vec::new();
    for (idx, span) in spans.iter().enumerate() {
        let is_root = span.parent_id.is_empty() || !known_ids.contains(span.parent_id.as_str());
        if is_root {
            root_indices.push(idx);
        } else {
            by_parent
                .entry(span.parent_id.clone())
                .or_default()
                .push(idx);
        }
    }

    let sort_by_start = |indices: &mut Vec<usize>, spans: &[WaterfallSpan]| {
        indices.sort_by(|&a, &b| {
            spans[a]
                .start_ms
                .partial_cmp(&spans[b].start_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    };

    sort_by_start(&mut root_indices, &spans);
    for children in by_parent.values_mut() {
        sort_by_start(children, &spans);
    }

    let mut order: Vec<usize> = Vec::with_capacity(spans.len());
    let mut stack: Vec<usize> = root_indices.into_iter().rev().collect();
    while let Some(idx) = stack.pop() {
        order.push(idx);
        if let Some(children) = by_parent.get(&spans[idx].id) {
            for &child in children.iter().rev() {
                stack.push(child);
            }
        }
    }

    // Defensive: if cyclic parent links left some spans unvisited, append
    // them at the end so the response always contains every collected span.
    if order.len() < spans.len() {
        let placed: std::collections::HashSet<usize> = order.iter().copied().collect();
        for i in 0..spans.len() {
            if !placed.contains(&i) {
                order.push(i);
            }
        }
    }

    let mut slots: Vec<Option<WaterfallSpan>> = spans.into_iter().map(Some).collect();
    order
        .into_iter()
        .filter_map(|idx| slots[idx].take())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;
    use bytes::Bytes;
    use chrono::Utc;
    use serde_json::json;

    fn make_trace_entry(payload: serde_json::Value) -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name: None,
            payload: Bytes::from(payload.to_string()),
        }
    }

    fn span_json(
        trace_id: &str,
        span_id: &str,
        parent_id: &str,
        name: &str,
        start_ns: u64,
        end_ns: u64,
        status: u64,
    ) -> serde_json::Value {
        json!({
            "traceId": trace_id,
            "spanId": span_id,
            "parentSpanId": parent_id,
            "name": name,
            "startTimeUnixNano": start_ns.to_string(),
            "endTimeUnixNano": end_ns.to_string(),
            "status": { "code": status },
            "attributes": [
                { "key": "http.method", "value": { "stringValue": "GET" } },
                { "key": "http.status_code", "value": { "intValue": "200" } }
            ]
        })
    }

    fn resource_spans(service: &str, spans: Vec<serde_json::Value>) -> serde_json::Value {
        json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": service } }
                    ]
                },
                "scopeSpans": [{
                    "scope": { "name": "test" },
                    "spans": spans
                }]
            }]
        })
    }

    #[test]
    fn test_build_waterfall_returns_spans_in_dfs_order() {
        // Given: a 3-level trace
        //   root (10..30)
        //   |-- child1 (12..18)
        //   |    `-- grand1 (13..16)
        //   `-- child2 (20..25)
        let trace_id = "00000000000000000000000000000001";
        let payload = resource_spans(
            "checkout",
            vec![
                span_json(trace_id, "child2", "root", "child2", 20, 25, 0),
                span_json(trace_id, "root", "", "root-op", 10, 30, 0),
                span_json(trace_id, "grand1", "child1", "grand1", 13, 16, 0),
                span_json(trace_id, "child1", "root", "child1", 12, 18, 0),
            ],
        );
        let entries = vec![make_trace_entry(payload)];

        // When
        let waterfall = build_waterfall(&entries, trace_id).unwrap();

        // Then: DFS pre-order, siblings by start_ms
        let names: Vec<&str> = waterfall.spans.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["root-op", "child1", "grand1", "child2"]);
    }

    #[test]
    fn test_build_waterfall_normalizes_start_ms_relative_to_trace() {
        // Given: trace starts at 10ns, root is 10ns, child is 13ns.
        let trace_id = "00000000000000000000000000000002";
        let payload = resource_spans(
            "svc",
            vec![
                span_json(trace_id, "root", "", "r", 10, 30, 0),
                span_json(trace_id, "child", "root", "c", 13, 18, 0),
            ],
        );
        let entries = vec![make_trace_entry(payload)];

        // When
        let waterfall = build_waterfall(&entries, trace_id).unwrap();

        // Then: root starts at 0ms, child starts at 3ns (=> 0.000003ms)
        assert!((waterfall.spans[0].start_ms - 0.0).abs() < 1e-9);
        assert!((waterfall.spans[1].start_ms - 0.000_003).abs() < 1e-9);
        assert!((waterfall.spans[0].duration_ms - 0.000_020).abs() < 1e-9);
    }

    #[test]
    fn test_build_waterfall_includes_service_name_and_attributes() {
        let trace_id = "00000000000000000000000000000003";
        let payload = resource_spans(
            "checkout-ui",
            vec![span_json(trace_id, "root", "", "render", 0, 100, 0)],
        );
        let entries = vec![make_trace_entry(payload)];

        let waterfall = build_waterfall(&entries, trace_id).unwrap();

        assert_eq!(waterfall.spans.len(), 1);
        assert_eq!(waterfall.spans[0].service_name, "checkout-ui");
        assert_eq!(
            waterfall.spans[0].attributes.get("http.method"),
            Some(&Value::from("GET"))
        );
        assert_eq!(
            waterfall.spans[0].attributes.get("http.status_code"),
            Some(&Value::from(200))
        );
    }

    #[test]
    fn test_build_waterfall_filters_by_trace_id() {
        let payload = json!({
            "resourceSpans": [{
                "resource": { "attributes": [] },
                "scopeSpans": [{
                    "scope": { "name": "x" },
                    "spans": [
                        span_json("aaaa", "s1", "", "a", 0, 5, 0),
                        span_json("bbbb", "s2", "", "b", 0, 5, 0)
                    ]
                }]
            }]
        });
        let entries = vec![make_trace_entry(payload)];

        let waterfall = build_waterfall(&entries, "aaaa").unwrap();
        assert_eq!(waterfall.spans.len(), 1);
        assert_eq!(waterfall.spans[0].id, "s1");
    }

    #[test]
    fn test_build_waterfall_returns_not_found_when_trace_missing() {
        let payload = resource_spans("svc", vec![span_json("aaaa", "root", "", "r", 0, 1, 0)]);
        let entries = vec![make_trace_entry(payload)];

        let err = build_waterfall(&entries, "nonexistent").unwrap_err();
        assert!(matches!(err, WaterfallError::TraceNotFound(_)));
    }

    #[test]
    fn test_build_waterfall_treats_orphan_parent_as_root() {
        // Given: a span declares a parent that is not present in the data.
        let trace_id = "trace1";
        let payload = resource_spans(
            "svc",
            vec![span_json(trace_id, "orphan", "missing", "op", 5, 10, 0)],
        );
        let entries = vec![make_trace_entry(payload)];

        let waterfall = build_waterfall(&entries, trace_id).unwrap();

        assert_eq!(waterfall.spans.len(), 1);
        assert_eq!(waterfall.spans[0].id, "orphan");
        // start_ms is normalized to 0 since orphan is the only span.
        assert!((waterfall.spans[0].start_ms - 0.0).abs() < 1e-9);
    }

    #[test]
    fn test_build_waterfall_skips_non_trace_entries() {
        // Given: a metrics entry mixed in -- it should be ignored.
        let trace_id = "trace2";
        let trace_payload =
            resource_spans("svc", vec![span_json(trace_id, "root", "", "r", 0, 100, 0)]);
        let metric_entry = NormalizedEntry {
            signal: Signal::Metrics,
            observed_at: Utc::now(),
            service_name: None,
            payload: Bytes::from_static(b"{}"),
        };
        let entries = vec![metric_entry, make_trace_entry(trace_payload)];

        let waterfall = build_waterfall(&entries, trace_id).unwrap();
        assert_eq!(waterfall.spans.len(), 1);
    }

    #[test]
    fn test_build_waterfall_aggregates_spans_across_entries() {
        // Given: two separate entries each contributing a span to the same trace.
        let trace_id = "split-trace";
        let p1 = resource_spans(
            "svc-a",
            vec![span_json(trace_id, "root", "", "r", 0, 100, 0)],
        );
        let p2 = resource_spans(
            "svc-b",
            vec![span_json(trace_id, "child", "root", "c", 10, 50, 0)],
        );
        let entries = vec![make_trace_entry(p1), make_trace_entry(p2)];

        let waterfall = build_waterfall(&entries, trace_id).unwrap();

        assert_eq!(waterfall.spans.len(), 2);
        assert_eq!(waterfall.spans[0].id, "root");
        assert_eq!(waterfall.spans[1].id, "child");
        assert_eq!(waterfall.spans[1].service_name, "svc-b");
    }
}
