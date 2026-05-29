//! Trace search and lookup helpers.
//!
//! Two entry points are exposed:
//!
//! - [`search_traces`] groups all OTLP spans found in `entries` by `trace_id`
//!   and returns one [`TraceListItem`] per trace, optionally filtered by
//!   service name and minimum duration (ms).
//! - [`lookup_trace`] returns the full [`TraceDetail`] (every span with its
//!   resource attributes, span attributes, kind and status) for a single
//!   `trace_id`. Returns `None` when the trace_id is unknown.
//!
//! Both helpers consume `&[NormalizedEntry]` and operate purely on memory --
//! they do not perform I/O.

use std::collections::HashMap;

use serde::Serialize;
use serde_json::Value;

use crate::apm::parse_otlp_nano as parse_nano;
use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_http::attribute_string_value;
use crate::ingest::otlp_pb::payload_as_value;

/// Filter for [`search_traces`].
///
/// Trace-level predicates (`service`, `min_duration_ms`, `only_errors`) are
/// evaluated against the aggregated [`TraceListItem`]. Span-level predicates
/// (`span_name`, `attribute`) match a trace when *at least one* of its spans
/// satisfies all of them together.
#[derive(Debug, Clone, Default)]
pub struct TraceSearchFilter {
    /// If set, only traces that contain at least one span whose
    /// `resource.service.name` equals this value are returned.
    pub service: Option<String>,
    /// If set, only traces whose total duration (max end - min start) is
    /// `>= min_duration_ms` are returned.
    pub min_duration_ms: Option<u64>,
    /// If set, only traces that contain at least one span whose `name`
    /// contains this substring (case-insensitive) are returned.
    pub span_name: Option<String>,
    /// If `true`, only traces with at least one error span (status code 2,
    /// i.e. `STATUS_CODE_ERROR`) are returned.
    pub only_errors: bool,
    /// If set, only traces that contain at least one span carrying this
    /// attribute (looked up in span attributes, then resource attributes) are
    /// returned.
    pub attribute: Option<AttributeFilter>,
}

/// Span/resource attribute predicate used by [`TraceSearchFilter`].
#[derive(Debug, Clone)]
pub struct AttributeFilter {
    /// Exact attribute key to match (e.g. `http.method`).
    pub key: String,
    /// If set, the attribute's string value must contain this substring
    /// (case-insensitive). If `None`, the key only needs to be present.
    pub value: Option<String>,
}

/// Lightweight description of a trace, used by the search list view.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct TraceListItem {
    pub trace_id: String,
    pub root_span_name: Option<String>,
    /// Sorted, deduplicated set of `service.name` values seen across the spans.
    pub service_names: Vec<String>,
    pub span_count: usize,
    pub duration_ns: u64,
    pub duration_ms: u64,
    pub started_at_ns: u64,
    pub has_error: bool,
}

/// A single span inside a [`TraceDetail`]. Carries the resource attributes
/// (copied from the parent `ResourceSpans`), the span attributes, and the
/// `kind` / `status` fields needed to render a per-span detail row.
#[derive(Debug, Serialize, PartialEq)]
pub struct TraceDetailSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub service_name: String,
    pub name: String,
    pub kind: u64,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub duration_ns: u64,
    pub status_code: u64,
    pub status_message: Option<String>,
    /// `resource.attributes` of the `ResourceSpans` block this span belongs to.
    /// Each item is the raw OTLP `KeyValue` JSON object (kept verbatim so the
    /// UI can decide how to render).
    pub resource_attributes: Vec<Value>,
    /// `span.attributes`. Same encoding as `resource_attributes`.
    pub span_attributes: Vec<Value>,
}

/// Full detail for a single trace, returned by [`lookup_trace`].
#[derive(Debug, Serialize, PartialEq)]
pub struct TraceDetail {
    pub trace_id: String,
    pub root_span_name: Option<String>,
    pub service_names: Vec<String>,
    pub span_count: usize,
    pub duration_ns: u64,
    pub duration_ms: u64,
    pub started_at_ns: u64,
    pub has_error: bool,
    pub spans: Vec<TraceDetailSpan>,
}

/// Returns the list of traces present in `entries`, optionally filtered.
pub fn search_traces(
    entries: &[NormalizedEntry],
    filter: &TraceSearchFilter,
) -> Vec<TraceListItem> {
    let mut spans_by_trace: HashMap<String, Vec<TraceDetailSpan>> = HashMap::new();
    collect_spans(entries, None, &mut spans_by_trace);

    let mut traces: Vec<TraceListItem> = spans_by_trace
        .into_iter()
        .filter(|(_, spans)| trace_spans_match(spans, filter))
        .map(|(trace_id, spans)| summarize_trace(trace_id, &spans))
        .filter(|trace| trace_matches_filter(trace, filter))
        .collect();

    traces.sort_by_key(|t| std::cmp::Reverse(t.started_at_ns));
    traces
}

/// Returns the full detail for a single `trace_id`, or `None` if no spans
/// for that trace_id are present in `entries`.
pub fn lookup_trace(entries: &[NormalizedEntry], trace_id: &str) -> Option<TraceDetail> {
    let mut spans_by_trace: HashMap<String, Vec<TraceDetailSpan>> = HashMap::new();
    collect_spans(entries, Some(trace_id), &mut spans_by_trace);
    let mut spans = spans_by_trace.remove(trace_id)?;
    if spans.is_empty() {
        return None;
    }
    spans.sort_by_key(|s| s.start_time_unix_nano);
    let summary = summarize_trace(trace_id.to_string(), &spans);
    Some(TraceDetail {
        trace_id: summary.trace_id,
        root_span_name: summary.root_span_name,
        service_names: summary.service_names,
        span_count: summary.span_count,
        duration_ns: summary.duration_ns,
        duration_ms: summary.duration_ms,
        started_at_ns: summary.started_at_ns,
        has_error: summary.has_error,
        spans,
    })
}

fn trace_matches_filter(trace: &TraceListItem, filter: &TraceSearchFilter) -> bool {
    if let Some(service) = filter.service.as_deref()
        && !trace.service_names.iter().any(|s| s == service)
    {
        return false;
    }
    if let Some(min_ms) = filter.min_duration_ms
        && trace.duration_ms < min_ms
    {
        return false;
    }
    if filter.only_errors && !trace.has_error {
        return false;
    }
    true
}

/// Returns `true` when the trace satisfies the span-level predicates
/// (`span_name`, `attribute`). A trace matches when at least one span
/// satisfies *all* active span-level predicates. Returns `true` when no
/// span-level predicate is set.
fn trace_spans_match(spans: &[TraceDetailSpan], filter: &TraceSearchFilter) -> bool {
    if filter.span_name.is_none() && filter.attribute.is_none() {
        return true;
    }
    spans.iter().any(|span| span_matches(span, filter))
}

fn span_matches(span: &TraceDetailSpan, filter: &TraceSearchFilter) -> bool {
    if let Some(name) = filter.span_name.as_deref()
        && !span.name.to_lowercase().contains(&name.to_lowercase())
    {
        return false;
    }
    if let Some(attr) = filter.attribute.as_ref()
        && !attribute_matches(&span.span_attributes, attr)
        && !attribute_matches(&span.resource_attributes, attr)
    {
        return false;
    }
    true
}

fn attribute_matches(attributes: &[Value], filter: &AttributeFilter) -> bool {
    match filter.value.as_deref() {
        Some(want) => attribute_scalar_as_string(attributes, &filter.key)
            .is_some_and(|value| value.to_lowercase().contains(&want.to_lowercase())),
        None => attributes
            .iter()
            .any(|kv| kv.get("key").and_then(Value::as_str) == Some(filter.key.as_str())),
    }
}

/// Returns the scalar value of the attribute `key` rendered as a string,
/// covering the OTLP scalar `AnyValue` variants (`stringValue`, `intValue`,
/// `boolValue`, `doubleValue`). Unlike [`attribute_string_value`], this lets
/// `attribute=key=value` filters match numeric/boolean attributes (e.g.
/// `http.status_code=500`). Non-scalar variants (array/kvlist/bytes) yield
/// `None`.
fn attribute_scalar_as_string(attributes: &[Value], key: &str) -> Option<String> {
    let value = attributes
        .iter()
        .find(|kv| kv.get("key").and_then(Value::as_str) == Some(key))?
        .get("value")?;

    if let Some(s) = value.get("stringValue").and_then(Value::as_str) {
        return Some(s.to_string());
    }
    if let Some(b) = value.get("boolValue").and_then(Value::as_bool) {
        return Some(b.to_string());
    }
    if let Some(int_value) = value.get("intValue") {
        // OTLP/JSON encodes int64 as a quoted string, but tolerate a raw number too.
        if let Some(s) = int_value.as_str() {
            return Some(s.to_string());
        }
        if let Some(n) = int_value.as_i64() {
            return Some(n.to_string());
        }
    }
    if let Some(d) = value.get("doubleValue").and_then(Value::as_f64) {
        return Some(d.to_string());
    }
    None
}

fn collect_spans(
    entries: &[NormalizedEntry],
    trace_id_filter: Option<&str>,
    spans_by_trace: &mut HashMap<String, Vec<TraceDetailSpan>>,
) {
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
            let resource_attributes: Vec<Value> = rs
                .get("resource")
                .and_then(|r| r.get("attributes"))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let service_name =
                attribute_string_value(&resource_attributes, "service.name").unwrap_or_default();

            let Some(scope_spans) = rs.get("scopeSpans").and_then(Value::as_array) else {
                continue;
            };

            for ss in scope_spans {
                let Some(spans) = ss.get("spans").and_then(Value::as_array) else {
                    continue;
                };
                for span in spans {
                    let trace_id = string_field(span, "traceId");
                    if trace_id.is_empty() {
                        continue;
                    }
                    if let Some(filter) = trace_id_filter
                        && filter != trace_id
                    {
                        continue;
                    }

                    let span_attributes = span
                        .get("attributes")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let start = parse_nano(span.get("startTimeUnixNano"));
                    let end = parse_nano(span.get("endTimeUnixNano"));
                    let duration = end.saturating_sub(start);
                    let detail = TraceDetailSpan {
                        trace_id: trace_id.clone(),
                        span_id: string_field(span, "spanId"),
                        parent_span_id: string_field(span, "parentSpanId"),
                        service_name: service_name.clone(),
                        name: string_field(span, "name"),
                        kind: span.get("kind").and_then(Value::as_u64).unwrap_or(0),
                        start_time_unix_nano: start,
                        end_time_unix_nano: end,
                        duration_ns: duration,
                        status_code: span
                            .get("status")
                            .and_then(|s| s.get("code"))
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        status_message: span
                            .get("status")
                            .and_then(|s| s.get("message"))
                            .and_then(Value::as_str)
                            .map(str::to_string),
                        resource_attributes: resource_attributes.clone(),
                        span_attributes,
                    };
                    spans_by_trace.entry(trace_id).or_default().push(detail);
                }
            }
        }
    }
}

fn summarize_trace(trace_id: String, spans: &[TraceDetailSpan]) -> TraceListItem {
    let mut started_at_ns = u64::MAX;
    let mut ended_at_ns = 0u64;
    let mut root_span_name: Option<String> = None;
    let mut has_error = false;
    let mut services = std::collections::BTreeSet::new();

    for s in spans {
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
            services.insert(s.service_name.clone());
        }
    }
    if started_at_ns == u64::MAX {
        started_at_ns = 0;
    }
    let duration_ns = ended_at_ns.saturating_sub(started_at_ns);
    TraceListItem {
        trace_id,
        root_span_name,
        service_names: services.into_iter().collect(),
        span_count: spans.len(),
        duration_ns,
        duration_ms: duration_ns / 1_000_000,
        started_at_ns,
        has_error,
    }
}

fn string_field(span: &Value, key: &str) -> String {
    span.get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use serde_json::json;

    fn trace_entry(payload: serde_json::Value) -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name: None,
            payload: Bytes::from(payload.to_string()),
        }
    }

    fn metric_entry() -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Metrics,
            observed_at: Utc::now(),
            service_name: None,
            payload: Bytes::from_static(b"{}"),
        }
    }

    fn span_payload(service: &str, trace_id: &str, spans: serde_json::Value) -> serde_json::Value {
        json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}},
                        {"key": "host.name", "value": {"stringValue": "host-a"}}
                    ]
                },
                "scopeSpans": [{
                    "scope": {"name": "test"},
                    "spans": spans
                }]
            }],
            "_traceIdEcho": trace_id,
        })
    }

    #[test]
    fn search_traces_groups_spans_by_trace_id() {
        let entries = vec![
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([
                    {
                        "traceId": "t1",
                        "spanId": "s1",
                        "name": "GET /a",
                        "kind": 2,
                        "startTimeUnixNano": "0",
                        "endTimeUnixNano": "200000000"
                    },
                    {
                        "traceId": "t1",
                        "spanId": "s2",
                        "parentSpanId": "s1",
                        "name": "child",
                        "kind": 3,
                        "startTimeUnixNano": "1000000",
                        "endTimeUnixNano": "150000000"
                    }
                ]),
            )),
            trace_entry(span_payload(
                "checkout",
                "t2",
                json!([
                    {
                        "traceId": "t2",
                        "spanId": "x1",
                        "name": "POST /b",
                        "kind": 2,
                        "startTimeUnixNano": "1000000000",
                        "endTimeUnixNano": "1500000000",
                        "status": {"code": 2}
                    }
                ]),
            )),
        ];

        let traces = search_traces(&entries, &TraceSearchFilter::default());

        assert_eq!(traces.len(), 2);
        let by_id: HashMap<&str, &TraceListItem> =
            traces.iter().map(|t| (t.trace_id.as_str(), t)).collect();
        let t1 = by_id["t1"];
        assert_eq!(t1.span_count, 2);
        assert_eq!(t1.root_span_name.as_deref(), Some("GET /a"));
        assert_eq!(t1.service_names, vec!["frontend".to_string()]);
        assert_eq!(t1.duration_ns, 200_000_000);
        assert_eq!(t1.duration_ms, 200);
        assert!(!t1.has_error);
        let t2 = by_id["t2"];
        assert!(t2.has_error);
        assert_eq!(t2.duration_ms, 500);
    }

    #[test]
    fn search_traces_filter_by_service() {
        let entries = vec![
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([{ "traceId": "t1", "spanId": "s1", "name": "a" }]),
            )),
            trace_entry(span_payload(
                "checkout",
                "t2",
                json!([{ "traceId": "t2", "spanId": "s2", "name": "b" }]),
            )),
        ];

        let filter = TraceSearchFilter {
            service: Some("checkout".to_string()),
            ..Default::default()
        };
        let traces = search_traces(&entries, &filter);
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].trace_id, "t2");
    }

    #[test]
    fn search_traces_filter_by_min_duration_ms() {
        let entries = vec![
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([{
                    "traceId": "t1", "spanId": "s1", "name": "fast",
                    "startTimeUnixNano": "0", "endTimeUnixNano": "50000000"
                }]),
            )),
            trace_entry(span_payload(
                "frontend",
                "t2",
                json!([{
                    "traceId": "t2", "spanId": "s2", "name": "slow",
                    "startTimeUnixNano": "0", "endTimeUnixNano": "300000000"
                }]),
            )),
        ];

        let filter = TraceSearchFilter {
            min_duration_ms: Some(100),
            ..Default::default()
        };
        let traces = search_traces(&entries, &filter);
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].trace_id, "t2");
    }

    #[test]
    fn search_traces_filter_by_span_name_substring() {
        let entries = vec![
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([{ "traceId": "t1", "spanId": "s1", "name": "GET /users" }]),
            )),
            trace_entry(span_payload(
                "frontend",
                "t2",
                json!([{ "traceId": "t2", "spanId": "s2", "name": "db.query SELECT" }]),
            )),
        ];

        let filter = TraceSearchFilter {
            // case-insensitive substring
            span_name: Some("db.QUERY".to_string()),
            ..Default::default()
        };
        let traces = search_traces(&entries, &filter);
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].trace_id, "t2");
    }

    #[test]
    fn search_traces_filter_only_errors() {
        let entries = vec![
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([{ "traceId": "t1", "spanId": "s1", "name": "ok" }]),
            )),
            trace_entry(span_payload(
                "frontend",
                "t2",
                json!([{ "traceId": "t2", "spanId": "s2", "name": "boom", "status": {"code": 2} }]),
            )),
        ];

        let filter = TraceSearchFilter {
            only_errors: true,
            ..Default::default()
        };
        let traces = search_traces(&entries, &filter);
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].trace_id, "t2");
    }

    #[test]
    fn search_traces_filter_by_attribute_key_and_value() {
        let entries = vec![
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([{
                    "traceId": "t1", "spanId": "s1", "name": "a",
                    "attributes": [{"key": "http.method", "value": {"stringValue": "GET"}}]
                }]),
            )),
            trace_entry(span_payload(
                "frontend",
                "t2",
                json!([{
                    "traceId": "t2", "spanId": "s2", "name": "b",
                    "attributes": [{"key": "http.method", "value": {"stringValue": "POST"}}]
                }]),
            )),
        ];

        // key=value: case-insensitive substring on the value
        let by_value = search_traces(
            &entries,
            &TraceSearchFilter {
                attribute: Some(AttributeFilter {
                    key: "http.method".to_string(),
                    value: Some("post".to_string()),
                }),
                ..Default::default()
            },
        );
        assert_eq!(by_value.len(), 1);
        assert_eq!(by_value[0].trace_id, "t2");

        // key-only: presence match -- both traces carry http.method
        let by_key = search_traces(
            &entries,
            &TraceSearchFilter {
                attribute: Some(AttributeFilter {
                    key: "http.method".to_string(),
                    value: None,
                }),
                ..Default::default()
            },
        );
        assert_eq!(by_key.len(), 2);
    }

    #[test]
    fn search_traces_filter_by_numeric_and_bool_attribute() {
        let entries = vec![
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([{
                    "traceId": "t1", "spanId": "s1", "name": "a",
                    "attributes": [
                        {"key": "http.status_code", "value": {"intValue": "500"}},
                        {"key": "cache.hit", "value": {"boolValue": false}}
                    ]
                }]),
            )),
            trace_entry(span_payload(
                "frontend",
                "t2",
                json!([{
                    "traceId": "t2", "spanId": "s2", "name": "b",
                    "attributes": [
                        {"key": "http.status_code", "value": {"intValue": "200"}},
                        {"key": "cache.hit", "value": {"boolValue": true}}
                    ]
                }]),
            )),
        ];

        // intValue (encoded as a quoted string in OTLP/JSON)
        let by_status = search_traces(
            &entries,
            &TraceSearchFilter {
                attribute: Some(AttributeFilter {
                    key: "http.status_code".to_string(),
                    value: Some("500".to_string()),
                }),
                ..Default::default()
            },
        );
        assert_eq!(by_status.len(), 1);
        assert_eq!(by_status[0].trace_id, "t1");

        // boolValue
        let by_bool = search_traces(
            &entries,
            &TraceSearchFilter {
                attribute: Some(AttributeFilter {
                    key: "cache.hit".to_string(),
                    value: Some("true".to_string()),
                }),
                ..Default::default()
            },
        );
        assert_eq!(by_bool.len(), 1);
        assert_eq!(by_bool[0].trace_id, "t2");
    }

    #[test]
    fn search_traces_matches_resource_attribute() {
        let entries = vec![trace_entry(span_payload(
            "frontend",
            "t1",
            json!([{ "traceId": "t1", "spanId": "s1", "name": "a" }]),
        ))];

        // host.name lives on the resource, not the span
        let traces = search_traces(
            &entries,
            &TraceSearchFilter {
                attribute: Some(AttributeFilter {
                    key: "host.name".to_string(),
                    value: Some("host-a".to_string()),
                }),
                ..Default::default()
            },
        );
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].trace_id, "t1");
    }

    #[test]
    fn search_traces_span_filters_require_same_span() {
        // span_name + attribute must be satisfied by the *same* span.
        let entries = vec![trace_entry(span_payload(
            "frontend",
            "t1",
            json!([
                {
                    "traceId": "t1", "spanId": "s1", "name": "db.query",
                    "attributes": [{"key": "db.system", "value": {"stringValue": "postgres"}}]
                },
                {
                    "traceId": "t1", "spanId": "s2", "name": "http.request",
                    "attributes": [{"key": "http.method", "value": {"stringValue": "GET"}}]
                }
            ]),
        ))];

        // "db.query" span does NOT carry http.method -> no match
        let mismatch = search_traces(
            &entries,
            &TraceSearchFilter {
                span_name: Some("db.query".to_string()),
                attribute: Some(AttributeFilter {
                    key: "http.method".to_string(),
                    value: None,
                }),
                ..Default::default()
            },
        );
        assert!(mismatch.is_empty());

        // "db.query" span with db.system -> match
        let hit = search_traces(
            &entries,
            &TraceSearchFilter {
                span_name: Some("db.query".to_string()),
                attribute: Some(AttributeFilter {
                    key: "db.system".to_string(),
                    value: Some("postgres".to_string()),
                }),
                ..Default::default()
            },
        );
        assert_eq!(hit.len(), 1);
    }

    #[test]
    fn search_traces_skips_non_trace_entries_and_invalid_payloads() {
        let entries = vec![
            metric_entry(),
            NormalizedEntry {
                signal: Signal::Traces,
                observed_at: Utc::now(),
                service_name: None,
                payload: Bytes::from_static(b"not-json"),
            },
            trace_entry(span_payload(
                "frontend",
                "t1",
                json!([{ "traceId": "t1", "spanId": "s1", "name": "a" }]),
            )),
        ];
        let traces = search_traces(&entries, &TraceSearchFilter::default());
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].trace_id, "t1");
    }

    #[test]
    fn lookup_trace_returns_full_spans_with_attributes() {
        let entries = vec![trace_entry(json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "frontend"}},
                        {"key": "host.name", "value": {"stringValue": "host-a"}}
                    ]
                },
                "scopeSpans": [{
                    "scope": {"name": "test"},
                    "spans": [{
                        "traceId": "t1",
                        "spanId": "s1",
                        "name": "GET /a",
                        "kind": 2,
                        "startTimeUnixNano": "0",
                        "endTimeUnixNano": "200000000",
                        "status": {"code": 1, "message": "ok"},
                        "attributes": [
                            {"key": "http.method", "value": {"stringValue": "GET"}}
                        ]
                    }]
                }]
            }]
        }))];

        let detail = lookup_trace(&entries, "t1").expect("trace should be found");
        assert_eq!(detail.trace_id, "t1");
        assert_eq!(detail.span_count, 1);
        let span = &detail.spans[0];
        assert_eq!(span.span_id, "s1");
        assert_eq!(span.kind, 2);
        assert_eq!(span.status_code, 1);
        assert_eq!(span.status_message.as_deref(), Some("ok"));
        assert_eq!(span.duration_ns, 200_000_000);
        assert_eq!(span.resource_attributes.len(), 2);
        assert_eq!(span.span_attributes.len(), 1);
        assert_eq!(span.service_name, "frontend");
    }

    #[test]
    fn lookup_trace_unknown_returns_none() {
        let entries = vec![trace_entry(span_payload(
            "frontend",
            "t1",
            json!([{ "traceId": "t1", "spanId": "s1", "name": "a" }]),
        ))];
        assert!(lookup_trace(&entries, "missing").is_none());
    }

    #[test]
    fn lookup_trace_sorts_spans_by_start_time() {
        let entries = vec![trace_entry(span_payload(
            "frontend",
            "t1",
            json!([
                {
                    "traceId": "t1", "spanId": "s2", "parentSpanId": "s1",
                    "name": "child",
                    "startTimeUnixNano": "100", "endTimeUnixNano": "200"
                },
                {
                    "traceId": "t1", "spanId": "s1", "name": "root",
                    "startTimeUnixNano": "0", "endTimeUnixNano": "300"
                }
            ]),
        ))];
        let detail = lookup_trace(&entries, "t1").unwrap();
        assert_eq!(detail.spans[0].span_id, "s1");
        assert_eq!(detail.spans[1].span_id, "s2");
        assert_eq!(detail.root_span_name.as_deref(), Some("root"));
    }
}
