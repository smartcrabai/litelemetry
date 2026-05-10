//! Service map extraction.
//!
//! Walks span parent/child relationships to derive a service-to-service
//! topology from ingested traces. For each span we determine its owning
//! service via the resource `service.name` attribute. An edge from service
//! `A` to service `B` is recorded when a child span owned by `B` has a
//! parent span owned by `A`. When the parent span is missing (cross-batch
//! or root), we fall back to the span's own `peer.service` /
//! `server.address` attributes so that client spans still produce edges.
//!
//! Per-node statistics (span count, error count, p95 duration in ms) are
//! computed from the spans owned by the node. Per-edge statistics are
//! computed from the child spans on the edge.

use crate::apm::parse_otlp_nano as parse_nano;
use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_http::attribute_string_value;
use crate::ingest::otlp_pb::payload_as_value;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;

/// Span status code emitted by OTLP when a span has explicitly errored.
const SPAN_STATUS_CODE_ERROR: u64 = 2;

/// Attributes considered when inferring the remote service for a client span
/// whose parent span is not present in the available batch.
const PEER_SERVICE_ATTRIBUTE_KEYS: &[&str] = &["peer.service", "server.address", "net.peer.name"];

/// Node in the service-map graph.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ServiceNode {
    pub name: String,
    pub span_count: u64,
    pub error_count: u64,
    pub p95_duration_ms: f64,
}

/// Edge in the service-map graph (parent service -> child service).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ServiceEdge {
    pub from: String,
    pub to: String,
    pub calls: u64,
    pub error_rate: f64,
}

/// Aggregated service-map response.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ServiceMap {
    pub nodes: Vec<ServiceNode>,
    pub edges: Vec<ServiceEdge>,
}

#[derive(Debug, Clone)]
struct SpanFacts {
    service: String,
    parent_span_id: String,
    duration_ms: f64,
    is_error: bool,
    peer_service: Option<String>,
}

#[derive(Debug, Default)]
struct NodeAccumulator {
    span_count: u64,
    error_count: u64,
    durations_ms: Vec<f64>,
}

#[derive(Debug, Default)]
struct EdgeAccumulator {
    calls: u64,
    errors: u64,
}

/// Build a service map from `entries` whose `observed_at` falls within the
/// `[now - lookback_ms, now]` window. Non-trace entries and entries outside
/// the window are ignored. `lookback_ms` of 0 or negative disables the
/// lookback filter (treats all entries as in-window).
pub fn build_service_map(
    entries: &[NormalizedEntry],
    now: DateTime<Utc>,
    lookback_ms: i64,
) -> ServiceMap {
    let cutoff = if lookback_ms > 0 {
        Some(now - chrono::Duration::milliseconds(lookback_ms))
    } else {
        None
    };

    // span_id -> service that owns the span (so children can find their parent).
    let mut owner_by_span: HashMap<String, String> = HashMap::new();
    let mut span_facts: Vec<SpanFacts> = Vec::new();

    for entry in entries {
        if entry.signal != Signal::Traces {
            continue;
        }
        if let Some(min_observed) = cutoff
            && entry.observed_at < min_observed
        {
            continue;
        }
        let Some(value) = payload_as_value(Signal::Traces, &entry.payload) else {
            continue;
        };
        collect_spans_from_value(&value, &mut owner_by_span, &mut span_facts);
    }

    let mut nodes: HashMap<String, NodeAccumulator> = HashMap::new();
    let mut edges: HashMap<(String, String), EdgeAccumulator> = HashMap::new();

    for span in &span_facts {
        let node = nodes.entry(span.service.clone()).or_default();
        node.span_count += 1;
        if span.is_error {
            node.error_count += 1;
        }
        node.durations_ms.push(span.duration_ms);

        let parent_service = if span.parent_span_id.is_empty() {
            None
        } else {
            owner_by_span.get(&span.parent_span_id).cloned()
        };

        let edge_key: Option<(String, String)> = match parent_service {
            Some(parent) if parent != span.service => Some((parent, span.service.clone())),
            // Parent is in another batch: fall back to peer attributes so that
            // we still record the call. Root spans (no parent_span_id) are
            // intentionally ignored — they have no caller to attribute to.
            None if !span.parent_span_id.is_empty() => span
                .peer_service
                .clone()
                .filter(|peer| peer != &span.service)
                .map(|peer| (span.service.clone(), peer)),
            _ => None,
        };

        if let Some(key) = edge_key {
            let acc = edges.entry(key).or_default();
            acc.calls += 1;
            if span.is_error {
                acc.errors += 1;
            }
        }
    }

    let mut node_list: Vec<ServiceNode> = nodes
        .into_iter()
        .map(|(name, acc)| ServiceNode {
            name,
            span_count: acc.span_count,
            error_count: acc.error_count,
            p95_duration_ms: percentile(&acc.durations_ms, 0.95),
        })
        .collect();
    node_list.sort_by(|a, b| a.name.cmp(&b.name));

    let mut edge_list: Vec<ServiceEdge> = edges
        .into_iter()
        .map(|((from, to), acc)| ServiceEdge {
            from,
            to,
            calls: acc.calls,
            error_rate: if acc.calls == 0 {
                0.0
            } else {
                acc.errors as f64 / acc.calls as f64
            },
        })
        .collect();
    edge_list.sort_by(|a, b| a.from.cmp(&b.from).then_with(|| a.to.cmp(&b.to)));

    ServiceMap {
        nodes: node_list,
        edges: edge_list,
    }
}

fn collect_spans_from_value(
    value: &serde_json::Value,
    owner_by_span: &mut HashMap<String, String>,
    span_facts: &mut Vec<SpanFacts>,
) {
    let Some(resource_spans) = value.get("resourceSpans").and_then(|v| v.as_array()) else {
        return;
    };

    for rs in resource_spans {
        let resource_attrs = rs
            .get("resource")
            .and_then(|r| r.get("attributes"))
            .and_then(serde_json::Value::as_array);
        let Some(service) = resource_attrs
            .and_then(|attrs| attribute_string_value(attrs, "service.name"))
            .filter(|s| !s.is_empty())
        else {
            continue;
        };

        let Some(scope_spans) = rs.get("scopeSpans").and_then(|v| v.as_array()) else {
            continue;
        };

        for ss in scope_spans {
            let Some(spans) = ss.get("spans").and_then(|v| v.as_array()) else {
                continue;
            };

            for span in spans {
                let span_id = span
                    .get("spanId")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let parent_span_id = span
                    .get("parentSpanId")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let start_ns = parse_nano(span.get("startTimeUnixNano"));
                let end_ns = parse_nano(span.get("endTimeUnixNano"));
                let duration_ms = if end_ns >= start_ns {
                    (end_ns - start_ns) as f64 / 1_000_000.0
                } else {
                    0.0
                };
                let status_code = span
                    .get("status")
                    .and_then(|s| s.get("code"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
                let span_attrs = span.get("attributes").and_then(serde_json::Value::as_array);
                let peer_service = span_attrs.and_then(|attrs| {
                    PEER_SERVICE_ATTRIBUTE_KEYS
                        .iter()
                        .find_map(|key| attribute_string_value(attrs, key))
                });

                if !span_id.is_empty() {
                    owner_by_span.insert(span_id, service.clone());
                }

                span_facts.push(SpanFacts {
                    service: service.clone(),
                    parent_span_id,
                    duration_ms,
                    is_error: status_code == SPAN_STATUS_CODE_ERROR,
                    peer_service,
                });
            }
        }
    }
}

/// Nearest-rank percentile of a slice of f64s. Returns 0.0 if empty.
fn percentile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q = q.clamp(0.0, 1.0);
    // Nearest-rank: smallest value v in the sorted set such that at least
    // ceil(q * n) values are <= v.
    let n = sorted.len();
    let rank = ((q * n as f64).ceil() as usize).max(1);
    let idx = rank.min(n) - 1;
    sorted[idx]
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::json;

    fn make_entry(payload: serde_json::Value, observed_at: DateTime<Utc>) -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at,
            service_name: None,
            payload: Bytes::from(payload.to_string()),
        }
    }

    fn span(
        span_id: &str,
        parent: &str,
        start_ns: u64,
        end_ns: u64,
        status: u64,
    ) -> serde_json::Value {
        json!({
            "spanId": span_id,
            "parentSpanId": parent,
            "name": format!("span-{span_id}"),
            "startTimeUnixNano": start_ns.to_string(),
            "endTimeUnixNano": end_ns.to_string(),
            "status": { "code": status },
            "attributes": []
        })
    }

    fn resource_spans(service: &str, spans_json: Vec<serde_json::Value>) -> serde_json::Value {
        json!({
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": service}}
                ]
            },
            "scopeSpans": [
                {
                    "scope": {"name": "test"},
                    "spans": spans_json
                }
            ]
        })
    }

    #[test]
    fn extracts_two_node_one_edge_topology() {
        let now = Utc::now();
        let payload = json!({
            "resourceSpans": [
                resource_spans("frontend", vec![span("0001", "", 0, 5_000_000, 0)]),
                resource_spans("backend", vec![span("0002", "0001", 1_000_000, 4_000_000, 0)]),
            ]
        });
        let entries = vec![make_entry(payload, now)];

        let map = build_service_map(&entries, now, 60_000);

        assert_eq!(map.nodes.len(), 2);
        assert_eq!(map.nodes[0].name, "backend");
        assert_eq!(map.nodes[0].span_count, 1);
        assert_eq!(map.nodes[1].name, "frontend");

        assert_eq!(map.edges.len(), 1);
        assert_eq!(map.edges[0].from, "frontend");
        assert_eq!(map.edges[0].to, "backend");
        assert_eq!(map.edges[0].calls, 1);
        assert!((map.edges[0].error_rate - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn counts_errors_per_node_and_edge() {
        let now = Utc::now();
        let payload = json!({
            "resourceSpans": [
                resource_spans("frontend", vec![span("aa", "", 0, 1_000_000, 0)]),
                resource_spans("backend", vec![
                    span("bb", "aa", 0, 2_000_000, 2),
                    span("cc", "aa", 0, 1_000_000, 0),
                ]),
            ]
        });
        let entries = vec![make_entry(payload, now)];

        let map = build_service_map(&entries, now, 60_000);

        let backend = map.nodes.iter().find(|n| n.name == "backend").unwrap();
        assert_eq!(backend.span_count, 2);
        assert_eq!(backend.error_count, 1);

        let edge = map
            .edges
            .iter()
            .find(|e| e.from == "frontend" && e.to == "backend")
            .unwrap();
        assert_eq!(edge.calls, 2);
        assert!((edge.error_rate - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn ignores_entries_outside_lookback_window() {
        let now = Utc::now();
        let stale = now - chrono::Duration::milliseconds(120_000);
        let payload = json!({
            "resourceSpans": [
                resource_spans("svc-a", vec![span("01", "", 0, 1_000_000, 0)])
            ]
        });
        let entries = vec![make_entry(payload, stale)];

        let map = build_service_map(&entries, now, 60_000);

        assert!(map.nodes.is_empty());
        assert!(map.edges.is_empty());
    }

    #[test]
    fn falls_back_to_peer_service_when_parent_missing() {
        let now = Utc::now();
        let payload = json!({
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": "client"}}
                        ]
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "test"},
                            "spans": [
                                {
                                    "spanId": "01",
                                    "parentSpanId": "deadbeefdeadbeef",
                                    "startTimeUnixNano": "0",
                                    "endTimeUnixNano": "1000000",
                                    "status": {"code": 0},
                                    "attributes": [
                                        {"key": "peer.service", "value": {"stringValue": "remote-api"}}
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        });
        let entries = vec![make_entry(payload, now)];

        let map = build_service_map(&entries, now, 60_000);

        let edge = map
            .edges
            .iter()
            .find(|e| e.from == "client" && e.to == "remote-api")
            .expect("edge should be inferred from peer.service");
        assert_eq!(edge.calls, 1);
    }

    #[test]
    fn computes_p95_duration_ms_per_node() {
        let now = Utc::now();
        // Spans of 1, 2, 3, ..., 10 ms. p95 should be 10 ms (nearest-rank).
        let spans: Vec<serde_json::Value> = (1..=10u64)
            .map(|i| {
                let id = format!("{i:016x}");
                span(&id, "", 0, i * 1_000_000, 0)
            })
            .collect();
        let payload = json!({
            "resourceSpans": [resource_spans("svc", spans)]
        });
        let entries = vec![make_entry(payload, now)];

        let map = build_service_map(&entries, now, 60_000);

        assert_eq!(map.nodes.len(), 1);
        assert!((map.nodes[0].p95_duration_ms - 10.0).abs() < 1e-6);
    }

    #[test]
    fn ignores_non_trace_signals() {
        let now = Utc::now();
        let entry = NormalizedEntry {
            signal: Signal::Logs,
            observed_at: now,
            service_name: Some("svc".to_string()),
            payload: Bytes::from_static(b"{}"),
        };

        let map = build_service_map(&[entry], now, 60_000);

        assert!(map.nodes.is_empty());
        assert!(map.edges.is_empty());
    }

    #[test]
    fn skips_self_edges_when_parent_in_same_service() {
        let now = Utc::now();
        let payload = json!({
            "resourceSpans": [
                resource_spans("svc", vec![
                    span("01", "", 0, 1_000_000, 0),
                    span("02", "01", 0, 1_000_000, 0),
                ])
            ]
        });
        let entries = vec![make_entry(payload, now)];

        let map = build_service_map(&entries, now, 60_000);

        assert_eq!(map.nodes.len(), 1);
        assert!(map.edges.is_empty());
    }
}
