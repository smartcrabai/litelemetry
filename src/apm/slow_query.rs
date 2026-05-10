//! Slow-query and N+1 detection over OTLP trace span attributes.
//!
//! Inputs are [`NormalizedEntry`] payloads (OTLP traces). We pull spans whose
//! attributes contain `db.statement` and group them by a normalized form of
//! the statement so that slightly different literals fold into the same query
//! shape. The rest of the file is straightforward percentile math + grouping.

use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

use regex::Regex;
use serde::Serialize;

use crate::apm::parse_otlp_nano as parse_nano;
use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_http::attribute_string_value;
use crate::ingest::otlp_pb::payload_as_value;

/// Default key under which DB statements are recorded in span attributes.
pub const DB_STATEMENT_ATTR: &str = "db.statement";
/// Default key under which DB systems are recorded in span attributes.
pub const DB_SYSTEM_ATTR: &str = "db.system";

/// A single DB span extracted from the trace stream. Used both for slow-query
/// aggregation and for N+1 detection.
#[derive(Debug, Clone)]
pub struct DbSpanFact {
    pub trace_id: String,
    pub statement_normalized: String,
    pub system: Option<String>,
    pub service_name: Option<String>,
    pub duration_ms: u64,
}

/// Per-(statement, system) statistics. Returned by the slow-query endpoint.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SlowQueryStats {
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    pub count: usize,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
    pub max_ms: u64,
}

/// Per-trace N+1 detection result.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct NPlusOneStats {
    pub trace_id: String,
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    pub count: usize,
    pub services: Vec<String>,
}

/// Returns a [`Regex`] that matches integer / decimal literals. Compiled once.
fn number_literal_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d+(?:\.\d+)?\b").expect("number literal regex compiles"))
}

/// Returns a [`Regex`] that matches runs of whitespace. Compiled once.
fn whitespace_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s+").expect("whitespace regex compiles"))
}

/// Normalize a SQL statement for grouping. Numeric literals become `?` and
/// whitespace runs collapse to a single space.
pub fn normalize_statement(stmt: &str) -> String {
    let trimmed = stmt.trim();
    let no_numbers = number_literal_regex().replace_all(trimmed, "?");
    whitespace_regex()
        .replace_all(&no_numbers, " ")
        .into_owned()
}

/// Compute an integer percentile (`0.0..=100.0`) over a *sorted* slice using
/// the nearest-rank method. Returns `0` for empty slices.
///
/// # Panics (debug)
///
/// Panics in debug builds if `p` is outside `0.0..=100.0`.
pub fn percentile(sorted_values: &[u64], p: f64) -> u64 {
    debug_assert!(
        (0.0..=100.0).contains(&p),
        "percentile argument must be in 0.0..=100.0, got {p}"
    );
    if sorted_values.is_empty() {
        return 0;
    }
    if p <= 0.0 {
        return sorted_values[0];
    }
    // nearest-rank: ceil(p/100 * N) - 1, clamped into the slice range.
    let rank = (p / 100.0 * sorted_values.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(sorted_values.len() - 1);
    sorted_values[idx]
}

/// Pull DB span facts out of trace entries.
///
/// Skips entries that are not traces, payloads we cannot decode, and spans
/// without a `db.statement` attribute.
pub fn extract_db_spans(entries: &[NormalizedEntry]) -> Vec<DbSpanFact> {
    let mut facts = Vec::new();

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
            let resource_service = rs
                .get("resource")
                .and_then(|r| r.get("attributes"))
                .and_then(serde_json::Value::as_array)
                .and_then(|attrs| attribute_string_value(attrs, "service.name"));

            let Some(scope_spans) = rs.get("scopeSpans").and_then(|v| v.as_array()) else {
                continue;
            };

            for ss in scope_spans {
                let Some(spans) = ss.get("spans").and_then(|v| v.as_array()) else {
                    continue;
                };
                for span in spans {
                    if let Some(fact) = extract_fact_from_span(span, resource_service.as_deref()) {
                        facts.push(fact);
                    }
                }
            }
        }
    }

    facts
}

fn extract_fact_from_span(
    span: &serde_json::Value,
    resource_service: Option<&str>,
) -> Option<DbSpanFact> {
    let attrs = span
        .get("attributes")
        .and_then(serde_json::Value::as_array)?;
    let statement = attribute_string_value(attrs, DB_STATEMENT_ATTR)?;
    let normalized = normalize_statement(&statement);
    if normalized.is_empty() {
        return None;
    }
    let system = attribute_string_value(attrs, DB_SYSTEM_ATTR);
    let trace_id = span
        .get("traceId")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())?
        .to_string();
    let start_ns = parse_nano(span.get("startTimeUnixNano"));
    let end_ns = parse_nano(span.get("endTimeUnixNano"));
    // Saturating to avoid wrap when a producer has clock skew.
    let duration_ns = end_ns.saturating_sub(start_ns);
    let duration_ms = duration_ns / 1_000_000;

    Some(DbSpanFact {
        trace_id,
        statement_normalized: normalized,
        system,
        service_name: resource_service.map(str::to_string),
        duration_ms,
    })
}

/// Group facts by `(statement, system)` and compute percentile latencies.
/// Drops groups whose `p95 < min_p95_ms`.
pub fn aggregate_slow_queries(facts: &[DbSpanFact], min_p95_ms: u64) -> Vec<SlowQueryStats> {
    let mut groups: HashMap<(String, Option<String>), Vec<u64>> = HashMap::new();
    for fact in facts {
        groups
            .entry((fact.statement_normalized.clone(), fact.system.clone()))
            .or_default()
            .push(fact.duration_ms);
    }

    let mut out = Vec::with_capacity(groups.len());
    for ((statement, system), mut durations) in groups {
        durations.sort_unstable();
        let p50 = percentile(&durations, 50.0);
        let p95 = percentile(&durations, 95.0);
        let p99 = percentile(&durations, 99.0);
        let max_ms = *durations.last().expect("durations is non-empty");
        if p95 < min_p95_ms {
            continue;
        }
        out.push(SlowQueryStats {
            statement,
            system,
            count: durations.len(),
            p50_ms: p50,
            p95_ms: p95,
            p99_ms: p99,
            max_ms,
        });
    }

    // Sort by p95 descending so the worst offenders bubble up first.
    out.sort_by(|a, b| b.p95_ms.cmp(&a.p95_ms).then(b.count.cmp(&a.count)));
    out
}

/// Group key for N+1 detection: (trace_id, normalized statement, db.system).
type NPlusOneKey = (String, String, Option<String>);

/// Aggregate stored per N+1 group: (occurrence count, unique service names).
struct NPlusOneAccum {
    count: usize,
    services: BTreeSet<String>,
}

/// Detect N+1 patterns: same normalized statement repeated `min_repetitions+`
/// times within a single trace.
pub fn detect_n_plus_one(facts: &[DbSpanFact], min_repetitions: usize) -> Vec<NPlusOneStats> {
    if min_repetitions == 0 {
        return Vec::new();
    }

    let mut groups: HashMap<NPlusOneKey, NPlusOneAccum> = HashMap::new();

    for fact in facts {
        if fact.trace_id.is_empty() {
            continue;
        }
        let entry = groups
            .entry((
                fact.trace_id.clone(),
                fact.statement_normalized.clone(),
                fact.system.clone(),
            ))
            .or_insert_with(|| NPlusOneAccum {
                count: 0,
                services: BTreeSet::new(),
            });
        entry.count += 1;
        if let Some(svc) = fact.service_name.as_ref() {
            entry.services.insert(svc.clone());
        }
    }

    let mut out: Vec<NPlusOneStats> = groups
        .into_iter()
        .filter(|(_, accum)| accum.count >= min_repetitions)
        .map(|((trace_id, statement, system), accum)| NPlusOneStats {
            trace_id,
            statement,
            system,
            count: accum.count,
            services: accum.services.into_iter().collect(),
        })
        .collect();

    // Highest repetition counts first.
    out.sort_by(|a, b| b.count.cmp(&a.count).then(a.statement.cmp(&b.statement)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::Utc;
    use serde_json::json;

    fn span_with_db(
        trace_id: &str,
        statement: &str,
        system: &str,
        start_ns: u64,
        end_ns: u64,
    ) -> serde_json::Value {
        json!({
            "traceId": trace_id,
            "spanId": "0000000000000001",
            "name": "db.query",
            "kind": 3,
            "startTimeUnixNano": start_ns.to_string(),
            "endTimeUnixNano": end_ns.to_string(),
            "attributes": [
                {"key": "db.statement", "value": {"stringValue": statement}},
                {"key": "db.system", "value": {"stringValue": system}},
            ]
        })
    }

    fn trace_payload(service: &str, spans: Vec<serde_json::Value>) -> Bytes {
        Bytes::from(
            json!({
                "resourceSpans": [{
                    "resource": {
                        "attributes": [
                            {"key": "service.name", "value": {"stringValue": service}}
                        ]
                    },
                    "scopeSpans": [{
                        "scope": {"name": "test"},
                        "spans": spans
                    }]
                }]
            })
            .to_string(),
        )
    }

    fn entry(payload: Bytes) -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name: Some("svc".to_string()),
            payload,
        }
    }

    #[test]
    fn test_normalize_statement_replaces_numbers_with_question_marks() {
        let normalized = normalize_statement("SELECT * FROM users WHERE id = 42");
        assert_eq!(normalized, "SELECT * FROM users WHERE id = ?");
    }

    #[test]
    fn test_normalize_statement_replaces_decimals() {
        let normalized = normalize_statement("UPDATE prices SET amount = 12.5 WHERE id = 7");
        assert_eq!(normalized, "UPDATE prices SET amount = ? WHERE id = ?");
    }

    #[test]
    fn test_normalize_statement_collapses_whitespace() {
        let normalized = normalize_statement("SELECT\t a,\n  b\nFROM\tusers");
        assert_eq!(normalized, "SELECT a, b FROM users");
    }

    #[test]
    fn test_normalize_statement_groups_similar_queries() {
        // Different literals should fold into one normalized form.
        let a = normalize_statement("SELECT * FROM orders WHERE id = 1");
        let b = normalize_statement("SELECT * FROM orders WHERE id = 999");
        assert_eq!(a, b);
    }

    #[test]
    fn test_percentile_empty_slice_returns_zero() {
        assert_eq!(percentile(&[], 95.0), 0);
    }

    #[test]
    fn test_percentile_p50_p95_p99() {
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&sorted, 50.0), 50);
        assert_eq!(percentile(&sorted, 95.0), 95);
        assert_eq!(percentile(&sorted, 99.0), 99);
    }

    #[test]
    fn test_percentile_single_value_returns_value() {
        assert_eq!(percentile(&[42], 95.0), 42);
    }

    #[test]
    #[should_panic(expected = "percentile argument must be in 0.0..=100.0")]
    fn test_percentile_invalid_value_panics() {
        percentile(&[1, 2, 3], -1.0);
    }

    #[test]
    fn test_extract_db_spans_includes_only_db_spans() {
        let payload = trace_payload(
            "checkout",
            vec![
                span_with_db("aa", "SELECT 1", "postgresql", 0, 5_000_000),
                json!({
                    "traceId": "aa",
                    "spanId": "no-db-1",
                    "name": "http.request",
                    "startTimeUnixNano": "0",
                    "endTimeUnixNano": "1000000",
                    "attributes": []
                }),
            ],
        );
        let facts = extract_db_spans(&[entry(payload)]);
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].statement_normalized, "SELECT ?");
        assert_eq!(facts[0].system.as_deref(), Some("postgresql"));
        assert_eq!(facts[0].service_name.as_deref(), Some("checkout"));
        assert_eq!(facts[0].duration_ms, 5);
    }

    #[test]
    fn test_aggregate_slow_queries_filters_below_threshold() {
        let facts: Vec<DbSpanFact> = (0..10)
            .map(|i| DbSpanFact {
                trace_id: format!("t{i}"),
                statement_normalized: "SELECT * FROM users WHERE id = ?".to_string(),
                system: Some("postgresql".to_string()),
                service_name: Some("svc".to_string()),
                duration_ms: (i + 1) * 10,
            })
            .collect();
        let stats = aggregate_slow_queries(&facts, 50);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].count, 10);
        // p95 of {10, 20, ..., 100} is 100.
        assert_eq!(stats[0].p95_ms, 100);
        assert_eq!(stats[0].max_ms, 100);
    }

    #[test]
    fn test_aggregate_slow_queries_drops_fast_queries() {
        let facts: Vec<DbSpanFact> = (0..5)
            .map(|i| DbSpanFact {
                trace_id: format!("t{i}"),
                statement_normalized: "SELECT 1".to_string(),
                system: None,
                service_name: None,
                duration_ms: 1,
            })
            .collect();
        let stats = aggregate_slow_queries(&facts, 100);
        assert!(stats.is_empty());
    }

    #[test]
    fn test_detect_n_plus_one_groups_per_trace() {
        let mut facts = Vec::new();
        for _ in 0..6 {
            facts.push(DbSpanFact {
                trace_id: "trace-a".to_string(),
                statement_normalized: "SELECT * FROM items WHERE order_id = ?".to_string(),
                system: Some("postgresql".to_string()),
                service_name: Some("orders".to_string()),
                duration_ms: 5,
            });
        }
        // Different trace, only 3 repeats - below threshold.
        for _ in 0..3 {
            facts.push(DbSpanFact {
                trace_id: "trace-b".to_string(),
                statement_normalized: "SELECT * FROM items WHERE order_id = ?".to_string(),
                system: Some("postgresql".to_string()),
                service_name: Some("orders".to_string()),
                duration_ms: 5,
            });
        }
        let detections = detect_n_plus_one(&facts, 5);
        assert_eq!(detections.len(), 1);
        assert_eq!(detections[0].trace_id, "trace-a");
        assert_eq!(detections[0].count, 6);
        assert_eq!(detections[0].services, vec!["orders".to_string()]);
    }

    #[test]
    fn test_detect_n_plus_one_skips_when_repetitions_zero() {
        let facts = vec![DbSpanFact {
            trace_id: "trace-a".to_string(),
            statement_normalized: "SELECT 1".to_string(),
            system: None,
            service_name: None,
            duration_ms: 1,
        }];
        assert!(detect_n_plus_one(&facts, 0).is_empty());
    }
}
