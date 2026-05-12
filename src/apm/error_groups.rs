//! Error grouping by exception fingerprint.
//!
//! Walks OTLP trace span events of name `exception` and OTLP log records with
//! `severity_text=ERROR`, derives a stable fingerprint from the exception
//! type/message (or log severity/body), and aggregates them into issue groups.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_http::attribute_string_value;
use crate::ingest::otlp_pb::payload_as_value;

/// Length of the hex prefix of the SHA-256 digest used as a fingerprint.
const FINGERPRINT_HEX_LEN: usize = 16;
/// Number of leading bytes from the digest required to render that hex prefix.
const FINGERPRINT_BYTE_PREFIX: usize = FINGERPRINT_HEX_LEN / 2;
const OTEL_ERROR_SEVERITY_NUMBER: i64 = 17;

/// One occurrence of an error event extracted from a payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorOccurrence {
    /// Stable fingerprint hex prefix.
    pub fingerprint: String,
    /// Human readable signature (e.g. `RuntimeError: connection refused`).
    pub signature: String,
    /// Exception type, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception_type: Option<String>,
    /// Exception message, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception_message: Option<String>,
    /// Exception stacktrace, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stacktrace: Option<String>,
    /// Service name, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// Originating signal type.
    pub signal: Signal,
    /// Time the entry was observed by the ingest layer.
    pub observed_at: DateTime<Utc>,
}

/// Aggregated view of a single error group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorGroup {
    pub fingerprint: String,
    pub signature: String,
    pub count: u64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub sample_payload: Value,
}

/// Compute the canonical fingerprint hex string for a given (type, message)
/// pair. Numbers are stripped from the message so that "id=42" and "id=43"
/// fold into the same bucket.
pub fn compute_fingerprint(
    exception_type: Option<&str>,
    exception_message: Option<&str>,
) -> String {
    let normalized_type = normalize_token(exception_type.unwrap_or(""));
    let normalized_message = normalize_token(exception_message.unwrap_or(""));
    let mut hasher = Sha256::new();
    hasher.update(normalized_type.as_bytes());
    hasher.update(b"\0");
    hasher.update(normalized_message.as_bytes());
    let digest = hasher.finalize();
    hex::encode(&digest[..FINGERPRINT_BYTE_PREFIX])
}

/// Normalize a string for fingerprinting.
///
/// - Lowercases
/// - Replaces UUID-like blobs (hex with optional hyphens, length >= 8) with `<hex>`
/// - Replaces remaining ASCII digit runs with `<num>`
/// - Collapses whitespace and trims
fn normalize_token(input: &str) -> String {
    use std::sync::OnceLock;
    static HEX_RE: OnceLock<regex::Regex> = OnceLock::new();
    static NUM_RE: OnceLock<regex::Regex> = OnceLock::new();

    let lowered = input.to_ascii_lowercase();

    // Match hex blobs of 8+ chars, optionally followed by hyphenated hex segments
    // (covers UUIDs, hashes, hex-id strings).
    let hex_re = HEX_RE.get_or_init(|| {
        regex::Regex::new(r"\b[0-9a-f]{8,}(?:-[0-9a-f]+)*\b").expect("hex regex must compile")
    });
    let stage1 = hex_re.replace_all(&lowered, "<hex>");

    // Then collapse remaining digit runs (covers things like "id=42").
    let num_re = NUM_RE.get_or_init(|| regex::Regex::new(r"\d+").expect("num regex must compile"));
    let stage2 = num_re.replace_all(&stage1, "<num>");

    stage2.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Build a human readable signature for an exception.
fn signature_from_exception(
    exception_type: Option<&str>,
    exception_message: Option<&str>,
) -> String {
    match (exception_type, exception_message) {
        (Some(t), Some(m)) if !t.is_empty() && !m.is_empty() => format!("{t}: {m}"),
        (Some(t), _) if !t.is_empty() => t.to_string(),
        (_, Some(m)) if !m.is_empty() => m.to_string(),
        _ => {
            tracing::debug!("exception event with no type or message, using fallback signature");
            "<unknown error>".to_string()
        }
    }
}

/// Walk an entry and emit at most a handful of error occurrences.
///
/// - Trace span events with name="exception"
/// - Log records with severity_text=ERROR (or severityNumber >= 17)
pub fn extract_error_occurrences(entry: &NormalizedEntry) -> Vec<ErrorOccurrence> {
    match entry.signal {
        Signal::Traces => extract_from_traces(entry),
        Signal::Logs => extract_from_logs(entry),
        Signal::Metrics => Vec::new(),
    }
}

fn extract_from_traces(entry: &NormalizedEntry) -> Vec<ErrorOccurrence> {
    let value = match payload_as_value(Signal::Traces, &entry.payload) {
        Some(v) => v,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    let Some(resource_spans) = value.get("resourceSpans").and_then(Value::as_array) else {
        return out;
    };
    for resource_span in resource_spans {
        let scope_spans = resource_span.get("scopeSpans").and_then(Value::as_array);
        let Some(scope_spans) = scope_spans else {
            continue;
        };
        for scope_span in scope_spans {
            let Some(spans) = scope_span.get("spans").and_then(Value::as_array) else {
                continue;
            };
            for span in spans {
                let Some(events) = span.get("events").and_then(Value::as_array) else {
                    continue;
                };
                for event in events {
                    if event.get("name").and_then(Value::as_str) != Some("exception") {
                        continue;
                    }
                    let attrs = event
                        .get("attributes")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::warn!("exception event missing attributes, using empty list");
                            Vec::new()
                        });
                    let exception_type = attribute_string_value(&attrs, "exception.type");
                    let exception_message = attribute_string_value(&attrs, "exception.message");
                    let stacktrace = attribute_string_value(&attrs, "exception.stacktrace");
                    let signature = signature_from_exception(
                        exception_type.as_deref(),
                        exception_message.as_deref(),
                    );
                    let fingerprint = compute_fingerprint(
                        exception_type.as_deref(),
                        exception_message.as_deref(),
                    );
                    out.push(ErrorOccurrence {
                        fingerprint,
                        signature,
                        exception_type,
                        exception_message,
                        stacktrace,
                        service_name: entry.service_name.clone(),
                        signal: Signal::Traces,
                        observed_at: entry.observed_at,
                    });
                }
            }
        }
    }
    out
}

fn extract_from_logs(entry: &NormalizedEntry) -> Vec<ErrorOccurrence> {
    let value = match payload_as_value(Signal::Logs, &entry.payload) {
        Some(v) => v,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    let Some(resource_logs) = value.get("resourceLogs").and_then(Value::as_array) else {
        return out;
    };
    for resource_log in resource_logs {
        let Some(scope_logs) = resource_log.get("scopeLogs").and_then(Value::as_array) else {
            continue;
        };
        for scope_log in scope_logs {
            let Some(records) = scope_log.get("logRecords").and_then(Value::as_array) else {
                continue;
            };
            for record in records {
                if !is_error_log_record(record) {
                    continue;
                }
                let body_text = record
                    .get("body")
                    .and_then(extract_body_text)
                    .unwrap_or_default();
                let severity_text = record
                    .get("severityText")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .or_else(|| {
                        record
                            .get("severityNumber")
                            .and_then(Value::as_i64)
                            .map(|num| format!("severity={num}"))
                    })
                    .unwrap_or_else(|| "ERROR".to_string());
                let signature = if body_text.is_empty() {
                    severity_text.clone()
                } else {
                    format!("{severity_text}: {body_text}")
                };
                let fingerprint = compute_fingerprint(Some(&severity_text), Some(&body_text));
                out.push(ErrorOccurrence {
                    fingerprint,
                    signature,
                    exception_type: Some(severity_text),
                    exception_message: Some(body_text),
                    stacktrace: None,
                    service_name: entry.service_name.clone(),
                    signal: Signal::Logs,
                    observed_at: entry.observed_at,
                });
            }
        }
    }
    out
}

fn is_error_log_record(record: &Value) -> bool {
    if let Some(text) = record.get("severityText").and_then(Value::as_str)
        && text.eq_ignore_ascii_case("error")
    {
        return true;
    }
    if let Some(num) = record.get("severityNumber").and_then(Value::as_i64) {
        // OTel spec: ERROR = 17..=20, FATAL = 21..=24
        if num >= OTEL_ERROR_SEVERITY_NUMBER {
            return true;
        }
    }
    false
}

fn extract_body_text(value: &Value) -> Option<String> {
    if let Some(s) = value.get("stringValue").and_then(Value::as_str) {
        return Some(s.to_string());
    }
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }
    None
}

/// In-memory aggregator that folds [`ErrorOccurrence`]s by fingerprint.
#[derive(Debug, Default)]
pub struct ErrorGroupAggregator {
    groups: HashMap<String, ErrorGroup>,
}

impl ErrorGroupAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one occurrence, updating count / first_seen / last_seen.
    pub fn record(&mut self, occ: ErrorOccurrence) {
        let payload = build_sample_payload(&occ);
        match self.groups.get_mut(&occ.fingerprint) {
            Some(existing) => {
                existing.count = existing.count.saturating_add(1);
                if occ.observed_at < existing.first_seen {
                    existing.first_seen = occ.observed_at;
                }
                if occ.observed_at > existing.last_seen {
                    existing.last_seen = occ.observed_at;
                    existing.sample_payload = payload;
                    existing.signature = occ.signature.clone();
                }
            }
            None => {
                self.groups.insert(
                    occ.fingerprint.clone(),
                    ErrorGroup {
                        fingerprint: occ.fingerprint.clone(),
                        signature: occ.signature.clone(),
                        count: 1,
                        first_seen: occ.observed_at,
                        last_seen: occ.observed_at,
                        sample_payload: payload,
                    },
                );
            }
        }
    }

    /// Drain the aggregator into a list of groups.
    pub fn into_groups(self) -> Vec<ErrorGroup> {
        self.groups.into_values().collect()
    }
}

fn build_sample_payload(occ: &ErrorOccurrence) -> Value {
    serde_json::json!({
        "exception_type": occ.exception_type,
        "exception_message": occ.exception_message,
        "stacktrace": occ.stacktrace,
        "service_name": occ.service_name,
        "signal": occ.signal,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use chrono::TimeZone;

    fn make_entry(signal: Signal, payload: serde_json::Value) -> NormalizedEntry {
        NormalizedEntry {
            signal,
            observed_at: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            service_name: Some("svc-a".to_string()),
            payload: Bytes::from(payload.to_string()),
        }
    }

    #[test]
    fn fingerprint_stable_for_same_input() {
        let a = compute_fingerprint(Some("RuntimeError"), Some("boom"));
        let b = compute_fingerprint(Some("RuntimeError"), Some("boom"));
        assert_eq!(a, b);
        assert_eq!(a.len(), FINGERPRINT_HEX_LEN);
    }

    #[test]
    fn fingerprint_normalizes_digits() {
        // numbers should not influence the bucket
        let a = compute_fingerprint(Some("RuntimeError"), Some("user_id=42 not found"));
        let b = compute_fingerprint(Some("RuntimeError"), Some("user_id=99999 not found"));
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_normalizes_uuids() {
        let a = compute_fingerprint(
            Some("RuntimeError"),
            Some("missing trace 550e8400-e29b-41d4-a716-446655440000"),
        );
        let b = compute_fingerprint(
            Some("RuntimeError"),
            Some("missing trace deadbeefdeadbeefdeadbeefdeadbeef"),
        );
        // both contain a long hex blob; after normalization they should fold
        assert_eq!(a, b);
    }

    #[test]
    fn fingerprint_distinct_for_different_types() {
        let a = compute_fingerprint(Some("RuntimeError"), Some("boom"));
        let b = compute_fingerprint(Some("ValueError"), Some("boom"));
        assert_ne!(a, b);
    }

    #[test]
    fn extracts_exception_from_trace_event() {
        let payload = serde_json::json!({
            "resourceSpans": [{
                "resource": {"attributes": []},
                "scopeSpans": [{
                    "spans": [{
                        "name": "GET /",
                        "events": [{
                            "name": "exception",
                            "attributes": [
                                {"key": "exception.type", "value": {"stringValue": "RuntimeError"}},
                                {"key": "exception.message", "value": {"stringValue": "kaboom 7"}},
                                {"key": "exception.stacktrace", "value": {"stringValue": "trace..."}}
                            ]
                        }]
                    }]
                }]
            }]
        });
        let entry = make_entry(Signal::Traces, payload);
        let occs = extract_error_occurrences(&entry);
        assert_eq!(occs.len(), 1);
        assert_eq!(occs[0].exception_type.as_deref(), Some("RuntimeError"));
        assert_eq!(occs[0].exception_message.as_deref(), Some("kaboom 7"));
        assert_eq!(occs[0].stacktrace.as_deref(), Some("trace..."));
        assert_eq!(occs[0].signal, Signal::Traces);
    }

    #[test]
    fn extracts_error_log_record() {
        let payload = serde_json::json!({
            "resourceLogs": [{
                "resource": {"attributes": []},
                "scopeLogs": [{
                    "logRecords": [
                        {"severityText": "ERROR", "body": {"stringValue": "db down"}},
                        {"severityText": "INFO", "body": {"stringValue": "all good"}},
                        {"severityNumber": 17, "body": {"stringValue": "low level"}}
                    ]
                }]
            }]
        });
        let entry = make_entry(Signal::Logs, payload);
        let occs = extract_error_occurrences(&entry);
        assert_eq!(occs.len(), 2);
        assert!(
            occs.iter()
                .any(|o| o.exception_message.as_deref() == Some("db down"))
        );
        assert!(
            occs.iter()
                .any(|o| o.exception_message.as_deref() == Some("low level"))
        );
    }

    #[test]
    fn aggregator_counts_occurrences_and_picks_latest_sample() {
        let mut agg = ErrorGroupAggregator::new();
        let early = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let late = Utc.with_ymd_and_hms(2024, 1, 1, 0, 1, 0).unwrap();

        let fingerprint = compute_fingerprint(Some("RuntimeError"), Some("boom 42"));
        agg.record(ErrorOccurrence {
            fingerprint: fingerprint.clone(),
            signature: "RuntimeError: boom 42".into(),
            exception_type: Some("RuntimeError".into()),
            exception_message: Some("boom 42".into()),
            stacktrace: None,
            service_name: Some("svc-a".into()),
            signal: Signal::Traces,
            observed_at: early,
        });
        agg.record(ErrorOccurrence {
            fingerprint,
            signature: "RuntimeError: boom 99".into(),
            exception_type: Some("RuntimeError".into()),
            exception_message: Some("boom 99".into()),
            stacktrace: None,
            service_name: Some("svc-b".into()),
            signal: Signal::Traces,
            observed_at: late,
        });

        let groups = agg.into_groups();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].count, 2);
        assert_eq!(groups[0].first_seen, early);
        assert_eq!(groups[0].last_seen, late);
        // The sample payload should reflect the latest occurrence.
        assert_eq!(groups[0].sample_payload["service_name"], "svc-b");
        assert_eq!(groups[0].signature, "RuntimeError: boom 99");
    }
}
