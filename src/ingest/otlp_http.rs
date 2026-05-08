use crate::domain::telemetry::{NormalizedEntry, SERVICE_NAME_ATTRIBUTE, Signal};
use crate::ingest::decode::{ContentType, DecodeError, parse_content_type};
use crate::ingest::otlp_pb::payload_to_json_value;
use bytes::Bytes;
use chrono::Utc;

/// Parses an OTLP/HTTP request and produces a NormalizedEntry (no I/O).
///
/// - Returns `DecodeError::UnsupportedContentType` if the content-type is unsupported.
/// - On success, returns a NormalizedEntry that holds the payload as-is.
///   For JSON payloads, extracts `service.name` according to the signal type.
pub fn parse_ingest_request(
    signal: Signal,
    content_type_header: Option<&str>,
    body: Bytes,
) -> Result<NormalizedEntry, DecodeError> {
    let ct = content_type_header.unwrap_or("");
    let content_type = parse_content_type(ct)?;
    Ok(NormalizedEntry {
        signal,
        observed_at: Utc::now(),
        service_name: extract_service_name(signal, content_type, &body),
        payload: body,
    })
}

fn extract_service_name(signal: Signal, content_type: ContentType, body: &Bytes) -> Option<String> {
    let value = match content_type {
        ContentType::Json => serde_json::from_slice(body).ok()?,
        ContentType::Protobuf => payload_to_json_value(signal, body)?,
    };
    extract_service_name_from_value(signal, &value)
}

pub fn extract_service_name_from_value(
    signal: Signal,
    value: &serde_json::Value,
) -> Option<String> {
    let resource_blocks = resource_blocks_for_signal(signal, value)?;

    for resource_block in resource_blocks {
        let Some(attributes) = resource_block
            .get("resource")
            .and_then(|resource| resource.get("attributes"))
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };

        if let Some(service_name) = attribute_string_value(attributes, SERVICE_NAME_ATTRIBUTE) {
            return Some(service_name);
        }
    }

    None
}

fn resource_blocks_for_signal(
    signal: Signal,
    value: &serde_json::Value,
) -> Option<&Vec<serde_json::Value>> {
    match signal {
        Signal::Traces => value.get("resourceSpans")?.as_array(),
        Signal::Metrics => value.get("resourceMetrics")?.as_array(),
        Signal::Logs => value.get("resourceLogs")?.as_array(),
    }
}

pub fn attribute_string_value(attributes: &[serde_json::Value], key: &str) -> Option<String> {
    for attribute in attributes {
        if attribute.get("key").and_then(serde_json::Value::as_str) != Some(key) {
            continue;
        }

        if let Some(value) = attribute
            .get("value")
            .and_then(|value| value.get("stringValue"))
            .and_then(serde_json::Value::as_str)
        {
            return Some(value.to_string());
        }
    }

    None
}

/// Decodes an OTLP/HTTP payload (JSON or protobuf) into its canonical JSON
/// representation for further inspection (e.g. attribute extraction).
///
/// Returns `None` when the content type is unsupported or decoding fails.
pub fn decode_payload_value(
    signal: Signal,
    content_type_header: Option<&str>,
    body: &Bytes,
) -> Option<serde_json::Value> {
    let ct = content_type_header.unwrap_or("");
    let content_type = parse_content_type(ct).ok()?;
    match content_type {
        ContentType::Json => serde_json::from_slice(body).ok(),
        ContentType::Protobuf => payload_to_json_value(signal, body),
    }
}

/// `(attribute_key, attribute_value)` pair to be written to the attribute
/// inverted index.
pub type IndexableAttribute = (String, String);

/// Extracts all indexable attribute (key, value) pairs from a decoded OTLP
/// payload (the JSON representation produced by [`decode_payload_value`]).
///
/// Walks resource attributes plus signal-specific attributes:
///   - Traces  : resource attributes + span attributes (e.g. `http.route`)
///   - Metrics : resource attributes + per-data-point attributes
///   - Logs    : resource attributes + log record attributes
///
/// Duplicates and `service.name` are filtered out (`service.name` already has
/// a dedicated index path on `NormalizedEntry::service_name`). Only
/// `stringValue`-typed attributes are indexed; numeric / array values are
/// dropped to keep the index focused on autocomplete-friendly text.
pub fn extract_indexable_attributes_from_value(
    signal: Signal,
    value: &serde_json::Value,
) -> Vec<IndexableAttribute> {
    use std::collections::HashSet;

    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut out: Vec<IndexableAttribute> = Vec::new();

    let Some(blocks) = resource_blocks_for_signal(signal, value) else {
        return out;
    };

    for block in blocks {
        if let Some(resource_attrs) = block.get("resource").and_then(|r| r.get("attributes")) {
            push_string_attrs(resource_attrs, &mut seen, &mut out);
        }

        match signal {
            Signal::Traces => walk_signal_attrs(block, "scopeSpans", "spans", &mut seen, &mut out),
            Signal::Logs => {
                walk_signal_attrs(block, "scopeLogs", "logRecords", &mut seen, &mut out)
            }
            Signal::Metrics => {
                let Some(scopes) = block
                    .get("scopeMetrics")
                    .and_then(serde_json::Value::as_array)
                else {
                    continue;
                };
                for scope in scopes {
                    let Some(metrics) = scope.get("metrics").and_then(serde_json::Value::as_array)
                    else {
                        continue;
                    };
                    for metric in metrics {
                        push_metric_data_point_attrs(metric, &mut seen, &mut out);
                    }
                }
            }
        }
    }

    out
}

fn walk_signal_attrs(
    block: &serde_json::Value,
    scopes_key: &str,
    items_key: &str,
    seen: &mut std::collections::HashSet<(String, String)>,
    out: &mut Vec<IndexableAttribute>,
) {
    let Some(scopes) = block.get(scopes_key).and_then(serde_json::Value::as_array) else {
        return;
    };
    for scope in scopes {
        let Some(items) = scope.get(items_key).and_then(serde_json::Value::as_array) else {
            continue;
        };
        for item in items {
            if let Some(attrs) = item.get("attributes") {
                push_string_attrs(attrs, seen, out);
            }
        }
    }
}

fn push_string_attrs(
    attrs: &serde_json::Value,
    seen: &mut std::collections::HashSet<(String, String)>,
    out: &mut Vec<IndexableAttribute>,
) {
    let Some(arr) = attrs.as_array() else {
        return;
    };
    for attr in arr {
        let Some(key) = attr.get("key").and_then(serde_json::Value::as_str) else {
            continue;
        };
        // service.name is already indexed by service_name extraction; skip to
        // avoid duplication and bloat in the inverted index.
        if key == SERVICE_NAME_ATTRIBUTE {
            continue;
        }
        let Some(value) = attr
            .get("value")
            .and_then(|v| v.get("stringValue"))
            .and_then(serde_json::Value::as_str)
        else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        let entry = (key.to_string(), value.to_string());
        if seen.insert(entry.clone()) {
            out.push(entry);
        }
    }
}

fn push_metric_data_point_attrs(
    metric: &serde_json::Value,
    seen: &mut std::collections::HashSet<(String, String)>,
    out: &mut Vec<IndexableAttribute>,
) {
    // Metric body shape varies (gauge / sum / histogram / summary /
    // exponentialHistogram) but each carries a `dataPoints` array whose
    // entries have `attributes`.
    for kind in [
        "gauge",
        "sum",
        "histogram",
        "summary",
        "exponentialHistogram",
    ] {
        let Some(body) = metric.get(kind) else {
            continue;
        };
        let Some(points) = body.get("dataPoints").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for point in points {
            if let Some(attrs) = point.get("attributes") {
                push_string_attrs(attrs, seen, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;
    use crate::ingest::decode::DecodeError;
    use bytes::Bytes;
    use chrono::Utc;

    // --- parse_ingest_request: happy path -----------------------------------

    #[test]
    fn test_parse_ingest_request_protobuf_traces_returns_entry() {
        // Given: traces signal, protobuf content-type, binary payload
        let signal = Signal::Traces;
        let body = Bytes::from_static(b"\x0a\x0b\x0c");

        // When: parse
        let result = parse_ingest_request(signal, Some("application/x-protobuf"), body.clone());

        // Then: a NormalizedEntry with the correct signal and payload is returned
        let entry = result.unwrap();
        assert_eq!(entry.signal, Signal::Traces);
        assert_eq!(entry.payload, body);
    }

    #[test]
    fn test_parse_ingest_request_json_traces_returns_entry() {
        // Given: traces signal, JSON content-type
        let result = parse_ingest_request(
            Signal::Traces,
            Some("application/json"),
            Bytes::from_static(b"{}"),
        );

        // Then: succeeds
        assert!(result.is_ok());
        assert_eq!(result.unwrap().signal, Signal::Traces);
    }

    #[test]
    fn test_parse_ingest_request_json_with_charset_returns_entry() {
        // Given: "application/json; charset=utf-8" (with charset parameter)
        let result = parse_ingest_request(
            Signal::Traces,
            Some("application/json; charset=utf-8"),
            Bytes::new(),
        );

        // Then: charset is ignored and the call succeeds
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_ingest_request_metrics_signal_propagates() {
        // Given: metrics signal
        let result = parse_ingest_request(
            Signal::Metrics,
            Some("application/x-protobuf"),
            Bytes::new(),
        );

        // Then: signal propagates as Metrics
        assert_eq!(result.unwrap().signal, Signal::Metrics);
    }

    #[test]
    fn test_parse_ingest_request_logs_signal_propagates() {
        // Given: logs signal
        let result =
            parse_ingest_request(Signal::Logs, Some("application/x-protobuf"), Bytes::new());

        // Then: signal propagates as Logs
        assert_eq!(result.unwrap().signal, Signal::Logs);
    }

    #[test]
    fn test_parse_ingest_request_preserves_payload_bytes() {
        // Given: a specific byte sequence
        let payload = Bytes::copy_from_slice(&[0x0a, 0x1b, 0x2c, 0x3d]);

        // When: parse
        let entry = parse_ingest_request(
            Signal::Traces,
            Some("application/x-protobuf"),
            payload.clone(),
        )
        .unwrap();

        // Then: payload is preserved unchanged
        assert_eq!(entry.payload, payload);
    }

    #[test]
    fn test_parse_ingest_request_empty_body_succeeds() {
        // Given: empty body
        let result =
            parse_ingest_request(Signal::Traces, Some("application/x-protobuf"), Bytes::new());

        // Then: succeeds (body contents are not validated)
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_ingest_request_observed_at_is_close_to_now() {
        // Given: record timestamps before and after parsing
        let before = Utc::now();

        // When: parse
        let entry =
            parse_ingest_request(Signal::Traces, Some("application/x-protobuf"), Bytes::new())
                .unwrap();

        let after = Utc::now();

        // Then: observed_at falls within the range [before, after]
        assert!(
            entry.observed_at >= before,
            "observed_at {:?} should be >= before {:?}",
            entry.observed_at,
            before
        );
        assert!(
            entry.observed_at <= after,
            "observed_at {:?} should be <= after {:?}",
            entry.observed_at,
            after
        );
    }

    // --- parse_ingest_request: error cases ----------------------------------

    #[test]
    fn test_parse_ingest_request_text_plain_returns_unsupported_error() {
        // Given: text/plain (unsupported content-type)
        let result = parse_ingest_request(Signal::Traces, Some("text/plain"), Bytes::new());

        // Then: UnsupportedContentType error
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_ingest_request_missing_content_type_returns_error() {
        // Given: no content-type header (None)
        let result = parse_ingest_request(Signal::Traces, None, Bytes::new());

        // Then: UnsupportedContentType error (treated as empty string)
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_ingest_request_empty_content_type_returns_error() {
        // Given: empty string content-type
        let result = parse_ingest_request(Signal::Traces, Some(""), Bytes::new());

        // Then: UnsupportedContentType error
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_ingest_request_multipart_returns_error() {
        // Given: multipart/form-data (unsupported)
        let result =
            parse_ingest_request(Signal::Traces, Some("multipart/form-data"), Bytes::new());

        // Then: UnsupportedContentType error
        assert!(matches!(
            result,
            Err(DecodeError::UnsupportedContentType(_))
        ));
    }

    // --- parse_ingest_request: boundary values ------------------------------

    #[test]
    fn test_parse_ingest_request_service_name_is_none_for_invalid_protobuf() {
        // Given: protobuf content-type but body is not a valid OTLP request
        let entry =
            parse_ingest_request(Signal::Traces, Some("application/x-protobuf"), Bytes::new())
                .unwrap();

        // Then: service_name is None (decode silently fails, payload retained)
        assert_eq!(entry.service_name, None);
    }

    // --- parse_ingest_request: service_name from protobuf -----------------

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_trace_protobuf() {
        use crate::otlp_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
        use crate::otlp_proto::opentelemetry::proto::trace::v1::ResourceSpans;
        use buffa::Message;

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: buffa::MessageField::some(resource_with_service_name("checkout-ui")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let body = request.encode_to_bytes();

        let entry =
            parse_ingest_request(Signal::Traces, Some("application/x-protobuf"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("checkout-ui"));
    }

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_metric_protobuf() {
        use crate::otlp_proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
        use crate::otlp_proto::opentelemetry::proto::metrics::v1::ResourceMetrics;
        use buffa::Message;

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: buffa::MessageField::some(resource_with_service_name("orders-api")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let body = request.encode_to_bytes();

        let entry =
            parse_ingest_request(Signal::Metrics, Some("application/x-protobuf"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("orders-api"));
    }

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_log_protobuf() {
        use crate::otlp_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
        use crate::otlp_proto::opentelemetry::proto::logs::v1::ResourceLogs;
        use buffa::Message;

        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: buffa::MessageField::some(resource_with_service_name("worker-billing")),
                ..Default::default()
            }],
            ..Default::default()
        };
        let body = request.encode_to_bytes();

        let entry =
            parse_ingest_request(Signal::Logs, Some("application/x-protobuf"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("worker-billing"));
    }

    fn resource_with_service_name(
        name: &str,
    ) -> crate::otlp_proto::opentelemetry::proto::resource::v1::Resource {
        use crate::otlp_proto::opentelemetry::proto::common::v1::__buffa::oneof::any_value;
        use crate::otlp_proto::opentelemetry::proto::common::v1::{AnyValue, KeyValue};
        use crate::otlp_proto::opentelemetry::proto::resource::v1::Resource;

        Resource {
            attributes: vec![KeyValue {
                key: SERVICE_NAME_ATTRIBUTE.to_string(),
                value: buffa::MessageField::some(AnyValue {
                    value: Some(any_value::Value::StringValue(name.to_string())),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    // --- parse_ingest_request: service_name from JSON ---------------------

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_trace_json() {
        let body = Bytes::from_static(
            br#"{
                "resourceSpans": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "checkout-ui" }
                                }
                            ]
                        }
                    }
                ]
            }"#,
        );

        let entry = parse_ingest_request(Signal::Traces, Some("application/json"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("checkout-ui"));
    }

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_later_resource_span() {
        let body = Bytes::from_static(
            br#"{
                "resourceSpans": [
                    {
                        "resource": {}
                    },
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "payments-api" }
                                }
                            ]
                        }
                    }
                ]
            }"#,
        );

        let entry = parse_ingest_request(Signal::Traces, Some("application/json"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("payments-api"));
    }

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_metric_json() {
        let body = Bytes::from_static(
            br#"{
                "resourceMetrics": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "orders-api" }
                                }
                            ]
                        }
                    }
                ]
            }"#,
        );

        let entry = parse_ingest_request(Signal::Metrics, Some("application/json"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("orders-api"));
    }

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_log_json() {
        let body = Bytes::from_static(
            br#"{
                "resourceLogs": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "worker-billing" }
                                }
                            ]
                        }
                    }
                ]
            }"#,
        );

        let entry = parse_ingest_request(Signal::Logs, Some("application/json"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("worker-billing"));
    }

    // --- extract_indexable_attributes_from_value ---------------------------

    #[test]
    fn test_extract_indexable_attributes_walks_resource_and_span_attributes() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{
                "resourceSpans": [
                    {
                        "resource": {
                            "attributes": [
                                { "key": "service.name", "value": { "stringValue": "checkout" } },
                                { "key": "k8s.pod.name", "value": { "stringValue": "checkout-7" } }
                            ]
                        },
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "attributes": [
                                            { "key": "http.route", "value": { "stringValue": "/api/x" } },
                                            { "key": "http.method", "value": { "stringValue": "GET" } }
                                        ]
                                    },
                                    {
                                        "attributes": [
                                            { "key": "http.route", "value": { "stringValue": "/api/x" } }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }"#,
        )
        .unwrap();

        let attrs = extract_indexable_attributes_from_value(Signal::Traces, &value);
        // service.name is intentionally filtered out
        assert!(attrs.iter().all(|(k, _)| k != "service.name"));
        assert!(attrs.contains(&("k8s.pod.name".to_string(), "checkout-7".to_string())));
        assert!(attrs.contains(&("http.route".to_string(), "/api/x".to_string())));
        assert!(attrs.contains(&("http.method".to_string(), "GET".to_string())));
        // duplicates collapsed
        let route_count = attrs.iter().filter(|(k, _)| k == "http.route").count();
        assert_eq!(route_count, 1);
    }

    #[test]
    fn test_extract_indexable_attributes_walks_log_records() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{
                "resourceLogs": [{
                    "resource": {"attributes": [{"key":"k8s.namespace","value":{"stringValue":"prod"}}]},
                    "scopeLogs": [{
                        "logRecords": [{
                            "attributes": [{"key":"log.level","value":{"stringValue":"ERROR"}}]
                        }]
                    }]
                }]
            }"#,
        )
        .unwrap();

        let attrs = extract_indexable_attributes_from_value(Signal::Logs, &value);
        assert!(attrs.contains(&("k8s.namespace".to_string(), "prod".to_string())));
        assert!(attrs.contains(&("log.level".to_string(), "ERROR".to_string())));
    }

    #[test]
    fn test_extract_indexable_attributes_walks_metric_data_points() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{
                "resourceMetrics": [{
                    "resource": {"attributes": []},
                    "scopeMetrics": [{
                        "metrics": [{
                            "name": "http.requests",
                            "sum": {
                                "dataPoints": [
                                    {"attributes":[{"key":"http.status","value":{"stringValue":"200"}}]},
                                    {"attributes":[{"key":"http.status","value":{"stringValue":"500"}}]}
                                ]
                            }
                        }]
                    }]
                }]
            }"#,
        )
        .unwrap();

        let attrs = extract_indexable_attributes_from_value(Signal::Metrics, &value);
        assert!(attrs.contains(&("http.status".to_string(), "200".to_string())));
        assert!(attrs.contains(&("http.status".to_string(), "500".to_string())));
    }

    #[test]
    fn test_extract_indexable_attributes_skips_non_string_values() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{
                "resourceSpans": [{
                    "resource": {"attributes": [
                        {"key":"int.attr","value":{"intValue":"42"}},
                        {"key":"text.attr","value":{"stringValue":"yes"}}
                    ]}
                }]
            }"#,
        )
        .unwrap();

        let attrs = extract_indexable_attributes_from_value(Signal::Traces, &value);
        assert!(attrs.iter().all(|(k, _)| k != "int.attr"));
        assert!(attrs.contains(&("text.attr".to_string(), "yes".to_string())));
    }

    #[test]
    fn test_decode_payload_value_decodes_json_payload() {
        let body = Bytes::from_static(br#"{"resourceSpans":[]}"#);
        let value = decode_payload_value(Signal::Traces, Some("application/json"), &body).unwrap();
        assert!(value.get("resourceSpans").is_some());
    }

    #[test]
    fn test_decode_payload_value_returns_none_for_unsupported_content_type() {
        let body = Bytes::from_static(b"{}");
        assert!(decode_payload_value(Signal::Traces, Some("text/plain"), &body).is_none());
    }
}
