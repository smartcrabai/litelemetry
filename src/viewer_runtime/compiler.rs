use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::domain::viewer::ViewerDefinition;
use bytes::Bytes;
use serde_json::Value;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("signal_mask has no valid signals: viewer must watch at least one signal")]
    ZeroSignalMask,
    #[error("lookback_ms must be positive, got {0}")]
    NonPositiveLookback(i64),
}

/// Compiled viewer. Generated from ViewerDefinition at startup.
/// `compile()` guarantees that signal_mask > 0 and lookback_ms > 0.
#[derive(Debug)]
pub struct CompiledViewer {
    definition: ViewerDefinition,
    /// Normalized (trimmed, lowercased) query extracted from definition_json.
    /// None means match-all.
    query: Option<String>,
}

impl CompiledViewer {
    /// Returns whether this entry matches the viewer's target signals
    pub fn matches_signal(&self, signal: Signal) -> bool {
        self.definition.signal_mask.contains(signal)
    }

    /// Case-insensitive substring match against service_name and signal-specific payload fields.
    /// Returns true (match-all) when no query is set.
    pub fn matches_entry(&self, entry: &NormalizedEntry) -> bool {
        let Some(query) = &self.query else {
            return true;
        };
        if let Some(svc) = &entry.service_name
            && svc.to_lowercase().contains(query.as_str())
        {
            return true;
        }
        if let Some(payload_text) = extract_searchable_payload_text(entry.signal, &entry.payload) {
            return payload_text.to_lowercase().contains(query.as_str());
        }
        false
    }

    /// Returns the normalized query string, or None if no filter is set.
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    pub fn definition(&self) -> &ViewerDefinition {
        &self.definition
    }

    pub fn lookback_ms(&self) -> i64 {
        self.definition.lookback_ms
    }

    pub fn update_definition_json(&mut self, definition_json: Value, layout_json: Value) {
        self.query = query_from_definition(&definition_json);
        self.definition.definition_json = definition_json;
        self.definition.layout_json = layout_json;
    }
}

/// Extracts and normalizes the query from definition_json.
/// Returns None if the "query" field is absent or blank after trimming.
/// Otherwise returns the trimmed, lowercased query.
pub fn query_from_definition(definition_json: &Value) -> Option<String> {
    let raw = definition_json.get("query")?.as_str()?;
    let normalized = raw.trim().to_lowercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

/// Extracts searchable text from the payload for each signal type.
/// Returns None if the payload is not parseable or contains no relevant fields.
fn extract_searchable_payload_text(signal: Signal, payload: &Bytes) -> Option<String> {
    let value: Value = serde_json::from_slice(payload).ok()?;
    match signal {
        Signal::Traces => {
            let resource_spans = value.get("resourceSpans")?.as_array()?;
            let names: Vec<&str> = resource_spans
                .iter()
                .flat_map(|rs| {
                    rs.get("scopeSpans")
                        .and_then(|v| v.as_array())
                        .into_iter()
                        .flatten()
                        .flat_map(|ss| {
                            ss.get("spans")
                                .and_then(|v| v.as_array())
                                .into_iter()
                                .flatten()
                                .filter_map(|span| span.get("name").and_then(|n| n.as_str()))
                        })
                })
                .collect();
            if names.is_empty() {
                return None;
            }
            Some(names.join(" "))
        }
        Signal::Metrics => {
            let resource_metrics = value.get("resourceMetrics")?.as_array()?;
            let names: Vec<&str> = resource_metrics
                .iter()
                .flat_map(|rm| {
                    rm.get("scopeMetrics")
                        .and_then(|v| v.as_array())
                        .into_iter()
                        .flatten()
                        .flat_map(|sm| {
                            sm.get("metrics")
                                .and_then(|v| v.as_array())
                                .into_iter()
                                .flatten()
                                .filter_map(|m| m.get("name").and_then(|n| n.as_str()))
                        })
                })
                .collect();
            if names.is_empty() {
                return None;
            }
            Some(names.join(" "))
        }
        Signal::Logs => {
            let resource_logs = value.get("resourceLogs")?.as_array()?;
            let mut texts: Vec<&str> = Vec::new();
            for rl in resource_logs {
                for sl in rl
                    .get("scopeLogs")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
                {
                    for lr in sl
                        .get("logRecords")
                        .and_then(|v| v.as_array())
                        .into_iter()
                        .flatten()
                    {
                        if let Some(s) = lr.get("severityText").and_then(|v| v.as_str()) {
                            texts.push(s);
                        }
                        if let Some(body) = lr.get("body") {
                            if let Some(s) = body.get("stringValue").and_then(|v| v.as_str()) {
                                texts.push(s);
                            } else if let Some(s) = body.as_str() {
                                texts.push(s);
                            }
                        }
                    }
                }
            }
            if texts.is_empty() {
                return None;
            }
            Some(texts.join(" "))
        }
    }
}

/// Validates and converts a ViewerDefinition into a CompiledViewer.
/// Returns an error if signal_mask is empty or lookback_ms is 0 or less.
pub fn compile(definition: ViewerDefinition) -> Result<CompiledViewer, CompileError> {
    if definition.signal_mask.is_empty() {
        return Err(CompileError::ZeroSignalMask);
    }
    if definition.lookback_ms <= 0 {
        return Err(CompileError::NonPositiveLookback(definition.lookback_ms));
    }
    let query = query_from_definition(&definition.definition_json);
    Ok(CompiledViewer { definition, query })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::SignalMask;
    use bytes::Bytes;
    use serde_json::json;
    use uuid::Uuid;

    fn make_viewer(signal_mask: SignalMask) -> ViewerDefinition {
        ViewerDefinition {
            id: Uuid::new_v4(),
            slug: "test-viewer".to_string(),
            name: "Test Viewer".to_string(),
            refresh_interval_ms: 5_000,
            lookback_ms: 60_000,
            signal_mask,
            definition_json: json!({}),
            layout_json: json!({}),
            revision: 1,
            enabled: true,
        }
    }

    // --- compile ------------------------------------------------------------

    #[test]
    fn test_compile_succeeds_for_traces_only() {
        // Given: a viewer definition targeting only traces
        let def = make_viewer(Signal::Traces.into());

        // When: compile
        let result = compile(def);

        // Then: succeeds
        assert!(result.is_ok());
    }

    #[test]
    fn test_compile_fails_for_zero_signal_mask() {
        // Given: signal_mask = 0 (no signal specified)
        let def = make_viewer(SignalMask::NONE);

        // When: compile
        let result = compile(def);

        // Then: ZeroSignalMask error
        assert!(
            matches!(result, Err(CompileError::ZeroSignalMask)),
            "expected ZeroSignalMask, got {result:?}"
        );
    }

    #[test]
    fn test_compile_fails_for_non_positive_lookback() {
        // Given: lookback_ms = 0
        let mut def = make_viewer(Signal::Traces.into());
        def.lookback_ms = 0;

        // When: compile
        let result = compile(def);

        // Then: NonPositiveLookback error
        assert!(
            matches!(result, Err(CompileError::NonPositiveLookback(0))),
            "expected NonPositiveLookback, got {result:?}"
        );
    }

    #[test]
    fn test_compile_preserves_definition_fields() {
        // Given: a viewer definition with a specific id / slug
        let def = make_viewer(Signal::Traces.into());
        let id = def.id;
        let slug = def.slug.clone();

        // When: compile
        let compiled = compile(def).unwrap();

        // Then: definition fields are preserved
        assert_eq!(compiled.definition.id, id);
        assert_eq!(compiled.definition.slug, slug);
    }

    // --- matches_signal -----------------------------------------------------

    #[test]
    fn test_matches_signal_traces_only() {
        // Given: a traces-only viewer
        let compiled = compile(make_viewer(Signal::Traces.into())).unwrap();

        // When/Then: only traces matches; others do not
        assert!(compiled.matches_signal(Signal::Traces));
        assert!(!compiled.matches_signal(Signal::Metrics));
        assert!(!compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_metrics_only() {
        // Given: a metrics-only viewer
        let compiled = compile(make_viewer(Signal::Metrics.into())).unwrap();

        // When/Then
        assert!(!compiled.matches_signal(Signal::Traces));
        assert!(compiled.matches_signal(Signal::Metrics));
        assert!(!compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_logs_only() {
        // Given: a logs-only viewer
        let compiled = compile(make_viewer(Signal::Logs.into())).unwrap();

        // When/Then
        assert!(!compiled.matches_signal(Signal::Traces));
        assert!(!compiled.matches_signal(Signal::Metrics));
        assert!(compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_all_signals() {
        // Given: a viewer for traces | metrics | logs
        let compiled =
            compile(make_viewer(Signal::Traces | Signal::Metrics | Signal::Logs)).unwrap();

        // When/Then: matches all signals
        assert!(compiled.matches_signal(Signal::Traces));
        assert!(compiled.matches_signal(Signal::Metrics));
        assert!(compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_traces_and_metrics() {
        // Given: a viewer for traces | metrics
        let compiled = compile(make_viewer(Signal::Traces | Signal::Metrics)).unwrap();

        // When/Then: does not match logs
        assert!(compiled.matches_signal(Signal::Traces));
        assert!(compiled.matches_signal(Signal::Metrics));
        assert!(!compiled.matches_signal(Signal::Logs));
    }

    // --- query_from_definition -----------------------------------------------

    #[test]
    fn test_query_from_definition_missing_returns_none() {
        // Given: definition_json with no "query" field
        // When: query_from_definition
        // Then: None
        assert_eq!(query_from_definition(&json!({})), None);
    }

    #[test]
    fn test_query_from_definition_blank_string_returns_none() {
        // Given: definition_json with empty query string
        // When: query_from_definition
        // Then: None
        assert_eq!(query_from_definition(&json!({ "query": "" })), None);
    }

    #[test]
    fn test_query_from_definition_whitespace_only_returns_none() {
        // Given: definition_json with whitespace-only query
        // When: query_from_definition
        // Then: None
        assert_eq!(query_from_definition(&json!({ "query": "   " })), None);
    }

    #[test]
    fn test_query_from_definition_normalizes_to_trimmed_lowercase() {
        // Given: definition_json with mixed-case query and surrounding spaces
        // When: query_from_definition
        // Then: trimmed and lowercased
        assert_eq!(
            query_from_definition(&json!({ "query": "  CheckOut-UI  " })),
            Some("checkout-ui".to_string())
        );
    }

    #[test]
    fn test_query_from_definition_preserves_inner_spaces() {
        // Given: query with inner whitespace
        // When: query_from_definition
        // Then: inner spaces preserved (only surrounding trimmed)
        assert_eq!(
            query_from_definition(&json!({ "query": "hello world" })),
            Some("hello world".to_string())
        );
    }

    // --- compile with query --------------------------------------------------

    #[test]
    fn test_compile_with_query_stores_normalized_query() {
        // Given: definition_json contains a mixed-case query
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({ "query": "CheckOut-UI" });

        // When: compile
        let compiled = compile(def).unwrap();

        // Then: the compiled viewer stores the normalized lowercase query
        assert_eq!(compiled.query(), Some("checkout-ui"));
    }

    #[test]
    fn test_compile_without_query_has_no_query() {
        // Given: definition_json has no query field
        let def = make_viewer(Signal::Traces.into());

        // When: compile
        let compiled = compile(def).unwrap();

        // Then: no query (match-all)
        assert_eq!(compiled.query(), None);
    }

    #[test]
    fn test_compile_blank_query_has_no_query() {
        // Given: definition_json has blank query (should be treated as absent)
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({ "query": "   " });

        // When: compile
        let compiled = compile(def).unwrap();

        // Then: no query
        assert_eq!(compiled.query(), None);
    }

    // --- matches_entry -------------------------------------------------------

    fn make_entry_with_service(signal: Signal, service_name: &str) -> NormalizedEntry {
        NormalizedEntry {
            signal,
            observed_at: chrono::Utc::now(),
            service_name: Some(service_name.to_string()),
            payload: Bytes::from_static(b"{}"),
        }
    }

    fn make_viewer_with_query(signal_mask: SignalMask, query: &str) -> ViewerDefinition {
        let mut def = make_viewer(signal_mask);
        def.definition_json = json!({ "query": query });
        def
    }

    #[test]
    fn test_matches_entry_no_query_always_matches() {
        // Given: viewer with no query (match-all)
        let compiled = compile(make_viewer(Signal::Traces.into())).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "any-service");

        // When/Then: entry always matches when no query is set
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_query_matches_service_name_substring() {
        // Given: viewer with query "checkout", entry with service_name "checkout-ui"
        let compiled = compile(make_viewer_with_query(Signal::Traces.into(), "checkout")).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: "checkout" is a substring of "checkout-ui"
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_query_case_insensitive() {
        // Given: viewer with query "CHECKOUT", entry with service_name "checkout-ui"
        let compiled = compile(make_viewer_with_query(Signal::Traces.into(), "CHECKOUT")).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: case-insensitive match
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_query_no_match_returns_false() {
        // Given: viewer with query "orders", entry with service_name "checkout-ui"
        let compiled = compile(make_viewer_with_query(Signal::Traces.into(), "orders")).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: "orders" is not in "checkout-ui"
        assert!(!compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_query_matches_payload_span_name() {
        // Given: viewer with query "render-checkout" and a trace entry containing that span name
        let compiled = compile(make_viewer_with_query(
            Signal::Traces.into(),
            "render-checkout",
        ))
        .unwrap();
        let payload = serde_json::json!({
            "resourceSpans": [{
                "resource": {
                    "attributes": [{"key": "service.name", "value": {"stringValue": "checkout-ui"}}]
                },
                "scopeSpans": [{
                    "spans": [{"traceId": "abc", "spanId": "001", "name": "render-checkout", "kind": 1}]
                }]
            }]
        })
        .to_string();
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: chrono::Utc::now(),
            service_name: Some("checkout-ui".to_string()),
            payload: Bytes::from(payload),
        };

        // When/Then: "render-checkout" is in the payload preview
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_query_does_not_match_different_span_name() {
        // Given: viewer with query "process-payment", entry with span name "render-checkout"
        let compiled = compile(make_viewer_with_query(
            Signal::Traces.into(),
            "process-payment",
        ))
        .unwrap();
        let payload = serde_json::json!({
            "resourceSpans": [{
                "scopeSpans": [{
                    "spans": [{"name": "render-checkout", "kind": 1}]
                }]
            }]
        })
        .to_string();
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: chrono::Utc::now(),
            service_name: Some("checkout-ui".to_string()),
            payload: Bytes::from(payload),
        };

        // When/Then: "process-payment" is not in service_name or payload preview
        assert!(!compiled.matches_entry(&entry));
    }
}
