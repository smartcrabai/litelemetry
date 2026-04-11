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
    #[error("unknown filter field '{0}': must be 'service_name' or 'payload'")]
    InvalidFilterField(String),
    #[error("unknown filter op '{0}': must be 'eq', 'contains', or 'regex'")]
    InvalidFilterOp(String),
    #[error("unknown filter_mode '{0}': must be 'and' or 'or'")]
    InvalidFilterMode(String),
    #[error("filter value must not be empty")]
    EmptyFilterValue,
    #[error("invalid regex pattern: {0}")]
    InvalidFilterRegex(regex::Error),
    #[error("'filters' must be an array")]
    FiltersNotArray,
}

/// Field to match against in a filter condition.
#[derive(Debug, Clone, PartialEq)]
enum FilterField {
    ServiceName,
    Payload,
}

/// Compiled matcher for a filter condition.
/// Patterns for Regex are compiled once at `compile()` / `update_definition_json()` time.
#[derive(Debug)]
enum FilterMatcher {
    Eq(String),          // case-insensitive exact match
    Contains(String),    // case-insensitive substring match
    Regex(regex::Regex), // regex match (compiled once at startup)
}

/// How multiple filters in a viewer are combined.
#[derive(Debug, Clone, PartialEq, Default)]
enum FilterMode {
    #[default]
    And,
    Or,
}

/// A single compiled filter condition ready for evaluation.
#[derive(Debug)]
struct CompiledFilter {
    field: FilterField,
    matcher: FilterMatcher,
}

/// Compiled viewer. Generated from ViewerDefinition at startup.
/// `compile()` guarantees that signal_mask > 0 and lookback_ms > 0.
#[derive(Debug)]
pub struct CompiledViewer {
    definition: ViewerDefinition,
    /// Normalized (trimmed, lowercased) query extracted from definition_json.
    /// None means match-all (when filters is also None/empty).
    query: Option<String>,
    /// Compiled filter conditions. None / empty means fall back to `query`.
    filters: Option<Vec<CompiledFilter>>,
    /// How multiple filters are combined.
    filter_mode: FilterMode,
}

impl CompiledViewer {
    /// Returns whether this entry matches the viewer's target signals
    pub fn matches_signal(&self, signal: Signal) -> bool {
        self.definition.signal_mask.contains(signal)
    }

    /// Evaluates whether an entry matches this viewer's filter criteria.
    ///
    /// Priority:
    ///   1. If `filters` is non-empty: evaluate using `filter_mode` (And/Or).
    ///   2. Else if `query` is set: case-insensitive substring match.
    ///   3. Otherwise: match-all (returns true).
    pub fn matches_entry(&self, entry: &NormalizedEntry) -> bool {
        if let Some(filters) = &self.filters {
            return match self.filter_mode {
                FilterMode::And => filters.iter().all(|f| eval_filter(f, entry)),
                FilterMode::Or => filters.iter().any(|f| eval_filter(f, entry)),
            };
        }
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

    /// Returns the normalized query string, or None if no query is set.
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
        self.filters = filters_from_definition(&definition_json).unwrap_or_else(|e| {
            tracing::error!("update_definition_json: invalid filters in persisted definition: {e}");
            None
        });
        self.filter_mode = filter_mode_from_definition(&definition_json).unwrap_or_else(|e| {
            tracing::error!(
                "update_definition_json: invalid filter_mode in persisted definition: {e}"
            );
            FilterMode::default()
        });
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

fn extract_field_text(field: &FilterField, entry: &NormalizedEntry) -> Option<String> {
    match field {
        FilterField::ServiceName => entry.service_name.clone(),
        FilterField::Payload => extract_searchable_payload_text(entry.signal, &entry.payload),
    }
}

/// Evaluates a single compiled filter against an entry.
fn eval_filter(filter: &CompiledFilter, entry: &NormalizedEntry) -> bool {
    match &filter.matcher {
        FilterMatcher::Eq(value) => extract_field_text(&filter.field, entry)
            .map(|t| t.to_lowercase() == *value)
            .unwrap_or(false),
        FilterMatcher::Contains(value) => extract_field_text(&filter.field, entry)
            .map(|t| t.to_lowercase().contains(value.as_str()))
            .unwrap_or(false),
        FilterMatcher::Regex(re) => extract_field_text(&filter.field, entry)
            .map(|t| re.is_match(&t))
            .unwrap_or(false),
    }
}

/// Extracts searchable text from the payload for each signal type.
/// Returns None if the payload is not parseable or contains no relevant fields.
pub(crate) fn extract_searchable_payload_text(signal: Signal, payload: &Bytes) -> Option<String> {
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

/// Parses filter conditions from definition_json.
/// Returns `Ok(None)` when the "filters" field is absent or empty.
/// Returns `Err` when the "filters" field is present but not an array, or a filter has an
/// unknown field, unknown op, empty value, or invalid regex.
fn filters_from_definition(
    definition_json: &Value,
) -> Result<Option<Vec<CompiledFilter>>, CompileError> {
    let Some(filters_val) = definition_json.get("filters") else {
        return Ok(None);
    };
    let arr = filters_val
        .as_array()
        .ok_or(CompileError::FiltersNotArray)?;
    if arr.is_empty() {
        return Ok(None);
    }

    let mut compiled = Vec::with_capacity(arr.len());
    for item in arr {
        let field_str = item.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let op_str = item.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let value_str = item.get("value").and_then(|v| v.as_str()).unwrap_or("");

        let field = match field_str {
            "service_name" => FilterField::ServiceName,
            "payload" => FilterField::Payload,
            other => return Err(CompileError::InvalidFilterField(other.to_string())),
        };

        if value_str.is_empty() {
            return Err(CompileError::EmptyFilterValue);
        }

        let matcher = match op_str {
            "eq" => FilterMatcher::Eq(value_str.to_lowercase()),
            "contains" => FilterMatcher::Contains(value_str.to_lowercase()),
            "regex" => {
                let re = regex::RegexBuilder::new(value_str)
                    .case_insensitive(true)
                    .build()
                    .map_err(CompileError::InvalidFilterRegex)?;
                FilterMatcher::Regex(re)
            }
            other => return Err(CompileError::InvalidFilterOp(other.to_string())),
        };

        compiled.push(CompiledFilter { field, matcher });
    }

    Ok(Some(compiled))
}

/// Parses the filter combination mode from definition_json.
/// Returns `FilterMode::And` when the "filter_mode" field is absent.
/// Returns `Err` when the value is present but not "and" or "or".
fn filter_mode_from_definition(definition_json: &Value) -> Result<FilterMode, CompileError> {
    let Some(mode_val) = definition_json.get("filter_mode") else {
        return Ok(FilterMode::And);
    };
    let mode_str = mode_val.as_str().unwrap_or("");
    match mode_str {
        "and" => Ok(FilterMode::And),
        "or" => Ok(FilterMode::Or),
        _ => Err(CompileError::InvalidFilterMode(mode_val.to_string())),
    }
}

/// Validates and converts a ViewerDefinition into a CompiledViewer.
/// Returns an error if signal_mask is empty, lookback_ms is 0 or less,
/// or any filter field/op/regex is invalid.
pub fn compile(definition: ViewerDefinition) -> Result<CompiledViewer, CompileError> {
    if definition.signal_mask.is_empty() {
        return Err(CompileError::ZeroSignalMask);
    }
    if definition.lookback_ms <= 0 {
        return Err(CompileError::NonPositiveLookback(definition.lookback_ms));
    }
    let query = query_from_definition(&definition.definition_json);
    let filters = filters_from_definition(&definition.definition_json)?;
    let filter_mode = filter_mode_from_definition(&definition.definition_json)?;
    Ok(CompiledViewer {
        definition,
        query,
        filters,
        filter_mode,
    })
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

    // --- helpers for filter tests -------------------------------------------

    fn make_viewer_with_filters(
        signal_mask: SignalMask,
        filters: serde_json::Value,
        filter_mode: Option<&str>,
    ) -> ViewerDefinition {
        let mut def = make_viewer(signal_mask);
        let mut json = serde_json::json!({ "filters": filters });
        if let Some(mode) = filter_mode {
            json["filter_mode"] = serde_json::json!(mode);
        }
        def.definition_json = json;
        def
    }

    fn make_trace_payload_for_span(span_name: &str) -> Bytes {
        Bytes::from(
            serde_json::json!({
                "resourceSpans": [{
                    "scopeSpans": [{
                        "spans": [{"name": span_name, "kind": 1}]
                    }]
                }]
            })
            .to_string(),
        )
    }

    // --- filters_from_definition --------------------------------------------

    #[test]
    fn test_filters_from_definition_missing_returns_none() {
        // Given: definition_json with no "filters" field
        // When: filters_from_definition
        // Then: Ok(None)
        let result = filters_from_definition(&json!({}));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_filters_from_definition_empty_array_returns_none() {
        // Given: definition_json with "filters": []
        // When: filters_from_definition
        // Then: Ok(None) -- empty is treated as absent
        let result = filters_from_definition(&json!({ "filters": [] }));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_filters_from_definition_valid_eq_service_name() {
        // Given: a valid eq filter on service_name field
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "service_name", "op": "eq", "value": "checkout" }]
        }));

        // Then: Ok(Some([...])), field=ServiceName, matcher=Eq
        assert!(result.is_ok());
        let filters = result.unwrap().expect("should have filters");
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].field, FilterField::ServiceName);
        assert!(matches!(filters[0].matcher, FilterMatcher::Eq(_)));
    }

    #[test]
    fn test_filters_from_definition_valid_contains_service_name() {
        // Given: a valid contains filter on service_name
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "service_name", "op": "contains", "value": "checkout" }]
        }));

        // Then: matcher=Contains
        assert!(result.is_ok());
        let filters = result.unwrap().expect("should have filters");
        assert!(matches!(filters[0].matcher, FilterMatcher::Contains(_)));
    }

    #[test]
    fn test_filters_from_definition_valid_regex_service_name() {
        // Given: a valid regex filter with a legal pattern
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "service_name", "op": "regex", "value": "^checkout.*" }]
        }));

        // Then: matcher=Regex (compiled successfully)
        assert!(result.is_ok());
        let filters = result.unwrap().expect("should have filters");
        assert!(matches!(filters[0].matcher, FilterMatcher::Regex(_)));
    }

    #[test]
    fn test_filters_from_definition_valid_payload_field() {
        // Given: a filter targeting the payload field
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "payload", "op": "contains", "value": "render" }]
        }));

        // Then: field=Payload
        assert!(result.is_ok());
        let filters = result.unwrap().expect("should have filters");
        assert_eq!(filters[0].field, FilterField::Payload);
    }

    #[test]
    fn test_filters_from_definition_multiple_filters() {
        // Given: two valid filters
        let result = filters_from_definition(&json!({
            "filters": [
                { "field": "service_name", "op": "eq",       "value": "checkout-ui" },
                { "field": "payload",      "op": "contains", "value": "render"       }
            ]
        }));

        // Then: both filters are parsed
        assert!(result.is_ok());
        let filters = result.unwrap().expect("should have two filters");
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn test_filters_from_definition_invalid_field_returns_error() {
        // Given: a filter with an unrecognised field name
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "trace_id", "op": "eq", "value": "abc" }]
        }));

        // Then: InvalidFilterField error
        assert!(
            matches!(result, Err(CompileError::InvalidFilterField(_))),
            "expected InvalidFilterField, got {result:?}"
        );
    }

    #[test]
    fn test_filters_from_definition_invalid_op_returns_error() {
        // Given: a filter with an unsupported op
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "service_name", "op": "like", "value": "foo" }]
        }));

        // Then: InvalidFilterOp error
        assert!(
            matches!(result, Err(CompileError::InvalidFilterOp(_))),
            "expected InvalidFilterOp, got {result:?}"
        );
    }

    #[test]
    fn test_filters_from_definition_empty_value_returns_error() {
        // Given: a filter with an empty string value
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "service_name", "op": "eq", "value": "" }]
        }));

        // Then: EmptyFilterValue error
        assert!(
            matches!(result, Err(CompileError::EmptyFilterValue)),
            "expected EmptyFilterValue, got {result:?}"
        );
    }

    #[test]
    fn test_filters_from_definition_invalid_regex_returns_error() {
        // Given: a regex filter with an invalid pattern
        let result = filters_from_definition(&json!({
            "filters": [{ "field": "service_name", "op": "regex", "value": "[invalid" }]
        }));

        // Then: InvalidFilterRegex error
        assert!(
            matches!(result, Err(CompileError::InvalidFilterRegex(_))),
            "expected InvalidFilterRegex, got {result:?}"
        );
    }

    // --- filter_mode_from_definition ----------------------------------------

    #[test]
    fn test_filter_mode_from_definition_missing_defaults_to_and() {
        // Given: definition_json with no "filter_mode" field
        // Then: default FilterMode::And
        let result = filter_mode_from_definition(&json!({}));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FilterMode::And);
    }

    #[test]
    fn test_filter_mode_from_definition_and() {
        // Given: "filter_mode": "and"
        // Then: FilterMode::And
        let result = filter_mode_from_definition(&json!({ "filter_mode": "and" }));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FilterMode::And);
    }

    #[test]
    fn test_filter_mode_from_definition_or() {
        // Given: "filter_mode": "or"
        // Then: FilterMode::Or
        let result = filter_mode_from_definition(&json!({ "filter_mode": "or" }));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FilterMode::Or);
    }

    #[test]
    fn test_filter_mode_from_definition_invalid_returns_error() {
        // Given: "filter_mode" set to an unknown value
        let result = filter_mode_from_definition(&json!({ "filter_mode": "neither" }));

        // Then: InvalidFilterMode error
        assert!(
            matches!(result, Err(CompileError::InvalidFilterMode(_))),
            "expected InvalidFilterMode, got {result:?}"
        );
    }

    // --- compile with invalid filters ----------------------------------------

    #[test]
    fn test_compile_invalid_filter_field_returns_error() {
        // Given: definition with an unknown filter field
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({
            "filters": [{ "field": "trace_id", "op": "eq", "value": "abc" }]
        });

        // When: compile
        let result = compile(def);

        // Then: propagates InvalidFilterField
        assert!(
            matches!(result, Err(CompileError::InvalidFilterField(_))),
            "expected InvalidFilterField, got {result:?}"
        );
    }

    #[test]
    fn test_compile_invalid_filter_op_returns_error() {
        // Given: definition with an unsupported filter op
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({
            "filters": [{ "field": "service_name", "op": "like", "value": "abc" }]
        });

        // When: compile
        let result = compile(def);

        // Then: propagates InvalidFilterOp
        assert!(
            matches!(result, Err(CompileError::InvalidFilterOp(_))),
            "expected InvalidFilterOp, got {result:?}"
        );
    }

    #[test]
    fn test_compile_invalid_regex_filter_returns_error() {
        // Given: definition with a malformed regex pattern
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({
            "filters": [{ "field": "service_name", "op": "regex", "value": "[invalid" }]
        });

        // When: compile
        let result = compile(def);

        // Then: propagates InvalidFilterRegex
        assert!(
            matches!(result, Err(CompileError::InvalidFilterRegex(_))),
            "expected InvalidFilterRegex, got {result:?}"
        );
    }

    #[test]
    fn test_compile_invalid_filter_mode_returns_error() {
        // Given: definition with an invalid filter_mode value
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({
            "filters": [{ "field": "service_name", "op": "eq", "value": "foo" }],
            "filter_mode": "both"
        });

        // When: compile
        let result = compile(def);

        // Then: propagates InvalidFilterMode
        assert!(
            matches!(result, Err(CompileError::InvalidFilterMode(_))),
            "expected InvalidFilterMode, got {result:?}"
        );
    }

    #[test]
    fn test_compile_empty_filter_value_returns_error() {
        // Given: filter with an empty value string
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({
            "filters": [{ "field": "service_name", "op": "contains", "value": "" }]
        });

        // When: compile
        let result = compile(def);

        // Then: propagates EmptyFilterValue
        assert!(
            matches!(result, Err(CompileError::EmptyFilterValue)),
            "expected EmptyFilterValue, got {result:?}"
        );
    }

    // --- matches_entry with filters -----------------------------------------

    #[test]
    fn test_matches_entry_filter_eq_service_name_matches() {
        // Given: viewer with eq filter on service_name = "checkout-ui"
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "service_name", "op": "eq", "value": "checkout-ui" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: entry whose service_name exactly matches passes
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filter_eq_service_name_no_match() {
        // Given: eq filter requires "checkout-ui", but entry comes from "orders-api"
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "service_name", "op": "eq", "value": "checkout-ui" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "orders-api");

        // When/Then: different service_name must not match
        assert!(!compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filter_eq_is_case_insensitive() {
        // Given: eq filter value in UPPERCASE
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "service_name", "op": "eq", "value": "CHECKOUT-UI" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: eq comparison is case-insensitive
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filter_contains_service_name_matches() {
        // Given: contains filter, entry service_name includes the substring
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "service_name", "op": "contains", "value": "checkout" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: "checkout" is contained in "checkout-ui"
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filter_regex_service_name_matches() {
        // Given: regex filter with pattern anchored at start
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "service_name", "op": "regex", "value": "^checkout" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: "checkout-ui" starts with "checkout" -> regex matches
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filter_regex_service_name_no_match() {
        // Given: regex anchored to "^orders", entry service is "checkout-ui"
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "service_name", "op": "regex", "value": "^orders" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: "checkout-ui" does not start with "orders"
        assert!(!compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filters_and_mode_all_must_match() {
        // Given: two contains filters in AND mode -- both must be satisfied
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([
                { "field": "service_name", "op": "contains", "value": "checkout" },
                { "field": "service_name", "op": "contains", "value": "ui"       }
            ]),
            Some("and"),
        );
        let compiled = compile(def).unwrap();

        // When: service_name = "checkout-ui" satisfies both conditions
        let matching = make_entry_with_service(Signal::Traces, "checkout-ui");
        assert!(compiled.matches_entry(&matching));
    }

    #[test]
    fn test_matches_entry_filters_and_mode_partial_fails() {
        // Given: two filters, entry satisfies only one
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([
                { "field": "service_name", "op": "contains", "value": "checkout" },
                { "field": "service_name", "op": "contains", "value": "payment"  }
            ]),
            Some("and"),
        );
        let compiled = compile(def).unwrap();

        // When: "checkout-ui" contains "checkout" but not "payment"
        let entry = make_entry_with_service(Signal::Traces, "checkout-ui");
        assert!(!compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filters_or_mode_any_matches() {
        // Given: two eq filters in OR mode -- any one is sufficient
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([
                { "field": "service_name", "op": "eq", "value": "checkout-ui" },
                { "field": "service_name", "op": "eq", "value": "orders-api"  }
            ]),
            Some("or"),
        );
        let compiled = compile(def).unwrap();

        // When: entry matches the second filter only
        let entry = make_entry_with_service(Signal::Traces, "orders-api");
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filters_or_mode_none_matches_returns_false() {
        // Given: two eq filters in OR mode, entry matches neither
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([
                { "field": "service_name", "op": "eq", "value": "checkout-ui" },
                { "field": "service_name", "op": "eq", "value": "orders-api"  }
            ]),
            Some("or"),
        );
        let compiled = compile(def).unwrap();

        // When: entry from a completely different service
        let entry = make_entry_with_service(Signal::Traces, "billing-svc");
        assert!(!compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filters_take_priority_over_query() {
        // Given: viewer with BOTH a query ("orders") AND a filter requiring "checkout-ui".
        // The query alone would match "orders-api", but filters take priority.
        let mut def = make_viewer(Signal::Traces.into());
        def.definition_json = json!({
            "query":  "orders",
            "filters": [{ "field": "service_name", "op": "eq", "value": "checkout-ui" }]
        });
        let compiled = compile(def).unwrap();

        let orders_entry = make_entry_with_service(Signal::Traces, "orders-api");
        let checkout_entry = make_entry_with_service(Signal::Traces, "checkout-ui");

        // When/Then: the filter wins -- "orders-api" should not pass
        assert!(
            !compiled.matches_entry(&orders_entry),
            "filters must take priority: orders-api should not pass the checkout-ui filter"
        );
        // And the entry that satisfies the filter does pass
        assert!(compiled.matches_entry(&checkout_entry));
    }

    #[test]
    fn test_matches_entry_no_filters_falls_back_to_query() {
        // Given: viewer with only a query (no filters) -- existing behavior preserved
        let compiled = compile(make_viewer_with_query(Signal::Traces.into(), "checkout")).unwrap();

        let checkout_entry = make_entry_with_service(Signal::Traces, "checkout-ui");
        let orders_entry = make_entry_with_service(Signal::Traces, "orders-api");

        // When/Then: query-based substring matching applies
        assert!(compiled.matches_entry(&checkout_entry));
        assert!(!compiled.matches_entry(&orders_entry));
    }

    #[test]
    fn test_matches_entry_filter_payload_field_matches() {
        // Given: payload filter looking for a span name inside the trace payload
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "payload", "op": "contains", "value": "render-checkout" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: chrono::Utc::now(),
            service_name: Some("my-service".to_string()),
            payload: make_trace_payload_for_span("render-checkout"),
        };

        // When/Then: span name in payload matches the filter
        assert!(compiled.matches_entry(&entry));
    }

    #[test]
    fn test_matches_entry_filter_payload_field_no_match() {
        // Given: payload filter requires "process-payment", payload has "render-checkout"
        let def = make_viewer_with_filters(
            Signal::Traces.into(),
            json!([{ "field": "payload", "op": "contains", "value": "process-payment" }]),
            None,
        );
        let compiled = compile(def).unwrap();
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: chrono::Utc::now(),
            service_name: Some("my-service".to_string()),
            payload: make_trace_payload_for_span("render-checkout"),
        };

        // When/Then: span name does not contain "process-payment"
        assert!(!compiled.matches_entry(&entry));
    }
}
