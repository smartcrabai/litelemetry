use crate::domain::telemetry::Signal;
use crate::domain::viewer::ViewerDefinition;
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
}

impl CompiledViewer {
    /// Returns whether this entry matches the viewer's target signals
    pub fn matches_signal(&self, signal: Signal) -> bool {
        self.definition.signal_mask.contains(signal)
    }

    pub fn definition(&self) -> &ViewerDefinition {
        &self.definition
    }

    pub fn lookback_ms(&self) -> i64 {
        self.definition.lookback_ms
    }

    pub fn update_definition_json(&mut self, definition_json: Value, layout_json: Value) {
        self.definition.definition_json = definition_json;
        self.definition.layout_json = layout_json;
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
    Ok(CompiledViewer { definition })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::SignalMask;
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
}
