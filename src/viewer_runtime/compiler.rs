use crate::domain::telemetry::Signal;
use crate::domain::viewer::ViewerDefinition;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("signal_mask has no valid signals: viewer must watch at least one signal")]
    ZeroSignalMask,
    #[error("lookback_ms must be positive, got {0}")]
    NonPositiveLookback(i64),
}

/// コンパイル済み viewer。起動時に ViewerDefinition から生成する。
/// `compile()` により signal_mask > 0 かつ lookback_ms > 0 が保証される。
#[derive(Debug)]
pub struct CompiledViewer {
    definition: ViewerDefinition,
}

impl CompiledViewer {
    /// このエントリが viewer の対象シグナルに合致するかを返す
    pub fn matches_signal(&self, signal: Signal) -> bool {
        self.definition.signal_mask.contains(signal)
    }

    pub fn definition(&self) -> &ViewerDefinition {
        &self.definition
    }

    pub fn lookback_ms(&self) -> i64 {
        self.definition.lookback_ms
    }
}

/// ViewerDefinition を検証・変換して CompiledViewer を返す。
/// signal_mask が空の場合、または lookback_ms が 0 以下の場合はエラー。
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

    // ─── compile ────────────────────────────────────────────────────────────

    #[test]
    fn test_compile_succeeds_for_traces_only() {
        // Given: traces だけを対象にした viewer 定義
        let def = make_viewer(Signal::Traces.into());

        // When: compile
        let result = compile(def);

        // Then: 成功する
        assert!(result.is_ok());
    }

    #[test]
    fn test_compile_fails_for_zero_signal_mask() {
        // Given: signal_mask = 0 (シグナル未指定)
        let def = make_viewer(SignalMask::NONE);

        // When: compile
        let result = compile(def);

        // Then: ZeroSignalMask エラー
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

        // Then: NonPositiveLookback エラー
        assert!(
            matches!(result, Err(CompileError::NonPositiveLookback(0))),
            "expected NonPositiveLookback, got {result:?}"
        );
    }

    #[test]
    fn test_compile_preserves_definition_fields() {
        // Given: 特定 id / slug の viewer 定義
        let def = make_viewer(Signal::Traces.into());
        let id = def.id;
        let slug = def.slug.clone();

        // When: compile
        let compiled = compile(def).unwrap();

        // Then: 定義フィールドが保持される
        assert_eq!(compiled.definition.id, id);
        assert_eq!(compiled.definition.slug, slug);
    }

    // ─── matches_signal ──────────────────────────────────────────────────────

    #[test]
    fn test_matches_signal_traces_only() {
        // Given: traces のみの viewer
        let compiled = compile(make_viewer(Signal::Traces.into())).unwrap();

        // When/Then: traces だけ合致、他は不合致
        assert!(compiled.matches_signal(Signal::Traces));
        assert!(!compiled.matches_signal(Signal::Metrics));
        assert!(!compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_metrics_only() {
        // Given: metrics のみの viewer
        let compiled = compile(make_viewer(Signal::Metrics.into())).unwrap();

        // When/Then
        assert!(!compiled.matches_signal(Signal::Traces));
        assert!(compiled.matches_signal(Signal::Metrics));
        assert!(!compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_logs_only() {
        // Given: logs のみの viewer
        let compiled = compile(make_viewer(Signal::Logs.into())).unwrap();

        // When/Then
        assert!(!compiled.matches_signal(Signal::Traces));
        assert!(!compiled.matches_signal(Signal::Metrics));
        assert!(compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_all_signals() {
        // Given: traces | metrics | logs の viewer
        let compiled =
            compile(make_viewer(Signal::Traces | Signal::Metrics | Signal::Logs)).unwrap();

        // When/Then: 全シグナルに合致
        assert!(compiled.matches_signal(Signal::Traces));
        assert!(compiled.matches_signal(Signal::Metrics));
        assert!(compiled.matches_signal(Signal::Logs));
    }

    #[test]
    fn test_matches_signal_traces_and_metrics() {
        // Given: traces | metrics の viewer
        let compiled = compile(make_viewer(Signal::Traces | Signal::Metrics)).unwrap();

        // When/Then: logs には合致しない
        assert!(compiled.matches_signal(Signal::Traces));
        assert!(compiled.matches_signal(Signal::Metrics));
        assert!(!compiled.matches_signal(Signal::Logs));
    }
}
