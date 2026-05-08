use crate::domain::slo::{SloDefinition, SloFilterList};
use crate::domain::telemetry::{NormalizedEntry, Signal, SignalMask};
use crate::domain::viewer::ViewerDefinition;
use crate::viewer_runtime::compiler::{CompileError, CompiledViewer, compile};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

/// Result of evaluating an SLO definition over a window of entries.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ErrorBudget {
    /// Target SLO percentage in [0, 100].
    pub target_pct: f64,
    /// Observed success ratio (`success_count / total_count`) in [0, 100].
    /// 100.0 when `total_count == 0`.
    pub current_pct: f64,
    /// Percentage of the error budget already burned, in [0, 100].
    pub consumed_pct: f64,
    /// Percentage of the error budget remaining, in [0, 100].
    pub remaining_pct: f64,
    /// Window length in milliseconds (echoed from the SLO definition).
    pub window_ms: i64,
    /// Number of entries that matched the total filter inside the window.
    pub total_count: u64,
    /// Number of entries that matched the success filter inside the window.
    pub success_count: u64,
    /// Server-side timestamp (UTC) at which the budget was evaluated.
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum CalculatorError {
    #[error("invalid success filter: {0}")]
    InvalidSuccessFilter(CompileError),
    #[error("invalid total filter: {0}")]
    InvalidTotalFilter(CompileError),
    #[error("window_ms must be positive, got {0}")]
    NonPositiveWindow(i64),
    #[error("target_pct must be in [0, 100], got {0}")]
    InvalidTarget(f64),
}

/// Compiles an SLO into a pair of matchers ready to evaluate entries.
pub struct CompiledSlo {
    success: CompiledViewer,
    total: CompiledViewer,
    target_pct: f64,
    window_ms: i64,
}

impl CompiledSlo {
    pub fn compile(slo: &SloDefinition) -> Result<Self, CalculatorError> {
        if slo.window_ms <= 0 {
            return Err(CalculatorError::NonPositiveWindow(slo.window_ms));
        }
        if !(0.0..=100.0).contains(&slo.target_pct) || slo.target_pct.is_nan() {
            return Err(CalculatorError::InvalidTarget(slo.target_pct));
        }

        let success = compile_filter_only(&slo.success_filter, slo.window_ms)
            .map_err(CalculatorError::InvalidSuccessFilter)?;
        let total = compile_filter_only(&slo.total_filter, slo.window_ms)
            .map_err(CalculatorError::InvalidTotalFilter)?;

        Ok(Self {
            success,
            total,
            target_pct: slo.target_pct,
            window_ms: slo.window_ms,
        })
    }

    /// Counts entries falling inside the rolling window that match the
    /// total / success filters and returns the resulting error budget.
    pub fn evaluate(&self, entries: &[NormalizedEntry], now: DateTime<Utc>) -> ErrorBudget {
        let cutoff = now - chrono::Duration::milliseconds(self.window_ms);

        let mut total_count: u64 = 0;
        let mut success_count: u64 = 0;
        for entry in entries {
            if entry.observed_at < cutoff {
                continue;
            }
            if !self.total.matches_entry(entry) {
                continue;
            }
            total_count += 1;
            if self.success.matches_entry(entry) {
                success_count += 1;
            }
        }

        budget_from_counts(
            self.target_pct,
            self.window_ms,
            total_count,
            success_count,
            now,
        )
    }
}

fn budget_from_counts(
    target_pct: f64,
    window_ms: i64,
    total_count: u64,
    success_count: u64,
    evaluated_at: DateTime<Utc>,
) -> ErrorBudget {
    let current_pct = if total_count == 0 {
        100.0
    } else {
        (success_count as f64 / total_count as f64) * 100.0
    };

    // Error budget = (100 - target_pct). Burn ratio = errors / budget.
    let allowed_error_pct = (100.0 - target_pct).max(0.0);
    let observed_error_pct = (100.0 - current_pct).max(0.0);
    let consumed_pct = match (allowed_error_pct, observed_error_pct) {
        (0.0, 0.0) => 0.0,
        (0.0, _) => 100.0,
        (allowed, observed) => ((observed / allowed) * 100.0).clamp(0.0, 100.0),
    };
    let remaining_pct = (100.0 - consumed_pct).clamp(0.0, 100.0);

    ErrorBudget {
        target_pct,
        current_pct,
        consumed_pct,
        remaining_pct,
        window_ms,
        total_count,
        success_count,
        evaluated_at,
    }
}

fn compile_filter_only(
    list: &SloFilterList,
    window_ms: i64,
) -> Result<CompiledViewer, CompileError> {
    let definition_json = if list.filters.is_empty() {
        json!({})
    } else {
        // SloFilterClause already serializes to {field, op, value}, the shape
        // expected by the viewer compiler.
        json!({
            "filters": list.filters,
            "filter_mode": "and",
        })
    };

    let definition = ViewerDefinition {
        id: Uuid::nil(),
        slug: String::new(),
        name: String::new(),
        refresh_interval_ms: 1_000,
        lookback_ms: window_ms,
        // signal_mask cannot be empty; the value does not affect filter matching
        // because we only call matches_entry (not matches_signal) on this viewer.
        signal_mask: SignalMask::from(Signal::Traces) | Signal::Metrics | Signal::Logs,
        definition_json,
        layout_json: json!({}),
        revision: 1,
        enabled: true,
    };
    compile(definition)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::slo::SloFilterClause;
    use bytes::Bytes;

    fn entry(service: &str, age_secs: i64) -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now() - chrono::Duration::seconds(age_secs),
            service_name: Some(service.to_string()),
            payload: Bytes::from_static(b"{}"),
        }
    }

    fn make_slo(success: SloFilterList, total: SloFilterList) -> SloDefinition {
        SloDefinition {
            id: Uuid::new_v4(),
            name: "test".into(),
            viewer_id: None,
            target_pct: 99.0,
            window_ms: 60_000,
            success_filter: success,
            total_filter: total,
            enabled: true,
        }
    }

    fn clause(field: &str, op: &str, value: &str) -> SloFilterClause {
        SloFilterClause {
            field: field.into(),
            op: op.into(),
            value: value.into(),
        }
    }

    #[test]
    fn empty_total_returns_100_pct() {
        let slo = make_slo(SloFilterList::default(), SloFilterList::default());
        let compiled = CompiledSlo::compile(&slo).unwrap();
        let budget = compiled.evaluate(&[], Utc::now());
        assert_eq!(budget.total_count, 0);
        assert_eq!(budget.success_count, 0);
        assert_eq!(budget.current_pct, 100.0);
        assert_eq!(budget.remaining_pct, 100.0);
        assert_eq!(budget.consumed_pct, 0.0);
    }

    #[test]
    fn current_pct_is_success_over_total() {
        // 8 successes / 10 totals = 80%
        let success_filter = SloFilterList {
            filters: vec![clause("service_name", "eq", "ok-svc")],
        };
        let slo = make_slo(success_filter, SloFilterList::default());
        let compiled = CompiledSlo::compile(&slo).unwrap();

        let mut entries = Vec::new();
        for _ in 0..8 {
            entries.push(entry("ok-svc", 5));
        }
        for _ in 0..2 {
            entries.push(entry("err-svc", 5));
        }

        let budget = compiled.evaluate(&entries, Utc::now());
        assert_eq!(budget.total_count, 10);
        assert_eq!(budget.success_count, 8);
        assert!((budget.current_pct - 80.0).abs() < 1e-9);
    }

    #[test]
    fn entries_outside_window_are_excluded() {
        let slo = make_slo(SloFilterList::default(), SloFilterList::default());
        let compiled = CompiledSlo::compile(&slo).unwrap();

        let entries = vec![
            entry("svc", 30),        // inside 60s window
            entry("svc", 30),        // inside
            entry("svc", 120),       // outside
            entry("svc", 1_000_000), // outside
        ];

        let budget = compiled.evaluate(&entries, Utc::now());
        assert_eq!(budget.total_count, 2);
        assert_eq!(budget.success_count, 2);
    }

    #[test]
    fn consumed_pct_uses_error_budget_ratio() {
        // target 99% -> allowed 1% errors; observed 80% -> 20% errors -> consumed = 100% (clamped)
        let success_filter = SloFilterList {
            filters: vec![clause("service_name", "eq", "ok-svc")],
        };
        let slo = make_slo(success_filter, SloFilterList::default());
        let compiled = CompiledSlo::compile(&slo).unwrap();

        let mut entries = Vec::new();
        for _ in 0..8 {
            entries.push(entry("ok-svc", 5));
        }
        for _ in 0..2 {
            entries.push(entry("err-svc", 5));
        }

        let budget = compiled.evaluate(&entries, Utc::now());
        assert_eq!(budget.consumed_pct, 100.0);
        assert_eq!(budget.remaining_pct, 0.0);
    }

    #[test]
    fn consumed_pct_partial_burn() {
        // target 90% -> allowed 10% errors; observed 95% -> 5% errors -> consumed = 50%
        let success_filter = SloFilterList {
            filters: vec![clause("service_name", "eq", "ok-svc")],
        };
        let mut slo = make_slo(success_filter, SloFilterList::default());
        slo.target_pct = 90.0;
        let compiled = CompiledSlo::compile(&slo).unwrap();

        let mut entries = Vec::new();
        for _ in 0..95 {
            entries.push(entry("ok-svc", 5));
        }
        for _ in 0..5 {
            entries.push(entry("err-svc", 5));
        }

        let budget = compiled.evaluate(&entries, Utc::now());
        assert!((budget.consumed_pct - 50.0).abs() < 1e-9);
        assert!((budget.remaining_pct - 50.0).abs() < 1e-9);
    }

    #[test]
    fn perfect_target_with_zero_errors_keeps_full_budget() {
        // target 100% -> allowed 0% errors; if observed 100%, consumed = 0%
        let slo_def = SloDefinition {
            id: Uuid::new_v4(),
            name: "perfect".into(),
            viewer_id: None,
            target_pct: 100.0,
            window_ms: 60_000,
            success_filter: SloFilterList::default(),
            total_filter: SloFilterList::default(),
            enabled: true,
        };
        let compiled = CompiledSlo::compile(&slo_def).unwrap();
        let entries = vec![entry("ok", 5), entry("ok", 5)];
        let budget = compiled.evaluate(&entries, Utc::now());
        assert_eq!(budget.consumed_pct, 0.0);
        assert_eq!(budget.remaining_pct, 100.0);
    }

    #[test]
    fn invalid_window_returns_error() {
        let mut slo = make_slo(SloFilterList::default(), SloFilterList::default());
        slo.window_ms = 0;
        assert!(matches!(
            CompiledSlo::compile(&slo),
            Err(CalculatorError::NonPositiveWindow(0))
        ));
    }

    #[test]
    fn invalid_target_returns_error() {
        let mut slo = make_slo(SloFilterList::default(), SloFilterList::default());
        slo.target_pct = 150.0;
        assert!(matches!(
            CompiledSlo::compile(&slo),
            Err(CalculatorError::InvalidTarget(_))
        ));
    }

    #[test]
    fn invalid_filter_propagates() {
        let bad = SloFilterList {
            filters: vec![clause("trace_id", "eq", "abc")],
        };
        let slo = make_slo(bad, SloFilterList::default());
        assert!(matches!(
            CompiledSlo::compile(&slo),
            Err(CalculatorError::InvalidSuccessFilter(_))
        ));
    }
}
