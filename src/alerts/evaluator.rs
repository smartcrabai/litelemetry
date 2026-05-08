use crate::domain::alert::{AlertCondition, AlertDefinition, AlertMetric};
use crate::server::SharedViewerRuntime;
use crate::storage::StorageError;
use crate::storage::alert_store::AlertStore;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// Default polling interval used by the runtime tick when no per-alert
/// schedule has yet elapsed. Individual alerts honour their own
/// `evaluation_interval_ms` field.
pub const DEFAULT_TICK_MS: u64 = 1_000;

#[derive(Debug, Error)]
pub enum AlertRuntimeError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

/// Result of a single alert evaluation cycle.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum EvaluationOutcome {
    /// The condition matched its threshold (alert is firing).
    Breach,
    /// The condition did not match (alert is OK).
    Ok,
    /// The viewer referenced by the alert does not exist.
    MissingViewer,
}

/// Snapshot of the most recent evaluation per alert id. Used by tests and
/// observability tooling.
#[derive(Debug, Clone)]
pub struct AlertEvaluation {
    pub alert_id: Uuid,
    pub viewer_id: Uuid,
    pub measured_value: f64,
    pub outcome: EvaluationOutcome,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Default)]
struct AlertRuntimeState {
    /// Last time each alert was evaluated.
    last_evaluated_at: HashMap<Uuid, DateTime<Utc>>,
    /// Most recent evaluation outcome per alert.
    last_outcome: HashMap<Uuid, AlertEvaluation>,
}

/// Periodically evaluates configured alerts against viewer runtime state.
#[derive(Clone)]
pub struct AlertRuntime {
    alert_store: AlertStore,
    viewer_runtime: SharedViewerRuntime,
    state: Arc<Mutex<AlertRuntimeState>>,
}

impl AlertRuntime {
    pub fn new(alert_store: AlertStore, viewer_runtime: SharedViewerRuntime) -> Self {
        Self {
            alert_store,
            viewer_runtime,
            state: Arc::new(Mutex::new(AlertRuntimeState::default())),
        }
    }

    /// Spawn the background evaluation loop. Returns a handle so callers
    /// (mostly tests) can abort it.
    pub fn spawn(self, tick_ms: u64) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(tick_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if let Err(e) = self.tick().await {
                    tracing::error!("alert runtime tick failed: {e}");
                }
            }
        })
    }

    /// Run one evaluation pass: fetch alerts, evaluate any that have hit
    /// their `evaluation_interval_ms`, and record outcomes.
    pub async fn tick(&self) -> Result<Vec<AlertEvaluation>, AlertRuntimeError> {
        let alerts = self.alert_store.load_all().await?;
        let now = Utc::now();
        let mut evaluations = Vec::new();

        for alert in alerts {
            if !alert.enabled {
                continue;
            }
            if !self.is_due(&alert, now).await {
                continue;
            }

            let evaluation = self.evaluate_one(&alert, now).await;
            self.record_outcome(&evaluation).await;
            log_outcome(&alert, &evaluation);
            evaluations.push(evaluation);
        }

        Ok(evaluations)
    }

    async fn is_due(&self, alert: &AlertDefinition, now: DateTime<Utc>) -> bool {
        let interval_ms = alert.evaluation_interval_ms.max(0) as i64;
        let guard = self.state.lock().await;
        match guard.last_evaluated_at.get(&alert.id) {
            None => true,
            Some(prev) => (now - *prev).num_milliseconds() >= interval_ms,
        }
    }

    async fn evaluate_one(&self, alert: &AlertDefinition, now: DateTime<Utc>) -> AlertEvaluation {
        let measurement = self.measure(alert).await;

        let outcome = match measurement {
            Some(value) => {
                if condition_matches(&alert.condition, value) {
                    EvaluationOutcome::Breach
                } else {
                    EvaluationOutcome::Ok
                }
            }
            None => EvaluationOutcome::MissingViewer,
        };

        AlertEvaluation {
            alert_id: alert.id,
            viewer_id: alert.viewer_id,
            measured_value: measurement.unwrap_or(0.0),
            outcome,
            evaluated_at: now,
        }
    }

    async fn measure(&self, alert: &AlertDefinition) -> Option<f64> {
        let metric = match &alert.condition {
            AlertCondition::Threshold { metric, .. } => *metric,
        };
        let runtime = self.viewer_runtime.lock().await;
        let (_, state) = runtime
            .viewers()
            .iter()
            .find(|(v, _)| v.definition().id == alert.viewer_id)?;
        Some(measure_metric(metric, state.entries.len()))
    }

    async fn record_outcome(&self, evaluation: &AlertEvaluation) {
        let mut guard = self.state.lock().await;
        guard
            .last_evaluated_at
            .insert(evaluation.alert_id, evaluation.evaluated_at);
        guard
            .last_outcome
            .insert(evaluation.alert_id, evaluation.clone());
    }

    /// Returns the most recent evaluation outcome for each alert.
    pub async fn snapshot(&self) -> HashMap<Uuid, AlertEvaluation> {
        let guard = self.state.lock().await;
        guard.last_outcome.clone()
    }
}

fn measure_metric(metric: AlertMetric, entry_count: usize) -> f64 {
    match metric {
        AlertMetric::Count => entry_count as f64,
    }
}

fn condition_matches(condition: &AlertCondition, value: f64) -> bool {
    match condition {
        AlertCondition::Threshold { op, value: rhs, .. } => op.evaluate(value, *rhs),
    }
}

fn log_outcome(alert: &AlertDefinition, evaluation: &AlertEvaluation) {
    match evaluation.outcome {
        EvaluationOutcome::Breach => {
            tracing::warn!(
                alert_id = %alert.id,
                alert_name = %alert.name,
                viewer_id = %alert.viewer_id,
                severity = alert.severity.as_str(),
                measured_value = evaluation.measured_value,
                "alert breach detected"
            );
        }
        EvaluationOutcome::Ok => {
            tracing::debug!(
                alert_id = %alert.id,
                alert_name = %alert.name,
                measured_value = evaluation.measured_value,
                "alert ok"
            );
        }
        EvaluationOutcome::MissingViewer => {
            tracing::warn!(
                alert_id = %alert.id,
                alert_name = %alert.name,
                viewer_id = %alert.viewer_id,
                "alert references missing viewer"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::alert::{AlertSeverity, ThresholdOp};

    fn sample_alert(viewer_id: Uuid, op: ThresholdOp, value: f64) -> AlertDefinition {
        AlertDefinition {
            id: Uuid::new_v4(),
            name: "test alert".into(),
            viewer_id,
            condition: AlertCondition::Threshold {
                op,
                value,
                metric: AlertMetric::Count,
            },
            severity: AlertSeverity::Warning,
            evaluation_interval_ms: 100,
            enabled: true,
            revision: 0,
        }
    }

    #[test]
    fn condition_matches_returns_true_when_threshold_breached() {
        let alert = sample_alert(Uuid::new_v4(), ThresholdOp::Gt, 10.0);
        assert!(condition_matches(&alert.condition, 11.0));
        assert!(!condition_matches(&alert.condition, 10.0));
    }

    #[test]
    fn measure_metric_count_uses_entry_count() {
        assert!((measure_metric(AlertMetric::Count, 5) - 5.0).abs() < f64::EPSILON);
    }
}
