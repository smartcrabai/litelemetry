use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Severity classifications for an alert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Warning,
    Critical,
}

impl AlertSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertSeverity::Warning => "warning",
            AlertSeverity::Critical => "critical",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "warning" => Some(AlertSeverity::Warning),
            "critical" => Some(AlertSeverity::Critical),
            _ => None,
        }
    }
}

/// Comparison operators supported by threshold conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThresholdOp {
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Lte,
    #[serde(rename = "==")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
}

impl ThresholdOp {
    pub fn evaluate(self, lhs: f64, rhs: f64) -> bool {
        match self {
            ThresholdOp::Gt => lhs > rhs,
            ThresholdOp::Gte => lhs >= rhs,
            ThresholdOp::Lt => lhs < rhs,
            ThresholdOp::Lte => lhs <= rhs,
            ThresholdOp::Eq => (lhs - rhs).abs() < f64::EPSILON,
            ThresholdOp::Ne => (lhs - rhs).abs() >= f64::EPSILON,
        }
    }
}

/// Metric extracted from a viewer snapshot for evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertMetric {
    /// Number of entries currently held by the viewer's state.
    Count,
}

impl AlertMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            AlertMetric::Count => "count",
        }
    }
}

/// Alert evaluation condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AlertCondition {
    Threshold {
        op: ThresholdOp,
        value: f64,
        metric: AlertMetric,
    },
}

/// Domain type corresponding to a row in the alert_definitions table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertDefinition {
    pub id: Uuid,
    pub name: String,
    pub viewer_id: Uuid,
    pub condition: AlertCondition,
    pub severity: AlertSeverity,
    pub evaluation_interval_ms: i32,
    pub enabled: bool,
    pub revision: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_op_compares_correctly() {
        assert!(ThresholdOp::Gt.evaluate(2.0, 1.0));
        assert!(!ThresholdOp::Gt.evaluate(1.0, 1.0));
        assert!(ThresholdOp::Gte.evaluate(1.0, 1.0));
        assert!(ThresholdOp::Lt.evaluate(0.5, 1.0));
        assert!(ThresholdOp::Lte.evaluate(1.0, 1.0));
        assert!(ThresholdOp::Eq.evaluate(2.0, 2.0));
        assert!(ThresholdOp::Ne.evaluate(2.0, 3.0));
    }

    #[test]
    fn alert_condition_round_trips_via_serde() {
        let json = serde_json::json!({
            "type": "threshold",
            "op": ">",
            "value": 100,
            "metric": "count"
        });
        let cond: AlertCondition = serde_json::from_value(json).unwrap();
        match cond.clone() {
            AlertCondition::Threshold { op, value, metric } => {
                assert_eq!(op, ThresholdOp::Gt);
                assert!((value - 100.0).abs() < f64::EPSILON);
                assert_eq!(metric, AlertMetric::Count);
            }
        }
        // round trip: deserialize -> serialize -> deserialize and compare struct.
        let re = serde_json::to_value(&cond).unwrap();
        let cond2: AlertCondition = serde_json::from_value(re).unwrap();
        assert_eq!(cond, cond2);
    }

    #[test]
    fn severity_parses_and_serializes() {
        assert_eq!(
            AlertSeverity::parse("warning"),
            Some(AlertSeverity::Warning)
        );
        assert_eq!(
            AlertSeverity::parse("critical"),
            Some(AlertSeverity::Critical)
        );
        assert_eq!(AlertSeverity::parse("bogus"), None);
        assert_eq!(AlertSeverity::Warning.as_str(), "warning");
    }
}
