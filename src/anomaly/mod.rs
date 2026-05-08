//! Anomaly detectors operating on a viewer's recent normalized entries.
//!
//! Two detector kinds are provided:
//! - [`nodata::NoDataDetector`]: trips when no entry has been observed within
//!   a sliding window.
//! - [`zscore::ZScoreDetector`]: trips when the latest bucket's count
//!   deviates from the mean by more than `threshold` standard deviations.
//!
//! Detectors are pure functions over `[NormalizedEntry]` and have no
//! dependency on the alert evaluator pipeline; they can be invoked directly
//! via `POST /api/anomaly/evaluate`.

pub mod nodata;
pub mod zscore;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::telemetry::NormalizedEntry;

/// Outcome of evaluating a detector against a slice of entries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetectorResult {
    /// True iff the condition is breached.
    pub breached: bool,
    /// Observed value (e.g. seconds since last entry, or latest bucket count).
    pub value: f64,
    /// Expected value (window threshold seconds for no_data, mean+threshold*std for zscore).
    pub expected: f64,
    /// Timestamp at which evaluation was performed.
    pub observed_at: DateTime<Utc>,
}

/// Trait implemented by all detector kinds.
pub trait Detector {
    /// Evaluate `entries` (ascending observed_at) at `now`.
    fn evaluate(&self, entries: &[NormalizedEntry], now: DateTime<Utc>) -> DetectorResult;
}

/// Tagged enum (`{"type": "no_data" | "zscore", ...}`) used by the API.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DetectorSpec {
    NoData(nodata::NoDataDetector),
    Zscore(zscore::ZScoreDetector),
}

impl DetectorSpec {
    pub fn evaluate(&self, entries: &[NormalizedEntry], now: DateTime<Utc>) -> DetectorResult {
        match self {
            DetectorSpec::NoData(d) => d.evaluate(entries, now),
            DetectorSpec::Zscore(d) => d.evaluate(entries, now),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn detector_spec_deserializes_no_data() {
        let v = json!({"type": "no_data", "window_ms": 60000});
        let spec: DetectorSpec = serde_json::from_value(v).unwrap();
        match spec {
            DetectorSpec::NoData(d) => assert_eq!(d.window_ms, 60000),
            _ => panic!("expected no_data variant"),
        }
    }

    #[test]
    fn detector_spec_deserializes_zscore() {
        let v = json!({"type": "zscore", "threshold": 3.0, "bucket_ms": 60000});
        let spec: DetectorSpec = serde_json::from_value(v).unwrap();
        match spec {
            DetectorSpec::Zscore(d) => {
                assert!((d.threshold - 3.0).abs() < f64::EPSILON);
                assert_eq!(d.bucket_ms, 60000);
            }
            _ => panic!("expected zscore variant"),
        }
    }
}
