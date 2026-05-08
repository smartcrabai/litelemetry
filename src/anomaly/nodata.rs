//! "No data within window" detector.
//!
//! Trips when no entry has been observed in the last `window_ms` milliseconds.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::telemetry::NormalizedEntry;

use super::{Detector, DetectorResult};

/// Trips if no entry has been observed within the last `window_ms` milliseconds.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct NoDataDetector {
    pub window_ms: i64,
}

impl Detector for NoDataDetector {
    fn evaluate(&self, entries: &[NormalizedEntry], now: DateTime<Utc>) -> DetectorResult {
        let window_secs = self.window_ms as f64 / 1000.0;
        let expected = window_secs;

        let latest = entries.iter().map(|e| e.observed_at).max();

        match latest {
            None => DetectorResult {
                breached: true,
                // No entries at all -> "infinite" age. Use window_secs * 2 as a
                // sentinel that the caller can recognise as "much greater than
                // expected" without needing f64::INFINITY (which JSON can't
                // represent).
                value: window_secs * 2.0,
                expected,
                observed_at: now,
            },
            Some(latest_at) => {
                let age_ms = (now - latest_at).num_milliseconds();
                let age_secs = age_ms as f64 / 1000.0;
                DetectorResult {
                    breached: age_ms > self.window_ms,
                    value: age_secs,
                    expected,
                    observed_at: now,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;
    use bytes::Bytes;
    use chrono::Duration;

    fn make_entry(observed_at: DateTime<Utc>) -> NormalizedEntry {
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at,
            service_name: Some("svc".into()),
            payload: Bytes::new(),
        }
    }

    #[test]
    fn no_entries_breaches() {
        let det = NoDataDetector { window_ms: 60_000 };
        let now = Utc::now();
        let res = det.evaluate(&[], now);
        assert!(res.breached);
    }

    #[test]
    fn recent_entry_does_not_breach() {
        let det = NoDataDetector { window_ms: 60_000 };
        let now = Utc::now();
        let entries = vec![make_entry(now - Duration::seconds(10))];
        let res = det.evaluate(&entries, now);
        assert!(!res.breached);
        assert!((res.value - 10.0).abs() < 0.5);
        assert!((res.expected - 60.0).abs() < f64::EPSILON);
    }

    #[test]
    fn old_entry_breaches() {
        let det = NoDataDetector { window_ms: 60_000 };
        let now = Utc::now();
        let entries = vec![make_entry(now - Duration::seconds(120))];
        let res = det.evaluate(&entries, now);
        assert!(res.breached);
        assert!(res.value > 60.0);
    }

    #[test]
    fn picks_latest_observed_at() {
        let det = NoDataDetector { window_ms: 60_000 };
        let now = Utc::now();
        let entries = vec![
            make_entry(now - Duration::seconds(300)),
            make_entry(now - Duration::seconds(5)),
            make_entry(now - Duration::seconds(120)),
        ];
        let res = det.evaluate(&entries, now);
        assert!(!res.breached);
        assert!(res.value < 60.0);
    }
}
