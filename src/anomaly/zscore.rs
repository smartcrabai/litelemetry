//! Z-score detector over bucketed entry counts.
//!
//! Buckets entries by `bucket_ms`, computes mean and standard deviation of
//! historical bucket counts (excluding the latest), and trips when the latest
//! bucket's count exceeds `mean + threshold * std`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::telemetry::NormalizedEntry;

use super::{Detector, DetectorResult};

/// Trips when the latest bucket's count is `> mean + threshold * std`.
///
/// `bucket_ms` controls aggregation granularity. The detector requires at
/// least two historical buckets (i.e. three buckets total including the
/// latest) to produce a meaningful standard deviation; otherwise it returns
/// `breached = false`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ZScoreDetector {
    pub threshold: f64,
    pub bucket_ms: i64,
}

impl Detector for ZScoreDetector {
    fn evaluate(&self, entries: &[NormalizedEntry], now: DateTime<Utc>) -> DetectorResult {
        if self.bucket_ms <= 0 {
            return DetectorResult {
                breached: false,
                value: 0.0,
                expected: 0.0,
                observed_at: now,
            };
        }

        // Bucket index 0 is the bucket containing `now`; index k is the
        // bucket k * bucket_ms before now. `history` is dense: gaps between
        // observed buckets are zero-filled so the baseline reflects idle
        // periods rather than only sampled activity.
        let now_ms = now.timestamp_millis();
        let mut latest_bucket_count: f64 = 0.0;
        let mut history: Vec<f64> = Vec::new();
        for entry in entries {
            let age_ms = now_ms - entry.observed_at.timestamp_millis();
            if age_ms < 0 {
                // Entry slightly in the future; treat as latest bucket.
                latest_bucket_count += 1.0;
                continue;
            }
            let bucket_idx = age_ms / self.bucket_ms;
            if bucket_idx == 0 {
                latest_bucket_count += 1.0;
            } else {
                let idx = bucket_idx as usize;
                if history.len() < idx {
                    history.resize(idx, 0.0);
                }
                history[idx - 1] += 1.0;
            }
        }

        if history.len() < 2 {
            // Not enough history for a meaningful baseline.
            return DetectorResult {
                breached: false,
                value: latest_bucket_count,
                expected: latest_bucket_count,
                observed_at: now,
            };
        }

        let n = history.len() as f64;
        let mean: f64 = history.iter().sum::<f64>() / n;
        let variance: f64 = history.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
        let std = variance.sqrt();

        let expected = mean + self.threshold * std;
        let breached = latest_bucket_count > expected;

        DetectorResult {
            breached,
            value: latest_bucket_count,
            expected,
            observed_at: now,
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
    fn empty_does_not_breach() {
        let det = ZScoreDetector {
            threshold: 3.0,
            bucket_ms: 60_000,
        };
        let now = Utc::now();
        let res = det.evaluate(&[], now);
        assert!(!res.breached);
    }

    #[test]
    fn flat_traffic_does_not_breach() {
        let det = ZScoreDetector {
            threshold: 3.0,
            bucket_ms: 60_000,
        };
        let now = Utc::now();
        let mut entries = Vec::new();
        // 5 buckets * 10 entries each, including the latest bucket.
        for bucket in 0..5 {
            for i in 0..10 {
                let offset_ms = bucket as i64 * 60_000 + i * 1_000;
                entries.push(make_entry(now - Duration::milliseconds(offset_ms)));
            }
        }
        let res = det.evaluate(&entries, now);
        assert!(!res.breached, "flat traffic should not trip; got {res:?}");
    }

    #[test]
    fn spike_breaches() {
        let det = ZScoreDetector {
            threshold: 2.0,
            bucket_ms: 60_000,
        };
        let now = Utc::now();
        let mut entries = Vec::new();
        // Latest bucket: 100 entries. Older buckets: 1 entry each.
        for i in 0..100 {
            entries.push(make_entry(now - Duration::milliseconds(i * 100)));
        }
        for bucket in 1..5 {
            entries.push(make_entry(
                now - Duration::milliseconds(bucket * 60_000 + 5_000),
            ));
        }
        let res = det.evaluate(&entries, now);
        assert!(res.breached, "100x spike should trip; got {res:?}");
        assert!((res.value - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn insufficient_history_does_not_breach() {
        let det = ZScoreDetector {
            threshold: 3.0,
            bucket_ms: 60_000,
        };
        let now = Utc::now();
        // Only the latest bucket has data.
        let entries = vec![
            make_entry(now - Duration::seconds(1)),
            make_entry(now - Duration::seconds(2)),
        ];
        let res = det.evaluate(&entries, now);
        assert!(!res.breached);
    }

    #[test]
    fn invalid_bucket_ms_does_not_breach() {
        let det = ZScoreDetector {
            threshold: 3.0,
            bucket_ms: 0,
        };
        let now = Utc::now();
        let entries = vec![make_entry(now)];
        let res = det.evaluate(&entries, now);
        assert!(!res.breached);
    }
}
