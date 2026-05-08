//! Time-bucket rollup store
//!
//! Maintains downsampled counts per signal for 1m / 5m / 1h windows so long
//! lookback queries can be served cheaply without scanning the raw streams.
//!
//! Storage layout (Redis):
//!   - Key: `lt:rollup:{signal}:{resolution}:{epoch_bucket_seconds}`
//!   - Type: hash
//!   - Fields:
//!     - `count`        -> total entries in the bucket
//!     - `svc:<name>`   -> per-service entry counts
//!
//! The same conceptual layout is mirrored by the in-memory store so standalone
//! mode (`STANDALONE=true`) exposes the same API.

use crate::domain::telemetry::{NormalizedEntry, Signal};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Supported rollup resolutions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Resolution {
    OneMinute,
    FiveMinutes,
    OneHour,
}

impl Resolution {
    pub const ALL: [Resolution; 3] = [
        Resolution::OneMinute,
        Resolution::FiveMinutes,
        Resolution::OneHour,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Resolution::OneMinute => "1m",
            Resolution::FiveMinutes => "5m",
            Resolution::OneHour => "1h",
        }
    }

    pub const fn bucket_seconds(self) -> i64 {
        match self {
            Resolution::OneMinute => 60,
            Resolution::FiveMinutes => 300,
            Resolution::OneHour => 3_600,
        }
    }

    /// TTL for the underlying Redis key. Long enough to serve a comfortable
    /// lookback for the resolution while preventing unbounded memory growth.
    pub const fn ttl_seconds(self) -> i64 {
        match self {
            Resolution::OneMinute => 6 * 3_600,
            Resolution::FiveMinutes => 24 * 3_600,
            Resolution::OneHour => 7 * 24 * 3_600,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "1m" => Some(Resolution::OneMinute),
            "5m" => Some(Resolution::FiveMinutes),
            "1h" => Some(Resolution::OneHour),
            _ => None,
        }
    }
}

fn signal_str(signal: Signal) -> &'static str {
    match signal {
        Signal::Traces => "traces",
        Signal::Metrics => "metrics",
        Signal::Logs => "logs",
    }
}

/// Returns the bucket-start epoch in seconds for an epoch-millisecond input.
pub fn bucket_start_seconds(epoch_ms: i64, resolution: Resolution) -> i64 {
    let seconds = epoch_ms.div_euclid(1_000);
    let bucket = resolution.bucket_seconds();
    seconds.div_euclid(bucket) * bucket
}

/// Returns the Redis key for a rollup bucket.
pub fn rollup_key(signal: Signal, resolution: Resolution, bucket_epoch_seconds: i64) -> String {
    format!(
        "lt:rollup:{}:{}:{}",
        signal_str(signal),
        resolution.as_str(),
        bucket_epoch_seconds
    )
}

/// Sanitises a service name into a stable Redis hash field.
///
/// Replaces characters that could collide with the hash-field namespace or
/// confuse downstream parsing (`:` is reserved as our delimiter; whitespace and
/// control characters are coerced to `_`). Names longer than 200 bytes are
/// truncated, since hash fields here are bounded by tag cardinality more than
/// by content length.
pub fn sanitize_service_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_control() || ch.is_whitespace() || ch == ':' {
            out.push('_');
        } else {
            out.push(ch);
        }
    }
    if out.len() > 200 {
        out.truncate(200);
    }
    out
}

const SERVICE_FIELD_PREFIX: &str = "svc:";

/// One bucket of rollup data, suitable for serialising in API responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupBucket {
    pub bucket_start_seconds: i64,
    pub count: u64,
    /// Counts per service. `None` for entries without `service.name`.
    pub by_service: BTreeMap<String, u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum RollupError {
    #[error("redis: {0}")]
    Redis(#[from] ::redis::RedisError),
}

/// Polymorphic rollup store enum (Redis or in-memory).
#[derive(Clone)]
pub enum RollupStore {
    Redis(RedisRollupStore),
    Memory(MemoryRollupStore),
}

impl RollupStore {
    /// Records an ingested entry across every supported resolution.
    pub async fn record_entry(&self, entry: &NormalizedEntry) -> Result<(), RollupError> {
        let epoch_ms = entry.observed_at.timestamp_millis();
        let service = entry.service_name.as_deref();
        for resolution in Resolution::ALL {
            self.increment(entry.signal, resolution, epoch_ms, service)
                .await?;
        }
        Ok(())
    }

    pub async fn increment(
        &self,
        signal: Signal,
        resolution: Resolution,
        epoch_ms: i64,
        service_name: Option<&str>,
    ) -> Result<(), RollupError> {
        match self {
            RollupStore::Redis(s) => {
                s.increment(signal, resolution, epoch_ms, service_name)
                    .await
            }
            RollupStore::Memory(s) => {
                s.increment(signal, resolution, epoch_ms, service_name)
                    .await;
                Ok(())
            }
        }
    }

    pub async fn fetch_range(
        &self,
        signal: Signal,
        resolution: Resolution,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<RollupBucket>, RollupError> {
        match self {
            RollupStore::Redis(s) => s.fetch_range(signal, resolution, from_ms, to_ms).await,
            RollupStore::Memory(s) => Ok(s.fetch_range(signal, resolution, from_ms, to_ms).await),
        }
    }
}

/// Redis-backed rollup store using `HINCRBY` for counts.
#[derive(Clone)]
pub struct RedisRollupStore {
    conn: ::redis::aio::MultiplexedConnection,
}

impl RedisRollupStore {
    pub async fn new(url: &str) -> Result<Self, ::redis::RedisError> {
        let client = ::redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self { conn })
    }

    pub async fn increment(
        &self,
        signal: Signal,
        resolution: Resolution,
        epoch_ms: i64,
        service_name: Option<&str>,
    ) -> Result<(), RollupError> {
        let bucket = bucket_start_seconds(epoch_ms, resolution);
        let key = rollup_key(signal, resolution, bucket);
        let mut conn = self.conn.clone();

        let mut pipe = ::redis::pipe();
        pipe.atomic();
        pipe.cmd("HINCRBY").arg(&key).arg("count").arg(1).ignore();
        if let Some(name) = service_name.filter(|n| !n.is_empty()) {
            let field = format!("{SERVICE_FIELD_PREFIX}{}", sanitize_service_name(name));
            pipe.cmd("HINCRBY").arg(&key).arg(&field).arg(1).ignore();
        }
        pipe.cmd("EXPIRE")
            .arg(&key)
            .arg(resolution.ttl_seconds())
            .ignore();
        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    pub async fn fetch_range(
        &self,
        signal: Signal,
        resolution: Resolution,
        from_ms: i64,
        to_ms: i64,
    ) -> Result<Vec<RollupBucket>, RollupError> {
        let (start_bucket, end_bucket) = aligned_range(from_ms, to_ms, resolution);
        if start_bucket > end_bucket {
            return Ok(Vec::new());
        }

        let mut conn = self.conn.clone();
        let mut buckets = Vec::new();
        let bucket_secs = resolution.bucket_seconds();
        let mut cursor = start_bucket;
        while cursor <= end_bucket {
            let key = rollup_key(signal, resolution, cursor);
            let raw: HashMap<String, i64> = ::redis::cmd("HGETALL")
                .arg(&key)
                .query_async(&mut conn)
                .await?;
            if !raw.is_empty() {
                buckets.push(parse_bucket_fields(cursor, raw));
            }
            cursor += bucket_secs;
        }
        Ok(buckets)
    }
}

fn aligned_range(from_ms: i64, to_ms: i64, resolution: Resolution) -> (i64, i64) {
    let start = bucket_start_seconds(from_ms, resolution);
    let end = bucket_start_seconds(to_ms, resolution);
    (start, end)
}

fn parse_bucket_fields(bucket_start_seconds: i64, raw: HashMap<String, i64>) -> RollupBucket {
    let mut count: u64 = 0;
    let mut by_service: BTreeMap<String, u64> = BTreeMap::new();
    for (field, value) in raw {
        let value = value.max(0) as u64;
        if field == "count" {
            count = value;
        } else if let Some(svc) = field.strip_prefix(SERVICE_FIELD_PREFIX) {
            by_service.insert(svc.to_string(), value);
        }
    }
    RollupBucket {
        bucket_start_seconds,
        count,
        by_service,
    }
}

/// In-memory rollup store used in standalone mode.
#[derive(Clone, Default)]
pub struct MemoryRollupStore {
    inner: Arc<Mutex<MemoryRollupData>>,
}

#[derive(Default)]
struct MemoryRollupData {
    /// (signal, resolution, bucket_start_seconds) -> bucket
    buckets: HashMap<(Signal, Resolution, i64), RollupBucket>,
}

impl MemoryRollupStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn increment(
        &self,
        signal: Signal,
        resolution: Resolution,
        epoch_ms: i64,
        service_name: Option<&str>,
    ) {
        let bucket_start = bucket_start_seconds(epoch_ms, resolution);
        let mut guard = self.inner.lock().await;
        let entry = guard
            .buckets
            .entry((signal, resolution, bucket_start))
            .or_insert(RollupBucket {
                bucket_start_seconds: bucket_start,
                count: 0,
                by_service: BTreeMap::new(),
            });
        entry.count += 1;
        if let Some(name) = service_name.filter(|n| !n.is_empty()) {
            let key = sanitize_service_name(name);
            *entry.by_service.entry(key).or_insert(0) += 1;
        }
    }

    pub async fn fetch_range(
        &self,
        signal: Signal,
        resolution: Resolution,
        from_ms: i64,
        to_ms: i64,
    ) -> Vec<RollupBucket> {
        let (start_bucket, end_bucket) = aligned_range(from_ms, to_ms, resolution);
        if start_bucket > end_bucket {
            return Vec::new();
        }

        let guard = self.inner.lock().await;
        let bucket_secs = resolution.bucket_seconds();
        let mut out = Vec::new();
        let mut cursor = start_bucket;
        while cursor <= end_bucket {
            if let Some(bucket) = guard.buckets.get(&(signal, resolution, cursor)) {
                out.push(bucket.clone());
            }
            cursor += bucket_secs;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;
    use bytes::Bytes;
    use chrono::{TimeZone, Utc};

    #[test]
    fn resolution_parse_round_trips() {
        for r in Resolution::ALL {
            assert_eq!(Resolution::parse(r.as_str()), Some(r));
        }
        assert_eq!(Resolution::parse("invalid"), None);
        assert_eq!(Resolution::parse(""), None);
    }

    #[test]
    fn bucket_start_aligns_to_resolution() {
        // 2024-01-01T00:01:30Z = 1_704_067_290 seconds
        let epoch_ms = 1_704_067_290_000;
        assert_eq!(
            bucket_start_seconds(epoch_ms, Resolution::OneMinute),
            1_704_067_260
        );
        assert_eq!(
            bucket_start_seconds(epoch_ms, Resolution::FiveMinutes),
            1_704_067_200
        );
        assert_eq!(
            bucket_start_seconds(epoch_ms, Resolution::OneHour),
            1_704_067_200
        );
    }

    #[test]
    fn rollup_key_is_namespaced_per_signal_and_resolution() {
        let traces_1m = rollup_key(Signal::Traces, Resolution::OneMinute, 1_700_000_000);
        let traces_5m = rollup_key(Signal::Traces, Resolution::FiveMinutes, 1_700_000_000);
        let metrics_1m = rollup_key(Signal::Metrics, Resolution::OneMinute, 1_700_000_000);

        assert_eq!(traces_1m, "lt:rollup:traces:1m:1700000000");
        assert_ne!(traces_1m, traces_5m);
        assert_ne!(traces_1m, metrics_1m);
    }

    #[test]
    fn sanitize_service_name_strips_delimiters() {
        assert_eq!(sanitize_service_name("checkout-ui"), "checkout-ui");
        assert_eq!(sanitize_service_name("ns:svc"), "ns_svc");
        assert_eq!(sanitize_service_name("with space"), "with_space");
    }

    #[tokio::test]
    async fn memory_store_records_entry_into_all_resolutions() {
        let store = RollupStore::Memory(MemoryRollupStore::new());
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc.timestamp_opt(1_704_067_290, 0).unwrap(),
            service_name: Some("checkout-ui".to_string()),
            payload: Bytes::new(),
        };

        store.record_entry(&entry).await.unwrap();

        for resolution in Resolution::ALL {
            let buckets = store
                .fetch_range(
                    Signal::Traces,
                    resolution,
                    1_704_067_200_000,
                    1_704_067_300_000,
                )
                .await
                .unwrap();
            assert_eq!(buckets.len(), 1, "{resolution:?}: expected one bucket");
            let bucket = &buckets[0];
            assert_eq!(bucket.count, 1);
            assert_eq!(bucket.by_service.get("checkout-ui").copied(), Some(1));
        }
    }

    #[tokio::test]
    async fn memory_store_aggregates_multiple_entries_in_same_bucket() {
        let store = MemoryRollupStore::new();
        for service in ["api", "api", "worker"] {
            store
                .increment(
                    Signal::Logs,
                    Resolution::OneMinute,
                    1_704_067_290_000,
                    Some(service),
                )
                .await;
        }

        let buckets = store
            .fetch_range(
                Signal::Logs,
                Resolution::OneMinute,
                1_704_067_200_000,
                1_704_067_300_000,
            )
            .await;

        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].count, 3);
        assert_eq!(buckets[0].by_service.get("api").copied(), Some(2));
        assert_eq!(buckets[0].by_service.get("worker").copied(), Some(1));
    }

    #[tokio::test]
    async fn memory_store_skips_empty_buckets() {
        let store = MemoryRollupStore::new();
        store
            .increment(
                Signal::Traces,
                Resolution::OneMinute,
                1_704_067_290_000,
                None,
            )
            .await;

        let buckets = store
            .fetch_range(
                Signal::Traces,
                Resolution::OneMinute,
                1_704_063_690_000, // 1h earlier
                1_704_067_300_000,
            )
            .await;

        // only the populated bucket is returned
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].count, 1);
        assert!(buckets[0].by_service.is_empty());
    }

    #[tokio::test]
    async fn memory_store_returns_empty_for_inverted_range() {
        let store = MemoryRollupStore::new();
        store
            .increment(
                Signal::Traces,
                Resolution::OneMinute,
                1_704_067_290_000,
                None,
            )
            .await;

        let buckets = store
            .fetch_range(
                Signal::Traces,
                Resolution::OneMinute,
                1_704_067_300_000,
                1_704_067_200_000,
            )
            .await;
        assert!(buckets.is_empty());
    }
}
