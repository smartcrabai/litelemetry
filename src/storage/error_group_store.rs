//! Persistence layer for error group aggregates.
//!
//! Two backends are exposed:
//! - `Postgres` upserts each fingerprint into the `error_groups` table.
//! - `Memory` keeps the same shape in-process for standalone mode.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::Row;
use tokio::sync::Mutex;

use crate::apm::error_groups::ErrorGroup;
use crate::storage::StorageError;

const CREATE_ERROR_GROUPS_SQL: &str = "
CREATE TABLE IF NOT EXISTS error_groups (
    fingerprint TEXT PRIMARY KEY,
    signature TEXT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    sample_payload JSONB NOT NULL DEFAULT '{}'::jsonb
)";

const CREATE_ERROR_GROUPS_LAST_SEEN_INDEX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS error_groups_last_seen_idx ON error_groups (last_seen)";

/// Backed by PostgreSQL `error_groups`.
#[derive(Clone)]
pub struct PostgresErrorGroupStore {
    pool: PgPool,
}

impl PostgresErrorGroupStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::query(CREATE_ERROR_GROUPS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_ERROR_GROUPS_LAST_SEEN_INDEX_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Upsert an error group, incrementing `count` and updating `last_seen`/sample.
    pub async fn upsert(&self, group: &ErrorGroup) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO error_groups
             (fingerprint, signature, first_seen, last_seen, count, sample_payload)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (fingerprint) DO UPDATE SET
               signature = EXCLUDED.signature,
               first_seen = LEAST(error_groups.first_seen, EXCLUDED.first_seen),
               last_seen = GREATEST(error_groups.last_seen, EXCLUDED.last_seen),
               count = error_groups.count + EXCLUDED.count,
               sample_payload = EXCLUDED.sample_payload",
        )
        .bind(&group.fingerprint)
        .bind(&group.signature)
        .bind(group.first_seen)
        .bind(group.last_seen)
        .bind(group.count as i64)
        .bind(&group.sample_payload)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Load groups whose `last_seen` is within the lookback window.
    pub async fn list_recent(&self, since: DateTime<Utc>) -> Result<Vec<ErrorGroup>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT fingerprint, signature, first_seen, last_seen, count, sample_payload
             FROM error_groups
             WHERE last_seen >= $1
             ORDER BY last_seen DESC",
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ErrorGroup {
                fingerprint: row.get("fingerprint"),
                signature: row.get("signature"),
                first_seen: row.get("first_seen"),
                last_seen: row.get("last_seen"),
                count: row.get::<i64, _>("count").max(0) as u64,
                sample_payload: row.get::<Value, _>("sample_payload"),
            })
            .collect())
    }
}

/// In-memory error group store for standalone mode.
#[derive(Clone, Default)]
pub struct MemoryErrorGroupStore {
    inner: Arc<Mutex<HashMap<String, ErrorGroup>>>,
}

impl MemoryErrorGroupStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn upsert(&self, group: &ErrorGroup) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        match guard.get_mut(&group.fingerprint) {
            Some(existing) => {
                existing.count = existing.count.saturating_add(group.count);
                if group.first_seen < existing.first_seen {
                    existing.first_seen = group.first_seen;
                }
                if group.last_seen > existing.last_seen {
                    existing.last_seen = group.last_seen;
                    existing.signature = group.signature.clone();
                    existing.sample_payload = group.sample_payload.clone();
                }
            }
            None => {
                guard.insert(group.fingerprint.clone(), group.clone());
            }
        }
        Ok(())
    }

    pub async fn list_recent(&self, since: DateTime<Utc>) -> Result<Vec<ErrorGroup>, StorageError> {
        let guard = self.inner.lock().await;
        let mut groups: Vec<ErrorGroup> = guard
            .values()
            .filter(|g| g.last_seen >= since)
            .cloned()
            .collect();
        groups.sort_by_key(|g| std::cmp::Reverse(g.last_seen));
        Ok(groups)
    }
}

/// Enum-dispatched store mirroring the [`StreamStore`]/[`ViewerStore`] pattern.
#[derive(Clone)]
pub enum ErrorGroupStore {
    Postgres(PostgresErrorGroupStore),
    Memory(MemoryErrorGroupStore),
}

impl ErrorGroupStore {
    pub async fn upsert(&self, group: &ErrorGroup) -> Result<(), StorageError> {
        match self {
            ErrorGroupStore::Postgres(s) => s.upsert(group).await.map_err(StorageError::Postgres),
            ErrorGroupStore::Memory(s) => s.upsert(group).await,
        }
    }

    pub async fn list_recent(&self, since: DateTime<Utc>) -> Result<Vec<ErrorGroup>, StorageError> {
        match self {
            ErrorGroupStore::Postgres(s) => {
                s.list_recent(since).await.map_err(StorageError::Postgres)
            }
            ErrorGroupStore::Memory(s) => s.list_recent(since).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[tokio::test]
    async fn memory_store_aggregates_repeated_fingerprints() {
        let store = MemoryErrorGroupStore::new();
        let early = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let late = Utc.with_ymd_and_hms(2024, 1, 1, 0, 1, 0).unwrap();
        let g1 = ErrorGroup {
            fingerprint: "abc".into(),
            signature: "RuntimeError: boom".into(),
            count: 1,
            first_seen: early,
            last_seen: early,
            sample_payload: serde_json::json!({"v": 1}),
        };
        let g2 = ErrorGroup {
            fingerprint: "abc".into(),
            signature: "RuntimeError: boom (newer)".into(),
            count: 2,
            first_seen: late,
            last_seen: late,
            sample_payload: serde_json::json!({"v": 2}),
        };

        store.upsert(&g1).await.unwrap();
        store.upsert(&g2).await.unwrap();

        let recent = store.list_recent(early).await.unwrap();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].count, 3);
        assert_eq!(recent[0].first_seen, early);
        assert_eq!(recent[0].last_seen, late);
        assert_eq!(recent[0].signature, "RuntimeError: boom (newer)");
        assert_eq!(recent[0].sample_payload["v"], 2);
    }

    #[tokio::test]
    async fn memory_store_respects_lookback_window() {
        let store = MemoryErrorGroupStore::new();
        let early = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let late = Utc.with_ymd_and_hms(2024, 1, 1, 0, 5, 0).unwrap();
        let cutoff = Utc.with_ymd_and_hms(2024, 1, 1, 0, 4, 0).unwrap();
        store
            .upsert(&ErrorGroup {
                fingerprint: "old".into(),
                signature: "old".into(),
                count: 1,
                first_seen: early,
                last_seen: early,
                sample_payload: Value::Null,
            })
            .await
            .unwrap();
        store
            .upsert(&ErrorGroup {
                fingerprint: "new".into(),
                signature: "new".into(),
                count: 1,
                first_seen: late,
                last_seen: late,
                sample_payload: Value::Null,
            })
            .await
            .unwrap();
        let visible = store.list_recent(cutoff).await.unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].fingerprint, "new");
    }
}
