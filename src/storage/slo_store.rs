use crate::domain::slo::SloDefinition;
use crate::storage::StorageError;
use serde_json::Value;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

/// DDL applied at startup.
pub const CREATE_SLO_DEFINITIONS_SQL: &str = "
CREATE TABLE IF NOT EXISTS slo_definitions (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    viewer_id UUID,
    target_pct DOUBLE PRECISION NOT NULL,
    window_ms BIGINT NOT NULL,
    success_filter_json JSONB NOT NULL,
    total_filter_json JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
)";

/// Trait-like enum dispatch for SLO persistence.
#[derive(Clone)]
pub enum SloStore {
    Postgres(PostgresSloStore),
    Memory(MemorySloStore),
}

impl SloStore {
    pub async fn insert(&self, slo: &SloDefinition) -> Result<(), StorageError> {
        match self {
            SloStore::Postgres(s) => s.insert(slo).await.map_err(StorageError::Postgres),
            SloStore::Memory(s) => s.insert(slo).await,
        }
    }

    pub async fn list(&self) -> Result<Vec<SloDefinition>, StorageError> {
        match self {
            SloStore::Postgres(s) => s.list().await.map_err(StorageError::Postgres),
            SloStore::Memory(s) => s.list().await,
        }
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<SloDefinition>, StorageError> {
        match self {
            SloStore::Postgres(s) => s.get(id).await.map_err(StorageError::Postgres),
            SloStore::Memory(s) => s.get(id).await,
        }
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, StorageError> {
        match self {
            SloStore::Postgres(s) => s.delete(id).await.map_err(StorageError::Postgres),
            SloStore::Memory(s) => s.delete(id).await,
        }
    }
}

/// Postgres-backed SLO store.
#[derive(Clone)]
pub struct PostgresSloStore {
    pool: PgPool,
}

impl PostgresSloStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::query(CREATE_SLO_DEFINITIONS_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert(&self, slo: &SloDefinition) -> Result<(), sqlx::Error> {
        // Serializing SloFilterList (a struct with a single Vec field) cannot fail.
        let success_json = serde_json::to_value(&slo.success_filter).expect("serialize success");
        let total_json = serde_json::to_value(&slo.total_filter).expect("serialize total");
        sqlx::query(
            "INSERT INTO slo_definitions
             (id, name, viewer_id, target_pct, window_ms,
              success_filter_json, total_filter_json, enabled)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(slo.id)
        .bind(&slo.name)
        .bind(slo.viewer_id)
        .bind(slo.target_pct)
        .bind(slo.window_ms)
        .bind(&success_json)
        .bind(&total_json)
        .bind(slo.enabled)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<SloDefinition>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, name, viewer_id, target_pct, window_ms,
                    success_filter_json, total_filter_json, enabled
             FROM slo_definitions
             ORDER BY name ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(row_to_slo).collect())
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<SloDefinition>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, name, viewer_id, target_pct, window_ms,
                    success_filter_json, total_filter_json, enabled
             FROM slo_definitions
             WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(row_to_slo))
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM slo_definitions WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

/// In-memory SLO store used in standalone mode.
#[derive(Clone, Default)]
pub struct MemorySloStore {
    inner: Arc<Mutex<Vec<SloDefinition>>>,
}

impl MemorySloStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, slo: &SloDefinition) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        guard.push(slo.clone());
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<SloDefinition>, StorageError> {
        let guard = self.inner.lock().await;
        let mut slos = guard.clone();
        slos.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(slos)
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<SloDefinition>, StorageError> {
        let guard = self.inner.lock().await;
        Ok(guard.iter().find(|s| s.id == id).cloned())
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, StorageError> {
        let mut guard = self.inner.lock().await;
        let before = guard.len();
        guard.retain(|s| s.id != id);
        Ok(guard.len() < before)
    }
}

fn row_to_slo(row: sqlx::postgres::PgRow) -> SloDefinition {
    use sqlx::Row;
    let success_json: Value = row.get("success_filter_json");
    let total_json: Value = row.get("total_filter_json");
    let success_filter = serde_json::from_value(success_json).unwrap_or_default();
    let total_filter = serde_json::from_value(total_json).unwrap_or_default();
    SloDefinition {
        id: row.get("id"),
        name: row.get("name"),
        viewer_id: row.get("viewer_id"),
        target_pct: row.get("target_pct"),
        window_ms: row.get("window_ms"),
        success_filter,
        total_filter,
        enabled: row.get("enabled"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::slo::{SloFilterClause, SloFilterList};

    fn make_slo() -> SloDefinition {
        SloDefinition {
            id: Uuid::new_v4(),
            name: "demo".into(),
            viewer_id: None,
            target_pct: 99.5,
            window_ms: 5 * 60_000,
            success_filter: SloFilterList {
                filters: vec![SloFilterClause {
                    field: "service_name".into(),
                    op: "eq".into(),
                    value: "ok-svc".into(),
                }],
            },
            total_filter: SloFilterList::default(),
            enabled: true,
        }
    }

    #[tokio::test]
    async fn memory_insert_list_get_delete() {
        let store = MemorySloStore::new();
        let slo = make_slo();
        store.insert(&slo).await.unwrap();
        let listed = store.list().await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, slo.id);
        let fetched = store.get(slo.id).await.unwrap().unwrap();
        assert_eq!(fetched.target_pct, 99.5);
        assert!(store.delete(slo.id).await.unwrap());
        assert!(store.get(slo.id).await.unwrap().is_none());
        assert!(!store.delete(slo.id).await.unwrap());
    }
}
