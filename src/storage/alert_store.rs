use crate::domain::alert::{AlertCondition, AlertDefinition, AlertSeverity};
use crate::storage::StorageError;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub const CREATE_ALERT_DEFINITIONS_SQL: &str = "
CREATE TABLE IF NOT EXISTS alert_definitions (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    viewer_id UUID NOT NULL,
    condition_json JSONB NOT NULL,
    severity TEXT NOT NULL,
    evaluation_interval_ms INTEGER NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    revision BIGINT NOT NULL DEFAULT 0
)";

/// Postgres-backed CRUD for alert definitions.
#[derive(Clone)]
pub struct PostgresAlertStore {
    pool: PgPool,
}

impl PostgresAlertStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::query(CREATE_ALERT_DEFINITIONS_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn insert(&self, def: &AlertDefinition) -> Result<(), sqlx::Error> {
        let condition_json = serde_json::to_value(&def.condition)
            .expect("AlertCondition serialization should never fail");
        sqlx::query(
            "INSERT INTO alert_definitions
             (id, name, viewer_id, condition_json, severity,
              evaluation_interval_ms, enabled, revision)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(def.id)
        .bind(&def.name)
        .bind(def.viewer_id)
        .bind(condition_json)
        .bind(def.severity.as_str())
        .bind(def.evaluation_interval_ms)
        .bind(def.enabled)
        .bind(def.revision)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn load_all(&self) -> Result<Vec<AlertDefinition>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, name, viewer_id, condition_json, severity,
                    evaluation_interval_ms, enabled, revision
             FROM alert_definitions
             ORDER BY name ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        let defs = rows
            .into_iter()
            .filter_map(|row| row_to_def(&row).ok())
            .collect();
        Ok(defs)
    }

    pub async fn load(&self, id: Uuid) -> Result<Option<AlertDefinition>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, name, viewer_id, condition_json, severity,
                    evaluation_interval_ms, enabled, revision
             FROM alert_definitions
             WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some(r) => row_to_def(&r).ok(),
            None => None,
        })
    }

    pub async fn update(&self, def: &AlertDefinition) -> Result<bool, sqlx::Error> {
        let condition_json = serde_json::to_value(&def.condition)
            .expect("AlertCondition serialization should never fail");
        let result = sqlx::query(
            "UPDATE alert_definitions
             SET name = $1,
                 viewer_id = $2,
                 condition_json = $3,
                 severity = $4,
                 evaluation_interval_ms = $5,
                 enabled = $6,
                 revision = revision + 1
             WHERE id = $7",
        )
        .bind(&def.name)
        .bind(def.viewer_id)
        .bind(condition_json)
        .bind(def.severity.as_str())
        .bind(def.evaluation_interval_ms)
        .bind(def.enabled)
        .bind(def.id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM alert_definitions WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}

fn row_to_def(row: &sqlx::postgres::PgRow) -> Result<AlertDefinition, AlertRowError> {
    use sqlx::Row;
    let id: Uuid = row.get("id");
    let condition_json: serde_json::Value = row.get("condition_json");
    let condition: AlertCondition = serde_json::from_value(condition_json).map_err(|e| {
        tracing::warn!("alert {id}: failed to parse condition_json: {e}");
        AlertRowError
    })?;
    let severity_str: String = row.get("severity");
    let severity = AlertSeverity::parse(&severity_str).ok_or_else(|| {
        tracing::warn!("alert {id}: unknown severity {severity_str:?}");
        AlertRowError
    })?;

    Ok(AlertDefinition {
        id,
        name: row.get("name"),
        viewer_id: row.get("viewer_id"),
        condition,
        severity,
        evaluation_interval_ms: row.get("evaluation_interval_ms"),
        enabled: row.get("enabled"),
        revision: row.get("revision"),
    })
}

#[derive(Debug)]
struct AlertRowError;

/// In-memory alert store, used in standalone mode.
#[derive(Clone, Default)]
pub struct MemoryAlertStore {
    inner: Arc<Mutex<Vec<AlertDefinition>>>,
}

impl MemoryAlertStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, def: &AlertDefinition) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        guard.push(def.clone());
        Ok(())
    }

    pub async fn load_all(&self) -> Result<Vec<AlertDefinition>, StorageError> {
        let guard = self.inner.lock().await;
        Ok(guard.clone())
    }

    pub async fn load(&self, id: Uuid) -> Result<Option<AlertDefinition>, StorageError> {
        let guard = self.inner.lock().await;
        Ok(guard.iter().find(|d| d.id == id).cloned())
    }

    pub async fn update(&self, def: &AlertDefinition) -> Result<bool, StorageError> {
        let mut guard = self.inner.lock().await;
        if let Some(existing) = guard.iter_mut().find(|d| d.id == def.id) {
            *existing = def.clone();
            existing.revision += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, StorageError> {
        let mut guard = self.inner.lock().await;
        let before = guard.len();
        guard.retain(|d| d.id != id);
        Ok(guard.len() < before)
    }
}

/// Enum that wraps the supported alert stores.
#[derive(Clone)]
pub enum AlertStore {
    Postgres(PostgresAlertStore),
    Memory(MemoryAlertStore),
}

impl AlertStore {
    pub async fn insert(&self, def: &AlertDefinition) -> Result<(), StorageError> {
        match self {
            AlertStore::Postgres(s) => s.insert(def).await.map_err(StorageError::Postgres),
            AlertStore::Memory(s) => s.insert(def).await,
        }
    }

    pub async fn load_all(&self) -> Result<Vec<AlertDefinition>, StorageError> {
        match self {
            AlertStore::Postgres(s) => s.load_all().await.map_err(StorageError::Postgres),
            AlertStore::Memory(s) => s.load_all().await,
        }
    }

    pub async fn load(&self, id: Uuid) -> Result<Option<AlertDefinition>, StorageError> {
        match self {
            AlertStore::Postgres(s) => s.load(id).await.map_err(StorageError::Postgres),
            AlertStore::Memory(s) => s.load(id).await,
        }
    }

    pub async fn update(&self, def: &AlertDefinition) -> Result<bool, StorageError> {
        match self {
            AlertStore::Postgres(s) => s.update(def).await.map_err(StorageError::Postgres),
            AlertStore::Memory(s) => s.update(def).await,
        }
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, StorageError> {
        match self {
            AlertStore::Postgres(s) => s.delete(id).await.map_err(StorageError::Postgres),
            AlertStore::Memory(s) => s.delete(id).await,
        }
    }
}
