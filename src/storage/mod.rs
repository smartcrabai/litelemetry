pub mod memory;
pub mod postgres;
pub mod redis;

use crate::domain::dashboard::DashboardDefinition;
use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::domain::viewer::ViewerDefinition;
use postgres::ViewerSnapshotRow;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("redis: {0}")]
    Redis(#[from] ::redis::RedisError),
    #[error("postgres: {0}")]
    Postgres(#[from] sqlx::Error),
}

/// Enum store responsible for stream read/write
#[derive(Clone)]
pub enum StreamStore {
    Redis(redis::RedisStore),
    Memory(memory::MemoryStreamStore),
}

impl StreamStore {
    pub async fn append_entry(&mut self, entry: &NormalizedEntry) -> Result<String, StorageError> {
        match self {
            StreamStore::Redis(s) => s.append_entry(entry).await.map_err(StorageError::Redis),
            StreamStore::Memory(s) => s.append_entry(entry).await,
        }
    }

    pub async fn read_entries_since(
        &mut self,
        signal: Signal,
        cursor: Option<&str>,
        count: usize,
    ) -> Result<Vec<(String, NormalizedEntry)>, StorageError> {
        match self {
            StreamStore::Redis(s) => s
                .read_entries_since(signal, cursor, count)
                .await
                .map_err(StorageError::Redis),
            StreamStore::Memory(s) => s.read_entries_since(signal, cursor, count).await,
        }
    }
}

/// Enum store responsible for viewer definition and snapshot management
#[derive(Clone)]
pub enum ViewerStore {
    Postgres(postgres::PostgresStore),
    Memory(memory::MemoryViewerStore),
}

impl ViewerStore {
    pub async fn load_viewer_definitions(&self) -> Result<Vec<ViewerDefinition>, StorageError> {
        match self {
            ViewerStore::Postgres(s) => s
                .load_viewer_definitions()
                .await
                .map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.load_viewer_definitions().await,
        }
    }

    pub async fn load_snapshots(
        &self,
        viewer_ids: &[Uuid],
    ) -> Result<Vec<ViewerSnapshotRow>, StorageError> {
        match self {
            ViewerStore::Postgres(s) => s
                .load_snapshots(viewer_ids)
                .await
                .map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.load_snapshots(viewer_ids).await,
        }
    }

    pub async fn insert_viewer_definition(
        &self,
        def: &ViewerDefinition,
    ) -> Result<(), StorageError> {
        match self {
            ViewerStore::Postgres(s) => s
                .insert_viewer_definition(def)
                .await
                .map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.insert_viewer_definition(def).await,
        }
    }

    pub async fn update_viewer_definition_json(
        &self,
        id: Uuid,
        definition_json: &Value,
        layout_json: &Value,
    ) -> Result<bool, StorageError> {
        match self {
            ViewerStore::Postgres(s) => s
                .update_viewer_definition_json(id, definition_json, layout_json)
                .await
                .map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => {
                s.update_viewer_definition_json(id, definition_json, layout_json)
                    .await
            }
        }
    }

    pub async fn upsert_snapshot(&self, snapshot: &ViewerSnapshotRow) -> Result<(), StorageError> {
        match self {
            ViewerStore::Postgres(s) => s
                .upsert_snapshot(snapshot)
                .await
                .map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.upsert_snapshot(snapshot).await,
        }
    }

    // --- Dashboard CRUD ------------------------------------------------------

    pub async fn insert_dashboard(
        &self,
        dashboard: &DashboardDefinition,
    ) -> Result<(), StorageError> {
        match self {
            ViewerStore::Postgres(s) => s
                .insert_dashboard(dashboard)
                .await
                .map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.insert_dashboard(dashboard).await,
        }
    }

    pub async fn load_dashboards(&self) -> Result<Vec<DashboardDefinition>, StorageError> {
        match self {
            ViewerStore::Postgres(s) => s.load_dashboards().await.map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.load_dashboards().await,
        }
    }

    pub async fn load_dashboard(
        &self,
        id: Uuid,
    ) -> Result<Option<DashboardDefinition>, StorageError> {
        match self {
            ViewerStore::Postgres(s) => s.load_dashboard(id).await.map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.load_dashboard(id).await,
        }
    }

    pub async fn update_dashboard(
        &self,
        id: Uuid,
        name: &str,
        layout_json: &Value,
    ) -> Result<bool, StorageError> {
        match self {
            ViewerStore::Postgres(s) => s
                .update_dashboard(id, name, layout_json)
                .await
                .map_err(StorageError::Postgres),
            ViewerStore::Memory(s) => s.update_dashboard(id, name, layout_json).await,
        }
    }

    pub async fn delete_dashboard(&self, id: Uuid) -> Result<bool, StorageError> {
        match self {
            ViewerStore::Postgres(s) => {
                s.delete_dashboard(id).await.map_err(StorageError::Postgres)
            }
            ViewerStore::Memory(s) => s.delete_dashboard(id).await,
        }
    }
}
