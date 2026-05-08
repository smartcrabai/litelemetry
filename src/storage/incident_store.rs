use crate::domain::incident::{Incident, IncidentStatus};
use crate::storage::StorageError;
use crate::storage::memory::MemoryIncidentStore;
use crate::storage::postgres::PostgresStore;
use uuid::Uuid;

/// Enum store responsible for incident lifecycle persistence.
#[derive(Clone)]
pub enum IncidentStore {
    Postgres(PostgresStore),
    Memory(MemoryIncidentStore),
}

impl IncidentStore {
    pub async fn insert(&self, incident: &Incident) -> Result<(), StorageError> {
        match self {
            IncidentStore::Postgres(s) => s
                .insert_incident(incident)
                .await
                .map_err(StorageError::Postgres),
            IncidentStore::Memory(s) => s.insert_incident(incident).await,
        }
    }

    pub async fn list(
        &self,
        status: Option<IncidentStatus>,
    ) -> Result<Vec<Incident>, StorageError> {
        match self {
            IncidentStore::Postgres(s) => s
                .list_incidents(status)
                .await
                .map_err(StorageError::Postgres),
            IncidentStore::Memory(s) => s.list_incidents(status).await,
        }
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Incident>, StorageError> {
        match self {
            IncidentStore::Postgres(s) => s.get_incident(id).await.map_err(StorageError::Postgres),
            IncidentStore::Memory(s) => s.get_incident(id).await,
        }
    }

    pub async fn update_status(
        &self,
        id: Uuid,
        new_status: IncidentStatus,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Incident>, StorageError> {
        match self {
            IncidentStore::Postgres(s) => s
                .update_incident_status(id, new_status, now)
                .await
                .map_err(StorageError::Postgres),
            IncidentStore::Memory(s) => s.update_incident_status(id, new_status, now).await,
        }
    }
}
