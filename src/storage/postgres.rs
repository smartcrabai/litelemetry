use crate::domain::dashboard::DashboardDefinition;
use crate::domain::incident::{Incident, IncidentStatus};
use crate::domain::telemetry::SignalMask;
use crate::domain::viewer::{ViewerDefinition, ViewerStatus};
use crate::notifications::NotificationChannel;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

/// Row in the viewer_snapshots table
#[derive(Debug, Clone)]
pub struct ViewerSnapshotRow {
    pub viewer_id: Uuid,
    pub revision: i64,
    pub last_cursor_json: Value,
    pub status: ViewerStatus,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

/// Store responsible for PostgreSQL access
#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

const CREATE_VIEWER_DEFINITIONS_SQL: &str = "
CREATE TABLE IF NOT EXISTS viewer_definitions (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    refresh_interval_ms INTEGER NOT NULL,
    lookback_ms BIGINT NOT NULL,
    signal_mask INTEGER NOT NULL,
    definition_json JSONB NOT NULL DEFAULT '{}',
    layout_json JSONB NOT NULL DEFAULT '{}',
    revision BIGINT NOT NULL,
    enabled BOOLEAN NOT NULL
)";

const CREATE_VIEWER_SNAPSHOTS_SQL: &str = "
CREATE TABLE IF NOT EXISTS viewer_snapshots (
    viewer_id UUID PRIMARY KEY,
    revision BIGINT NOT NULL,
    last_cursor_json JSONB NOT NULL DEFAULT '{}',
    status_json JSONB NOT NULL DEFAULT '{\"type\":\"ok\"}',
    generated_at TIMESTAMPTZ NOT NULL
)";

const CREATE_DASHBOARD_DEFINITIONS_SQL: &str = "
CREATE TABLE IF NOT EXISTS dashboard_definitions (
    id UUID PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    layout_json JSONB NOT NULL DEFAULT '{}',
    revision BIGINT NOT NULL,
    enabled BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)";

const CREATE_INCIDENTS_SQL: &str = "
CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY,
    alert_id UUID,
    status TEXT NOT NULL DEFAULT 'open',
    severity TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
)";

const CREATE_INCIDENTS_STATUS_IDX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS incidents_status_idx ON incidents (status)";

const CREATE_NOTIFICATION_CHANNELS_SQL: &str = "
CREATE TABLE IF NOT EXISTS notification_channels (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    config_json JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE
)";

impl PostgresStore {
    pub async fn new(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    /// Returns the underlying connection pool, used to construct sibling stores.
    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }

    /// For startup bootstrap: creates the required schema
    pub async fn create_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::query(CREATE_VIEWER_DEFINITIONS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_VIEWER_SNAPSHOTS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_DASHBOARD_DEFINITIONS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(crate::storage::alert_store::CREATE_ALERT_DEFINITIONS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(crate::storage::attr_index::CREATE_ATTRIBUTE_INDEX_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(crate::storage::attr_index::CREATE_ATTRIBUTE_INDEX_OBSERVED_AT_IDX_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(crate::storage::attr_index::CREATE_ATTRIBUTE_INDEX_KEY_IDX_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_INCIDENTS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_INCIDENTS_STATUS_IDX_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_NOTIFICATION_CHANNELS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(crate::storage::slo_store::CREATE_SLO_DEFINITIONS_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// For testing: inserts a record into viewer_definitions
    pub async fn insert_viewer_definition(
        &self,
        def: &ViewerDefinition,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO viewer_definitions
             (id, slug, name, refresh_interval_ms, lookback_ms, signal_mask,
              definition_json, layout_json, revision, enabled)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(def.id)
        .bind(&def.slug)
        .bind(&def.name)
        .bind(def.refresh_interval_ms as i32)
        .bind(def.lookback_ms)
        .bind(def.signal_mask.raw() as i32)
        .bind(&def.definition_json)
        .bind(&def.layout_json)
        .bind(def.revision)
        .bind(def.enabled)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fetches all enabled viewer definitions
    pub async fn load_viewer_definitions(&self) -> Result<Vec<ViewerDefinition>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, slug, name, refresh_interval_ms, lookback_ms, signal_mask,
                    definition_json, layout_json, revision, enabled
             FROM viewer_definitions
             WHERE enabled = true",
        )
        .fetch_all(&self.pool)
        .await?;

        let defs = rows
            .into_iter()
            .map(|row| {
                use sqlx::Row;
                ViewerDefinition {
                    id: row.get("id"),
                    slug: row.get("slug"),
                    name: row.get("name"),
                    refresh_interval_ms: row.get::<i32, _>("refresh_interval_ms") as u32,
                    lookback_ms: row.get("lookback_ms"),
                    signal_mask: SignalMask::from_raw(row.get::<i32, _>("signal_mask") as u32),
                    definition_json: row.get("definition_json"),
                    layout_json: row.get("layout_json"),
                    revision: row.get("revision"),
                    enabled: row.get("enabled"),
                }
            })
            .collect();

        Ok(defs)
    }

    /// Bulk-fetches multiple viewer snapshots. viewer_ids that do not exist are excluded from the result.
    pub async fn load_snapshots(
        &self,
        viewer_ids: &[Uuid],
    ) -> Result<Vec<ViewerSnapshotRow>, sqlx::Error> {
        if viewer_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            "SELECT viewer_id, revision, last_cursor_json,
                    status_json, generated_at
             FROM viewer_snapshots
             WHERE viewer_id = ANY($1)",
        )
        .bind(viewer_ids)
        .fetch_all(&self.pool)
        .await?;

        use sqlx::Row;
        let snapshots = rows
            .into_iter()
            .map(|row| {
                let viewer_id: Uuid = row.get("viewer_id");
                let status_json: Value = row.get("status_json");
                let status: ViewerStatus =
                    serde_json::from_value(status_json).unwrap_or_else(|e| {
                        tracing::warn!(
                            "viewer {viewer_id}: failed to parse status_json from DB: {e}"
                        );
                        ViewerStatus::Degraded {
                            reason: "invalid status in DB".to_string(),
                        }
                    });
                ViewerSnapshotRow {
                    viewer_id,
                    revision: row.get("revision"),
                    last_cursor_json: row.get("last_cursor_json"),
                    status,
                    generated_at: row.get("generated_at"),
                }
            })
            .collect();

        Ok(snapshots)
    }

    /// Updates the viewer's definition_json / layout_json (also increments revision by +1)
    pub async fn update_viewer_definition_json(
        &self,
        id: Uuid,
        definition_json: &Value,
        layout_json: &Value,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE viewer_definitions
             SET definition_json = $1,
                 layout_json = $2,
                 revision = revision + 1
             WHERE id = $3",
        )
        .bind(definition_json)
        .bind(layout_json)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Upserts a viewer snapshot
    pub async fn upsert_snapshot(&self, snapshot: &ViewerSnapshotRow) -> Result<(), sqlx::Error> {
        let status_json = serde_json::to_value(&snapshot.status)
            .expect("ViewerStatus serialization should never fail");

        sqlx::query(
            "INSERT INTO viewer_snapshots
             (viewer_id, revision, last_cursor_json,
              status_json, generated_at)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (viewer_id) DO UPDATE SET
               revision         = EXCLUDED.revision,
               last_cursor_json = EXCLUDED.last_cursor_json,
               status_json      = EXCLUDED.status_json,
               generated_at     = EXCLUDED.generated_at",
        )
        .bind(snapshot.viewer_id)
        .bind(snapshot.revision)
        .bind(&snapshot.last_cursor_json)
        .bind(status_json)
        .bind(snapshot.generated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // --- Dashboard CRUD ------------------------------------------------------

    /// Inserts a record into dashboard_definitions
    pub async fn insert_dashboard(
        &self,
        dashboard: &DashboardDefinition,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO dashboard_definitions
             (id, slug, name, layout_json, revision, enabled)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(dashboard.id)
        .bind(&dashboard.slug)
        .bind(&dashboard.name)
        .bind(&dashboard.layout_json)
        .bind(dashboard.revision)
        .bind(dashboard.enabled)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Fetches all enabled dashboard definitions
    pub async fn load_dashboards(&self) -> Result<Vec<DashboardDefinition>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, slug, name, layout_json, revision, enabled
             FROM dashboard_definitions
             WHERE enabled = true
             ORDER BY created_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        use sqlx::Row;
        Ok(rows
            .into_iter()
            .map(|row| DashboardDefinition {
                id: row.get("id"),
                slug: row.get("slug"),
                name: row.get("name"),
                layout_json: row.get("layout_json"),
                revision: row.get("revision"),
                enabled: row.get("enabled"),
            })
            .collect())
    }

    /// Fetches a dashboard by ID
    pub async fn load_dashboard(
        &self,
        id: Uuid,
    ) -> Result<Option<DashboardDefinition>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, slug, name, layout_json, revision, enabled
             FROM dashboard_definitions
             WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        use sqlx::Row;
        Ok(row.map(|row| DashboardDefinition {
            id: row.get("id"),
            slug: row.get("slug"),
            name: row.get("name"),
            layout_json: row.get("layout_json"),
            revision: row.get("revision"),
            enabled: row.get("enabled"),
        }))
    }

    /// Updates the dashboard's name and layout_json (also increments revision by +1)
    pub async fn update_dashboard(
        &self,
        id: Uuid,
        name: &str,
        layout_json: &Value,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE dashboard_definitions
             SET name = $1,
                 layout_json = $2,
                 revision = revision + 1,
                 updated_at = NOW()
             WHERE id = $3",
        )
        .bind(name)
        .bind(layout_json)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Deletes a viewer and its snapshots
    pub async fn delete_viewer(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM viewer_snapshots WHERE viewer_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        let result = sqlx::query("DELETE FROM viewer_definitions WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(result.rows_affected() > 0)
    }

    /// Deletes a dashboard
    pub async fn delete_dashboard(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM dashboard_definitions WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(result.rows_affected() > 0)
    }

    // --- Incident CRUD -------------------------------------------------------

    /// Inserts an incident row.
    pub async fn insert_incident(&self, incident: &Incident) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO incidents
             (id, alert_id, status, severity, opened_at,
              acknowledged_at, resolved_at, details_json)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(incident.id)
        .bind(incident.alert_id)
        .bind(incident.status.as_str())
        .bind(&incident.severity)
        .bind(incident.opened_at)
        .bind(incident.acknowledged_at)
        .bind(incident.resolved_at)
        .bind(&incident.details_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lists incidents, optionally filtered by status.
    pub async fn list_incidents(
        &self,
        status: Option<IncidentStatus>,
    ) -> Result<Vec<Incident>, sqlx::Error> {
        let rows = if let Some(s) = status {
            sqlx::query(&format!(
                "SELECT {INCIDENT_COLUMNS} FROM incidents WHERE status = $1 ORDER BY opened_at DESC"
            ))
            .bind(s.as_str())
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query(&format!(
                "SELECT {INCIDENT_COLUMNS} FROM incidents ORDER BY opened_at DESC"
            ))
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows.iter().filter_map(incident_from_row).collect())
    }

    /// Fetches a single incident by id.
    pub async fn get_incident(&self, id: Uuid) -> Result<Option<Incident>, sqlx::Error> {
        let row = sqlx::query(&format!(
            "SELECT {INCIDENT_COLUMNS} FROM incidents WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.as_ref().and_then(incident_from_row))
    }

    /// Updates an incident's status, setting the appropriate timestamp column.
    /// Returns the updated row, or `None` if the incident does not exist.
    ///
    /// The `validate_transition` helper guarantees the API never asks us to
    /// transition *to* `Open`, so we only have two SQL variants here.
    pub async fn update_incident_status(
        &self,
        id: Uuid,
        new_status: IncidentStatus,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Incident>, sqlx::Error> {
        // COALESCE keeps the original timestamp on idempotent transitions.
        let timestamp_column = match new_status {
            IncidentStatus::Acknowledged => "acknowledged_at",
            IncidentStatus::Resolved => "resolved_at",
            IncidentStatus::Open => {
                // Forward-only transitions never target Open; the API rejects
                // these before reaching this method.
                return self.get_incident(id).await;
            }
        };
        let sql = format!(
            "UPDATE incidents
             SET status = $1, {timestamp_column} = COALESCE({timestamp_column}, $2)
             WHERE id = $3
             RETURNING {INCIDENT_COLUMNS}"
        );
        let row = sqlx::query(&sql)
            .bind(new_status.as_str())
            .bind(now)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.as_ref().and_then(incident_from_row))
    }
}

const INCIDENT_COLUMNS: &str =
    "id, alert_id, status, severity, opened_at, acknowledged_at, resolved_at, details_json";

fn incident_from_row(row: &sqlx::postgres::PgRow) -> Option<Incident> {
    use sqlx::Row;
    let id: Uuid = row.get("id");
    let status_raw: String = row.get("status");
    let Some(status) = IncidentStatus::parse(&status_raw) else {
        tracing::warn!("incident {id}: unknown status `{status_raw}` -- skipping");
        return None;
    };
    Some(Incident {
        id,
        alert_id: row.get("alert_id"),
        status,
        severity: row.get("severity"),
        opened_at: row.get("opened_at"),
        acknowledged_at: row.get("acknowledged_at"),
        resolved_at: row.get("resolved_at"),
        details_json: row.get("details_json"),
    })
}

impl PostgresStore {
    // --- Notification channel CRUD ------------------------------------------

    pub async fn insert_notification_channel(
        &self,
        channel: &NotificationChannel,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO notification_channels (id, name, kind, config_json, enabled)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(channel.id)
        .bind(&channel.name)
        .bind(&channel.kind)
        .bind(&channel.config_json)
        .bind(channel.enabled)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_notification_channels(
        &self,
    ) -> Result<Vec<NotificationChannel>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, name, kind, config_json, enabled FROM notification_channels ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;

        use sqlx::Row;
        Ok(rows
            .into_iter()
            .map(|row| NotificationChannel {
                id: row.get("id"),
                name: row.get("name"),
                kind: row.get("kind"),
                config_json: row.get("config_json"),
                enabled: row.get("enabled"),
            })
            .collect())
    }

    pub async fn load_notification_channel(
        &self,
        id: Uuid,
    ) -> Result<Option<NotificationChannel>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, name, kind, config_json, enabled FROM notification_channels WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        use sqlx::Row;
        Ok(row.map(|row| NotificationChannel {
            id: row.get("id"),
            name: row.get("name"),
            kind: row.get("kind"),
            config_json: row.get("config_json"),
            enabled: row.get("enabled"),
        }))
    }

    pub async fn delete_notification_channel(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM notification_channels WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }
}
