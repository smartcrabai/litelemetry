use crate::domain::telemetry::SignalMask;
use crate::domain::viewer::{ViewerDefinition, ViewerStatus};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

/// viewer_snapshots テーブルの行
#[derive(Debug, Clone)]
pub struct ViewerSnapshotRow {
    pub viewer_id: Uuid,
    pub revision: i64,
    pub last_cursor_json: Value,
    pub status: ViewerStatus,
    pub generated_at: chrono::DateTime<chrono::Utc>,
}

/// PostgreSQL へのアクセスを担うストア
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

impl PostgresStore {
    pub async fn new(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    /// 起動時 bootstrap 用: 必要なスキーマを作成する
    pub async fn create_schema(&self) -> Result<(), sqlx::Error> {
        sqlx::query(CREATE_VIEWER_DEFINITIONS_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_VIEWER_SNAPSHOTS_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// テスト用: viewer_definitions にレコードを挿入する
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

    /// 有効な viewer 定義を全件取得する
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

    /// 複数の viewer snapshot を一括取得する。存在しない viewer_id は結果に含まれない。
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

    /// viewer の definition_json / layout_json を更新する (revision も +1)
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

    /// viewer の snapshot を upsert する
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
}
