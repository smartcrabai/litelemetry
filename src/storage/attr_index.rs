//! Attribute inverted index (Postgres-backed).
//!
//! Allows fast attribute-value autocomplete and reverse lookup ("which streams
//! contain `http.route=/checkout`?") even when attribute values are
//! high-cardinality.
//!
//! Standalone mode (no `DATABASE_URL`) does not construct this store, so all
//! call sites must gate on `Option<AttrIndexStore>` and treat `None` as a
//! no-op.

use crate::domain::telemetry::Signal;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use thiserror::Error;

/// DDL for the inverted index. Kept in sync with
/// `docker/postgres/initdb/001-init.sql`.
pub const CREATE_ATTRIBUTE_INDEX_SQL: &str = "
CREATE TABLE IF NOT EXISTS attribute_index (
    attribute_key TEXT NOT NULL,
    attribute_value TEXT NOT NULL,
    signal SMALLINT NOT NULL,
    stream_id TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (attribute_key, attribute_value, stream_id)
)";

pub const CREATE_ATTRIBUTE_INDEX_OBSERVED_AT_IDX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS attribute_index_observed_at_idx ON attribute_index (observed_at)";

pub const CREATE_ATTRIBUTE_INDEX_KEY_IDX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS attribute_index_key_idx ON attribute_index (attribute_key)";

/// Maximum values returned by [`AttrIndexStore::list_values`].
pub const MAX_VALUE_SUGGESTIONS: i64 = 50;

#[derive(Debug, Error)]
pub enum AttrIndexError {
    #[error("postgres: {0}")]
    Postgres(#[from] sqlx::Error),
}

/// Postgres-backed inverted index over OTLP attributes.
#[derive(Clone)]
pub struct AttrIndexStore {
    pool: PgPool,
}

impl AttrIndexStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Bootstrap the schema. Idempotent (uses `CREATE TABLE IF NOT EXISTS`).
    pub async fn create_schema(&self) -> Result<(), AttrIndexError> {
        sqlx::query(CREATE_ATTRIBUTE_INDEX_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_ATTRIBUTE_INDEX_OBSERVED_AT_IDX_SQL)
            .execute(&self.pool)
            .await?;
        sqlx::query(CREATE_ATTRIBUTE_INDEX_KEY_IDX_SQL)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Insert a batch of `(key, value)` attribute pairs for a single ingested
    /// entry.
    ///
    /// Conflicts on the (key, value, stream_id) primary key are ignored, so
    /// re-ingesting the same stream entry is safe. Takes ownership of the
    /// pairs to avoid copying them into the bind buffers.
    pub async fn record_attributes(
        &self,
        signal: Signal,
        stream_id: &str,
        observed_at: DateTime<Utc>,
        attributes: Vec<(String, String)>,
    ) -> Result<(), AttrIndexError> {
        if attributes.is_empty() {
            return Ok(());
        }

        let signal_byte = signal_as_i16(signal);
        let mut keys = Vec::with_capacity(attributes.len());
        let mut values = Vec::with_capacity(attributes.len());
        for (k, v) in attributes {
            keys.push(k);
            values.push(v);
        }

        // Single round-trip insert via UNNEST; ON CONFLICT DO NOTHING keeps it
        // idempotent for retries / replay.
        sqlx::query(
            "INSERT INTO attribute_index
                 (attribute_key, attribute_value, signal, stream_id, observed_at)
             SELECT k, v, $3::SMALLINT, $4, $5
             FROM UNNEST($1::TEXT[], $2::TEXT[]) AS t(k, v)
             ON CONFLICT (attribute_key, attribute_value, stream_id) DO NOTHING",
        )
        .bind(&keys)
        .bind(&values)
        .bind(signal_byte)
        .bind(stream_id)
        .bind(observed_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Return all distinct attribute keys that have been indexed, sorted.
    pub async fn list_keys(&self) -> Result<Vec<String>, AttrIndexError> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT attribute_key FROM attribute_index ORDER BY attribute_key ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(k,)| k).collect())
    }

    /// Return up to [`MAX_VALUE_SUGGESTIONS`] distinct values for a key,
    /// optionally filtered by case-insensitive prefix.
    pub async fn list_values(
        &self,
        key: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, AttrIndexError> {
        let rows: Vec<(String,)> = match prefix {
            Some(p) if !p.is_empty() => {
                let pattern = format!("{}%", escape_like(p));
                sqlx::query_as(
                    "SELECT DISTINCT attribute_value
                     FROM attribute_index
                     WHERE attribute_key = $1 AND attribute_value ILIKE $2 ESCAPE '\\'
                     ORDER BY attribute_value ASC
                     LIMIT $3",
                )
                .bind(key)
                .bind(pattern)
                .bind(MAX_VALUE_SUGGESTIONS)
                .fetch_all(&self.pool)
                .await?
            }
            _ => {
                sqlx::query_as(
                    "SELECT DISTINCT attribute_value
                     FROM attribute_index
                     WHERE attribute_key = $1
                     ORDER BY attribute_value ASC
                     LIMIT $2",
                )
                .bind(key)
                .bind(MAX_VALUE_SUGGESTIONS)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows.into_iter().map(|(v,)| v).collect())
    }
}

fn signal_as_i16(signal: Signal) -> i16 {
    match signal {
        Signal::Traces => 1,
        Signal::Metrics => 2,
        Signal::Logs => 3,
    }
}

/// Escape `%`, `_`, and `\` for safe use in `ILIKE ... ESCAPE '\'`.
fn escape_like(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '\\' | '%' | '_' => {
                out.push('\\');
                out.push(c);
            }
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_as_i16_distinct_values() {
        assert_eq!(signal_as_i16(Signal::Traces), 1);
        assert_eq!(signal_as_i16(Signal::Metrics), 2);
        assert_eq!(signal_as_i16(Signal::Logs), 3);
    }

    #[test]
    fn escape_like_escapes_special_chars() {
        assert_eq!(escape_like("foo"), "foo");
        assert_eq!(escape_like("100%"), "100\\%");
        assert_eq!(escape_like("a_b"), "a\\_b");
        assert_eq!(escape_like("c:\\d"), "c:\\\\d");
    }
}
