use crate::domain::api_key::ApiKey;
use crate::storage::StorageError;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

pub const CREATE_API_KEYS_SQL: &str = "
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)";

pub const CREATE_API_KEYS_HASH_INDEX_SQL: &str =
    "CREATE INDEX IF NOT EXISTS api_keys_hash_idx ON api_keys (key_hash)";

#[derive(Clone)]
pub struct PostgresApiKeyStore {
    pool: PgPool,
}

impl PostgresApiKeyStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_hash(&self, key_hash: &str) -> Result<Option<ApiKey>, sqlx::Error> {
        let row =
            sqlx::query("SELECT id, name, key_hash, created_at FROM api_keys WHERE key_hash = $1")
                .bind(key_hash)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.as_ref().map(row_to_api_key))
    }

    pub async fn insert(&self, key: &ApiKey) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO api_keys (id, name, key_hash, created_at) VALUES ($1, $2, $3, $4)",
        )
        .bind(key.id)
        .bind(&key.name)
        .bind(&key.key_hash)
        .bind(key.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<ApiKey>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, name, key_hash, created_at FROM api_keys ORDER BY created_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_api_key).collect())
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query("DELETE FROM api_keys WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn count(&self) -> Result<u64, sqlx::Error> {
        let row = sqlx::query("SELECT COUNT(*) as cnt FROM api_keys")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<i64, _>("cnt") as u64)
    }
}

fn row_to_api_key(row: &sqlx::postgres::PgRow) -> ApiKey {
    ApiKey {
        id: row.get("id"),
        name: row.get("name"),
        key_hash: row.get("key_hash"),
        created_at: row.get("created_at"),
    }
}

#[derive(Default)]
struct MemoryApiKeyStoreInner {
    by_hash: HashMap<String, ApiKey>,
    by_id: HashMap<Uuid, String>, // id -> key_hash, for O(1) delete
}

#[derive(Clone, Default)]
pub struct MemoryApiKeyStore {
    inner: Arc<RwLock<MemoryApiKeyStoreInner>>,
}

impl MemoryApiKeyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn find_by_hash(&self, key_hash: &str) -> Result<Option<ApiKey>, StorageError> {
        let guard = self.inner.read().await;
        Ok(guard.by_hash.get(key_hash).cloned())
    }

    pub async fn insert(&self, key: &ApiKey) -> Result<(), StorageError> {
        let mut guard = self.inner.write().await;
        if guard.by_hash.contains_key(&key.key_hash) {
            return Err(StorageError::DuplicateKey(key.key_hash.clone()));
        }
        guard.by_id.insert(key.id, key.key_hash.clone());
        guard.by_hash.insert(key.key_hash.clone(), key.clone());
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<ApiKey>, StorageError> {
        let mut keys: Vec<ApiKey> = self.inner.read().await.by_hash.values().cloned().collect();
        keys.sort_by_key(|k| k.created_at);
        Ok(keys)
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, StorageError> {
        let mut guard = self.inner.write().await;
        if let Some(hash) = guard.by_id.remove(&id) {
            guard.by_hash.remove(&hash);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn count(&self) -> Result<u64, StorageError> {
        Ok(self.inner.read().await.by_hash.len() as u64)
    }
}

#[derive(Clone)]
pub enum ApiKeyStore {
    Memory(MemoryApiKeyStore),
    Postgres(PostgresApiKeyStore),
}

impl ApiKeyStore {
    pub async fn find_by_hash(&self, key_hash: &str) -> Result<Option<ApiKey>, StorageError> {
        match self {
            ApiKeyStore::Memory(s) => s.find_by_hash(key_hash).await,
            ApiKeyStore::Postgres(s) => s
                .find_by_hash(key_hash)
                .await
                .map_err(StorageError::Postgres),
        }
    }

    pub async fn insert(&self, key: &ApiKey) -> Result<(), StorageError> {
        match self {
            ApiKeyStore::Memory(s) => s.insert(key).await,
            ApiKeyStore::Postgres(s) => s.insert(key).await.map_err(StorageError::Postgres),
        }
    }

    pub async fn list(&self) -> Result<Vec<ApiKey>, StorageError> {
        match self {
            ApiKeyStore::Memory(s) => s.list().await,
            ApiKeyStore::Postgres(s) => s.list().await.map_err(StorageError::Postgres),
        }
    }

    pub async fn delete(&self, id: Uuid) -> Result<bool, StorageError> {
        match self {
            ApiKeyStore::Memory(s) => s.delete(id).await,
            ApiKeyStore::Postgres(s) => s.delete(id).await.map_err(StorageError::Postgres),
        }
    }

    pub async fn count(&self) -> Result<u64, StorageError> {
        match self {
            ApiKeyStore::Memory(s) => s.count().await,
            ApiKeyStore::Postgres(s) => s.count().await.map_err(StorageError::Postgres),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_key(name: &str, key_hash: &str) -> ApiKey {
        ApiKey {
            id: Uuid::new_v4(),
            name: name.to_string(),
            key_hash: key_hash.to_string(),
            created_at: Utc::now(),
        }
    }

    // --- find_by_hash ---

    #[tokio::test]
    async fn find_by_hash_returns_matching_key() {
        // Given: a store containing a key with hash "abc123"
        let store = MemoryApiKeyStore::new();
        let key = make_key("production", "abc123");
        store.insert(&key).await.unwrap();

        // When: looking up by that hash
        let result = store.find_by_hash("abc123").await.unwrap();

        // Then: the correct key is returned
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, key.id);
    }

    #[tokio::test]
    async fn find_by_hash_returns_none_for_unknown_hash() {
        // Given: an empty store
        let store = MemoryApiKeyStore::new();

        // When: looking up a hash that was never inserted
        let result = store.find_by_hash("does-not-exist").await.unwrap();

        // Then: None is returned
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn find_by_hash_is_case_sensitive() {
        // Given: a store with a lowercase hash
        let store = MemoryApiKeyStore::new();
        store.insert(&make_key("k", "abcdef")).await.unwrap();

        // When: looking up the uppercase variant
        let result = store.find_by_hash("ABCDEF").await.unwrap();

        // Then: no match is found (SHA-256 hex is always lowercase)
        assert!(result.is_none());
    }

    // --- insert ---

    #[tokio::test]
    async fn insert_then_list_shows_key() {
        // Given: an empty store
        let store = MemoryApiKeyStore::new();

        // When: a key is inserted
        let key = make_key("ci-token", "hash1");
        store.insert(&key).await.unwrap();

        // Then: it appears in the list
        let keys = store.list().await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].name, "ci-token");
    }

    #[tokio::test]
    async fn insert_multiple_keys_all_visible() {
        // Given: a store with three keys inserted
        let store = MemoryApiKeyStore::new();
        store.insert(&make_key("alpha", "h1")).await.unwrap();
        store.insert(&make_key("beta", "h2")).await.unwrap();
        store.insert(&make_key("gamma", "h3")).await.unwrap();

        // When: listing
        let keys = store.list().await.unwrap();

        // Then: all three are returned
        assert_eq!(keys.len(), 3);
    }

    // --- delete ---

    #[tokio::test]
    async fn delete_existing_key_returns_true_and_removes_it() {
        // Given: a store containing a key
        let store = MemoryApiKeyStore::new();
        let key = make_key("temp", "hash-temp");
        store.insert(&key).await.unwrap();

        // When: the key is deleted
        let deleted = store.delete(key.id).await.unwrap();

        // Then: delete returns true and the key is no longer findable
        assert!(deleted);
        assert!(store.find_by_hash("hash-temp").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_nonexistent_key_returns_false() {
        // Given: an empty store
        let store = MemoryApiKeyStore::new();

        // When: deleting a key with a random id
        let deleted = store.delete(Uuid::new_v4()).await.unwrap();

        // Then: false is returned
        assert!(!deleted);
    }

    #[tokio::test]
    async fn delete_one_key_leaves_others_intact() {
        // Given: a store with two keys
        let store = MemoryApiKeyStore::new();
        let key_a = make_key("keep", "hash-keep");
        let key_b = make_key("remove", "hash-remove");
        store.insert(&key_a).await.unwrap();
        store.insert(&key_b).await.unwrap();

        // When: only key_b is deleted
        store.delete(key_b.id).await.unwrap();

        // Then: key_a is still present
        let keys = store.list().await.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].id, key_a.id);
    }

    // --- list ---

    #[tokio::test]
    async fn list_empty_store_returns_empty_vec() {
        // Given: an empty store
        let store = MemoryApiKeyStore::new();

        // When: listing
        let keys = store.list().await.unwrap();

        // Then: an empty vec is returned
        assert!(keys.is_empty());
    }
}
