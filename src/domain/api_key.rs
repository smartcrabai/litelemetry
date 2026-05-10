use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub fn hash_api_key(raw_key: &str) -> String {
    hex::encode(Sha256::digest(raw_key.as_bytes()))
}

#[derive(Debug, Clone)]
pub struct ApiKey {
    pub id: Uuid,
    pub name: String,
    pub key_hash: String,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_api_key_produces_consistent_hash() {
        let hash1 = hash_api_key("lt_mysecretkey");
        let hash2 = hash_api_key("lt_mysecretkey");
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64);
    }

    #[test]
    fn hash_api_key_produces_different_hash_for_different_input() {
        let hash1 = hash_api_key("lt_key1");
        let hash2 = hash_api_key("lt_key2");
        assert_ne!(hash1, hash2);
    }
}
