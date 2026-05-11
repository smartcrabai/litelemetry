use axum::{
    extract::{Request, State},
    http::{HeaderMap, Method, StatusCode, header::AUTHORIZATION},
    middleware::Next,
    response::Response,
};

use crate::domain::api_key::hash_api_key;
use crate::server::AppState;

pub async fn api_key_auth_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if !state.api_keys_enabled {
        return Ok(next.run(request).await);
    }

    let is_api_keys_create =
        request.method() == Method::POST && request.uri().path() == "/api/api-keys";

    if is_api_keys_create {
        let store = state.require_api_key_store()?;
        let count = store.count().await.map_err(|e| {
            tracing::error!("api key count failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        if count == 0 {
            return Ok(next.run(request).await);
        }
    }

    let raw_key = extract_bearer_token(request.headers()).ok_or(StatusCode::UNAUTHORIZED)?;
    let hash = hash_api_key(raw_key);

    let store = state.require_api_key_store()?;

    store
        .find_by_hash(&hash)
        .await
        .map_err(|e| {
            tracing::error!("api key lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::UNAUTHORIZED)?;

    Ok(next.run(request).await)
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let token = headers
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    if token.is_empty() {
        return None;
    }
    Some(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    // --- extract_bearer_token ---

    #[test]
    fn extract_bearer_token_with_valid_lt_key() {
        // Given: an Authorization header with a valid "lt_..." Bearer token
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_static("Bearer lt_abc1234567890"),
        );

        // When: extracting the bearer token
        let token = extract_bearer_token(&headers);

        // Then: the raw key (without the "Bearer " prefix) is returned
        assert_eq!(token, Some("lt_abc1234567890"));
    }

    #[test]
    fn extract_bearer_token_with_no_authorization_header() {
        // Given: headers that contain no Authorization field
        let headers = HeaderMap::new();

        // When: extracting
        let token = extract_bearer_token(&headers);

        // Then: None is returned
        assert!(token.is_none());
    }

    #[test]
    fn extract_bearer_token_with_basic_scheme_returns_none() {
        // Given: an Authorization header using Basic auth instead of Bearer
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_static("Basic dXNlcjpwYXNz"),
        );

        // When: extracting
        let token = extract_bearer_token(&headers);

        // Then: None is returned because the prefix does not match
        assert!(token.is_none());
    }

    #[test]
    fn extract_bearer_token_with_bearer_prefix_only_returns_none() {
        // Given: "Bearer " with nothing after it (empty token)
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", HeaderValue::from_static("Bearer "));

        // When: extracting
        let token = extract_bearer_token(&headers);

        // Then: None is returned — empty tokens are rejected to avoid spurious DB lookups
        assert!(token.is_none());
    }

    #[test]
    fn extract_bearer_token_is_case_sensitive_for_scheme() {
        // Given: lowercase "bearer" instead of "Bearer"
        let mut headers = HeaderMap::new();
        headers.insert("Authorization", HeaderValue::from_static("bearer lt_mykey"));

        // When: extracting
        let token = extract_bearer_token(&headers);

        // Then: None is returned because HTTP Bearer scheme is case-sensitive here
        assert!(token.is_none());
    }

    #[test]
    fn extract_bearer_token_with_extra_spaces_in_value() {
        // Given: a token value that contains spaces (edge case)
        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_static("Bearer lt_key with spaces"),
        );

        // When: extracting
        let token = extract_bearer_token(&headers);

        // Then: the entire remainder after "Bearer " is returned as-is
        assert_eq!(token, Some("lt_key with spaces"));
    }
}
