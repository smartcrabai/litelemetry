//! Webhook implementation of [`NotificationDispatcher`].

use std::time::Duration;

use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Value;

use crate::notifications::NotificationChannel;
use crate::notifications::dispatcher::{
    NotificationDispatcher, NotificationError, NotificationPayload,
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Configuration extracted from `NotificationChannel.config_json` for the
/// `"webhook"` kind.
#[derive(Debug, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    #[serde(default)]
    pub headers: Option<Value>,
}

impl WebhookConfig {
    pub fn from_channel(channel: &NotificationChannel) -> Result<Self, NotificationError> {
        let cfg: Self = serde_json::from_value(channel.config_json.clone())
            .map_err(|e| NotificationError::InvalidConfig(format!("parse config_json: {e}")))?;
        if cfg.url.trim().is_empty() {
            return Err(NotificationError::InvalidConfig(
                "webhook url is empty".to_string(),
            ));
        }
        if !(cfg.url.starts_with("http://") || cfg.url.starts_with("https://")) {
            return Err(NotificationError::InvalidConfig(
                "webhook url must start with http:// or https://".to_string(),
            ));
        }
        Ok(cfg)
    }

    /// Build a `reqwest::HeaderMap` from the optional `headers` field.
    pub fn build_headers(&self) -> Result<HeaderMap, NotificationError> {
        let mut headers = HeaderMap::new();
        let Some(value) = &self.headers else {
            return Ok(headers);
        };
        let map = value.as_object().ok_or_else(|| {
            NotificationError::InvalidConfig("headers must be a JSON object".to_string())
        })?;
        for (k, v) in map {
            let v_str = v.as_str().ok_or_else(|| {
                NotificationError::InvalidConfig(format!("header '{k}' must have a string value"))
            })?;
            let name = HeaderName::try_from(k.as_str()).map_err(|e| {
                NotificationError::InvalidConfig(format!("invalid header name '{k}': {e}"))
            })?;
            let value = HeaderValue::try_from(v_str).map_err(|e| {
                NotificationError::InvalidConfig(format!("invalid header value for '{k}': {e}"))
            })?;
            headers.insert(name, value);
        }
        Ok(headers)
    }
}

/// HTTP POST dispatcher backed by a shared `reqwest::Client`.
pub struct WebhookDispatcher {
    client: Client,
}

impl WebhookDispatcher {
    pub fn new() -> Result<Self, reqwest::Error> {
        let client = Client::builder().timeout(DEFAULT_TIMEOUT).build()?;
        Ok(Self { client })
    }
}

impl NotificationDispatcher for WebhookDispatcher {
    fn kind(&self) -> &'static str {
        "webhook"
    }

    async fn dispatch(
        &self,
        channel: &NotificationChannel,
        payload: &NotificationPayload,
    ) -> Result<(), NotificationError> {
        let cfg = WebhookConfig::from_channel(channel)?;
        let headers = cfg.build_headers()?;

        let response = self
            .client
            .post(&cfg.url)
            .headers(headers)
            .json(payload)
            .send()
            .await
            .map_err(|e| NotificationError::Transport(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(NotificationError::NonSuccessStatus(status.as_u16()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn webhook_config_rejects_missing_scheme() {
        let ch = NotificationChannel {
            id: Uuid::new_v4(),
            name: "x".to_string(),
            kind: "webhook".to_string(),
            config_json: serde_json::json!({ "url": "example.com" }),
            enabled: true,
        };
        let err = WebhookConfig::from_channel(&ch).unwrap_err();
        assert!(matches!(err, NotificationError::InvalidConfig(_)));
    }

    #[test]
    fn webhook_config_rejects_empty_url() {
        let ch = NotificationChannel {
            id: Uuid::new_v4(),
            name: "x".to_string(),
            kind: "webhook".to_string(),
            config_json: serde_json::json!({ "url": "" }),
            enabled: true,
        };
        let err = WebhookConfig::from_channel(&ch).unwrap_err();
        assert!(matches!(err, NotificationError::InvalidConfig(_)));
    }

    #[test]
    fn webhook_config_parses_headers_object() {
        let ch = NotificationChannel {
            id: Uuid::new_v4(),
            name: "x".to_string(),
            kind: "webhook".to_string(),
            config_json: serde_json::json!({
                "url": "https://example.com/hook",
                "headers": { "x-token": "abc" }
            }),
            enabled: true,
        };
        let cfg = WebhookConfig::from_channel(&ch).unwrap();
        let headers = cfg.build_headers().unwrap();
        assert_eq!(headers.get("x-token").unwrap(), "abc");
    }

    #[test]
    fn webhook_config_rejects_non_object_headers() {
        let ch = NotificationChannel {
            id: Uuid::new_v4(),
            name: "x".to_string(),
            kind: "webhook".to_string(),
            config_json: serde_json::json!({
                "url": "https://example.com/hook",
                "headers": "not-an-object"
            }),
            enabled: true,
        };
        let cfg = WebhookConfig::from_channel(&ch).unwrap();
        let err = cfg.build_headers().unwrap_err();
        assert!(matches!(err, NotificationError::InvalidConfig(_)));
    }
}
