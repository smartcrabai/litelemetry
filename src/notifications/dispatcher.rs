//! `NotificationDispatcher` trait and shared payload type.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::notifications::NotificationChannel;
use crate::notifications::webhook::WebhookDispatcher;

/// Payload delivered to a notification channel.
///
/// Designed to be small and self-describing so other units (U6 alert evaluator,
/// U8 incident manager) can populate it without coupling to dispatcher internals.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPayload {
    /// Short subject line (e.g. "Alert: high error rate").
    pub title: String,
    /// Free-form human-readable detail.
    pub message: String,
    /// One of `info`, `warning`, `critical`. Free-form: dispatchers should not
    /// reject unknown values.
    pub severity: String,
    /// Arbitrary structured fields (e.g. alert id, incident id, query, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl NotificationPayload {
    /// Build a minimal test payload used by `/api/notification-channels/{id}/test`.
    pub fn test_message(channel_name: &str) -> Self {
        Self {
            title: "litelemetry test notification".to_string(),
            message: format!("This is a test notification for channel '{channel_name}'."),
            severity: "info".to_string(),
            data: Some(serde_json::json!({ "test": true })),
        }
    }
}

/// Errors raised by a [`NotificationDispatcher`].
#[derive(Debug, thiserror::Error)]
pub enum NotificationError {
    #[error("unsupported channel kind: {0}")]
    UnsupportedKind(String),
    #[error("invalid channel config: {0}")]
    InvalidConfig(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("remote returned non-success status: {0}")]
    NonSuccessStatus(u16),
}

/// Abstract notification sink.
///
/// Implementations must be `Send + Sync` so they can be shared across tasks.
/// Uses native async fn in trait (Rust edition 2024).
pub trait NotificationDispatcher: Send + Sync {
    /// Returns the channel kind this dispatcher handles (e.g. `"webhook"`).
    fn kind(&self) -> &'static str;

    /// Delivers `payload` to `channel`.
    fn dispatch(
        &self,
        channel: &NotificationChannel,
        payload: &NotificationPayload,
    ) -> impl std::future::Future<Output = Result<(), NotificationError>> + Send;
}

/// Convenience: dispatches `payload` to `channel`, picking a built-in
/// implementation based on `channel.kind`.
///
/// Currently supports the `"webhook"` kind. When U6/U8 integrate, they should
/// call this helper rather than re-implementing the dispatch decision. Callers
/// that respect the channel's `enabled` flag should check it before calling.
pub async fn dispatch_to_channel(
    channel: &NotificationChannel,
    payload: &NotificationPayload,
) -> Result<(), NotificationError> {
    match channel.kind.as_str() {
        "webhook" => {
            let dispatcher = WebhookDispatcher::new()
                .map_err(|e| NotificationError::Transport(e.to_string()))?;
            dispatcher.dispatch(channel, payload).await
        }
        other => Err(NotificationError::UnsupportedKind(other.to_string())),
    }
}
