//! Notification dispatch subsystem.
//!
//! Provides:
//! - [`NotificationChannel`] domain type representing a configured destination
//!   (e.g. webhook URL).
//! - [`NotificationDispatcher`] trait — abstract interface for delivering an
//!   alert payload to a channel. Other units (alert evaluator, incident
//!   manager) integrate with this trait without knowing the concrete impl.
//! - [`webhook::WebhookDispatcher`] — HTTP POST implementation backed by
//!   `reqwest`.
//! - [`NotificationStore`] — CRUD over the `notification_channels` table.

pub mod dispatcher;
pub mod webhook;

pub use dispatcher::{
    NotificationDispatcher, NotificationError, NotificationPayload, dispatch_to_channel,
};
pub use webhook::WebhookDispatcher;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A configured notification destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationChannel {
    pub id: Uuid,
    pub name: String,
    pub kind: String,
    pub config_json: Value,
    pub enabled: bool,
}

impl NotificationChannel {
    /// Convenience constructor for a webhook channel.
    pub fn webhook(id: Uuid, name: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            kind: "webhook".to_string(),
            config_json: serde_json::json!({ "url": url.into() }),
            enabled: true,
        }
    }
}
