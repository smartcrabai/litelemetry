use crate::domain::telemetry::SignalMask;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Domain type corresponding to a row in the viewer_definitions table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewerDefinition {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    /// Auto-refresh interval for the viewer (milliseconds)
    pub refresh_interval_ms: u32,
    /// Lookback period referenced by the viewer (milliseconds)
    pub lookback_ms: i64,
    /// Bitmask representing the target signals
    pub signal_mask: SignalMask,
    pub definition_json: serde_json::Value,
    pub layout_json: serde_json::Value,
    pub revision: i64,
    pub enabled: bool,
}

/// Enum representing the current status of a viewer
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ViewerStatus {
    Ok,
    Degraded { reason: String },
}
