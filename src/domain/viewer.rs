use crate::domain::telemetry::SignalMask;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// viewer_definitions テーブルの行に対応するドメイン型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewerDefinition {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    /// viewer を自動更新する間隔 (ミリ秒)
    pub refresh_interval_ms: u32,
    /// viewer が参照するルックバック期間 (ミリ秒)
    pub lookback_ms: i64,
    /// 対象シグナルを表すビットマスク
    pub signal_mask: SignalMask,
    pub definition_json: serde_json::Value,
    pub layout_json: serde_json::Value,
    pub revision: i64,
    pub enabled: bool,
}

/// viewer の現在状態を表す enum
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ViewerStatus {
    Ok,
    Degraded { reason: String },
}
