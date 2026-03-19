use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

/// Domain type corresponding to a row in the dashboard_definitions table
///
/// Structure of layout_json:
/// ```json
/// {
///   "columns": 2,
///   "panels": [
///     { "viewer_id": "uuid-1", "position": 0 },
///     { "viewer_id": "uuid-2", "position": 1 }
///   ]
/// }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardDefinition {
    pub id: Uuid,
    pub slug: String,
    pub name: String,
    pub layout_json: Value,
    pub revision: i64,
    pub enabled: bool,
}

pub fn build_layout_json(viewer_ids: &[Uuid], columns: u32) -> Value {
    let panels: Vec<Value> = viewer_ids
        .iter()
        .enumerate()
        .map(|(i, id)| json!({ "viewer_id": id, "position": i }))
        .collect();
    json!({ "columns": columns, "panels": panels })
}
