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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_layout_json_assigns_positions_in_array_order() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();

        let layout = build_layout_json(&ids, 2);

        let panels = layout["panels"].as_array().unwrap();
        assert_eq!(panels.len(), 3);
        for (i, panel) in panels.iter().enumerate() {
            assert_eq!(
                panel["position"].as_u64().unwrap(),
                i as u64,
                "panels[{i}].position should be {i}"
            );
            assert_eq!(
                panel["viewer_id"].as_str().unwrap(),
                ids[i].to_string(),
                "panels[{i}].viewer_id should match ids[{i}]"
            );
        }
    }

    #[test]
    fn test_build_layout_json_with_empty_viewers_produces_empty_panels() {
        let layout = build_layout_json(&[], 2);

        let panels = layout["panels"].as_array().unwrap();
        assert!(panels.is_empty());
    }

    #[test]
    fn test_build_layout_json_sets_columns_field() {
        let layout = build_layout_json(&[], 3);

        assert_eq!(layout["columns"].as_u64().unwrap(), 3);
    }
}
