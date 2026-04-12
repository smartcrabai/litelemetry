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
///     { "viewer_id": "uuid-1", "position": 0, "col_span": 1, "row_span": 1 },
///     { "viewer_id": "uuid-2", "position": 1, "col_span": 2, "row_span": 1 }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PanelInput {
    pub viewer_id: Uuid,
    pub col_span: u32,
    pub row_span: u32,
}

pub fn panel_inputs_from_viewer_ids(ids: &[Uuid]) -> Vec<PanelInput> {
    ids.iter()
        .map(|id| PanelInput {
            viewer_id: *id,
            col_span: 1,
            row_span: 1,
        })
        .collect()
}

pub fn build_layout_json(panels: &[PanelInput], columns: u32) -> Value {
    let panel_values: Vec<Value> = panels
        .iter()
        .enumerate()
        .map(|(i, p)| {
            json!({
                "viewer_id": p.viewer_id,
                "position": i,
                "col_span": p.col_span,
                "row_span": p.row_span,
            })
        })
        .collect();
    json!({ "columns": columns, "panels": panel_values })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_layout_json_assigns_positions_in_array_order() {
        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        let inputs = panel_inputs_from_viewer_ids(&ids);

        let layout = build_layout_json(&inputs, 2);

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

    #[test]
    fn test_build_layout_json_includes_col_span_and_row_span() {
        let id = Uuid::new_v4();
        let inputs = vec![PanelInput {
            viewer_id: id,
            col_span: 2,
            row_span: 3,
        }];

        let layout = build_layout_json(&inputs, 4);

        let panels = layout["panels"].as_array().unwrap();
        assert_eq!(panels.len(), 1);
        assert_eq!(panels[0]["col_span"].as_u64().unwrap(), 2);
        assert_eq!(panels[0]["row_span"].as_u64().unwrap(), 3);
    }

    #[test]
    fn test_build_layout_json_with_multiple_panels_preserves_spans() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let inputs = vec![
            PanelInput {
                viewer_id: id1,
                col_span: 2,
                row_span: 1,
            },
            PanelInput {
                viewer_id: id2,
                col_span: 1,
                row_span: 2,
            },
        ];

        let layout = build_layout_json(&inputs, 3);

        let panels = layout["panels"].as_array().unwrap();
        assert_eq!(panels.len(), 2);
        assert_eq!(panels[0]["col_span"].as_u64().unwrap(), 2);
        assert_eq!(panels[0]["row_span"].as_u64().unwrap(), 1);
        assert_eq!(panels[1]["col_span"].as_u64().unwrap(), 1);
        assert_eq!(panels[1]["row_span"].as_u64().unwrap(), 2);
    }
}
