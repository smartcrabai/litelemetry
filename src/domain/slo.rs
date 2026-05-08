use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single filter clause used inside an SLO success/total filter list.
///
/// Mirrors the structure consumed by the viewer compiler's FilterMatcher
/// (`field` ∈ {service_name, payload}, `op` ∈ {eq, contains, regex}).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SloFilterClause {
    pub field: String,
    pub op: String,
    pub value: String,
}

/// A list of filter clauses combined with `filter_mode = and`.
///
/// An empty list means "match all entries observed by the source viewer".
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SloFilterList {
    #[serde(default)]
    pub filters: Vec<SloFilterClause>,
}

/// Domain type corresponding to a row in the slo_definitions table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloDefinition {
    pub id: Uuid,
    pub name: String,
    /// Source viewer that supplies the entry stream evaluated against the
    /// success / total filters. None means no viewer is bound yet (budget = 0/0).
    pub viewer_id: Option<Uuid>,
    /// Target percentage in the [0, 100] range (e.g. 99.5).
    pub target_pct: f64,
    /// Rolling window length in milliseconds.
    pub window_ms: i64,
    pub success_filter: SloFilterList,
    pub total_filter: SloFilterList,
    pub enabled: bool,
}
