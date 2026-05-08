use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

/// Lifecycle status for an incident.
///
/// Allowed transitions:
///   open -> acknowledged -> resolved
///   open -> resolved
///
/// Reverse transitions are forbidden (e.g. resolved -> acknowledged).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus {
    Open,
    Acknowledged,
    Resolved,
}

impl IncidentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            IncidentStatus::Open => "open",
            IncidentStatus::Acknowledged => "acknowledged",
            IncidentStatus::Resolved => "resolved",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(IncidentStatus::Open),
            "acknowledged" => Some(IncidentStatus::Acknowledged),
            "resolved" => Some(IncidentStatus::Resolved),
            _ => None,
        }
    }

    /// Order index for forward-only validation.
    fn rank(self) -> u8 {
        match self {
            IncidentStatus::Open => 0,
            IncidentStatus::Acknowledged => 1,
            IncidentStatus::Resolved => 2,
        }
    }
}

impl fmt::Display for IncidentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IncidentTransitionError {
    #[error("invalid transition: {from} -> {to}")]
    InvalidTransition {
        from: IncidentStatus,
        to: IncidentStatus,
    },
    #[error("incident already in target status: {0}")]
    AlreadyInStatus(IncidentStatus),
}

/// Validates a state transition. Returns the target status on success.
///
/// Rules:
///   * forward-only (rank must strictly increase)
///   * idempotent same-status transitions are rejected (caller can decide)
pub fn validate_transition(
    from: IncidentStatus,
    to: IncidentStatus,
) -> Result<IncidentStatus, IncidentTransitionError> {
    if from == to {
        return Err(IncidentTransitionError::AlreadyInStatus(to));
    }
    if to.rank() <= from.rank() {
        return Err(IncidentTransitionError::InvalidTransition { from, to });
    }
    Ok(to)
}

/// Domain type corresponding to a row in the incidents table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alert_id: Option<Uuid>,
    pub status: IncidentStatus,
    pub severity: String,
    pub opened_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acknowledged_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<DateTime<Utc>>,
    pub details_json: Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_to_acknowledged_is_allowed() {
        assert_eq!(
            validate_transition(IncidentStatus::Open, IncidentStatus::Acknowledged).unwrap(),
            IncidentStatus::Acknowledged
        );
    }

    #[test]
    fn open_to_resolved_is_allowed() {
        assert_eq!(
            validate_transition(IncidentStatus::Open, IncidentStatus::Resolved).unwrap(),
            IncidentStatus::Resolved
        );
    }

    #[test]
    fn acknowledged_to_resolved_is_allowed() {
        assert_eq!(
            validate_transition(IncidentStatus::Acknowledged, IncidentStatus::Resolved).unwrap(),
            IncidentStatus::Resolved
        );
    }

    #[test]
    fn resolved_to_acknowledged_is_rejected() {
        assert_eq!(
            validate_transition(IncidentStatus::Resolved, IncidentStatus::Acknowledged)
                .unwrap_err(),
            IncidentTransitionError::InvalidTransition {
                from: IncidentStatus::Resolved,
                to: IncidentStatus::Acknowledged,
            }
        );
    }

    #[test]
    fn acknowledged_to_open_is_rejected() {
        assert_eq!(
            validate_transition(IncidentStatus::Acknowledged, IncidentStatus::Open).unwrap_err(),
            IncidentTransitionError::InvalidTransition {
                from: IncidentStatus::Acknowledged,
                to: IncidentStatus::Open,
            }
        );
    }

    #[test]
    fn resolved_to_open_is_rejected() {
        assert_eq!(
            validate_transition(IncidentStatus::Resolved, IncidentStatus::Open).unwrap_err(),
            IncidentTransitionError::InvalidTransition {
                from: IncidentStatus::Resolved,
                to: IncidentStatus::Open,
            }
        );
    }

    #[test]
    fn same_status_is_rejected() {
        assert_eq!(
            validate_transition(IncidentStatus::Open, IncidentStatus::Open).unwrap_err(),
            IncidentTransitionError::AlreadyInStatus(IncidentStatus::Open)
        );
        assert_eq!(
            validate_transition(IncidentStatus::Resolved, IncidentStatus::Resolved).unwrap_err(),
            IncidentTransitionError::AlreadyInStatus(IncidentStatus::Resolved)
        );
    }

    #[test]
    fn parse_round_trip() {
        for s in [
            IncidentStatus::Open,
            IncidentStatus::Acknowledged,
            IncidentStatus::Resolved,
        ] {
            assert_eq!(IncidentStatus::parse(s.as_str()), Some(s));
        }
        assert_eq!(IncidentStatus::parse("unknown"), None);
    }
}
