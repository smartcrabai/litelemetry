use crate::domain::telemetry::NormalizedEntry;
use crate::viewer_runtime::compiler::CompiledViewer;
use crate::viewer_runtime::state::ViewerState;
use chrono::{DateTime, Utc};

/// Applies an entry to the viewer.
/// - Ignores entries that do not match the viewer's signal_mask.
/// - If they match, adds the entry to state.entries.
///
/// **Precondition**: The caller is responsible for passing entries in ascending time order (oldest first).
/// prune_stale_buckets assumes entries are in ascending time order;
/// if the order is disrupted, pruning will not work correctly.
pub fn apply_entry(state: &mut ViewerState, viewer: &CompiledViewer, entry: NormalizedEntry) {
    if viewer.matches_signal(entry.signal) && viewer.matches_entry(&entry) {
        state.entries.push(entry);
    }
}

/// Removes entries from state that are older than lookback_ms before now.
/// - Returns the number of removed entries.
/// - Entries satisfying `entry.observed_at <= now - lookback_ms` are removed (boundary included).
/// - Assumes state.entries is in ascending time order (oldest first).
pub fn prune_stale_buckets(state: &mut ViewerState, lookback_ms: i64, now: DateTime<Utc>) -> usize {
    let cutoff = now - chrono::Duration::milliseconds(lookback_ms);
    // Assumes ascending time order; use binary search to find the cut position in O(log n + removed count)
    let pos = state.entries.partition_point(|e| e.observed_at <= cutoff);
    state.entries.drain(..pos);
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;
    use crate::domain::viewer::ViewerDefinition;
    use crate::viewer_runtime::compiler::compile;
    use bytes::Bytes;
    use chrono::{Duration, Utc};
    use serde_json::json;
    use uuid::Uuid;

    fn make_compiled_viewer(signal_mask: crate::domain::telemetry::SignalMask) -> CompiledViewer {
        compile(ViewerDefinition {
            id: Uuid::new_v4(),
            slug: "test".to_string(),
            name: "Test".to_string(),
            refresh_interval_ms: 5_000,
            lookback_ms: 60_000,
            signal_mask,
            definition_json: json!({}),
            layout_json: json!({}),
            revision: 1,
            enabled: true,
        })
        .unwrap()
    }

    fn make_compiled_viewer_with_query(
        signal_mask: crate::domain::telemetry::SignalMask,
        query: &str,
    ) -> CompiledViewer {
        compile(ViewerDefinition {
            id: Uuid::new_v4(),
            slug: "test".to_string(),
            name: "Test".to_string(),
            refresh_interval_ms: 5_000,
            lookback_ms: 60_000,
            signal_mask,
            definition_json: json!({ "query": query }),
            layout_json: json!({}),
            revision: 1,
            enabled: true,
        })
        .unwrap()
    }

    fn make_entry(signal: Signal, age_ms: i64) -> NormalizedEntry {
        NormalizedEntry {
            signal,
            observed_at: Utc::now() - Duration::milliseconds(age_ms),
            service_name: Some("test-service".to_string()),
            payload: Bytes::from_static(b"test"),
        }
    }

    fn make_state() -> ViewerState {
        ViewerState::new(Uuid::new_v4(), 1)
    }

    // --- apply_entry --------------------------------------------------------

    #[test]
    fn test_apply_entry_matching_signal_is_added() {
        // Given: a viewer targeting traces, and an empty state
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let entry = make_entry(Signal::Traces, 0);

        // When: applying a traces entry
        apply_entry(&mut state, &viewer, entry);

        // Then: the entry is added
        assert_eq!(state.entries.len(), 1);
    }

    #[test]
    fn test_apply_entry_non_matching_signal_is_ignored() {
        // Given: a viewer targeting only traces
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let entry = make_entry(Signal::Metrics, 0);

        // When: applying a metrics entry
        apply_entry(&mut state, &viewer, entry);

        // Then: the entry is ignored
        assert_eq!(state.entries.len(), 0);
    }

    #[test]
    fn test_apply_multiple_entries_all_matching() {
        // Given: a viewer targeting all signals
        let viewer = make_compiled_viewer(Signal::Traces | Signal::Metrics | Signal::Logs);
        let mut state = make_state();

        // When: applying entries for each signal
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 0));
        apply_entry(&mut state, &viewer, make_entry(Signal::Metrics, 0));
        apply_entry(&mut state, &viewer, make_entry(Signal::Logs, 0));

        // Then: all entries are added
        assert_eq!(state.entries.len(), 3);
    }

    #[test]
    fn test_apply_entry_logs_ignored_by_traces_viewer() {
        // Given: a traces-only viewer
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();

        // When: applying a logs entry
        apply_entry(&mut state, &viewer, make_entry(Signal::Logs, 0));

        // Then: ignored
        assert_eq!(state.entries.len(), 0);
    }

    // --- prune_stale_buckets ------------------------------------------------
    // Note: entries must be inserted in ascending time order (oldest first).

    #[test]
    fn test_prune_removes_entries_older_than_lookback() {
        // Given: 2 old entries (120s, 90s ago) and 1 recent entry (10s ago),
        //        inserted in ascending order (oldest to newest). lookback = 60_000ms
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 120_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 90_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 10_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: the 2 old entries are removed and 1 recent entry remains
        assert_eq!(pruned, 2);
        assert_eq!(state.entries.len(), 1);
    }

    #[test]
    fn test_prune_empty_state_returns_zero() {
        // Given: empty state
        let mut state = make_state();
        let now = Utc::now();

        // When: prune
        let pruned = prune_stale_buckets(&mut state, 60_000, now);

        // Then: 0 entries removed
        assert_eq!(pruned, 0);
        assert_eq!(state.entries.len(), 0);
    }

    #[test]
    fn test_prune_all_within_lookback_removes_nothing() {
        // Given: all entries are within lookback (30s, 10s ago) - inserted in ascending order
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 30_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 10_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: nothing removed
        assert_eq!(pruned, 0);
        assert_eq!(state.entries.len(), 2);
    }

    #[test]
    fn test_prune_all_stale_removes_all() {
        // Given: all entries are outside lookback (180s, 90s ago) - inserted in ascending order
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 180_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 90_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: all entries removed
        assert_eq!(pruned, 2);
        assert_eq!(state.entries.len(), 0);
    }

    // --- apply_entry with query filter --------------------------------------

    #[test]
    fn test_apply_entry_matching_query_is_added() {
        // Given: viewer with query "checkout-ui", entry whose service_name contains the query
        let viewer = make_compiled_viewer_with_query(Signal::Traces.into(), "checkout-ui");
        let mut state = make_state();
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name: Some("checkout-ui".to_string()),
            payload: Bytes::from_static(b"{}"),
        };

        // When: apply_entry
        apply_entry(&mut state, &viewer, entry);

        // Then: entry is added because query matches
        assert_eq!(state.entries.len(), 1);
    }

    #[test]
    fn test_apply_entry_non_matching_query_is_ignored() {
        // Given: viewer with query "orders", entry with service_name "checkout-ui" (non-matching)
        let viewer = make_compiled_viewer_with_query(Signal::Traces.into(), "orders");
        let mut state = make_state();
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name: Some("checkout-ui".to_string()),
            payload: Bytes::from_static(b"{}"),
        };

        // When: apply_entry
        apply_entry(&mut state, &viewer, entry);

        // Then: entry is ignored because query does not match
        assert_eq!(state.entries.len(), 0);
    }

    #[test]
    fn test_apply_entry_no_query_allows_all_matching_signal_entries() {
        // Given: viewer with no query (match-all)
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();

        // When: applying multiple entries with different service names
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 200));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 100));

        // Then: all entries with matching signal are added
        assert_eq!(state.entries.len(), 2);
    }

    #[test]
    fn test_apply_entry_query_case_insensitive_match() {
        // Given: viewer with uppercase query "CHECKOUT-UI"
        let viewer = make_compiled_viewer_with_query(Signal::Traces.into(), "CHECKOUT-UI");
        let mut state = make_state();
        let entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name: Some("checkout-ui".to_string()),
            payload: Bytes::from_static(b"{}"),
        };

        // When: apply_entry
        apply_entry(&mut state, &viewer, entry);

        // Then: case-insensitive match adds the entry
        assert_eq!(state.entries.len(), 1);
    }

    #[test]
    fn test_prune_boundary_entry_is_removed() {
        // Given: 1 entry exactly lookback_ms ago and 1 more recent entry
        //        The boundary (= lookback_ms ago) is subject to removal (boundary included)
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        // Exactly at the boundary (60s ago) - add the older entry first
        let boundary_entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: now - Duration::milliseconds(lookback_ms),
            service_name: None,
            payload: Bytes::from_static(b"boundary"),
        };
        state.entries.push(boundary_entry);

        // More recent than the boundary (30s ago)
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 30_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: the boundary entry is removed and only 1 recent entry remains
        assert_eq!(pruned, 1);
        assert_eq!(state.entries.len(), 1);
    }
}
