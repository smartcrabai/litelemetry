use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::domain::viewer::ViewerDefinition;
use crate::domain::viewer::ViewerStatus;
use crate::storage::postgres::ViewerSnapshotRow;
use crate::storage::redis::{cmp_stream_id, parse_stream_id};
use crate::storage::{StorageError, StreamStore, ViewerStore};
use crate::viewer_runtime::compiler::{CompileError, CompiledViewer, compile};
use crate::viewer_runtime::reducer::{
    apply_entry, lookback_start_index, prune_stale_buckets, recompute_aggregation,
};
use crate::viewer_runtime::state::{StreamCursor, ViewerState};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("compile error: {0}")]
    Compile(#[from] CompileError),
}

/// In-memory runtime for viewers.
/// - `build()` constructs the initial state from store definitions + snapshots + stream diffs.
/// - `apply_diff_batch()` fetches diffs from the stream and fans them out to all viewers.
pub struct ViewerRuntime {
    viewers: Vec<(CompiledViewer, ViewerState)>,
    /// Maps viewer id -> index into `viewers`, kept in sync on every mutation.
    /// Lets `get_by_id` lookups stay O(1) instead of O(N).
    viewer_index: HashMap<Uuid, usize>,
    stream_store: StreamStore,
    viewer_store: ViewerStore,
}

fn sanitize_stream_cursor(viewer_id: Uuid, mut cursor: StreamCursor) -> StreamCursor {
    for signal in Signal::all() {
        let Some(cursor_id) = cursor.get(signal).map(str::to_string) else {
            continue;
        };
        if parse_stream_id(&cursor_id).is_some() {
            continue;
        }

        tracing::warn!(
            "viewer {viewer_id}: invalid snapshot cursor {cursor_id:?} for {}, clearing it",
            signal.as_str()
        );
        cursor.clear(signal);
    }

    cursor
}

impl ViewerRuntime {
    /// Constructs the initial state from the store and streams.
    ///
    /// Reads the stream once per signal and fans out to all viewers.
    pub async fn build(
        viewer_store: ViewerStore,
        mut stream_store: StreamStore,
    ) -> Result<Self, RuntimeError> {
        let definitions = viewer_store.load_viewer_definitions().await?;

        // Bulk-fetch all viewer snapshots and store in a HashMap (avoids N+1 queries)
        let def_ids: Vec<_> = definitions.iter().map(|d| d.id).collect();
        let snapshots: HashMap<_, _> = viewer_store
            .load_snapshots(&def_ids)
            .await?
            .into_iter()
            .map(|s| (s.viewer_id, s))
            .collect();

        let mut viewers: Vec<(CompiledViewer, ViewerState)> = Vec::new();

        for def in definitions {
            let def_id = def.id;
            let def_revision = def.revision;

            let viewer = match compile(def) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("viewer {def_id}: compile failed: {e}");
                    continue;
                }
            };

            // Resume from the snapshot cursor if revisions match
            let cursor = match snapshots.get(&def_id) {
                Some(snapshot) if snapshot.revision == def_revision => serde_json::from_value::<
                    StreamCursor,
                >(
                    snapshot.last_cursor_json.clone(),
                )
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        "viewer {def_id}: failed to parse snapshot cursor: {e}, using default"
                    );
                    StreamCursor::default()
                }),
                _ => StreamCursor::default(),
            };

            let mut state = ViewerState::new(def_id, def_revision);
            state.last_cursor = sanitize_stream_cursor(def_id, cursor);
            viewers.push((viewer, state));
        }

        // Read the stream once per signal and fan out to all viewers
        let now = Utc::now();
        for signal in Signal::all() {
            fan_out_signal_entries(&mut viewers, &mut stream_store, signal).await?;
        }

        // Prune entries that exceed the lookback window, then recompute aggregations
        for (viewer, state) in &mut viewers {
            prune_stale_buckets(state, viewer.lookback_ms(), now);
            recompute_aggregation(state, viewer);
        }

        let viewer_index = viewers
            .iter()
            .enumerate()
            .map(|(idx, (v, _))| (v.definition().id, idx))
            .collect();

        Ok(Self {
            viewers,
            viewer_index,
            stream_store,
            viewer_store,
        })
    }

    /// Fetches diffs from the stream for all viewers and updates them.
    ///
    /// Fans out a single read result to multiple viewers watching the same signal.
    /// Upserts snapshots to the store after updating.
    pub async fn apply_diff_batch(&mut self) -> Result<(), RuntimeError> {
        let now = Utc::now();

        // Capture cursor state beforehand to detect changes
        let prev_cursors: Vec<StreamCursor> = self
            .viewers
            .iter()
            .map(|(_, s)| s.last_cursor.clone())
            .collect();

        for signal in Signal::all() {
            fan_out_signal_entries(&mut self.viewers, &mut self.stream_store, signal).await?;
        }

        // Prune entries that exceed the lookback window, then recompute aggregations
        for (viewer, state) in &mut self.viewers {
            prune_stale_buckets(state, viewer.lookback_ms(), now);
            recompute_aggregation(state, viewer);
        }

        // Upsert snapshots only for viewers whose cursor advanced (failures are retried in the next batch)
        for (i, (_, state)) in self.viewers.iter().enumerate() {
            if state.last_cursor == prev_cursors[i] {
                continue;
            }
            let snapshot = ViewerSnapshotRow {
                viewer_id: state.viewer_id,
                revision: state.revision,
                last_cursor_json: serde_json::to_value(&state.last_cursor)
                    .expect("StreamCursor serialization should never fail"),
                status: state.status.clone(),
                generated_at: now,
            };
            if let Err(e) = self.viewer_store.upsert_snapshot(&snapshot).await {
                tracing::error!("viewer {}: snapshot upsert failed: {e}", state.viewer_id);
            }
        }

        Ok(())
    }

    /// Persists the current snapshot for the viewer identified by `id`.
    /// Returns true if the viewer was found and the snapshot was upserted.
    pub async fn persist_viewer_snapshot(&self, id: Uuid) -> Result<bool, RuntimeError> {
        let (_, state) = match self.get_by_id(id) {
            Some(v) => v,
            None => return Ok(false),
        };
        let snapshot = ViewerSnapshotRow {
            viewer_id: state.viewer_id,
            revision: state.revision,
            last_cursor_json: serde_json::to_value(&state.last_cursor)
                .expect("StreamCursor serialization should never fail"),
            status: state.status.clone(),
            generated_at: Utc::now(),
        };
        self.viewer_store.upsert_snapshot(&snapshot).await?;
        Ok(true)
    }

    pub async fn add_viewer(&mut self, definition: ViewerDefinition) -> Result<(), RuntimeError> {
        let viewer_id = definition.id;
        let revision = definition.revision;
        let viewer = compile(definition)?;
        let mut state = ViewerState::new(viewer_id, revision);
        populate_state_from_stream(&mut self.stream_store, &viewer, &mut state).await?;
        let idx = self.viewers.len();
        self.viewers.push((viewer, state));
        self.viewer_index.insert(viewer_id, idx);
        Ok(())
    }

    pub fn update_viewer_definition(
        &mut self,
        id: Uuid,
        definition_json: Value,
        layout_json: Value,
    ) -> bool {
        let Some(&idx) = self.viewer_index.get(&id) else {
            return false;
        };
        let (viewer, state) = &mut self.viewers[idx];
        viewer.update_definition_json(definition_json, layout_json);
        state.revision += 1;
        // Recompute buckets so a newly-added or modified aggregation
        // block is reflected in the next API response.
        recompute_aggregation(state, viewer);
        true
    }

    pub fn update_viewer_name(&mut self, id: Uuid, name: &str) -> bool {
        let Some(&idx) = self.viewer_index.get(&id) else {
            return false;
        };
        let (viewer, state) = &mut self.viewers[idx];
        viewer.update_name(name);
        state.revision += 1;
        true
    }

    /// Rebuilds a viewer's state from scratch using the updated definition_json/layout_json.
    /// Re-scans the stream from the beginning so that stale entries (no longer matching the
    /// new query) are evicted and new matches are included.
    /// Returns true if the viewer was found and rebuilt.
    pub async fn rebuild_viewer(
        &mut self,
        id: Uuid,
        definition_json: Value,
        layout_json: Value,
    ) -> Result<bool, RuntimeError> {
        let Some(&idx) = self.viewer_index.get(&id) else {
            return Ok(false);
        };

        let mut updated_def = self.viewers[idx].0.definition().clone();
        updated_def.definition_json = definition_json;
        updated_def.layout_json = layout_json;
        updated_def.revision += 1;

        let viewer = compile(updated_def)?;
        let mut state = ViewerState::new(viewer.definition().id, viewer.definition().revision);
        populate_state_from_stream(&mut self.stream_store, &viewer, &mut state).await?;
        // The viewer id is invariant across rebuilds, so the index entry stays valid.
        self.viewers[idx] = (viewer, state);
        Ok(true)
    }

    pub fn remove_viewer(&mut self, id: Uuid) {
        let Some(idx) = self.viewer_index.remove(&id) else {
            return;
        };
        // `swap_remove` is O(1); patch the moved entry's index back into the map.
        let last_idx = self.viewers.len() - 1;
        self.viewers.swap_remove(idx);
        if idx != last_idx {
            let moved_id = self.viewers[idx].0.definition().id;
            self.viewer_index.insert(moved_id, idx);
        }
    }

    pub fn viewers(&self) -> &[(CompiledViewer, ViewerState)] {
        &self.viewers
    }

    /// O(1) lookup of a viewer + its state by id. Returns `None` when no
    /// viewer with that id is currently registered with the runtime.
    pub fn get_by_id(&self, id: Uuid) -> Option<(&CompiledViewer, &ViewerState)> {
        let &idx = self.viewer_index.get(&id)?;
        let (viewer, state) = &self.viewers[idx];
        Some((viewer, state))
    }

    pub fn collect_service_names(&self) -> Vec<String> {
        let mut names: BTreeSet<String> = BTreeSet::new();
        for (_, state) in &self.viewers {
            for entry in &state.entries {
                if let Some(ref name) = entry.service_name {
                    names.insert(name.clone());
                }
            }
        }
        names.into_iter().collect()
    }

    pub fn get_dashboard_viewer<'a>(
        &'a self,
        viewer_id: &Uuid,
        lookback_override: Option<i64>,
        now: DateTime<Utc>,
    ) -> Option<DashboardViewerSlice<'a>> {
        let (viewer, state) = self.get_by_id(*viewer_id)?;
        let effective_lookback = lookback_override.unwrap_or(viewer.lookback_ms());
        let start_idx = lookback_start_index(&state.entries, effective_lookback, now);
        let filtered_entries = &state.entries[start_idx..];
        Some(DashboardViewerSlice {
            viewer,
            entries: filtered_entries,
            status: &state.status,
            effective_lookback_ms: effective_lookback,
            aggregated_buckets: state.aggregated_buckets.as_slice(),
        })
    }
}

/// Borrowed view of one viewer slice destined for a dashboard panel.
pub struct DashboardViewerSlice<'a> {
    pub viewer: &'a CompiledViewer,
    pub entries: &'a [NormalizedEntry],
    pub status: &'a ViewerStatus,
    pub effective_lookback_ms: i64,
    pub aggregated_buckets: &'a [crate::viewer_runtime::aggregator::Bucket],
}

/// Scans the stream for all signals the viewer cares about, applies matching entries to
/// `state`, and prunes stale buckets.  Used by both `add_viewer` and `rebuild_viewer`.
async fn populate_state_from_stream(
    stream_store: &mut StreamStore,
    viewer: &CompiledViewer,
    state: &mut ViewerState,
) -> Result<(), RuntimeError> {
    let now = Utc::now();
    for signal in Signal::all() {
        if !viewer.matches_signal(signal) {
            continue;
        }
        let entries = stream_store
            .read_entries_since(signal, None, 100_000)
            .await?;
        for (entry_id, entry) in entries {
            apply_entry(state, viewer, entry);
            state.last_cursor.set(signal, entry_id);
        }
    }
    prune_stale_buckets(state, viewer.lookback_ms(), now);
    recompute_aggregation(state, viewer);
    Ok(())
}

/// Reads the stream once for the specified signal and fans out entries to all viewers.
async fn fan_out_signal_entries(
    viewers: &mut [(CompiledViewer, ViewerState)],
    stream_store: &mut StreamStore,
    signal: Signal,
) -> Result<(), RuntimeError> {
    if !viewers.iter().any(|(v, _)| v.matches_signal(signal)) {
        return Ok(());
    }

    // Use the oldest cursor among all viewers (= the one requiring the most entries to be read).
    // None = treated as the minimum because a viewer needs to read from the beginning.
    let min_cursor: Option<String> = viewers
        .iter()
        .filter(|(v, _)| v.matches_signal(signal))
        .map(|(_, state)| state.last_cursor.get(signal))
        .min_by(|a, b| match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(x), Some(y)) => cmp_stream_id(x, y),
        })
        .flatten()
        .map(str::to_string);

    let entries = stream_store
        .read_entries_since(signal, min_cursor.as_deref(), 100_000)
        .await?;

    for (entry_id, entry) in &entries {
        for (viewer, state) in viewers.iter_mut() {
            if !viewer.matches_signal(signal) {
                continue;
            }
            // Only apply entries that come after the viewer's own cursor
            if let Some(vc) = state.last_cursor.get(signal)
                && !cmp_stream_id(entry_id, vc).is_gt()
            {
                continue;
            }
            apply_entry(state, viewer, entry.clone());
            state.last_cursor.set(signal, entry_id.clone());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::{MemoryStreamStore, MemoryViewerStore};
    use serde_json::json;

    #[test]
    fn test_sanitize_stream_cursor_clears_only_invalid_ids() {
        let cursor = StreamCursor {
            traces: Some("1710000000000-1".to_string()),
            metrics: Some("not-a-stream-id".to_string()),
            logs: Some("1710000000000-2".to_string()),
        };

        let sanitized = sanitize_stream_cursor(Uuid::nil(), cursor);

        assert_eq!(sanitized.traces.as_deref(), Some("1710000000000-1"));
        assert_eq!(sanitized.metrics, None);
        assert_eq!(sanitized.logs.as_deref(), Some("1710000000000-2"));
    }

    fn sample_definition(id: Uuid, name: &str) -> ViewerDefinition {
        ViewerDefinition {
            id,
            slug: format!("slug-{name}"),
            name: name.to_string(),
            refresh_interval_ms: 1_000,
            lookback_ms: 60_000,
            signal_mask: Signal::Traces.into(),
            definition_json: json!({ "kind": "table", "signal": "traces" }),
            layout_json: json!({}),
            revision: 1,
            enabled: true,
        }
    }

    async fn empty_runtime() -> ViewerRuntime {
        let stream_store = StreamStore::Memory(MemoryStreamStore::new(1_000));
        let viewer_store = ViewerStore::Memory(MemoryViewerStore::new());
        ViewerRuntime::build(viewer_store, stream_store).await.unwrap()
    }

    fn assert_index_matches_viewers(runtime: &ViewerRuntime) {
        assert_eq!(runtime.viewer_index.len(), runtime.viewers.len());
        for (idx, (viewer, _)) in runtime.viewers.iter().enumerate() {
            let id = viewer.definition().id;
            assert_eq!(runtime.viewer_index.get(&id).copied(), Some(idx));
        }
    }

    #[tokio::test]
    async fn viewer_index_tracks_add_and_remove() {
        let mut runtime = empty_runtime().await;
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();

        runtime.add_viewer(sample_definition(a, "a")).await.unwrap();
        runtime.add_viewer(sample_definition(b, "b")).await.unwrap();
        runtime.add_viewer(sample_definition(c, "c")).await.unwrap();
        assert_index_matches_viewers(&runtime);

        // Remove the middle viewer -> triggers swap_remove path.
        runtime.remove_viewer(b);
        assert!(runtime.get_by_id(b).is_none());
        assert!(runtime.get_by_id(a).is_some());
        assert!(runtime.get_by_id(c).is_some());
        assert_index_matches_viewers(&runtime);

        // Remove the last viewer (no swap needed).
        runtime.remove_viewer(c);
        assert_index_matches_viewers(&runtime);

        // Removing an unknown id should be a no-op.
        runtime.remove_viewer(Uuid::new_v4());
        assert_index_matches_viewers(&runtime);
    }

    #[tokio::test]
    async fn viewer_index_lookup_is_consistent_after_updates() {
        let mut runtime = empty_runtime().await;
        let a = Uuid::new_v4();
        runtime.add_viewer(sample_definition(a, "a")).await.unwrap();

        assert!(runtime.update_viewer_name(a, "renamed"));
        let (viewer, _) = runtime.get_by_id(a).unwrap();
        assert_eq!(viewer.definition().name, "renamed");

        assert!(
            runtime.update_viewer_definition(a, json!({ "kind": "table", "signal": "traces" }), json!({}))
        );

        assert!(
            runtime
                .rebuild_viewer(a, json!({ "kind": "table", "signal": "traces" }), json!({}))
                .await
                .unwrap()
        );
        assert_index_matches_viewers(&runtime);
        assert!(runtime.get_by_id(a).is_some());
    }
}
