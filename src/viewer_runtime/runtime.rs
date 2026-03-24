use crate::domain::telemetry::Signal;
use crate::domain::viewer::ViewerDefinition;
use crate::storage::postgres::ViewerSnapshotRow;
use crate::storage::redis::cmp_stream_id;
use crate::storage::{StorageError, StreamStore, ViewerStore};
use crate::viewer_runtime::compiler::{CompileError, CompiledViewer, compile};
use crate::viewer_runtime::reducer::{apply_entry, prune_stale_buckets};
use crate::viewer_runtime::state::{StreamCursor, ViewerState};
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;
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
    stream_store: StreamStore,
    viewer_store: ViewerStore,
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
            state.last_cursor = cursor;
            viewers.push((viewer, state));
        }

        // Read the stream once per signal and fan out to all viewers
        let now = Utc::now();
        for signal in Signal::all() {
            fan_out_signal_entries(&mut viewers, &mut stream_store, signal).await?;
        }

        // Prune entries that exceed the lookback window
        for (viewer, state) in &mut viewers {
            prune_stale_buckets(state, viewer.lookback_ms(), now);
        }

        Ok(Self {
            viewers,
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

        // Prune entries that exceed the lookback window
        for (viewer, state) in &mut self.viewers {
            prune_stale_buckets(state, viewer.lookback_ms(), now);
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

    pub async fn add_viewer(&mut self, definition: ViewerDefinition) -> Result<(), RuntimeError> {
        let viewer_id = definition.id;
        let revision = definition.revision;
        let viewer = compile(definition)?;
        let mut state = ViewerState::new(viewer_id, revision);
        populate_state_from_stream(&mut self.stream_store, &viewer, &mut state).await?;
        self.viewers.push((viewer, state));
        Ok(())
    }

    pub fn update_viewer_definition(
        &mut self,
        id: Uuid,
        definition_json: Value,
        layout_json: Value,
    ) -> bool {
        for (viewer, state) in &mut self.viewers {
            if viewer.definition().id == id {
                viewer.update_definition_json(definition_json, layout_json);
                state.revision += 1;
                return true;
            }
        }
        false
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
        let idx = match self
            .viewers
            .iter()
            .position(|(v, _)| v.definition().id == id)
        {
            Some(i) => i,
            None => return Ok(false),
        };

        let mut updated_def = self.viewers[idx].0.definition().clone();
        updated_def.definition_json = definition_json;
        updated_def.layout_json = layout_json;
        updated_def.revision += 1;

        let viewer = compile(updated_def)?;
        let mut state = ViewerState::new(viewer.definition().id, viewer.definition().revision);
        populate_state_from_stream(&mut self.stream_store, &viewer, &mut state).await?;
        self.viewers[idx] = (viewer, state);
        Ok(true)
    }

    pub fn remove_viewer(&mut self, id: Uuid) {
        self.viewers.retain(|(v, _)| v.definition().id != id);
    }

    pub fn viewers(&self) -> &[(CompiledViewer, ViewerState)] {
        &self.viewers
    }
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
