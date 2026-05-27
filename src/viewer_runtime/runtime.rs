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
use std::time::{Duration, Instant};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("compile error: {0}")]
    Compile(#[from] CompileError),
}

/// Quiet ticks emit `tick_ms` at debug; ticks above this threshold escalate to info.
/// Polling tick latency is the primary suspect for read-lock starvation, so anything
/// over ~half a second is worth surfacing without users having to flip log levels.
const APPLY_DIFF_BATCH_INFO_THRESHOLD_MS: u64 = 500;

/// RAII guard that emits one structured log line per `apply_diff_batch` call,
/// regardless of whether the function returns Ok or propagates an error via `?`.
/// Each field is populated as the function progresses; on early-return paths
/// `outcome` stays `"error"` and partial timings are still reported.
struct ApplyDiffBatchMetrics {
    tick_start: Instant,
    redis_elapsed: Duration,
    fan_out_cpu_elapsed: Duration,
    aggregation_elapsed: Duration,
    snapshot_elapsed: Duration,
    snapshots_upserted: usize,
    viewer_count: usize,
    outcome: &'static str,
}

impl ApplyDiffBatchMetrics {
    fn new() -> Self {
        Self {
            tick_start: Instant::now(),
            redis_elapsed: Duration::ZERO,
            fan_out_cpu_elapsed: Duration::ZERO,
            aggregation_elapsed: Duration::ZERO,
            snapshot_elapsed: Duration::ZERO,
            snapshots_upserted: 0,
            viewer_count: 0,
            outcome: "error",
        }
    }
}

impl Drop for ApplyDiffBatchMetrics {
    fn drop(&mut self) {
        let tick_ms = self.tick_start.elapsed().as_millis() as u64;
        let redis_ms = self.redis_elapsed.as_millis() as u64;
        let fan_out_cpu_ms = self.fan_out_cpu_elapsed.as_millis() as u64;
        let aggregation_ms = self.aggregation_elapsed.as_millis() as u64;
        let snapshot_ms = self.snapshot_elapsed.as_millis() as u64;
        // Errors always escalate so failures aren't silently buried at debug.
        if tick_ms >= APPLY_DIFF_BATCH_INFO_THRESHOLD_MS || self.outcome != "ok" {
            tracing::info!(
                tick_ms,
                redis_ms,
                fan_out_cpu_ms,
                aggregation_ms,
                snapshot_ms,
                snapshots_upserted = self.snapshots_upserted,
                viewer_count = self.viewer_count,
                outcome = self.outcome,
                "apply_diff_batch"
            );
        } else {
            tracing::debug!(
                tick_ms,
                redis_ms,
                fan_out_cpu_ms,
                aggregation_ms,
                snapshot_ms,
                snapshots_upserted = self.snapshots_upserted,
                viewer_count = self.viewer_count,
                outcome = self.outcome,
                "apply_diff_batch"
            );
        }
    }
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

        // Read the stream once per signal and fan out to all viewers.
        // Drain stats are discarded -- this path only runs at startup and has no
        // tick-level summary log to fold the timings into.
        let now = Utc::now();
        for signal in Signal::all() {
            let _ = fan_out_signal_entries(&mut viewers, &mut stream_store, signal, now).await?;
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
        let mut metrics = ApplyDiffBatchMetrics::new();

        // Capture cursor state beforehand to detect changes
        let prev_cursors: Vec<StreamCursor> = self
            .viewers
            .iter()
            .map(|(_, s)| s.last_cursor.clone())
            .collect();

        let mut drain_total = DrainStats::default();
        for signal in Signal::all() {
            drain_total +=
                fan_out_signal_entries(&mut self.viewers, &mut self.stream_store, signal, now)
                    .await?;
        }
        metrics.redis_elapsed = drain_total.redis_elapsed;
        metrics.fan_out_cpu_elapsed = drain_total.cpu_elapsed;

        // Prune entries that exceed the lookback window, then recompute aggregations
        let aggregation_start = Instant::now();
        for (viewer, state) in &mut self.viewers {
            prune_stale_buckets(state, viewer.lookback_ms(), now);
            recompute_aggregation(state, viewer);
        }
        metrics.aggregation_elapsed = aggregation_start.elapsed();

        // Upsert snapshots only for viewers whose cursor advanced (failures are retried in the next batch)
        let snapshot_start = Instant::now();
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
            match self.viewer_store.upsert_snapshot(&snapshot).await {
                Ok(()) => metrics.snapshots_upserted += 1,
                Err(e) => {
                    tracing::error!("viewer {}: snapshot upsert failed: {e}", state.viewer_id);
                }
            }
        }
        metrics.snapshot_elapsed = snapshot_start.elapsed();
        metrics.viewer_count = self.viewers.len();
        metrics.outcome = "ok";

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
        // The aggregation spec may have changed (bucket size, fn, group_by, even
        // its accumulator type), so the running incremental state is stale. Drop
        // it and let recompute_aggregation rebuild it from entries under the new
        // spec, so a newly-added or modified aggregation block is reflected in
        // the next API response.
        state.agg_state = None;
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

/// Maximum number of entries fetched in a single XREAD when catching up the stream.
///
/// Streams can hold entries with multi-hundred-KB payloads, so a single XREAD that grabs
/// e.g. 100_000 entries can produce a multi-GB response and trip Redis/Dragonfly response
/// timeouts or output buffer limits. We instead read in bounded chunks and loop until the
/// stream is drained, so each round-trip stays comfortably within client/server limits.
#[cfg(not(test))]
pub(crate) const STREAM_READ_CHUNK_SIZE: usize = 500;
/// Tests use a small chunk size so any single-shot regression in the chunked loop is
/// detected — production-size data inserted with this smaller chunk forces multiple
/// XREAD iterations.
#[cfg(test)]
pub(crate) const STREAM_READ_CHUNK_SIZE: usize = 17;

/// Upper bound on the total entries `fan_out_signal_entries` consumes in a single call.
///
/// `apply_diff_batch` holds the runtime write lock while calling fan-out, and producers
/// may sustain XADD rates that match the chunked drain rate. Without an upper bound the
/// drain loop can either (a) hold the lock for minutes while catching up a large backlog,
/// or (b) never terminate when ingest ≥ drain. Catching up more than this many entries
/// per signal is intentionally spread across multiple polling ticks.
#[cfg(not(test))]
pub(crate) const MAX_ENTRIES_PER_FAN_OUT_CALL: usize = 100_000;
#[cfg(test)]
pub(crate) const MAX_ENTRIES_PER_FAN_OUT_CALL: usize = 50;

/// Time accounting collected while draining a Redis stream.
///
/// `redis_elapsed` covers only the `read_entries_since` awaits; `cpu_elapsed`
/// covers the per-chunk `apply` callback dispatch. Bookkeeping between chunks
/// (cursor updates, consumed counter) is negligible and left out of both.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DrainStats {
    pub redis_elapsed: Duration,
    pub cpu_elapsed: Duration,
}

impl std::ops::AddAssign for DrainStats {
    fn add_assign(&mut self, other: Self) {
        self.redis_elapsed += other.redis_elapsed;
        self.cpu_elapsed += other.cpu_elapsed;
    }
}

/// Drains `signal`'s stream from `start_cursor` in `STREAM_READ_CHUNK_SIZE` chunks,
/// invoking `apply` on every entry.
///
/// Termination conditions:
/// - the next XREAD returns an empty batch (stream is exhausted), or
/// - the optional `max_total` cap is reached.
///
/// Importantly, the loop does NOT terminate on a "short read" (`entries.len() < chunk`):
/// `parse_xread_reply` silently drops malformed entries, so a short read does not imply
/// the stream is exhausted. Using `is_empty()` as the only termination signal keeps the
/// loop correct in the face of skipped entries.
async fn drain_signal_entries<F>(
    stream_store: &mut StreamStore,
    signal: Signal,
    start_cursor: Option<String>,
    max_total: Option<usize>,
    mut apply: F,
) -> Result<DrainStats, RuntimeError>
where
    F: FnMut(String, NormalizedEntry),
{
    let mut cursor = start_cursor;
    let mut consumed: usize = 0;
    let mut stats = DrainStats::default();
    loop {
        let chunk_count = match max_total {
            Some(max) => {
                if consumed >= max {
                    break;
                }
                (max - consumed).min(STREAM_READ_CHUNK_SIZE)
            }
            None => STREAM_READ_CHUNK_SIZE,
        };
        let redis_start = Instant::now();
        let entries = stream_store
            .read_entries_since(signal, cursor.as_deref(), chunk_count)
            .await?;
        stats.redis_elapsed += redis_start.elapsed();
        if entries.is_empty() {
            break;
        }
        // Measure CPU work for the whole chunk so the per-entry overhead of
        // calling `Instant::now()` does not show up in the reported `cpu_elapsed`.
        let cpu_start = Instant::now();
        for (entry_id, entry) in entries {
            consumed += 1;
            cursor = Some(entry_id.clone());
            apply(entry_id, entry);
        }
        stats.cpu_elapsed += cpu_start.elapsed();
    }
    Ok(stats)
}

/// Returns `true` when an entry observed at `observed_at` is already older than the
/// viewer's lookback window and would be pruned away immediately after being applied.
fn is_outside_lookback(now: DateTime<Utc>, observed_at: DateTime<Utc>, lookback_ms: i64) -> bool {
    (now - observed_at).num_milliseconds() > lookback_ms
}

/// Scans the stream for all signals the viewer cares about, applies matching entries to
/// `state`, and prunes stale buckets.  Used by both `add_viewer` and `rebuild_viewer`.
async fn populate_state_from_stream(
    stream_store: &mut StreamStore,
    viewer: &CompiledViewer,
    state: &mut ViewerState,
) -> Result<(), RuntimeError> {
    let now = Utc::now();
    let lookback_ms = viewer.lookback_ms();
    for signal in Signal::all() {
        if !viewer.matches_signal(signal) {
            continue;
        }
        // Drain stats are discarded here -- this path runs during initialization
        // and add_viewer/rebuild_viewer, where there is no tick-level summary log
        // to fold the timings into.
        let _ = drain_signal_entries(stream_store, signal, None, None, |entry_id, entry| {
            // Skip entries already outside lookback so state.entries does not balloon
            // when the stream history far exceeds lookback. The cursor still advances
            // so the next call resumes past these entries.
            if is_outside_lookback(now, entry.observed_at, lookback_ms) {
                state.last_cursor.set(signal, entry_id);
                return;
            }
            apply_entry(state, viewer, entry);
            state.last_cursor.set(signal, entry_id);
        })
        .await?;
    }
    // Re-capture `now` for prune — the drain loop above may have run for a while.
    prune_stale_buckets(state, lookback_ms, Utc::now());
    recompute_aggregation(state, viewer);
    Ok(())
}

/// Reads the stream for the specified signal in bounded chunks and fans entries out to
/// all viewers. Capped at `MAX_ENTRIES_PER_FAN_OUT_CALL` entries per call so the polling
/// loop's write lock never stays held longer than that much work, and so that producers
/// out-pacing the drain cannot starve the loop's termination.
async fn fan_out_signal_entries(
    viewers: &mut [(CompiledViewer, ViewerState)],
    stream_store: &mut StreamStore,
    signal: Signal,
    now: DateTime<Utc>,
) -> Result<DrainStats, RuntimeError> {
    if !viewers.iter().any(|(v, _)| v.matches_signal(signal)) {
        return Ok(DrainStats::default());
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

    drain_signal_entries(
        stream_store,
        signal,
        min_cursor,
        Some(MAX_ENTRIES_PER_FAN_OUT_CALL),
        |entry_id, entry| {
            for (viewer, state) in viewers.iter_mut() {
                if !viewer.matches_signal(signal) {
                    continue;
                }
                // Only apply entries that come after the viewer's own cursor
                if let Some(vc) = state.last_cursor.get(signal)
                    && !cmp_stream_id(entry_id.as_str(), vc).is_gt()
                {
                    continue;
                }
                // Skip entries already outside this viewer's lookback so state.entries
                // does not balloon when one viewer's stale cursor drags `min_cursor` far back.
                if is_outside_lookback(now, entry.observed_at, viewer.lookback_ms()) {
                    state.last_cursor.set(signal, entry_id.clone());
                    continue;
                }
                apply_entry(state, viewer, entry.clone());
                state.last_cursor.set(signal, entry_id.clone());
            }
        },
    )
    .await
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
        ViewerRuntime::build(viewer_store, stream_store)
            .await
            .unwrap()
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

    /// Inserts `count` synthetic trace entries with strictly ascending `observed_at`
    /// (matching the reducer's ascending-time precondition) and returns the inserted
    /// stream ids in order.
    async fn seed_traces(
        stream_store: &mut MemoryStreamStore,
        now: DateTime<Utc>,
        count: usize,
    ) -> Vec<String> {
        use bytes::Bytes;
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let id = stream_store
                .append_entry(&NormalizedEntry {
                    signal: Signal::Traces,
                    // i=0 -> oldest, i=count-1 -> newest. Stay inside the test
                    // viewer's lookback window so entries survive prune.
                    observed_at: now - chrono::Duration::milliseconds((count - 1 - i) as i64),
                    service_name: Some(format!("svc-{i}")),
                    payload: Bytes::from_static(b"{}"),
                })
                .await
                .unwrap();
            ids.push(id);
        }
        ids
    }

    #[tokio::test]
    async fn build_drains_streams_via_chunked_reads() {
        use crate::storage::memory::MemoryStreamStore;

        // Insert more entries than STREAM_READ_CHUNK_SIZE but fewer than
        // MAX_ENTRIES_PER_FAN_OUT_CALL so build() can consume them all in one go.
        // With cfg(test) the chunk size is small (17) and the cap is 50; pick a
        // total that requires multiple chunks but stays under the cap.
        let total_entries = MAX_ENTRIES_PER_FAN_OUT_CALL;
        assert!(total_entries > STREAM_READ_CHUNK_SIZE);
        let mut stream_store = MemoryStreamStore::new(total_entries * 2);
        let now = Utc::now();
        let ids = seed_traces(&mut stream_store, now, total_entries).await;

        let viewer_store = MemoryViewerStore::new();
        let def_id = Uuid::new_v4();
        let mut def = sample_definition(def_id, "drain");
        def.lookback_ms = 24 * 60 * 60 * 1000; // 1 day so nothing gets pruned
        viewer_store.insert_viewer_definition(&def).await.unwrap();

        let runtime = ViewerRuntime::build(
            ViewerStore::Memory(viewer_store),
            StreamStore::Memory(stream_store),
        )
        .await
        .unwrap();

        let (_, state) = runtime.get_by_id(def_id).unwrap();
        assert_eq!(state.entries.len(), total_entries);
        // The cursor must point at the LAST inserted entry — a single-shot read
        // (STREAM_READ_CHUNK_SIZE worth of entries only) would leave it earlier.
        assert_eq!(
            state.last_cursor.get(Signal::Traces),
            Some(ids.last().unwrap().as_str())
        );
    }

    #[tokio::test]
    async fn add_viewer_drains_streams_via_chunked_reads() {
        use crate::storage::memory::MemoryStreamStore;

        // add_viewer's path through populate_state_from_stream has NO cap, so
        // exercising > STREAM_READ_CHUNK_SIZE is enough to verify chunked drain.
        let total_entries = STREAM_READ_CHUNK_SIZE * 3 + 5;
        let mut stream_store = MemoryStreamStore::new(total_entries * 2);
        let now = Utc::now();
        let ids = seed_traces(&mut stream_store, now, total_entries).await;

        let viewer_store = ViewerStore::Memory(MemoryViewerStore::new());
        let mut runtime = ViewerRuntime::build(viewer_store, StreamStore::Memory(stream_store))
            .await
            .unwrap();

        let def_id = Uuid::new_v4();
        let mut def = sample_definition(def_id, "drain-add");
        def.lookback_ms = 24 * 60 * 60 * 1000;
        runtime.add_viewer(def).await.unwrap();

        let (_, state) = runtime.get_by_id(def_id).unwrap();
        assert_eq!(state.entries.len(), total_entries);
        assert_eq!(
            state.last_cursor.get(Signal::Traces),
            Some(ids.last().unwrap().as_str())
        );
    }

    #[tokio::test]
    async fn fan_out_caps_entries_per_call_and_resumes_next_tick() {
        use crate::storage::memory::MemoryStreamStore;

        // More entries than MAX_ENTRIES_PER_FAN_OUT_CALL: a single
        // apply_diff_batch tick must NOT drain the whole stream — instead it
        // should stop at the cap and the next tick continues.
        let total_entries = MAX_ENTRIES_PER_FAN_OUT_CALL * 2 + 3;
        let mut stream_store = MemoryStreamStore::new(total_entries * 2);
        let now = Utc::now();
        let ids = seed_traces(&mut stream_store, now, total_entries).await;

        let viewer_store = MemoryViewerStore::new();
        let def_id = Uuid::new_v4();
        let mut def = sample_definition(def_id, "cap");
        def.lookback_ms = 24 * 60 * 60 * 1000;
        viewer_store.insert_viewer_definition(&def).await.unwrap();

        let mut runtime = ViewerRuntime::build(
            ViewerStore::Memory(viewer_store),
            StreamStore::Memory(stream_store),
        )
        .await
        .unwrap();

        // After build (which itself runs fan_out), the runtime has drained at
        // most MAX_ENTRIES_PER_FAN_OUT_CALL entries.
        let consumed_after_build = runtime.get_by_id(def_id).unwrap().1.entries.len();
        assert_eq!(consumed_after_build, MAX_ENTRIES_PER_FAN_OUT_CALL);
        assert_eq!(
            runtime
                .get_by_id(def_id)
                .unwrap()
                .1
                .last_cursor
                .get(Signal::Traces),
            Some(ids[MAX_ENTRIES_PER_FAN_OUT_CALL - 1].as_str())
        );

        // A second tick drains another cap's worth.
        runtime.apply_diff_batch().await.unwrap();
        let consumed_after_second = runtime.get_by_id(def_id).unwrap().1.entries.len();
        assert_eq!(consumed_after_second, 2 * MAX_ENTRIES_PER_FAN_OUT_CALL);

        // A third tick drains the remainder.
        runtime.apply_diff_batch().await.unwrap();
        let (_, state) = runtime.get_by_id(def_id).unwrap();
        assert_eq!(state.entries.len(), total_entries);
        assert_eq!(
            state.last_cursor.get(Signal::Traces),
            Some(ids.last().unwrap().as_str())
        );
    }

    #[tokio::test]
    async fn populate_state_skips_entries_outside_lookback_to_bound_memory() {
        use crate::storage::memory::MemoryStreamStore;
        use bytes::Bytes;

        let now = Utc::now();
        let mut stream_store = MemoryStreamStore::new(200);
        // 50 ancient entries (older than lookback) then 30 recent ones.
        for i in 0..50 {
            stream_store
                .append_entry(&NormalizedEntry {
                    signal: Signal::Traces,
                    observed_at: now - chrono::Duration::hours(24) - chrono::Duration::seconds(i),
                    service_name: None,
                    payload: Bytes::from_static(b"{}"),
                })
                .await
                .unwrap();
        }
        for i in 0..30 {
            stream_store
                .append_entry(&NormalizedEntry {
                    signal: Signal::Traces,
                    observed_at: now - chrono::Duration::seconds(i),
                    service_name: None,
                    payload: Bytes::from_static(b"{}"),
                })
                .await
                .unwrap();
        }

        let viewer_store = ViewerStore::Memory(MemoryViewerStore::new());
        let mut runtime = ViewerRuntime::build(viewer_store, StreamStore::Memory(stream_store))
            .await
            .unwrap();

        let def_id = Uuid::new_v4();
        let mut def = sample_definition(def_id, "lookback");
        def.lookback_ms = 60_000; // 1 minute - excludes all the ancient entries
        runtime.add_viewer(def).await.unwrap();

        let (_, state) = runtime.get_by_id(def_id).unwrap();
        // Only the 30 recent entries are kept; the 50 ancient ones never enter
        // state.entries (they would have ballooned memory before being pruned).
        assert_eq!(state.entries.len(), 30);
    }

    #[tokio::test]
    async fn viewer_index_lookup_is_consistent_after_updates() {
        let mut runtime = empty_runtime().await;
        let a = Uuid::new_v4();
        runtime.add_viewer(sample_definition(a, "a")).await.unwrap();

        assert!(runtime.update_viewer_name(a, "renamed"));
        let (viewer, _) = runtime.get_by_id(a).unwrap();
        assert_eq!(viewer.definition().name, "renamed");

        assert!(runtime.update_viewer_definition(
            a,
            json!({ "kind": "table", "signal": "traces" }),
            json!({})
        ));

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
