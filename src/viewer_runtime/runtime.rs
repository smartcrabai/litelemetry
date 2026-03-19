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

/// viewer のインメモリランタイム。
/// - `build()` でストア定義 + snapshot + stream 差分から初期状態を構築する。
/// - `apply_diff_batch()` でストリームから差分を取得し全 viewer にファンアウトする。
pub struct ViewerRuntime {
    viewers: Vec<(CompiledViewer, ViewerState)>,
    stream_store: StreamStore,
    viewer_store: ViewerStore,
}

impl ViewerRuntime {
    /// ストア + ストリームから初期状態を構築する。
    ///
    /// signal ごとに 1 回だけストリームを読み、全 viewer にファンアウトする。
    pub async fn build(
        viewer_store: ViewerStore,
        mut stream_store: StreamStore,
    ) -> Result<Self, RuntimeError> {
        let definitions = viewer_store.load_viewer_definitions().await?;

        // 全 viewer の snapshot を一括取得して HashMap に格納 (N+1 クエリ回避)
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

            // snapshot の revision が一致すればそのカーソルから resume
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

        // signal ごとに 1 回だけストリームを読み、全 viewer にファンアウト
        let now = Utc::now();
        for signal in Signal::all() {
            fan_out_signal_entries(&mut viewers, &mut stream_store, signal).await?;
        }

        // lookback を超えたエントリを prune
        for (viewer, state) in &mut viewers {
            prune_stale_buckets(state, viewer.lookback_ms(), now);
        }

        Ok(Self {
            viewers,
            stream_store,
            viewer_store,
        })
    }

    /// 全 viewer の差分をストリームから取得して更新する。
    ///
    /// 同じ signal を監視する複数の viewer には、1 回の読み出し結果をファンアウトする。
    /// snapshot は更新後にストアへ upsert する。
    pub async fn apply_diff_batch(&mut self) -> Result<(), RuntimeError> {
        let now = Utc::now();

        // カーソル変化を検知するため事前にキャプチャ
        let prev_cursors: Vec<StreamCursor> = self
            .viewers
            .iter()
            .map(|(_, s)| s.last_cursor.clone())
            .collect();

        for signal in Signal::all() {
            fan_out_signal_entries(&mut self.viewers, &mut self.stream_store, signal).await?;
        }

        // lookback を超えたエントリを prune
        for (viewer, state) in &mut self.viewers {
            prune_stale_buckets(state, viewer.lookback_ms(), now);
        }

        // カーソルが進んだ viewer のみ snapshot を upsert (失敗しても次バッチで再試行されるので継続する)
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
        let now = Utc::now();

        for signal in Signal::all() {
            if !viewer.matches_signal(signal) {
                continue;
            }

            let entries = self
                .stream_store
                .read_entries_since(signal, None, 100_000)
                .await?;
            for (entry_id, entry) in entries {
                apply_entry(&mut state, &viewer, entry);
                state.last_cursor.set(signal, entry_id);
            }
        }

        prune_stale_buckets(&mut state, viewer.lookback_ms(), now);
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

    pub fn viewers(&self) -> &[(CompiledViewer, ViewerState)] {
        &self.viewers
    }
}

/// 指定 signal についてストリームを 1 回読み、全 viewer にエントリをファンアウトする。
async fn fan_out_signal_entries(
    viewers: &mut [(CompiledViewer, ViewerState)],
    stream_store: &mut StreamStore,
    signal: Signal,
) -> Result<(), RuntimeError> {
    if !viewers.iter().any(|(v, _)| v.matches_signal(signal)) {
        return Ok(());
    }

    // 全 viewer の中で最も古いカーソル (= 最も多くのエントリを読む必要がある) を使って読み出す。
    // None = 先頭から読む必要がある viewer が存在するため最小値として扱う。
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
            // viewer 固有のカーソルより後のエントリだけ適用
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
