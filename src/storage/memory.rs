use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::domain::viewer::ViewerDefinition;
use crate::storage::StorageError;
use crate::storage::postgres::ViewerSnapshotRow;
use crate::storage::redis::cmp_stream_id;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

struct MemoryStreams {
    data: HashMap<Signal, VecDeque<(String, NormalizedEntry)>>,
    /// signal ごとの単調増加シーケンス番号 (ID 生成用)
    seq: HashMap<Signal, u64>,
}

impl MemoryStreams {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            seq: HashMap::new(),
        }
    }

    /// `{timestamp_ms}-{seq}` 形式の ID を生成する
    fn next_id(&mut self, signal: Signal) -> String {
        let ms = chrono::Utc::now().timestamp_millis() as u64;
        let seq = self.seq.entry(signal).or_insert(0);
        let id = format!("{ms}-{seq}");
        *seq += 1;
        id
    }
}

/// インメモリ stream ストア
#[derive(Clone)]
pub struct MemoryStreamStore {
    inner: Arc<Mutex<MemoryStreams>>,
    max_entries: usize,
}

impl MemoryStreamStore {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryStreams::new())),
            max_entries,
        }
    }

    pub async fn append_entry(&mut self, entry: &NormalizedEntry) -> Result<String, StorageError> {
        let mut guard = self.inner.lock().await;
        let id = guard.next_id(entry.signal);
        let deque = guard.data.entry(entry.signal).or_default();
        deque.push_back((id.clone(), entry.clone()));
        while deque.len() > self.max_entries {
            deque.pop_front();
        }
        Ok(id)
    }

    pub async fn read_entries_since(
        &self,
        signal: Signal,
        cursor: Option<&str>,
        count: usize,
    ) -> Result<Vec<(String, NormalizedEntry)>, StorageError> {
        let guard = self.inner.lock().await;
        let Some(deque) = guard.data.get(&signal) else {
            return Ok(Vec::new());
        };
        let start = if let Some(c) = cursor {
            deque.partition_point(|(id, _)| cmp_stream_id(id, c).is_le())
        } else {
            0
        };
        let entries = deque
            .iter()
            .skip(start)
            .take(count)
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect();
        Ok(entries)
    }
}

struct MemoryViewerData {
    definitions: Vec<ViewerDefinition>,
    snapshots: HashMap<Uuid, ViewerSnapshotRow>,
}

impl MemoryViewerData {
    fn new() -> Self {
        Self {
            definitions: Vec::new(),
            snapshots: HashMap::new(),
        }
    }
}

/// インメモリ viewer ストア
#[derive(Clone)]
pub struct MemoryViewerStore {
    inner: Arc<Mutex<MemoryViewerData>>,
}

impl Default for MemoryViewerStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryViewerStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MemoryViewerData::new())),
        }
    }

    pub async fn insert_viewer_definition(
        &self,
        def: &ViewerDefinition,
    ) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        guard.definitions.push(def.clone());
        Ok(())
    }

    pub async fn load_viewer_definitions(&self) -> Result<Vec<ViewerDefinition>, StorageError> {
        let guard = self.inner.lock().await;
        Ok(guard
            .definitions
            .iter()
            .filter(|d| d.enabled)
            .cloned()
            .collect())
    }

    pub async fn load_snapshots(
        &self,
        viewer_ids: &[Uuid],
    ) -> Result<Vec<ViewerSnapshotRow>, StorageError> {
        let guard = self.inner.lock().await;
        let snapshots = viewer_ids
            .iter()
            .filter_map(|id| guard.snapshots.get(id).cloned())
            .collect();
        Ok(snapshots)
    }

    pub async fn update_viewer_definition_json(
        &self,
        id: Uuid,
        definition_json: &Value,
        layout_json: &Value,
    ) -> Result<bool, StorageError> {
        let mut guard = self.inner.lock().await;
        if let Some(def) = guard.definitions.iter_mut().find(|d| d.id == id) {
            def.definition_json = definition_json.clone();
            def.layout_json = layout_json.clone();
            def.revision += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn upsert_snapshot(&self, snapshot: &ViewerSnapshotRow) -> Result<(), StorageError> {
        let mut guard = self.inner.lock().await;
        guard.snapshots.insert(snapshot.viewer_id, snapshot.clone());
        Ok(())
    }
}

/// スタンドアロンモードで起動時に挿入するデフォルト viewer 定義
pub fn default_viewer_definitions() -> Vec<ViewerDefinition> {
    let lookback_ms = 5 * 60 * 1_000_i64;
    vec![
        ViewerDefinition {
            id: Uuid::new_v4(),
            slug: "all-traces".to_string(),
            name: "All Traces".to_string(),
            refresh_interval_ms: 1_000,
            lookback_ms,
            signal_mask: Signal::Traces.into(),
            definition_json: serde_json::json!({"kind": "table", "signal": "traces"}),
            layout_json: serde_json::json!({"default_view": "table"}),
            revision: 1,
            enabled: true,
        },
        ViewerDefinition {
            id: Uuid::new_v4(),
            slug: "all-metrics".to_string(),
            name: "All Metrics".to_string(),
            refresh_interval_ms: 1_000,
            lookback_ms,
            signal_mask: Signal::Metrics.into(),
            definition_json: serde_json::json!({"kind": "table", "signal": "metrics"}),
            layout_json: serde_json::json!({"default_view": "table"}),
            revision: 1,
            enabled: true,
        },
        ViewerDefinition {
            id: Uuid::new_v4(),
            slug: "all-logs".to_string(),
            name: "All Logs".to_string(),
            refresh_interval_ms: 1_000,
            lookback_ms,
            signal_mask: Signal::Logs.into(),
            definition_json: serde_json::json!({"kind": "table", "signal": "logs"}),
            layout_json: serde_json::json!({"default_view": "table"}),
            revision: 1,
            enabled: true,
        },
    ]
}
