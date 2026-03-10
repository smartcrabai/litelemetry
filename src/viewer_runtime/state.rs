use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::domain::viewer::ViewerStatus;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// signal 別の Redis Stream cursor
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamCursor {
    pub traces: Option<String>,
    pub metrics: Option<String>,
    pub logs: Option<String>,
}

impl StreamCursor {
    pub fn get(&self, signal: Signal) -> Option<&str> {
        match signal {
            Signal::Traces => self.traces.as_deref(),
            Signal::Metrics => self.metrics.as_deref(),
            Signal::Logs => self.logs.as_deref(),
        }
    }

    pub fn set(&mut self, signal: Signal, id: String) {
        match signal {
            Signal::Traces => self.traces = Some(id),
            Signal::Metrics => self.metrics = Some(id),
            Signal::Logs => self.logs = Some(id),
        }
    }
}

/// viewer のインメモリ実行時状態
#[derive(Debug, Clone)]
pub struct ViewerState {
    pub viewer_id: Uuid,
    pub revision: i64,
    /// 取り込み済みエントリ (時刻昇順)
    pub entries: Vec<NormalizedEntry>,
    pub last_cursor: StreamCursor,
    pub status: ViewerStatus,
}

impl ViewerState {
    pub fn new(viewer_id: Uuid, revision: i64) -> Self {
        Self {
            viewer_id,
            revision,
            entries: Vec::new(),
            last_cursor: StreamCursor::default(),
            status: ViewerStatus::Ok,
        }
    }
}
