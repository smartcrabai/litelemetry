use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// OTel signal の種別
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
}

impl Signal {
    /// signal_mask のビット値を返す
    pub const fn as_mask(self) -> u32 {
        match self {
            Signal::Traces => 0b001,
            Signal::Metrics => 0b010,
            Signal::Logs => 0b100,
        }
    }

    /// 全 signal の配列を返す
    pub const fn all() -> [Signal; 3] {
        [Signal::Traces, Signal::Metrics, Signal::Logs]
    }
}

/// Signal のビットマスクを表す型安全なラッパー
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalMask(u32);

impl SignalMask {
    pub const NONE: Self = SignalMask(0);
    /// 有効なシグナルビット (Traces | Metrics | Logs) のみを残すマスク
    const VALID_BITS: u32 =
        Signal::Traces.as_mask() | Signal::Metrics.as_mask() | Signal::Logs.as_mask();

    pub fn contains(self, signal: Signal) -> bool {
        self.0 & signal.as_mask() != 0
    }

    /// 生の u32 値から SignalMask を生成する。
    /// 有効なシグナルビット (bits 0-2) 以外は無視される。
    pub fn from_raw(v: u32) -> Self {
        SignalMask(v & Self::VALID_BITS)
    }

    pub fn raw(self) -> u32 {
        self.0
    }

    /// 有効なシグナルビットがひとつも立っていない場合に true を返す。
    pub fn is_empty(self) -> bool {
        self.0 & Self::VALID_BITS == 0
    }
}

impl From<Signal> for SignalMask {
    fn from(s: Signal) -> Self {
        SignalMask(s.as_mask())
    }
}

impl std::ops::BitOr for Signal {
    type Output = SignalMask;
    fn bitor(self, rhs: Signal) -> SignalMask {
        SignalMask(self.as_mask() | rhs.as_mask())
    }
}

impl std::ops::BitOr<Signal> for SignalMask {
    type Output = SignalMask;
    fn bitor(self, rhs: Signal) -> SignalMask {
        SignalMask(self.0 | rhs.as_mask())
    }
}

/// Redis stream に書き込む正規化済みエントリ
#[derive(Debug, Clone)]
pub struct NormalizedEntry {
    pub signal: Signal,
    pub observed_at: DateTime<Utc>,
    /// resource.service.name があれば Some
    pub service_name: Option<String>,
    /// 元の OTLP バイナリ payload
    pub payload: Bytes,
}
