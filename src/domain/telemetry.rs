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
}

/// Signal のビットマスクを表す型安全なラッパー
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalMask(u32);

impl SignalMask {
    pub const NONE: Self = SignalMask(0);

    pub fn contains(self, signal: Signal) -> bool {
        self.0 & signal.as_mask() != 0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
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
