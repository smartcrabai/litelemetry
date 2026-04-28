use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// OTel signal type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    Traces,
    Metrics,
    Logs,
}

impl Signal {
    /// Returns the bit value for signal_mask
    pub const fn as_mask(self) -> u32 {
        match self {
            Signal::Traces => 0b001,
            Signal::Metrics => 0b010,
            Signal::Logs => 0b100,
        }
    }

    /// Returns an array of all signals
    pub const fn all() -> [Signal; 3] {
        [Signal::Traces, Signal::Metrics, Signal::Logs]
    }
}

/// Type-safe wrapper representing a bitmask of Signals
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalMask(u32);

impl SignalMask {
    pub const NONE: Self = SignalMask(0);
    /// Mask that retains only valid signal bits (Traces | Metrics | Logs)
    const VALID_BITS: u32 =
        Signal::Traces.as_mask() | Signal::Metrics.as_mask() | Signal::Logs.as_mask();

    pub fn contains(self, signal: Signal) -> bool {
        self.0 & signal.as_mask() != 0
    }

    /// Creates a SignalMask from a raw u32 value.
    /// Bits outside the valid signal bits (bits 0-2) are ignored.
    pub fn from_raw(v: u32) -> Self {
        SignalMask(v & Self::VALID_BITS)
    }

    pub fn raw(self) -> u32 {
        self.0
    }

    /// Returns true if no valid signal bits are set.
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

pub const SERVICE_NAME_ATTRIBUTE: &str = "service.name";

/// Normalized entry written to the Redis stream
#[derive(Debug, Clone)]
pub struct NormalizedEntry {
    pub signal: Signal,
    pub observed_at: DateTime<Utc>,
    /// Some if resource.service.name is present
    pub service_name: Option<String>,
    /// Original OTLP binary payload
    pub payload: Bytes,
}
