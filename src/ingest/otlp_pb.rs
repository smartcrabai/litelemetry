use buffa::view::OwnedView;
use bytes::Bytes;

use crate::domain::telemetry::Signal;
use crate::otlp_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequestView;
use crate::otlp_proto::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequestView;
use crate::otlp_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequestView;

/// Decode an OTLP protobuf request body for `signal` and return its
/// OTLP-canonical JSON representation. Returns `None` if the body is not
/// valid protobuf for the expected message type.
pub fn payload_to_json_value(signal: Signal, body: &Bytes) -> Option<serde_json::Value> {
    match signal {
        Signal::Traces => {
            let view =
                OwnedView::<ExportTraceServiceRequestView<'static>>::decode(body.clone()).ok()?;
            serde_json::to_value(view.to_owned_message()).ok()
        }
        Signal::Metrics => {
            let view =
                OwnedView::<ExportMetricsServiceRequestView<'static>>::decode(body.clone()).ok()?;
            serde_json::to_value(view.to_owned_message()).ok()
        }
        Signal::Logs => {
            let view =
                OwnedView::<ExportLogsServiceRequestView<'static>>::decode(body.clone()).ok()?;
            serde_json::to_value(view.to_owned_message()).ok()
        }
    }
}

pub fn payload_as_value(signal: Signal, payload: &Bytes) -> Option<serde_json::Value> {
    if looks_like_json(payload)
        && let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload)
    {
        return Some(value);
    }
    payload_to_json_value(signal, payload)
}

fn looks_like_json(payload: &Bytes) -> bool {
    payload
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| matches!(*b, b'{' | b'['))
}
