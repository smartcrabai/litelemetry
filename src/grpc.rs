use std::sync::Arc;

use buffa::view::OwnedView;
use chrono::Utc;
use connectrpc::{ConnectError, Context, Router as ConnectRouter};

use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::otlp_proto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequestView, ExportLogsServiceResponse, LogsService, LogsServiceExt,
};
use crate::otlp_proto::opentelemetry::proto::collector::metrics::v1::{
    ExportMetricsServiceRequestView, ExportMetricsServiceResponse, MetricsService,
    MetricsServiceExt,
};
use crate::otlp_proto::opentelemetry::proto::collector::trace::v1::{
    ExportTraceServiceRequestView, ExportTraceServiceResponse, TraceService, TraceServiceExt,
};
use crate::otlp_proto::opentelemetry::proto::common::v1::any_value;
use crate::otlp_proto::opentelemetry::proto::resource::v1::ResourceView;
use crate::server::AppState;

const SERVICE_NAME_ATTRIBUTE: &str = "service.name";

pub fn build_connect_router(state: AppState) -> ConnectRouter {
    let service = Arc::new(OtlpService { state });
    let router = TraceServiceExt::register(service.clone(), ConnectRouter::new());
    let router = MetricsServiceExt::register(service.clone(), router);
    LogsServiceExt::register(service, router)
}

struct OtlpService {
    state: AppState,
}

impl OtlpService {
    async fn store(&self, entry: NormalizedEntry) -> Result<(), ConnectError> {
        let mut store = self.state.stream_store.clone();
        store
            .append_entry(&entry)
            .await
            .map(|_| ())
            .map_err(|error| {
                tracing::error!("stream append_entry failed: {error}");
                ConnectError::internal(format!("stream append_entry failed: {error}"))
            })
    }
}

impl TraceService for OtlpService {
    async fn export(
        &self,
        ctx: Context,
        request: OwnedView<ExportTraceServiceRequestView<'static>>,
    ) -> Result<(ExportTraceServiceResponse, Context), ConnectError> {
        let service_name = request
            .resource_spans
            .iter()
            .find_map(|rs| service_name_from_resource(rs.resource.as_option()));
        let payload = request.into_bytes();
        self.store(NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name,
            payload,
        })
        .await?;
        Ok((ExportTraceServiceResponse::default(), ctx))
    }
}

impl MetricsService for OtlpService {
    async fn export(
        &self,
        ctx: Context,
        request: OwnedView<ExportMetricsServiceRequestView<'static>>,
    ) -> Result<(ExportMetricsServiceResponse, Context), ConnectError> {
        let service_name = request
            .resource_metrics
            .iter()
            .find_map(|rm| service_name_from_resource(rm.resource.as_option()));
        let payload = request.into_bytes();
        self.store(NormalizedEntry {
            signal: Signal::Metrics,
            observed_at: Utc::now(),
            service_name,
            payload,
        })
        .await?;
        Ok((ExportMetricsServiceResponse::default(), ctx))
    }
}

impl LogsService for OtlpService {
    async fn export(
        &self,
        ctx: Context,
        request: OwnedView<ExportLogsServiceRequestView<'static>>,
    ) -> Result<(ExportLogsServiceResponse, Context), ConnectError> {
        let service_name = request
            .resource_logs
            .iter()
            .find_map(|rl| service_name_from_resource(rl.resource.as_option()));
        let payload = request.into_bytes();
        self.store(NormalizedEntry {
            signal: Signal::Logs,
            observed_at: Utc::now(),
            service_name,
            payload,
        })
        .await?;
        Ok((ExportLogsServiceResponse::default(), ctx))
    }
}

fn service_name_from_resource(resource: Option<&ResourceView<'_>>) -> Option<String> {
    let resource = resource?;
    for attribute in resource.attributes.iter() {
        if attribute.key != SERVICE_NAME_ATTRIBUTE {
            continue;
        }
        let any_value = attribute.value.as_option()?;
        if let Some(any_value::ValueView::StringValue(s)) = &any_value.value {
            return Some((*s).to_string());
        }
    }
    None
}
