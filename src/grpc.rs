use std::sync::Arc;

use buffa::view::OwnedView;
use chrono::Utc;
use connectrpc::{ConnectError, RequestContext, Response, Router as ConnectRouter};

use crate::domain::telemetry::{NormalizedEntry, SERVICE_NAME_ATTRIBUTE, Signal};
use crate::otlp_proto::opentelemetry::proto::collector::logs::v1::{
    __buffa::view::ExportLogsServiceRequestView, ExportLogsServiceResponse, LogsService,
    LogsServiceExt,
};
use crate::otlp_proto::opentelemetry::proto::collector::metrics::v1::{
    __buffa::view::ExportMetricsServiceRequestView, ExportMetricsServiceResponse, MetricsService,
    MetricsServiceExt,
};
use crate::otlp_proto::opentelemetry::proto::collector::trace::v1::{
    __buffa::view::ExportTraceServiceRequestView, ExportTraceServiceResponse, TraceService,
    TraceServiceExt,
};
use crate::otlp_proto::opentelemetry::proto::common::v1::__buffa::view::oneof::any_value;
use crate::otlp_proto::opentelemetry::proto::resource::v1::__buffa::view::ResourceView;
use crate::server::AppState;

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
                ConnectError::internal("ingest failed")
            })
    }
}

impl TraceService for OtlpService {
    #[allow(refining_impl_trait)]
    async fn export(
        &self,
        _ctx: RequestContext,
        request: OwnedView<ExportTraceServiceRequestView<'static>>,
    ) -> Result<Response<ExportTraceServiceResponse>, ConnectError> {
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
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

impl MetricsService for OtlpService {
    #[allow(refining_impl_trait)]
    async fn export(
        &self,
        _ctx: RequestContext,
        request: OwnedView<ExportMetricsServiceRequestView<'static>>,
    ) -> Result<Response<ExportMetricsServiceResponse>, ConnectError> {
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
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

impl LogsService for OtlpService {
    #[allow(refining_impl_trait)]
    async fn export(
        &self,
        _ctx: RequestContext,
        request: OwnedView<ExportLogsServiceRequestView<'static>>,
    ) -> Result<Response<ExportLogsServiceResponse>, ConnectError> {
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
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

fn service_name_from_resource(resource: Option<&ResourceView<'_>>) -> Option<String> {
    let resource = resource?;
    for attribute in resource.attributes.iter() {
        if attribute.key != SERVICE_NAME_ATTRIBUTE {
            continue;
        }
        let any_value = attribute.value.as_option()?;
        if let Some(any_value::Value::StringValue(s)) = &any_value.value {
            return Some((*s).to_string());
        }
    }
    None
}
