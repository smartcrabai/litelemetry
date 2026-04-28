fn main() {
    connectrpc_build::Config::new()
        .files(&[
            "proto/opentelemetry/proto/common/v1/common.proto",
            "proto/opentelemetry/proto/resource/v1/resource.proto",
            "proto/opentelemetry/proto/trace/v1/trace.proto",
            "proto/opentelemetry/proto/metrics/v1/metrics.proto",
            "proto/opentelemetry/proto/logs/v1/logs.proto",
            "proto/opentelemetry/proto/collector/trace/v1/trace_service.proto",
            "proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
            "proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
        ])
        .includes(&["proto/"])
        .include_file("_otlp.rs")
        .compile()
        .unwrap();
    println!("cargo:rerun-if-changed=proto");
}
