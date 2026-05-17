//! Synthetic OTLP protobuf fixture encoders.
//!
//! These helpers own OTLP protobuf construction for benchmark and test payloads.
//! Callers own workload selection, pacing, reporting, and profile validation.

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    metrics::v1::{
        metric, number_data_point, AggregationTemporality, Gauge, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum,
    },
    resource::v1::Resource,
    trace::v1::{span, status, ResourceSpans, ScopeSpans, Span, Status},
};
use prost::Message;

/// OTLP protobuf HTTP content type.
pub const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Synthetic fixture profile.
///
/// This describes payload shape only. Benchmark duration, pacing, byte mix,
/// pass/fail criteria, and reporting belong in the benchmark harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixtureProfile {
    /// Number of service/resource groups.
    pub service_count: usize,
    /// Number of log records per service.
    pub log_record_count: usize,
    /// Deterministic bytes appended to each log body.
    pub log_body_bytes: usize,
    /// Number of spans per service.
    pub trace_span_count: usize,
    /// Deterministic bytes in each trace payload sample attribute.
    pub trace_attribute_bytes: usize,
    /// Number of gauge and sum series per service.
    pub metric_series_count: usize,
    /// Deterministic bytes in each metric description.
    pub metric_description_bytes: usize,
    /// Scenario name added as fixture metadata.
    pub scenario_name: String,
    /// Deployment environment resource attribute value.
    pub deployment_environment: String,
    /// Instrumentation scope name.
    pub scope_name: String,
    /// Instrumentation scope version.
    pub scope_version: String,
    /// Deterministic seed for generated strings and IDs.
    pub deterministic_seed: u64,
    /// Service name for service index 0.
    pub primary_service_name: String,
    /// Prefix for service indexes greater than 0.
    pub service_name_prefix: String,
    /// Route attribute value.
    pub route: String,
    /// Prefix for generated log body text.
    pub log_message_prefix: String,
    /// Span name for generated trace spans.
    pub trace_span_name: String,
    /// Gauge metric name.
    pub metric_gauge_name: String,
    /// Sum metric name.
    pub metric_sum_name: String,
}

impl Default for FixtureProfile {
    fn default() -> Self {
        Self {
            service_count: 1,
            log_record_count: 8,
            log_body_bytes: 120_000,
            trace_span_count: 16,
            trace_attribute_bytes: 48_000,
            metric_series_count: 40,
            metric_description_bytes: 192,
            scenario_name: "throughput-iteration".to_string(),
            deployment_environment: "bench".to_string(),
            scope_name: "throughput_iteration".to_string(),
            scope_version: "0.0.0".to_string(),
            deterministic_seed: 0xCA4A_D57A_C5AC,
            primary_service_name: "bench-checkout".to_string(),
            service_name_prefix: "bench-service".to_string(),
            route: "/bench".to_string(),
            log_message_prefix: "canardstack-v0-iteration".to_string(),
            trace_span_name: "GET /bench".to_string(),
            metric_gauge_name: "canardstack.bench.gauge".to_string(),
            metric_sum_name: "canardstack.bench.sum".to_string(),
        }
    }
}

/// Encode a synthetic OTLP logs export request.
pub fn encode_logs(profile: &FixtureProfile, base_nanos: i64) -> Vec<u8> {
    ExportLogsServiceRequest {
        resource_logs: (0..profile.service_count)
            .map(|service_idx| ResourceLogs {
                resource: Some(resource(profile, service_idx)),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope(profile)),
                    log_records: (0..profile.log_record_count as i64)
                        .map(|idx| {
                            let logical_idx = service_idx as i64 * 1_000 + idx;
                            let nanos = nanos(base_nanos, logical_idx);
                            LogRecord {
                                time_unix_nano: nanos,
                                observed_time_unix_nano: nanos,
                                severity_number: SeverityNumber::Info as i32,
                                severity_text: "INFO".to_string(),
                                body: Some(any_str(format!(
                                    "{} log event {} {}",
                                    profile.log_message_prefix,
                                    logical_idx,
                                    deterministic_ascii(
                                        profile.deterministic_seed,
                                        logical_idx as u64,
                                        profile.log_body_bytes,
                                    )
                                ))),
                                attributes: vec![
                                    kv_str("http.route", &profile.route),
                                    kv_str("workload.id", &profile.scenario_name),
                                ],
                                trace_id: deterministic_bytes(
                                    profile.deterministic_seed,
                                    logical_idx as u64,
                                    16,
                                ),
                                span_id: deterministic_bytes(
                                    profile.deterministic_seed,
                                    logical_idx as u64 + 10_000,
                                    8,
                                ),
                                ..Default::default()
                            }
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
    .encode_to_vec()
}

/// Encode a synthetic OTLP traces export request.
pub fn encode_traces(profile: &FixtureProfile, base_nanos: i64) -> Vec<u8> {
    ExportTraceServiceRequest {
        resource_spans: (0..profile.service_count)
            .map(|service_idx| ResourceSpans {
                resource: Some(resource(profile, service_idx)),
                scope_spans: vec![ScopeSpans {
                    scope: Some(scope(profile)),
                    spans: (0..profile.trace_span_count as i64)
                        .map(|idx| {
                            let logical_idx = service_idx as i64 * 10_000 + idx;
                            let start = nanos(base_nanos, logical_idx);
                            Span {
                                trace_id: deterministic_bytes(
                                    profile.deterministic_seed,
                                    service_idx as u64,
                                    16,
                                ),
                                span_id: deterministic_bytes(
                                    profile.deterministic_seed,
                                    logical_idx as u64 + 20_000,
                                    8,
                                ),
                                name: profile.trace_span_name.clone(),
                                kind: span::SpanKind::Server as i32,
                                start_time_unix_nano: start,
                                end_time_unix_nano: start
                                    + 12_000_000
                                    + (idx % 17) as u64 * 1_000_000,
                                attributes: vec![
                                    kv_str("http.request.method", "GET"),
                                    kv_i64("http.response.status_code", 200),
                                    kv_str("http.route", &profile.route),
                                    kv_str("workload.bucket", format!("bucket-{}", idx % 16)),
                                    kv_str(
                                        "payload.sample",
                                        deterministic_ascii(
                                            profile.deterministic_seed,
                                            logical_idx as u64 + 1_000,
                                            profile.trace_attribute_bytes,
                                        ),
                                    ),
                                ],
                                status: Some(Status {
                                    code: status::StatusCode::Ok as i32,
                                    message: String::new(),
                                }),
                                ..Default::default()
                            }
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
    .encode_to_vec()
}

/// Encode a synthetic OTLP metrics export request with gauge and sum metrics.
pub fn encode_metrics(profile: &FixtureProfile, base_nanos: i64) -> Vec<u8> {
    ExportMetricsServiceRequest {
        resource_metrics: (0..profile.service_count)
            .map(|service_idx| ResourceMetrics {
                resource: Some(resource(profile, service_idx)),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope(profile)),
                    metrics: vec![
                        Metric {
                            name: profile.metric_gauge_name.clone(),
                            description: deterministic_ascii(
                                profile.deterministic_seed,
                                30_000 + service_idx as u64,
                                profile.metric_description_bytes,
                            ),
                            unit: "1".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: (0..profile.metric_series_count as i64)
                                    .map(|idx| {
                                        number_point(
                                            base_nanos,
                                            service_idx,
                                            idx,
                                            &profile.route,
                                            "gauge",
                                            NumberValue::Double(100.0 + (idx % 23) as f64),
                                        )
                                    })
                                    .collect(),
                            })),
                            metadata: Vec::new(),
                        },
                        Metric {
                            name: profile.metric_sum_name.clone(),
                            description: deterministic_ascii(
                                profile.deterministic_seed,
                                40_000 + service_idx as u64,
                                profile.metric_description_bytes,
                            ),
                            unit: "1".to_string(),
                            data: Some(metric::Data::Sum(Sum {
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                                is_monotonic: true,
                                data_points: (0..profile.metric_series_count as i64)
                                    .map(|idx| {
                                        number_point(
                                            base_nanos,
                                            service_idx,
                                            idx,
                                            &profile.route,
                                            "sum",
                                            NumberValue::Int(10_000 + idx),
                                        )
                                    })
                                    .collect(),
                            })),
                            metadata: Vec::new(),
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
    .encode_to_vec()
}

fn kv_str(key: impl Into<String>, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(any_str(value)),
    }
}

fn kv_i64(key: impl Into<String>, value: i64) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn resource(profile: &FixtureProfile, service_idx: usize) -> Resource {
    Resource {
        attributes: vec![
            kv_str("service.name", service_name(profile, service_idx)),
            kv_str("deployment.environment", &profile.deployment_environment),
            kv_str("benchmark.scenario", &profile.scenario_name),
        ],
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    }
}

fn scope(profile: &FixtureProfile) -> InstrumentationScope {
    InstrumentationScope {
        name: profile.scope_name.clone(),
        version: profile.scope_version.clone(),
        attributes: Vec::new(),
        dropped_attributes_count: 0,
    }
}

fn any_str(value: impl Into<String>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.into())),
    }
}

enum NumberValue {
    Double(f64),
    Int(i64),
}

fn number_point(
    base_nanos: i64,
    service_idx: usize,
    idx: i64,
    route: &str,
    series_prefix: &str,
    value: NumberValue,
) -> NumberDataPoint {
    NumberDataPoint {
        attributes: vec![
            kv_str("route", route),
            kv_str("series", format!("{series_prefix}-{service_idx}-{idx}")),
        ],
        start_time_unix_nano: nanos(base_nanos, 0),
        time_unix_nano: nanos(base_nanos, service_idx as i64 * 10_000 + idx),
        exemplars: Vec::new(),
        flags: 0,
        value: Some(match value {
            NumberValue::Double(value) => number_data_point::Value::AsDouble(value),
            NumberValue::Int(value) => number_data_point::Value::AsInt(value),
        }),
    }
}

fn nanos(base_nanos: i64, idx: i64) -> u64 {
    (base_nanos + idx * 1_000_000) as u64
}

fn service_name(profile: &FixtureProfile, service_idx: usize) -> String {
    if service_idx == 0 {
        profile.primary_service_name.clone()
    } else {
        format!("{}-{service_idx}", profile.service_name_prefix)
    }
}

fn deterministic_ascii(seed: u64, item_seed: u64, len: usize) -> String {
    let mut state = seed ^ item_seed;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state ^= state << 7;
        state ^= state >> 9;
        state ^= state << 8;
        let ch = b'a' + (state % 26) as u8;
        out.push(ch as char);
    }
    out
}

fn deterministic_bytes(seed: u64, item_seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed ^ item_seed;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        out.push((state & 0xff) as u8);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        common::v1::any_value,
        metrics::v1::metric,
    };

    #[test]
    fn encodes_logs_fixture() {
        let profile = small_profile();
        let bytes = encode_logs(&profile, 1_700_000_000_000_000_000);
        let request = ExportLogsServiceRequest::decode(bytes.as_slice()).unwrap();

        assert_eq!(request.resource_logs.len(), 2);
        assert_eq!(request.resource_logs[0].scope_logs[0].log_records.len(), 3);
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(record.trace_id.len(), 16);
        assert_eq!(record.span_id.len(), 8);
        assert!(matches!(
            record.body.as_ref().unwrap().value.as_ref().unwrap(),
            any_value::Value::StringValue(value) if value.starts_with("canardstack-v0-iteration")
        ));
    }

    #[test]
    fn encodes_traces_fixture() {
        let profile = small_profile();
        let bytes = encode_traces(&profile, 1_700_000_000_000_000_000);
        let request = ExportTraceServiceRequest::decode(bytes.as_slice()).unwrap();

        assert_eq!(request.resource_spans.len(), 2);
        assert_eq!(request.resource_spans[1].scope_spans[0].spans.len(), 4);
        let span = &request.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.trace_id.len(), 16);
        assert_eq!(span.span_id.len(), 8);
        assert_eq!(span.name, "GET /bench");
        assert_eq!(span.attributes.len(), 5);
    }

    #[test]
    fn encodes_metrics_fixture() {
        let profile = small_profile();
        let bytes = encode_metrics(&profile, 1_700_000_000_000_000_000);
        let request = ExportMetricsServiceRequest::decode(bytes.as_slice()).unwrap();

        assert_eq!(request.resource_metrics.len(), 2);
        let metrics = &request.resource_metrics[0].scope_metrics[0].metrics;
        assert_eq!(metrics.len(), 2);
        assert!(matches!(metrics[0].data, Some(metric::Data::Gauge(_))));
        assert!(matches!(metrics[1].data, Some(metric::Data::Sum(_))));
    }

    #[test]
    fn fixture_encoding_is_deterministic() {
        let profile = small_profile();
        let first = encode_traces(&profile, 1_700_000_000_000_000_000);
        let second = encode_traces(&profile, 1_700_000_000_000_000_000);

        assert_eq!(first, second);
    }

    fn small_profile() -> FixtureProfile {
        FixtureProfile {
            service_count: 2,
            log_record_count: 3,
            log_body_bytes: 16,
            trace_span_count: 4,
            trace_attribute_bytes: 12,
            metric_series_count: 5,
            metric_description_bytes: 8,
            scope_version: "test-version".to_string(),
            ..FixtureProfile::default()
        }
    }
}
