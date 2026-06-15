use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        exponential_histogram_data_point, metric::Data, number_data_point, summary_data_point,
        Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
        SummaryDataPoint,
    },
    resource::v1::Resource,
    trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status},
};
use otlp2records::{transform_logs, transform_metrics, transform_traces, InputFormat};
use prost::Message;

fn attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn bytes16() -> Vec<u8> {
    (0..16).collect()
}

fn bytes8() -> Vec<u8> {
    (0..8).collect()
}

#[test]
fn transform_apis_emit_normalized_schema() {
    let logs = log_request().encode_to_vec();
    let traces = trace_request().encode_to_vec();
    let metrics = metrics_request().encode_to_vec();

    assert!(transform_logs(&logs, InputFormat::Protobuf)
        .unwrap()
        .schema()
        .field_with_name("time_unix_nano")
        .is_ok());
    assert!(transform_traces(&traces, InputFormat::Protobuf)
        .unwrap()
        .schema()
        .field_with_name("start_time_unix_nano")
        .is_ok());
    assert!(transform_metrics(&metrics, InputFormat::Protobuf)
        .unwrap()
        .gauge
        .unwrap()
        .schema()
        .field_with_name("double_value")
        .is_ok());
}

fn log_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "svc")],
                dropped_attributes_count: 2,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "scope".to_string(),
                    version: "1".to_string(),
                    attributes: vec![attr("scope.key", "scope-value")],
                    dropped_attributes_count: 3,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    observed_time_unix_nano: 1_700_000_000_000_000_100,
                    trace_id: bytes16(),
                    span_id: bytes8(),
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("hello".to_string())),
                    }),
                    attributes: vec![attr("log.key", "log-value")],
                    dropped_attributes_count: 4,
                    flags: 1,
                    event_name: "login".to_string(),
                }],
                schema_url: "scope-schema".to_string(),
            }],
            schema_url: "resource-schema".to_string(),
        }],
    }
}

fn trace_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "svc")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "scope".to_string(),
                    version: "1".to_string(),
                    attributes: vec![attr("scope.key", "scope-value")],
                    dropped_attributes_count: 0,
                }),
                spans: vec![Span {
                    trace_id: bytes16(),
                    span_id: bytes8(),
                    parent_span_id: vec![8; 8],
                    name: "span".to_string(),
                    kind: 1,
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_000_010_000,
                    attributes: vec![attr("span.key", "span-value")],
                    events: vec![span::Event {
                        time_unix_nano: 1_700_000_000_000_000_500,
                        name: "event".to_string(),
                        attributes: vec![attr("event.key", "event-value")],
                        dropped_attributes_count: 0,
                    }],
                    links: vec![span::Link {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        trace_state: "state".to_string(),
                        attributes: vec![attr("link.key", "link-value")],
                        dropped_attributes_count: 0,
                        flags: 0,
                    }],
                    status: Some(Status {
                        message: "ok".to_string(),
                        code: 1,
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn number_point(value: f64, attr_key: &str, exemplars: Vec<Exemplar>) -> NumberDataPoint {
    NumberDataPoint {
        attributes: vec![attr(attr_key, "value")],
        start_time_unix_nano: 1,
        time_unix_nano: 2,
        value: Some(number_data_point::Value::AsDouble(value)),
        exemplars,
        flags: 1,
    }
}

fn exemplar() -> Exemplar {
    Exemplar {
        filtered_attributes: vec![attr("exemplar.key", "exemplar-value")],
        time_unix_nano: 3,
        span_id: bytes8(),
        trace_id: bytes16(),
        value: Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(7.0)),
    }
}

fn metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![attr("service.name", "svc")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "scope".to_string(),
                    ..Default::default()
                }),
                metrics: vec![
                    Metric {
                        name: "gauge".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![number_point(1.0, "gauge.attr", vec![exemplar()])],
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "sum".to_string(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![number_point(2.0, "sum.attr", vec![])],
                            aggregation_temporality: 2,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "histogram".to_string(),
                        data: Some(Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![attr("hist.attr", "value")],
                                start_time_unix_nano: 1,
                                time_unix_nano: 2,
                                count: 3,
                                sum: Some(4.0),
                                bucket_counts: vec![1, 2],
                                explicit_bounds: vec![5.0],
                                exemplars: vec![exemplar()],
                                flags: 1,
                                min: Some(1.0),
                                max: Some(5.0),
                            }],
                            aggregation_temporality: 2,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "exp_histogram".to_string(),
                        data: Some(Data::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint {
                                attributes: vec![attr("exp.attr", "value")],
                                start_time_unix_nano: 1,
                                time_unix_nano: 2,
                                count: 3,
                                sum: Some(4.0),
                                scale: 1,
                                zero_count: 0,
                                positive: Some(exponential_histogram_data_point::Buckets {
                                    offset: 0,
                                    bucket_counts: vec![1, 2],
                                }),
                                negative: None,
                                flags: 1,
                                exemplars: vec![exemplar()],
                                min: Some(1.0),
                                max: Some(5.0),
                                zero_threshold: 0.25,
                            }],
                            aggregation_temporality: 2,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        name: "summary".to_string(),
                        data: Some(Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: vec![attr("summary.attr", "value")],
                                start_time_unix_nano: 1,
                                time_unix_nano: 2,
                                count: 3,
                                sum: 4.0,
                                quantile_values: vec![
                                    summary_data_point::ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 2.0,
                                    },
                                    summary_data_point::ValueAtQuantile {
                                        quantile: 0.99,
                                        value: 3.0,
                                    },
                                ],
                                flags: 1,
                            }],
                        })),
                        ..Default::default()
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}
