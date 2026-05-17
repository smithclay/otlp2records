//! Public API behavior tests moved out of the crate facade.

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::metrics::v1::ExportMetricsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use otlp2records::*;
use prost::Message;

// ========================================================================
// Helper functions for creating test data
// ========================================================================

fn create_test_log_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "test-lib".to_string(),
                    version: "1.0.0".to_string(),
                    ..Default::default()
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    observed_time_unix_nano: 1_700_000_000_100_000_000,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "Test log message".to_string(),
                        )),
                    }),
                    attributes: vec![KeyValue {
                        key: "log.key".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("log-value".to_string())),
                        }),
                    }],
                    trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn create_test_trace_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "test-lib".to_string(),
                    version: "1.0.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                    parent_span_id: vec![],
                    name: "test-span".to_string(),
                    kind: 1, // INTERNAL
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_100_000_000,
                    attributes: vec![KeyValue {
                        key: "span.key".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("span-value".to_string())),
                        }),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn create_test_metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "test-lib".to_string(),
                    version: "1.0.0".to_string(),
                    ..Default::default()
                }),
                metrics: vec![
                    // Gauge metric
                    Metric {
                        name: "test.gauge".to_string(),
                        description: "A test gauge".to_string(),
                        unit: "1".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                start_time_unix_nano: 1_699_999_000_000_000_000,
                                value: Some(
                                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.5),
                                ),
                                attributes: vec![KeyValue {
                                    key: "metric.key".to_string(),
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            "metric-value".to_string(),
                                        )),
                                    }),
                                }],
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    },
                    // Sum metric
                    Metric {
                        name: "test.sum".to_string(),
                        description: "A test sum".to_string(),
                        unit: "bytes".to_string(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                start_time_unix_nano: 1_699_999_000_000_000_000,
                                value: Some(
                                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(100.0),
                                ),
                                attributes: vec![],
                                ..Default::default()
                            }],
                            aggregation_temporality: 2, // CUMULATIVE
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn create_gauge_only_metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope::default()),
                metrics: vec![Metric {
                    name: "test.gauge".to_string(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_700_000_000_000_000_000,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(1.0),
                            ),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn create_sum_only_metrics_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope::default()),
                metrics: vec![Metric {
                    name: "test.sum".to_string(),
                    data: Some(Data::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_700_000_000_000_000_000,
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(2.0),
                            ),
                            ..Default::default()
                        }],
                        aggregation_temporality: 1,
                        is_monotonic: false,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

// ========================================================================
// High-level API tests
// ========================================================================

#[derive(Default)]
struct TestObserver {
    phases: Vec<TransformPhase>,
    counters: Vec<TransformCounter>,
}

impl TransformObserver for TestObserver {
    fn on_phase(&mut self, timing: TransformPhaseTiming) {
        self.phases.push(timing.phase);
    }

    fn on_counter(&mut self, counter: TransformCounterValue) {
        self.counters.push(counter.counter);
    }
}

#[test]
fn test_transform_logs_protobuf() {
    let request = create_test_log_request();
    let bytes = request.encode_to_vec();

    let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert!(batch.num_columns() > 0);

    // Verify some expected columns exist
    let schema = batch.schema();
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("service_name").is_ok());
    assert!(schema.field_with_name("severity_number").is_ok());
}

#[test]
fn test_transform_logs_with_observer_matches_default() {
    let request = create_test_log_request();
    let bytes = request.encode_to_vec();

    let default_batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();
    let mut observer = TestObserver::default();
    let observed_batch =
        transform_logs_with_observer(&bytes, InputFormat::Protobuf, &mut observer).unwrap();

    assert_eq!(observed_batch.num_rows(), default_batch.num_rows());
    assert!(observer.phases.contains(&TransformPhase::ProtobufDecode));
    assert!(observer.phases.contains(&TransformPhase::ResourceLogsBuild));
    assert!(observer
        .phases
        .contains(&TransformPhase::ResourceContextBuild));
    assert!(observer.phases.contains(&TransformPhase::ScopeLogsBuild));
    assert!(observer.phases.contains(&TransformPhase::ScopeContextBuild));
    assert!(observer.phases.contains(&TransformPhase::LogRecordBuild));
    assert!(observer.phases.contains(&TransformPhase::LogAttributesJson));
    assert!(observer.phases.contains(&TransformPhase::ArrowFinalize));
    assert!(observer.counters.contains(&TransformCounter::OutputRows));
    assert!(observer
        .counters
        .contains(&TransformCounter::ResourceContextDuplicateMiss));
    assert!(observer
        .counters
        .contains(&TransformCounter::ScopeContextDuplicateMiss));
    assert!(observer
        .counters
        .contains(&TransformCounter::ResourceAttributesRowCopies));
}

#[test]
fn test_transform_observer_reports_duplicate_context_hits() {
    let mut request = create_test_log_request();
    request.resource_logs.push(request.resource_logs[0].clone());
    let bytes = request.encode_to_vec();

    let mut observer = TestObserver::default();
    let batch = transform_logs_with_observer(&bytes, InputFormat::Protobuf, &mut observer).unwrap();

    assert_eq!(batch.num_rows(), 2);
    assert!(observer
        .counters
        .contains(&TransformCounter::ResourceContextDuplicateMiss));
    assert!(observer
        .counters
        .contains(&TransformCounter::ResourceContextDuplicateHit));
    assert!(observer
        .counters
        .contains(&TransformCounter::ScopeContextDuplicateMiss));
    assert!(observer
        .counters
        .contains(&TransformCounter::ScopeContextDuplicateHit));
}

#[test]
fn test_transform_logs_json() {
    let json = r#"{
        "resourceLogs": [{
            "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "json-svc" } }]},
            "scopeLogs": [{
                "scope": { "name": "lib", "version": "1" },
                "logRecords": [{
                    "timeUnixNano": "1700000000000000000",
                    "observedTimeUnixNano": "1700000000100000000",
                    "severityNumber": 9,
                    "severityText": "INFO",
                    "body": { "stringValue": "JSON log" }
                }]
            }]
        }]
    }"#;

    let batch = transform_logs(json.as_bytes(), InputFormat::Json).unwrap();

    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_transform_logs_empty() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![],
    };
    let bytes = request.encode_to_vec();

    let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();

    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_transform_traces_protobuf() {
    let request = create_test_trace_request();
    let bytes = request.encode_to_vec();

    let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify some expected columns exist
    let schema = batch.schema();
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("trace_id").is_ok());
    assert!(schema.field_with_name("span_id").is_ok());
    assert!(schema.field_with_name("span_name").is_ok());
}

#[test]
fn test_transform_traces_with_observer_matches_default() {
    let request = create_test_trace_request();
    let bytes = request.encode_to_vec();

    let default_batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();
    let mut observer = TestObserver::default();
    let observed_batch =
        transform_traces_with_observer(&bytes, InputFormat::Protobuf, &mut observer).unwrap();

    assert_eq!(observed_batch.num_rows(), default_batch.num_rows());
    assert!(observer.phases.contains(&TransformPhase::ProtobufDecode));
    assert!(observer
        .phases
        .contains(&TransformPhase::ResourceSpansBuild));
    assert!(observer
        .phases
        .contains(&TransformPhase::ResourceContextBuild));
    assert!(observer.phases.contains(&TransformPhase::ScopeSpansBuild));
    assert!(observer.phases.contains(&TransformPhase::ScopeContextBuild));
    assert!(observer.phases.contains(&TransformPhase::SpanBuild));
    assert!(observer
        .phases
        .contains(&TransformPhase::SpanAttributesJson));
    assert!(observer.phases.contains(&TransformPhase::ArrowFinalize));
    assert!(observer.counters.contains(&TransformCounter::OutputRows));
    assert!(observer
        .counters
        .contains(&TransformCounter::ResourceContextDuplicateMiss));
    assert!(observer
        .counters
        .contains(&TransformCounter::ScopeContextDuplicateMiss));
    assert!(observer
        .counters
        .contains(&TransformCounter::ResourceAttributesRowCopies));
}

#[test]
fn test_transform_traces_json() {
    let json = r#"{
        "resourceSpans": [{
            "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "json-svc" } }]},
            "scopeSpans": [{
                "scope": { "name": "lib" },
                "spans": [{
                    "traceId": "00010203040506070809101112131415",
                    "spanId": "0001020304050607",
                    "name": "json-span",
                    "kind": 1,
                    "startTimeUnixNano": "1700000000000000000",
                    "endTimeUnixNano": "1700000000100000000"
                }]
            }]
        }]
    }"#;

    let batch = transform_traces(json.as_bytes(), InputFormat::Json).unwrap();

    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_transform_traces_empty() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![],
    };
    let bytes = request.encode_to_vec();

    let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();

    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_transform_metrics_protobuf() {
    let request = create_test_metrics_request();
    let bytes = request.encode_to_vec();

    let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

    // Should have both gauge and sum
    assert!(batches.gauge.is_some());
    assert!(batches.sum.is_some());

    let gauge = batches.gauge.unwrap();
    let sum = batches.sum.unwrap();

    assert_eq!(gauge.num_rows(), 1);
    assert_eq!(sum.num_rows(), 1);

    // Verify gauge schema
    let gauge_schema = gauge.schema();
    assert!(gauge_schema.field_with_name("metric_name").is_ok());
    assert!(gauge_schema.field_with_name("value").is_ok());

    // Verify sum schema has extra fields
    let sum_schema = sum.schema();
    assert!(sum_schema
        .field_with_name("aggregation_temporality")
        .is_ok());
    assert!(sum_schema.field_with_name("is_monotonic").is_ok());
}

#[test]
fn test_transform_metrics_with_observer_matches_default() {
    let request = create_test_metrics_request();
    let bytes = request.encode_to_vec();

    let default_batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();
    let mut observer = TestObserver::default();
    let observed_batches =
        transform_metrics_with_observer(&bytes, InputFormat::Protobuf, &mut observer).unwrap();

    assert_eq!(
        observed_batches
            .gauge
            .as_ref()
            .map(|batch| batch.num_rows()),
        default_batches.gauge.as_ref().map(|batch| batch.num_rows())
    );
    assert_eq!(
        observed_batches.sum.as_ref().map(|batch| batch.num_rows()),
        default_batches.sum.as_ref().map(|batch| batch.num_rows())
    );
    assert!(observer.phases.contains(&TransformPhase::ProtobufDecode));
    assert!(observer.phases.contains(&TransformPhase::MetricsCapacity));
    assert!(observer
        .phases
        .contains(&TransformPhase::ResourceContextBuild));
    assert!(observer.phases.contains(&TransformPhase::ScopeContextBuild));
    assert!(observer.phases.contains(&TransformPhase::ArrowAppend));
    assert!(observer.phases.contains(&TransformPhase::ArrowFinalize));
    assert!(observer.counters.contains(&TransformCounter::OutputRows));
    assert!(observer
        .counters
        .contains(&TransformCounter::ResourceContextDuplicateMiss));
    assert!(observer
        .counters
        .contains(&TransformCounter::ScopeContextDuplicateMiss));
}

#[test]
fn test_transform_metrics_gauge_only() {
    let request = create_gauge_only_metrics_request();
    let bytes = request.encode_to_vec();

    let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

    assert!(batches.gauge.is_some());
    assert!(batches.sum.is_none());
}

#[test]
fn test_transform_metrics_sum_only() {
    let request = create_sum_only_metrics_request();
    let bytes = request.encode_to_vec();

    let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

    assert!(batches.gauge.is_none());
    assert!(batches.sum.is_some());
}

#[test]
fn test_transform_metrics_empty() {
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![],
    };
    let bytes = request.encode_to_vec();

    let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

    assert!(batches.gauge.is_none());
    assert!(batches.sum.is_none());
}

// ========================================================================
// Error handling tests
// ========================================================================

#[test]
fn test_transform_logs_invalid_protobuf() {
    let result = transform_logs(b"not valid protobuf", InputFormat::Protobuf);
    assert!(result.is_err());
}

#[test]
fn test_transform_logs_invalid_json() {
    let result = transform_logs(b"not valid json", InputFormat::Json);
    assert!(result.is_err());
}

#[test]
fn test_transform_traces_invalid_protobuf() {
    let result = transform_traces(b"not valid protobuf", InputFormat::Protobuf);
    assert!(result.is_err());
}

#[test]
fn test_transform_metrics_invalid_protobuf() {
    let result = transform_metrics(b"not valid protobuf", InputFormat::Protobuf);
    assert!(result.is_err());
}

// ========================================================================
// Struct tests
// ========================================================================

#[test]
fn test_metric_batches_debug() {
    let batches = MetricBatches {
        gauge: None,
        sum: None,
        histogram: None,
        exp_histogram: None,
        skipped: SkippedMetrics::default(),
    };
    let debug_str = format!("{batches:?}");
    assert!(debug_str.contains("MetricBatches"));
}

// ========================================================================
// Integration tests
// ========================================================================

#[test]
fn test_full_pipeline_logs_to_json_output() {
    let request = create_test_log_request();
    let bytes = request.encode_to_vec();

    let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();
    let json_output = to_json(&batch).unwrap();

    // Should be valid NDJSON
    assert!(!json_output.is_empty());
    // Should contain at least one newline or be non-empty
    let json_str = String::from_utf8(json_output).unwrap();
    assert!(json_str.contains('\n') || !json_str.is_empty());
}

#[test]
fn test_full_pipeline_traces_to_ipc_output() {
    let request = create_test_trace_request();
    let bytes = request.encode_to_vec();

    let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();
    let ipc_output = to_ipc(&batch).unwrap();

    // Should produce some bytes
    assert!(!ipc_output.is_empty());
}

#[test]
fn test_full_pipeline_metrics_round_trip() {
    let request = create_test_metrics_request();
    let bytes = request.encode_to_vec();

    // Transform
    let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

    // Verify we can serialize both
    if let Some(gauge) = &batches.gauge {
        let json = to_json(gauge).unwrap();
        assert!(!json.is_empty());
    }

    if let Some(sum) = &batches.sum {
        let json = to_json(sum).unwrap();
        assert!(!json.is_empty());
    }
}

// ========================================================================
// Timestamp validation tests - ensure timestamps are not 1970 dates
// ========================================================================

#[test]
fn test_timestamp_not_epoch_traces() {
    // Use a known timestamp: 1703265600000000000 ns = Dec 22, 2023 @ 00:00:00 UTC
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans: vec![Span {
                    trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                    span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                    name: "test-span".to_string(),
                    start_time_unix_nano: 1_703_265_600_000_000_000, // Dec 22, 2023
                    end_time_unix_nano: 1_703_265_600_100_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = request.encode_to_vec();

    let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();
    assert_eq!(batch.num_rows(), 1);

    // Get the timestamp column and verify it's not 0 (epoch)
    let schema = batch.schema();
    let ts_idx = schema.index_of("timestamp").unwrap();
    let ts_column = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<::arrow_array::TimestampMicrosecondArray>()
        .expect("timestamp should be TimestampMicrosecondArray");

    let ts_value = ts_column.value(0);
    // Expected: 1703265600000000000 ns / 1000 = 1703265600000000 microseconds
    let expected_micros: i64 = 1_703_265_600_000_000;

    assert_eq!(
        ts_value, expected_micros,
        "Timestamp should be Dec 22, 2023, not epoch (1970)"
    );
    // Sanity check: value should be much greater than 0 (year 2023 >> year 1970)
    assert!(
        ts_value > 1_600_000_000_000_000,
        "Timestamp {ts_value} appears to be too small, possibly 1970 date"
    );
}

#[test]
fn test_timestamp_not_epoch_logs() {
    // Use a known timestamp: 1703265600000000000 ns = Dec 22, 2023 @ 00:00:00 UTC
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_703_265_600_000_000_000, // Dec 22, 2023
                    observed_time_unix_nano: 1_703_265_600_100_000_000,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("Test log".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = request.encode_to_vec();

    let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();
    assert_eq!(batch.num_rows(), 1);

    // Get the timestamp column and verify it's not 0 (epoch)
    let schema = batch.schema();
    let ts_idx = schema.index_of("timestamp").unwrap();
    let ts_column = batch
        .column(ts_idx)
        .as_any()
        .downcast_ref::<::arrow_array::TimestampMicrosecondArray>()
        .expect("timestamp should be TimestampMicrosecondArray");

    let ts_value = ts_column.value(0);
    // Expected: 1703265600000000000 ns / 1000 = 1703265600000000 microseconds
    let expected_micros: i64 = 1_703_265_600_000_000;

    assert_eq!(
        ts_value, expected_micros,
        "Timestamp should be Dec 22, 2023, not epoch (1970)"
    );
    // Sanity check: value should be much greater than 0 (year 2023 >> year 1970)
    assert!(
        ts_value > 1_600_000_000_000_000,
        "Timestamp {ts_value} appears to be too small, possibly 1970 date"
    );
}

#[test]
fn test_timestamp_not_epoch_metrics() {
    // Use a known timestamp: 1703265600000000000 ns = Dec 22, 2023 @ 00:00:00 UTC
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("test-service".to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope::default()),
                metrics: vec![Metric {
                    name: "test.gauge".to_string(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_703_265_600_000_000_000, // Dec 22, 2023
                            value: Some(
                                opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.0),
                            ),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = request.encode_to_vec();

    let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();
    let gauge = batches.gauge.expect("should have gauge metrics");
    assert_eq!(gauge.num_rows(), 1);

    // Get the timestamp column and verify it's not 0 (epoch)
    let schema = gauge.schema();
    let ts_idx = schema.index_of("timestamp").unwrap();
    let ts_column = gauge
        .column(ts_idx)
        .as_any()
        .downcast_ref::<::arrow_array::TimestampMicrosecondArray>()
        .expect("timestamp should be TimestampMicrosecondArray");

    let ts_value = ts_column.value(0);
    // Expected: 1703265600000000000 ns / 1000 = 1703265600000000 microseconds
    let expected_micros: i64 = 1_703_265_600_000_000;

    assert_eq!(
        ts_value, expected_micros,
        "Timestamp should be Dec 22, 2023, not epoch (1970)"
    );
    // Sanity check: value should be much greater than 0 (year 2023 >> year 1970)
    assert!(
        ts_value > 1_600_000_000_000_000,
        "Timestamp {ts_value} appears to be too small, possibly 1970 date"
    );
}
