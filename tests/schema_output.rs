use std::str::FromStr;

use arrow_array::{
    Array, BinaryArray, Float64Array, StringArray, UInt16Array, UInt32Array, UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList},
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
use otlp2records::{
    transform_logs, transform_logs_with_schema, transform_metrics, transform_metrics_with_schema,
    transform_traces, transform_traces_with_schema, InputFormat, LogsOutput, MetricsOutput,
    SchemaOutput, TracesOutput,
};
use prost::Message;

fn attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn attr_any(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

fn bytes16() -> Vec<u8> {
    (0..16).collect()
}

fn bytes8() -> Vec<u8> {
    (0..8).collect()
}

#[test]
fn parses_schema_output_aliases() {
    for value in ["", "default", "normalized", "clickstack", "clickstack-mode"] {
        assert_eq!(
            SchemaOutput::from_str(value).unwrap(),
            SchemaOutput::Normalized
        );
    }
    assert_eq!(
        SchemaOutput::from_str("otap-star").unwrap(),
        SchemaOutput::OtapStar
    );
    assert_eq!(
        SchemaOutput::from_str("otap_star").unwrap(),
        SchemaOutput::OtapStar
    );
    assert!(SchemaOutput::from_str("flat").is_err());
}

#[test]
fn existing_transform_apis_stay_normalized() {
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

#[test]
fn otap_logs_emit_star_tables_and_schema_types() {
    let output = transform_logs_with_schema(
        &log_request().encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap();
    let LogsOutput::OtapStar(batches) = output else {
        panic!("expected otap-star logs");
    };

    assert_eq!(batches.logs.num_rows(), 1);
    assert_eq!(batches.resource_attrs.num_rows(), 1);
    assert_eq!(batches.scope_attrs.num_rows(), 1);
    assert_eq!(batches.log_attrs.num_rows(), 1);
    assert_eq!(u16_col(&batches.logs, "resource_id"), vec![0]);
    assert_eq!(u16_col(&batches.logs, "scope_id"), vec![0]);
    assert_eq!(
        batches
            .logs
            .schema()
            .field_with_name("trace_id")
            .unwrap()
            .data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        batches
            .logs
            .schema()
            .field_with_name("time_unix_nano")
            .unwrap()
            .data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert!(batches
        .logs
        .schema()
        .field_with_name("time_unix_nano")
        .unwrap()
        .is_nullable());
    assert_eq!(
        batches
            .logs
            .schema()
            .field_with_name("event_name")
            .unwrap()
            .data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        string_col(&batches.logs, "event_name"),
        vec![Some("login".to_string())]
    );
    assert_eq!(u16_col(&batches.resource_attrs, "parent_id"), vec![0]);
    assert_eq!(u16_col(&batches.scope_attrs, "parent_id"), vec![0]);
    assert_eq!(u8_col(&batches.log_attrs, "type"), vec![1]);
}

#[test]
fn otap_traces_emit_event_link_child_tables() {
    let output = transform_traces_with_schema(
        &trace_request().encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap();
    let TracesOutput::OtapStar(batches) = output else {
        panic!("expected otap-star traces");
    };

    assert_eq!(batches.spans.num_rows(), 1);
    assert_eq!(batches.span_events.num_rows(), 1);
    assert_eq!(batches.span_event_attrs.num_rows(), 1);
    assert_eq!(batches.span_links.num_rows(), 1);
    assert_eq!(batches.span_link_attrs.num_rows(), 1);
    assert_eq!(u16_col(&batches.spans, "resource_id"), vec![0]);
    assert_eq!(u16_col(&batches.spans, "scope_id"), vec![0]);
    assert_eq!(u16_col(&batches.span_events, "parent_id"), vec![0]);
    assert_eq!(u32_col(&batches.span_event_attrs, "parent_id"), vec![0]);
    assert!(!batches
        .span_links
        .schema()
        .field_with_name("trace_id")
        .unwrap()
        .is_nullable());
    assert!(!batches
        .span_links
        .schema()
        .field_with_name("span_id")
        .unwrap()
        .is_nullable());
    assert_eq!(
        batches
            .spans
            .schema()
            .field_with_name("duration_time_unix_nano")
            .unwrap()
            .data_type(),
        &DataType::Duration(TimeUnit::Nanosecond)
    );
}

#[test]
fn otap_span_with_end_before_start_clamps_duration_to_zero() {
    // A malformed span where end < start must clamp the Duration to 0 rather
    // than emit a negative value. The normalized path already does this; OTAP
    // must match (regression test for branch B1).
    let mut request = trace_request();
    let span = &mut request.resource_spans[0].scope_spans[0].spans[0];
    span.start_time_unix_nano = 1_700_000_000_000_000_100;
    span.end_time_unix_nano = 1_700_000_000_000_000_000;

    let TracesOutput::OtapStar(otap) = transform_traces_with_schema(
        &request.encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap() else {
        panic!("expected otap-star traces");
    };
    let normalized = transform_traces(&request.encode_to_vec(), InputFormat::Protobuf).unwrap();

    let otap_duration = otap
        .spans
        .column_by_name("duration_time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::DurationNanosecondArray>()
        .unwrap()
        .value(0);
    let normalized_duration = normalized
        .column_by_name("duration_time_unix_nano")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow_array::DurationNanosecondArray>()
        .unwrap()
        .value(0);

    assert_eq!(otap_duration, 0, "OTAP duration must clamp at 0");
    assert_eq!(
        normalized_duration, 0,
        "normalized duration must clamp at 0"
    );
}

#[test]
fn otap_metrics_emit_all_metric_point_tables() {
    let output = transform_metrics_with_schema(
        &metrics_request().encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap();
    let MetricsOutput::OtapStar(batches) = output else {
        panic!("expected otap-star metrics");
    };

    assert_eq!(batches.metrics.num_rows(), 5);
    assert_eq!(batches.number_data_points.num_rows(), 2);
    assert_eq!(batches.number_dp_attrs.num_rows(), 2);
    assert_eq!(batches.number_dp_exemplars.num_rows(), 1);
    assert_eq!(batches.summary_data_points.num_rows(), 1);
    assert_eq!(batches.quantile.num_rows(), 2);
    assert_eq!(batches.histogram_data_points.num_rows(), 1);
    assert_eq!(batches.histogram_dp_exemplars.num_rows(), 1);
    assert_eq!(batches.exp_histogram_data_points.num_rows(), 1);
    assert_eq!(batches.exp_histogram_dp_exemplars.num_rows(), 1);
    assert_eq!(batches.skipped.total(), 0);
    assert_eq!(
        u16_col(&batches.metrics, "resource_id"),
        vec![0, 0, 0, 0, 0]
    );
    assert_eq!(u16_col(&batches.metrics, "scope_id"), vec![0, 0, 0, 0, 0]);
    assert_eq!(u16_col(&batches.resource_attrs, "parent_id"), vec![0]);
    assert_eq!(
        u16_col(&batches.number_data_points, "parent_id"),
        vec![0, 1]
    );
    assert!(!batches
        .number_data_points
        .schema()
        .field_with_name("time_unix_nano")
        .unwrap()
        .is_nullable());
    // Quantiles live only in the separate `quantile` child table — there is no
    // embedded list in summary_data_points (B3).
    assert!(batches
        .summary_data_points
        .schema()
        .field_with_name("quantile")
        .is_err());
    assert_eq!(
        quantile_pairs(&batches.quantile),
        vec![(0.5, 2.0), (0.99, 3.0)]
    );
    assert_eq!(
        batches
            .histogram_data_points
            .schema()
            .field_with_name("bucket_counts")
            .unwrap()
            .data_type(),
        &DataType::List(arrow_schema::Field::new("item", DataType::UInt64, true).into())
    );
    assert_eq!(
        batches
            .exp_histogram_data_points
            .schema()
            .field_with_name("zero_threshold")
            .unwrap()
            .data_type(),
        &DataType::Float64
    );
    assert_eq!(
        f64_col(&batches.exp_histogram_data_points, "zero_threshold"),
        vec![0.25]
    );
}

#[test]
fn otap_metrics_skip_nan_inf_and_missing_values_and_bump_counters() {
    // OTAP must drop NaN / Infinity / missing number data points and increment
    // SkippedMetrics, matching the normalized path (regression test for B2).
    let request = ExportMetricsServiceRequest {
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
                metrics: vec![Metric {
                    name: "g".to_string(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![
                            NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1,
                                time_unix_nano: 2,
                                value: Some(number_data_point::Value::AsDouble(f64::NAN)),
                                exemplars: vec![],
                                flags: 0,
                            },
                            NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1,
                                time_unix_nano: 2,
                                value: Some(number_data_point::Value::AsDouble(f64::INFINITY)),
                                exemplars: vec![],
                                flags: 0,
                            },
                            NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 1,
                                time_unix_nano: 2,
                                value: None,
                                exemplars: vec![],
                                flags: 0,
                            },
                            NumberDataPoint {
                                attributes: vec![attr("ok", "yes")],
                                start_time_unix_nano: 1,
                                time_unix_nano: 2,
                                value: Some(number_data_point::Value::AsDouble(1.5)),
                                exemplars: vec![],
                                flags: 0,
                            },
                        ],
                    })),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let MetricsOutput::OtapStar(batches) = transform_metrics_with_schema(
        &request.encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap() else {
        panic!("expected otap-star metrics");
    };

    assert_eq!(batches.skipped.nan_values, 1);
    assert_eq!(batches.skipped.infinity_values, 1);
    assert_eq!(batches.skipped.missing_values, 1);
    assert_eq!(batches.skipped.summaries, 0);
    assert_eq!(batches.skipped.total(), 3);
    // Only the one valid point makes it into the child tables.
    assert_eq!(batches.number_data_points.num_rows(), 1);
    assert_eq!(batches.number_dp_attrs.num_rows(), 1);
}

#[test]
fn otap_metric_with_no_data_variant_is_skipped() {
    // A Metric with `data: None` must not be written into the metrics table
    // (it would carry metric_type=0, which is not a defined OTAP code). B6.
    let request = ExportMetricsServiceRequest {
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
                        name: "no-data".to_string(),
                        data: None,
                        ..Default::default()
                    },
                    Metric {
                        name: "g".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![number_point(1.0, "k", vec![])],
                        })),
                        ..Default::default()
                    },
                ],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };

    let MetricsOutput::OtapStar(batches) = transform_metrics_with_schema(
        &request.encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap() else {
        panic!("expected otap-star metrics");
    };

    assert_eq!(batches.metrics.num_rows(), 1);
    assert_eq!(u8_col(&batches.metrics, "metric_type"), vec![1]); // METRIC_GAUGE
    assert_eq!(
        string_col(&batches.metrics, "name"),
        vec![Some("g".to_string())]
    );
}

#[test]
fn otap_logs_dedupe_repeated_resource_and_scope_contexts() {
    // Two resource_logs entries with identical resource attributes, schema_url
    // and dropped_attributes_count must share one resource_id and emit a single
    // resource_attrs row. Same for scope. (Regression test for B4.)
    let resource = Resource {
        attributes: vec![attr("service.name", "svc")],
        dropped_attributes_count: 0,
        ..Default::default()
    };
    let scope = InstrumentationScope {
        name: "scope".to_string(),
        version: "1".to_string(),
        attributes: vec![attr("scope.key", "scope-value")],
        dropped_attributes_count: 0,
    };
    let make_record = |body: &str| LogRecord {
        time_unix_nano: 1_700_000_000_000_000_000,
        observed_time_unix_nano: 1_700_000_000_000_000_100,
        trace_id: bytes16(),
        span_id: bytes8(),
        severity_number: 9,
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(body.to_string())),
        }),
        attributes: vec![],
        dropped_attributes_count: 0,
        flags: 0,
        event_name: String::new(),
    };
    let request = ExportLogsServiceRequest {
        resource_logs: vec![
            ResourceLogs {
                resource: Some(resource.clone()),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope.clone()),
                    log_records: vec![make_record("a")],
                    schema_url: "scope-schema".to_string(),
                }],
                schema_url: "resource-schema".to_string(),
            },
            ResourceLogs {
                resource: Some(resource),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope),
                    log_records: vec![make_record("b")],
                    schema_url: "scope-schema".to_string(),
                }],
                schema_url: "resource-schema".to_string(),
            },
        ],
    };

    let LogsOutput::OtapStar(batches) = transform_logs_with_schema(
        &request.encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap() else {
        panic!("expected otap-star logs");
    };

    // Two log records — but the resource and scope are deduped.
    assert_eq!(batches.logs.num_rows(), 2);
    assert_eq!(u16_col(&batches.logs, "resource_id"), vec![0, 0]);
    assert_eq!(u16_col(&batches.logs, "scope_id"), vec![0, 0]);
    assert_eq!(batches.resource_attrs.num_rows(), 1);
    assert_eq!(batches.scope_attrs.num_rows(), 1);
}

#[test]
fn otap_anyvalue_uses_official_type_codes_and_cbor_ser() {
    let mut request = log_request();
    request.resource_logs[0].scope_logs[0].log_records[0]
        .attributes
        .push(attr_any(
            "array.key",
            any_value::Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(any_value::Value::StringValue("a".to_string())),
                    },
                    AnyValue {
                        value: Some(any_value::Value::IntValue(2)),
                    },
                ],
            }),
        ));
    request.resource_logs[0].scope_logs[0].log_records[0]
        .attributes
        .push(attr_any(
            "map.key",
            any_value::Value::KvlistValue(KeyValueList {
                values: vec![attr("nested", "value")],
            }),
        ));

    let LogsOutput::OtapStar(batches) = transform_logs_with_schema(
        &request.encode_to_vec(),
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap() else {
        panic!("expected otap-star logs");
    };

    assert_eq!(u8_col(&batches.log_attrs, "type"), vec![1, 6, 5]);
    let ser = binary_col(&batches.log_attrs, "ser");
    assert_eq!(ser[0], None);
    assert_eq!(ser[1].as_deref(), Some(&[0x82, 0x61, b'a', 0x02][..]));
    assert_eq!(
        ser[2].as_deref(),
        Some(
            &[0xa1, 0x66, b'n', b'e', b's', b't', b'e', b'd', 0x65, b'v', b'a', b'l', b'u', b'e'][..]
        )
    );
}

#[test]
fn json_and_protobuf_inputs_have_same_otap_schema() {
    let json_logs = include_bytes!("fixtures/sample_otlp.json");
    let proto_logs = log_request().encode_to_vec();
    let LogsOutput::OtapStar(json) =
        transform_logs_with_schema(json_logs, InputFormat::Json, SchemaOutput::OtapStar).unwrap()
    else {
        panic!("expected otap-star logs");
    };
    let LogsOutput::OtapStar(proto) =
        transform_logs_with_schema(&proto_logs, InputFormat::Protobuf, SchemaOutput::OtapStar)
            .unwrap()
    else {
        panic!("expected otap-star logs");
    };
    assert_eq!(json.logs.schema(), proto.logs.schema());
    assert_eq!(json.log_attrs.schema(), proto.log_attrs.schema());

    let json_metrics = include_bytes!("fixtures/sample_otlp_metrics.json");
    let proto_metrics = metrics_request().encode_to_vec();
    let MetricsOutput::OtapStar(json) =
        transform_metrics_with_schema(json_metrics, InputFormat::Json, SchemaOutput::OtapStar)
            .unwrap()
    else {
        panic!("expected otap-star metrics");
    };
    let MetricsOutput::OtapStar(proto) = transform_metrics_with_schema(
        &proto_metrics,
        InputFormat::Protobuf,
        SchemaOutput::OtapStar,
    )
    .unwrap() else {
        panic!("expected otap-star metrics");
    };
    assert_eq!(json.metrics.schema(), proto.metrics.schema());
    assert_eq!(
        json.exp_histogram_data_points.schema(),
        proto.exp_histogram_data_points.schema()
    );
}

fn u16_col(batch: &arrow_array::RecordBatch, name: &str) -> Vec<u16> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap();
    (0..col.len()).map(|idx| col.value(idx)).collect()
}

fn u32_col(batch: &arrow_array::RecordBatch, name: &str) -> Vec<u32> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();
    (0..col.len()).map(|idx| col.value(idx)).collect()
}

fn u8_col(batch: &arrow_array::RecordBatch, name: &str) -> Vec<u8> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    (0..col.len()).map(|idx| col.value(idx)).collect()
}

fn f64_col(batch: &arrow_array::RecordBatch, name: &str) -> Vec<f64> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..col.len()).map(|idx| col.value(idx)).collect()
}

fn string_col(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<String>> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..col.len())
        .map(|idx| {
            if col.is_null(idx) {
                None
            } else {
                Some(col.value(idx).to_string())
            }
        })
        .collect()
}

fn quantile_pairs(batch: &arrow_array::RecordBatch) -> Vec<(f64, f64)> {
    let quantile = batch
        .column_by_name("quantile")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let value = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..batch.num_rows())
        .map(|idx| (quantile.value(idx), value.value(idx)))
        .collect()
}

fn binary_col(batch: &arrow_array::RecordBatch, name: &str) -> Vec<Option<Vec<u8>>> {
    let col = batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    (0..col.len())
        .map(|idx| {
            if col.is_null(idx) {
                None
            } else {
                Some(col.value(idx).to_vec())
            }
        })
        .collect()
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
