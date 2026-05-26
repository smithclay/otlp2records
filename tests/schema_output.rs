use std::str::FromStr;

use arrow_array::{
    Array, BinaryArray, Float64Array, ListArray, StringArray, StructArray, UInt16Array,
    UInt32Array, UInt8Array,
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
        .field_with_name("service_name")
        .is_ok());
    assert!(transform_traces(&traces, InputFormat::Protobuf)
        .unwrap()
        .schema()
        .field_with_name("events_json")
        .is_ok());
    assert!(transform_metrics(&metrics, InputFormat::Protobuf)
        .unwrap()
        .gauge
        .unwrap()
        .schema()
        .field_with_name("metric_attributes")
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
    assert_summary_quantile_schema(batches.summary_data_points.schema().as_ref());
    assert_eq!(
        summary_quantiles(&batches.summary_data_points),
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

fn summary_quantiles(batch: &arrow_array::RecordBatch) -> Vec<(f64, f64)> {
    let col = batch
        .column_by_name("quantile")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = col.value(0);
    let values = values.as_any().downcast_ref::<StructArray>().unwrap();
    let quantiles = values
        .column_by_name("quantile")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let quantile_values = values
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..values.len())
        .map(|idx| (quantiles.value(idx), quantile_values.value(idx)))
        .collect()
}

fn assert_summary_quantile_schema(schema: &arrow_schema::Schema) {
    let field = schema.field_with_name("quantile").unwrap();
    let DataType::List(item) = field.data_type() else {
        panic!("expected summary quantile list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("expected summary quantile list item struct");
    };
    assert_eq!(fields[0].name(), "quantile");
    assert_eq!(fields[0].data_type(), &DataType::Float64);
    assert_eq!(fields[1].name(), "value");
    assert_eq!(fields[1].data_type(), &DataType::Float64);
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
                    ..Default::default()
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
