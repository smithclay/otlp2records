use std::{env, fs, path::PathBuf};

use arrow_ipc::CompressionType;
use otap_df_pdata::{
    encode::{encode_logs_otap_batch, encode_metrics_otap_batch, encode_spans_otap_batch},
    encode::producer::ProducerOptions,
    proto::opentelemetry::{
        common::v1::{
            any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
        },
        logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs},
        metrics::v1::{
            exemplar, exponential_histogram_data_point::Buckets, metric, number_data_point,
            AggregationTemporality, Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint,
            Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
            ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
        },
        resource::v1::Resource,
        trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status, TracesData},
    },
    Producer,
};
use prost::Message;

fn main() {
    let output = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("fixture output directory argument");
    fs::create_dir_all(&output).unwrap();

    let first = logs_data("first", 1);
    let second = logs_data("second", 3);
    let mut producer = Producer::new_with_options(ProducerOptions {
        ipc_compression: None,
    });
    let first_bar = producer
        .produce_bar(&mut encode_logs_otap_batch(&first).unwrap())
        .unwrap();
    let second_bar = producer
        .produce_bar(&mut encode_logs_otap_batch(&second).unwrap())
        .unwrap();

    fs::write(output.join("logs-initial.bar"), first_bar.encode_to_vec()).unwrap();
    fs::write(output.join("logs-reuse.bar"), second_bar.encode_to_vec()).unwrap();
    fs::write(output.join("logs-initial.otlp"), first.encode_to_vec()).unwrap();
    fs::write(output.join("logs-reuse.otlp"), second.encode_to_vec()).unwrap();

    let mut zstd_producer = Producer::new_with_options(ProducerOptions {
        ipc_compression: Some(CompressionType::ZSTD),
    });
    let zstd_bar = zstd_producer
        .produce_bar(&mut encode_logs_otap_batch(&first).unwrap())
        .unwrap();
    fs::write(output.join("logs-zstd.bar"), zstd_bar.encode_to_vec()).unwrap();

    let first = traces_data("first", 1_700_000_000_000_000_000);
    let second = traces_data("second", 1_700_000_001_000_000_000);
    let mut producer = Producer::new_with_options(ProducerOptions {
        ipc_compression: None,
    });
    let first_bar = producer
        .produce_bar(&mut encode_spans_otap_batch(&first).unwrap())
        .unwrap();
    let second_bar = producer
        .produce_bar(&mut encode_spans_otap_batch(&second).unwrap())
        .unwrap();
    fs::write(
        output.join("traces-initial.bar"),
        first_bar.encode_to_vec(),
    )
    .unwrap();
    fs::write(
        output.join("traces-reuse.bar"),
        second_bar.encode_to_vec(),
    )
    .unwrap();
    fs::write(output.join("traces-initial.otlp"), first.encode_to_vec()).unwrap();
    fs::write(output.join("traces-reuse.otlp"), second.encode_to_vec()).unwrap();

    let first = metrics_data("first", 1_700_000_000_000_000_000);
    let second = metrics_data("second", 1_700_000_001_000_000_000);
    let mut producer = Producer::new_with_options(ProducerOptions {
        ipc_compression: None,
    });
    let first_bar = producer
        .produce_bar(&mut encode_metrics_otap_batch(&first).unwrap())
        .unwrap();
    let second_bar = producer
        .produce_bar(&mut encode_metrics_otap_batch(&second).unwrap())
        .unwrap();
    fs::write(
        output.join("metrics-initial.bar"),
        first_bar.encode_to_vec(),
    )
    .unwrap();
    fs::write(
        output.join("metrics-reuse.bar"),
        second_bar.encode_to_vec(),
    )
    .unwrap();
    fs::write(output.join("metrics-initial.otlp"), first.encode_to_vec()).unwrap();
    fs::write(output.join("metrics-reuse.otlp"), second.encode_to_vec()).unwrap();
}

fn metrics_data(label: &str, time: u64) -> MetricsData {
    let exemplar = Exemplar {
        filtered_attributes: vec![kv("exemplar.attr", string(label))],
        time_unix_nano: time + 1,
        span_id: (1..=8).collect(),
        trace_id: (1..=16).collect(),
        value: Some(exemplar::Value::AsDouble(2.5)),
    };
    let number = |value| NumberDataPoint {
        attributes: vec![kv("point.attr", string(label))],
        start_time_unix_nano: time - 100,
        time_unix_nano: time,
        exemplars: vec![exemplar.clone()],
        flags: 1,
        value: Some(value),
    };
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", string("fixture-service"))],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "fixture-scope".into(),
                    version: "1.2.3".into(),
                    attributes: vec![kv("scope.bool", boolean(true))],
                    ..Default::default()
                }),
                metrics: vec![
                    Metric {
                        name: format!("{label}.gauge"),
                        description: "gauge".into(),
                        unit: "1".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![number(number_data_point::Value::AsDouble(1.5))],
                        })),
                        metadata: vec![kv("metric.attr", string(label))],
                    },
                    Metric {
                        name: format!("{label}.sum"),
                        description: "sum".into(),
                        unit: "ms".into(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![number(number_data_point::Value::AsInt(7))],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            is_monotonic: true,
                        })),
                        metadata: vec![],
                    },
                    Metric {
                        name: format!("{label}.histogram"),
                        description: "histogram".into(),
                        unit: "ms".into(),
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes: vec![kv("point.map", map(vec![kv("k", string(label))]))],
                                start_time_unix_nano: time - 100,
                                time_unix_nano: time,
                                count: 3,
                                sum: Some(6.0),
                                bucket_counts: vec![1, 2],
                                explicit_bounds: vec![1.0],
                                exemplars: vec![exemplar.clone()],
                                flags: 2,
                                min: Some(1.0),
                                max: Some(3.0),
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        })),
                        metadata: vec![],
                    },
                    Metric {
                        name: format!("{label}.exp"),
                        description: "exp".into(),
                        unit: "By".into(),
                        data: Some(metric::Data::ExponentialHistogram(
                            ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    attributes: vec![kv("point.bytes", bytes(b"metric"))],
                                    start_time_unix_nano: time - 100,
                                    time_unix_nano: time,
                                    count: 4,
                                    sum: Some(8.0),
                                    scale: 2,
                                    zero_count: 1,
                                    positive: Some(Buckets {
                                        offset: -1,
                                        bucket_counts: vec![1, 3],
                                    }),
                                    negative: Some(Buckets {
                                        offset: 1,
                                        bucket_counts: vec![2],
                                    }),
                                    flags: 3,
                                    exemplars: vec![exemplar],
                                    min: Some(-2.0),
                                    max: Some(4.0),
                                    zero_threshold: 0.01,
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            },
                        )),
                        metadata: vec![],
                    },
                    Metric {
                        name: format!("{label}.summary"),
                        description: "summary".into(),
                        unit: "1".into(),
                        data: Some(metric::Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: time - 100,
                                time_unix_nano: time,
                                count: 1,
                                sum: 1.0,
                                quantile_values: vec![],
                                flags: 0,
                            }],
                        })),
                        metadata: vec![],
                    },
                ],
                schema_url: "https://example.test/scope".into(),
            }],
            schema_url: "https://example.test/resource".into(),
        }],
    }
}

fn traces_data(label: &str, start: u64) -> TracesData {
    let trace_id: Vec<u8> = (1..=16).collect();
    let span_id: Vec<u8> = (21..=28).collect();
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", string("fixture-service")),
                    kv("resource.map", map(vec![kv("region", string("west"))])),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "fixture-scope".into(),
                    version: "1.2.3".into(),
                    attributes: vec![kv("scope.bool", boolean(true))],
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: trace_id.clone(),
                    span_id: span_id.clone(),
                    trace_state: "vendor=value".into(),
                    parent_span_id: (31..=38).collect(),
                    flags: 1,
                    name: format!("{label}-span"),
                    kind: 2,
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + 500,
                    attributes: vec![
                        kv("span.str", string(label)),
                        kv("span.int", integer(7)),
                    ],
                    dropped_attributes_count: 1,
                    events: vec![span::Event {
                        time_unix_nano: start + 100,
                        name: format!("{label}-event"),
                        attributes: vec![kv(
                            "event.map",
                            map(vec![kv("nested", string(label))]),
                        )],
                        dropped_attributes_count: 2,
                    }],
                    dropped_events_count: 3,
                    links: vec![span::Link {
                        trace_id,
                        span_id,
                        trace_state: "linked=value".into(),
                        attributes: vec![kv("link.bytes", bytes(b"link"))],
                        dropped_attributes_count: 4,
                        flags: 1,
                    }],
                    dropped_links_count: 5,
                    status: Some(Status {
                        message: format!("{label}-status"),
                        code: 2,
                    }),
                }],
                schema_url: "https://example.test/scope".into(),
            }],
            schema_url: "https://example.test/resource".into(),
        }],
    }
}

fn logs_data(label: &str, start: u64) -> LogsData {
    let repeated = kv("repeated", string("same"));
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    kv("service.name", string("fixture-service")),
                    kv("resource.bool", boolean(true)),
                    kv("resource.map", map(vec![kv("region", string("west"))])),
                ],
                dropped_attributes_count: 2,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "fixture-scope".into(),
                    version: "1.2.3".into(),
                    attributes: vec![
                        kv("scope.int", integer(42)),
                        kv("scope.slice", array(vec![string("a"), integer(2)])),
                    ],
                    dropped_attributes_count: 1,
                }),
                log_records: vec![
                    record(label, start, repeated.clone(), map_body(label)),
                    record(label, start + 1, repeated, array_body(label)),
                    empty_record(start + 2),
                ],
                schema_url: "https://example.test/scope".into(),
            }],
            schema_url: "https://example.test/resource".into(),
        }],
    }
}

fn empty_record(time: u64) -> LogRecord {
    LogRecord {
        time_unix_nano: time,
        ..Default::default()
    }
}

fn record(label: &str, time: u64, repeated: KeyValue, body: AnyValue) -> LogRecord {
    LogRecord {
        time_unix_nano: time,
        observed_time_unix_nano: time + 100,
        severity_number: 9,
        severity_text: "INFO".into(),
        body: Some(body),
        attributes: vec![
            repeated,
            kv("str", string(label)),
            kv("int", integer(7)),
            kv("double", double(2.5)),
            kv("bool", boolean(false)),
            kv("bytes", bytes(&[0, 1, 2])),
            kv("map", map(vec![kv("nested", string(label))])),
            kv("slice", array(vec![integer(1), boolean(true)])),
        ],
        dropped_attributes_count: 3,
        flags: 1,
        trace_id: (0..16).map(|value| value + start_byte(time)).collect(),
        span_id: (0..8).map(|value| value + start_byte(time)).collect(),
        event_name: format!("{label}-event"),
    }
}

fn start_byte(value: u64) -> u8 {
    (value % 32) as u8
}

fn map_body(label: &str) -> AnyValue {
    map(vec![
        kv("message", string(label)),
        kv("number", integer(99)),
        kv("ratio", double(5.0)),
        kv("enabled", boolean(true)),
        kv("raw", bytes(b"123")),
        kv("empty", AnyValue { value: None }),
        kv("nested", array(vec![string("child")])),
    ])
}

fn array_body(label: &str) -> AnyValue {
    array(vec![string(label), integer(12), boolean(false)])
}

fn kv(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.into(),
        value: Some(value),
    }
}

fn string(value: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.into())),
    }
}

fn boolean(value: bool) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BoolValue(value)),
    }
}

fn integer(value: i64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::IntValue(value)),
    }
}

fn double(value: f64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::DoubleValue(value)),
    }
}

fn bytes(value: &[u8]) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::BytesValue(value.to_vec())),
    }
}

fn array(values: Vec<AnyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue { values })),
    }
}

fn map(values: Vec<KeyValue>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::KvlistValue(KeyValueList { values })),
    }
}
