use std::{fs, path::Path};

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    metrics::v1::{
        exemplar, exponential_histogram_data_point, metric, number_data_point, summary_data_point,
        AggregationTemporality, Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint,
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum, Summary, SummaryDataPoint,
    },
    resource::v1::Resource,
};
use prost::Message;

const BASE_NANOS: u64 = 1_700_000_000_000_000_000;
const GAUGE_ROWS: usize = 4_096;
const SUM_ROWS: usize = 4_096;
const HISTOGRAM_ROWS: usize = 2_048;
const EXP_HISTOGRAM_ROWS: usize = 2_048;
const MIXED_ROWS_PER_TYPE: usize = 1_024;
const SUMMARY_ROWS: usize = 256;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = Path::new("testdata");
    fs::create_dir_all(out_dir)?;

    write_fixture(
        out_dir,
        "metrics_gauge.pb",
        request(vec![gauge_metric("system.cpu.utilization", GAUGE_ROWS, 0)]),
    )?;
    write_fixture(
        out_dir,
        "metrics_sum.pb",
        request(vec![sum_metric("http.server.request.count", SUM_ROWS, 0)]),
    )?;
    write_fixture(
        out_dir,
        "metrics_histogram.pb",
        request(vec![histogram_metric(
            "http.server.duration",
            HISTOGRAM_ROWS,
            0,
        )]),
    )?;
    write_fixture(
        out_dir,
        "metrics_exponential_histogram.pb",
        request(vec![exp_histogram_metric(
            "rpc.server.duration",
            EXP_HISTOGRAM_ROWS,
            0,
        )]),
    )?;
    write_fixture(
        out_dir,
        "metrics_mixed.pb",
        request(vec![
            gauge_metric("process.runtime.memory", MIXED_ROWS_PER_TYPE, 0),
            sum_metric("messaging.operation.count", MIXED_ROWS_PER_TYPE, 1),
            histogram_metric("db.client.operation.duration", MIXED_ROWS_PER_TYPE, 2),
            exp_histogram_metric("queue.processing.duration", MIXED_ROWS_PER_TYPE, 3),
        ]),
    )?;
    write_fixture(
        out_dir,
        "metrics_summary.pb",
        request(vec![summary_metric(
            "legacy.request.duration",
            SUMMARY_ROWS,
            0,
        )]),
    )?;

    Ok(())
}

fn write_fixture(
    out_dir: &Path,
    filename: &str,
    request: ExportMetricsServiceRequest,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = out_dir.join(filename);
    let bytes = request.encode_to_vec();
    fs::write(&path, &bytes)?;
    println!("wrote {} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

fn request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", "checkout"),
                    string_kv("service.namespace", "benchmark"),
                    string_kv("service.instance.id", "checkout-0"),
                    string_kv("deployment.environment", "loadtest"),
                    string_kv("host.name", "bench-host-0"),
                    int_kv("process.pid", 4242),
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "otlp2records.fixture.metrics".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![string_kv("scope.component", "fixture-generator")],
                    ..Default::default()
                }),
                metrics,
                schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
            }],
            schema_url: "https://opentelemetry.io/schemas/1.27.0".to_string(),
        }],
    }
}

fn gauge_metric(name: &str, rows: usize, seed: usize) -> Metric {
    Metric {
        name: name.to_string(),
        description: "synthetic gauge benchmark fixture".to_string(),
        unit: "1".to_string(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: (0..rows)
                .map(|idx| NumberDataPoint {
                    attributes: point_attrs(idx, seed),
                    start_time_unix_nano: BASE_NANOS,
                    time_unix_nano: timestamp(idx),
                    exemplars: exemplars(idx),
                    flags: (idx % 4) as u32,
                    value: Some(number_data_point::Value::AsDouble(
                        0.25 + (idx % 100) as f64 / 100.0,
                    )),
                })
                .collect(),
        })),
        ..Default::default()
    }
}

fn sum_metric(name: &str, rows: usize, seed: usize) -> Metric {
    Metric {
        name: name.to_string(),
        description: "synthetic sum benchmark fixture".to_string(),
        unit: "{request}".to_string(),
        data: Some(metric::Data::Sum(Sum {
            data_points: (0..rows)
                .map(|idx| NumberDataPoint {
                    attributes: point_attrs(idx, seed),
                    start_time_unix_nano: BASE_NANOS,
                    time_unix_nano: timestamp(idx),
                    exemplars: exemplars(idx),
                    flags: (idx % 4) as u32,
                    value: Some(number_data_point::Value::AsInt((idx * 17 + seed) as i64)),
                })
                .collect(),
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        })),
        ..Default::default()
    }
}

fn histogram_metric(name: &str, rows: usize, seed: usize) -> Metric {
    Metric {
        name: name.to_string(),
        description: "synthetic explicit histogram benchmark fixture".to_string(),
        unit: "ms".to_string(),
        data: Some(metric::Data::Histogram(Histogram {
            data_points: (0..rows)
                .map(|idx| {
                    let bucket_counts = histogram_bucket_counts(idx, seed);
                    let count = bucket_counts.iter().sum::<u64>();
                    HistogramDataPoint {
                        attributes: point_attrs(idx, seed),
                        start_time_unix_nano: BASE_NANOS,
                        time_unix_nano: timestamp(idx),
                        count,
                        sum: Some(count as f64 * (12.5 + (idx % 16) as f64)),
                        bucket_counts,
                        explicit_bounds: explicit_bounds(),
                        exemplars: exemplars(idx),
                        flags: (idx % 4) as u32,
                        min: Some((idx % 10) as f64),
                        max: Some(500.0 + (idx % 100) as f64),
                    }
                })
                .collect(),
            aggregation_temporality: AggregationTemporality::Delta as i32,
        })),
        ..Default::default()
    }
}

fn exp_histogram_metric(name: &str, rows: usize, seed: usize) -> Metric {
    Metric {
        name: name.to_string(),
        description: "synthetic exponential histogram benchmark fixture".to_string(),
        unit: "ms".to_string(),
        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: (0..rows)
                .map(|idx| {
                    let positive = exp_bucket_counts(idx, seed);
                    let negative = exp_bucket_counts(idx + 3, seed);
                    let zero_count = (idx % 5) as u64;
                    let count =
                        zero_count + positive.iter().sum::<u64>() + negative.iter().sum::<u64>();
                    ExponentialHistogramDataPoint {
                        attributes: point_attrs(idx, seed),
                        start_time_unix_nano: BASE_NANOS,
                        time_unix_nano: timestamp(idx),
                        count,
                        sum: Some(count as f64 * (3.5 + (idx % 8) as f64)),
                        scale: 4,
                        zero_count,
                        positive: Some(exponential_histogram_data_point::Buckets {
                            offset: -8 + (idx % 4) as i32,
                            bucket_counts: positive,
                        }),
                        negative: Some(exponential_histogram_data_point::Buckets {
                            offset: -12 + (idx % 4) as i32,
                            bucket_counts: negative,
                        }),
                        flags: (idx % 4) as u32,
                        exemplars: exemplars(idx),
                        min: Some(-25.0 + (idx % 7) as f64),
                        max: Some(250.0 + (idx % 80) as f64),
                        zero_threshold: 0.001,
                    }
                })
                .collect(),
            aggregation_temporality: AggregationTemporality::Delta as i32,
        })),
        ..Default::default()
    }
}

fn summary_metric(name: &str, rows: usize, seed: usize) -> Metric {
    Metric {
        name: name.to_string(),
        description: "synthetic summary fixture for skipped metric coverage".to_string(),
        unit: "ms".to_string(),
        data: Some(metric::Data::Summary(Summary {
            data_points: (0..rows)
                .map(|idx| SummaryDataPoint {
                    attributes: point_attrs(idx, seed),
                    start_time_unix_nano: BASE_NANOS,
                    time_unix_nano: timestamp(idx),
                    count: 100 + (idx % 25) as u64,
                    sum: 1_000.0 + idx as f64,
                    quantile_values: vec![
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.5,
                            value: 25.0 + (idx % 10) as f64,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.95,
                            value: 75.0 + (idx % 20) as f64,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.99,
                            value: 150.0 + (idx % 40) as f64,
                        },
                    ],
                    flags: (idx % 4) as u32,
                })
                .collect(),
        })),
        ..Default::default()
    }
}

fn point_attrs(idx: usize, seed: usize) -> Vec<KeyValue> {
    vec![
        string_kv("http.route", ROUTES[(idx + seed) % ROUTES.len()]),
        int_kv(
            "http.response.status_code",
            STATUS_CODES[idx % STATUS_CODES.len()],
        ),
        string_kv("cloud.region", REGIONS[(idx + seed * 2) % REGIONS.len()]),
        bool_kv("feature.enabled", (idx + seed).is_multiple_of(2)),
        int_kv("shard", ((idx + seed) % 64) as i64),
        string_kv("customer.tier", TIERS[(idx + seed) % TIERS.len()]),
    ]
}

fn exemplars(idx: usize) -> Vec<Exemplar> {
    if idx % 16 != 0 {
        return Vec::new();
    }

    vec![Exemplar {
        filtered_attributes: vec![
            string_kv("sample.reason", "tail_latency"),
            int_kv("sample.index", idx as i64),
        ],
        time_unix_nano: timestamp(idx) + 500_000,
        span_id: id_bytes(idx as u64, 8),
        trace_id: id_bytes(idx as u64 * 17 + 3, 16),
        value: Some(exemplar::Value::AsDouble(42.0 + (idx % 100) as f64)),
    }]
}

fn histogram_bucket_counts(idx: usize, seed: usize) -> Vec<u64> {
    (0..=16)
        .map(|bucket| 1 + ((idx + seed + bucket * 7) % 97) as u64)
        .collect()
}

fn explicit_bounds() -> Vec<f64> {
    vec![
        0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 150.0, 250.0, 500.0, 750.0, 1_000.0,
        2_500.0, 5_000.0,
    ]
}

fn exp_bucket_counts(idx: usize, seed: usize) -> Vec<u64> {
    (0..24)
        .map(|bucket| ((idx + seed + bucket * 11) % 29) as u64)
        .collect()
}

fn timestamp(idx: usize) -> u64 {
    BASE_NANOS + idx as u64 * 1_000_000_000
}

fn id_bytes(seed: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| ((seed.wrapping_mul(31) + idx as u64 * 17) & 0xff) as u8)
        .collect()
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn int_kv(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn bool_kv(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(value)),
        }),
    }
}

const ROUTES: &[&str] = &[
    "/api/cart",
    "/api/checkout",
    "/api/catalog",
    "/api/payment",
    "/api/search",
    "/api/profile",
];

const STATUS_CODES: &[i64] = &[200, 200, 200, 201, 204, 400, 404, 429, 500, 503];
const REGIONS: &[&str] = &["us-west-2", "us-east-1", "eu-west-1", "ap-south-1"];
const TIERS: &[&str] = &["free", "pro", "enterprise"];
