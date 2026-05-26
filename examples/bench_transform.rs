use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::KeyValue,
    metrics::v1::{metric::Data, number_data_point},
};
use otlp2records::{
    transform_logs, transform_logs_with_schema, transform_metrics, transform_metrics_with_schema,
    transform_traces, transform_traces_with_schema, InputFormat, LogsOutput, MetricsOutput,
    SchemaOutput, TracesOutput,
};
use prost::Message;

const WARMUP_ITERS: usize = 5;
const MEASURE_ITERS: usize = 50;

#[derive(Clone, Copy)]
enum WorkloadKind {
    Logs,
    Traces,
    Metrics,
}

struct Workload {
    name: &'static str,
    fixture: &'static str,
    kind: WorkloadKind,
}

struct Measurement {
    workload: &'static str,
    path: &'static str,
    table: &'static str,
    fixture_bytes: usize,
    rows_per_iter: usize,
    iterations: usize,
    total: Duration,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workloads = [
        Workload {
            name: "logs_large",
            fixture: "testdata/logs_large.pb",
            kind: WorkloadKind::Logs,
        },
        Workload {
            name: "traces_large",
            fixture: "testdata/traces_large.pb",
            kind: WorkloadKind::Traces,
        },
        Workload {
            name: "metrics_gauge",
            fixture: "testdata/metrics_gauge.pb",
            kind: WorkloadKind::Metrics,
        },
        Workload {
            name: "metrics_sum",
            fixture: "testdata/metrics_sum.pb",
            kind: WorkloadKind::Metrics,
        },
        Workload {
            name: "metrics_histogram",
            fixture: "testdata/metrics_histogram.pb",
            kind: WorkloadKind::Metrics,
        },
        Workload {
            name: "metrics_exponential_histogram",
            fixture: "testdata/metrics_exponential_histogram.pb",
            kind: WorkloadKind::Metrics,
        },
        Workload {
            name: "metrics_mixed",
            fixture: "testdata/metrics_mixed.pb",
            kind: WorkloadKind::Metrics,
        },
    ];

    println!(
        "workload,path,table,fixture_bytes,rows_per_iter,iterations,total_ms,ms_per_iter,rows_per_sec,mib_per_sec,speedup_vs_default"
    );
    println!(
        "metric_shape_header,workload,gauge_rows,sum_rows,histogram_rows,exp_histogram_rows,hist_bucket_values,hist_bound_values,exp_positive_bucket_values,exp_negative_bucket_values,metric_attr_kvs,metric_attr_points,exemplars,exemplar_attr_kvs,resource_attr_kv_row_copies,scope_attr_kv_row_copies,metric_identity_bytes"
    );
    for workload in workloads {
        if !Path::new(workload.fixture).exists() {
            eprintln!("skipping missing fixture {}", workload.fixture);
            continue;
        }

        let bytes = fs::read(workload.fixture)?;
        let default = measure(workload.kind, PathKind::Default, &bytes)?;
        let otap_star = measure(workload.kind, PathKind::OtapStar, &bytes)?;
        let decode = measure(workload.kind, PathKind::Decode, &bytes)?;
        let decoded = decode_for_measurement(workload.kind, &bytes)?;
        let clone_only = measure_decoded_clone(&decoded)?;

        for (table, rows) in &default.rows {
            let measurement = Measurement {
                workload: workload.name,
                path: "default",
                table,
                fixture_bytes: bytes.len(),
                rows_per_iter: *rows,
                iterations: MEASURE_ITERS,
                total: default.total,
            };
            print_measurement(measurement, 1.0);
        }

        let otap_star_speedup = default.total.as_secs_f64() / otap_star.total.as_secs_f64();
        for (table, rows) in &otap_star.rows {
            let measurement = Measurement {
                workload: workload.name,
                path: "otap_star",
                table,
                fixture_bytes: bytes.len(),
                rows_per_iter: *rows,
                iterations: MEASURE_ITERS,
                total: otap_star.total,
            };
            print_measurement(measurement, otap_star_speedup);
        }

        let decode_speedup = default.total.as_secs_f64() / decode.total.as_secs_f64();
        for (table, rows) in &decode.rows {
            let measurement = Measurement {
                workload: workload.name,
                path: "decode",
                table,
                fixture_bytes: bytes.len(),
                rows_per_iter: *rows,
                iterations: MEASURE_ITERS,
                total: decode.total,
            };
            print_measurement(measurement, decode_speedup);
        }

        let clone_only_speedup = default.total.as_secs_f64() / clone_only.total.as_secs_f64();
        for (table, rows) in &clone_only.rows {
            let measurement = Measurement {
                workload: workload.name,
                path: "clone_only",
                table,
                fixture_bytes: bytes.len(),
                rows_per_iter: *rows,
                iterations: MEASURE_ITERS,
                total: clone_only.total,
            };
            print_measurement(measurement, clone_only_speedup);
        }

        if let DecodedRequest::Metrics(request) = &decoded {
            print_metric_shape(workload.name, &metric_shape(request));
        }
    }

    Ok(())
}

enum DecodedRequest {
    Logs(ExportLogsServiceRequest),
    Traces(ExportTraceServiceRequest),
    Metrics(ExportMetricsServiceRequest),
}

#[derive(Clone, Copy)]
enum PathKind {
    Default,
    OtapStar,
    Decode,
}

struct RunMeasurement {
    rows: Vec<(&'static str, usize)>,
    total: Duration,
}

fn measure(
    kind: WorkloadKind,
    path: PathKind,
    bytes: &[u8],
) -> Result<RunMeasurement, Box<dyn std::error::Error>> {
    for _ in 0..WARMUP_ITERS {
        run_once(kind, path, bytes)?;
    }

    let mut rows = Vec::new();
    let start = Instant::now();
    for _ in 0..MEASURE_ITERS {
        rows = run_once(kind, path, bytes)?;
    }
    let total = start.elapsed();

    Ok(RunMeasurement { rows, total })
}

fn decode_for_measurement(
    kind: WorkloadKind,
    bytes: &[u8],
) -> Result<DecodedRequest, Box<dyn std::error::Error>> {
    Ok(match kind {
        WorkloadKind::Logs => DecodedRequest::Logs(ExportLogsServiceRequest::decode(bytes)?),
        WorkloadKind::Traces => DecodedRequest::Traces(ExportTraceServiceRequest::decode(bytes)?),
        WorkloadKind::Metrics => {
            DecodedRequest::Metrics(ExportMetricsServiceRequest::decode(bytes)?)
        }
    })
}

fn measure_decoded_clone(
    decoded: &DecodedRequest,
) -> Result<RunMeasurement, Box<dyn std::error::Error>> {
    for _ in 0..WARMUP_ITERS {
        run_decoded_clone_once(decoded);
    }

    let rows = decoded.rows();
    let start = Instant::now();
    for _ in 0..MEASURE_ITERS {
        run_decoded_clone_once(decoded);
    }
    let total = start.elapsed();

    Ok(RunMeasurement { rows, total })
}

fn run_decoded_clone_once(decoded: &DecodedRequest) {
    match decoded {
        DecodedRequest::Logs(request) => {
            black_box(request.clone());
        }
        DecodedRequest::Traces(request) => {
            black_box(request.clone());
        }
        DecodedRequest::Metrics(request) => {
            black_box(request.clone());
        }
    }
}

impl DecodedRequest {
    fn rows(&self) -> Vec<(&'static str, usize)> {
        match self {
            DecodedRequest::Logs(request) => {
                let rows = request
                    .resource_logs
                    .iter()
                    .map(|resource_logs| {
                        resource_logs
                            .scope_logs
                            .iter()
                            .map(|scope_logs| scope_logs.log_records.len())
                            .sum::<usize>()
                    })
                    .sum();
                vec![("logs", rows)]
            }
            DecodedRequest::Traces(request) => {
                let rows = request
                    .resource_spans
                    .iter()
                    .map(|resource_spans| {
                        resource_spans
                            .scope_spans
                            .iter()
                            .map(|scope_spans| scope_spans.spans.len())
                            .sum::<usize>()
                    })
                    .sum();
                vec![("traces", rows)]
            }
            DecodedRequest::Metrics(request) => metric_request_rows(request),
        }
    }
}

fn run_once(
    kind: WorkloadKind,
    path: PathKind,
    bytes: &[u8],
) -> Result<Vec<(&'static str, usize)>, Box<dyn std::error::Error>> {
    match kind {
        WorkloadKind::Logs => {
            let batch = match path {
                PathKind::Default => transform_logs(bytes, InputFormat::Protobuf)?,
                PathKind::OtapStar => {
                    let LogsOutput::OtapStar(batches) = transform_logs_with_schema(
                        bytes,
                        InputFormat::Protobuf,
                        SchemaOutput::OtapStar,
                    )?
                    else {
                        unreachable!("requested otap-star logs output")
                    };
                    return Ok(batches
                        .iter_named_batches()
                        .map(|(name, batch)| (name, batch.num_rows()))
                        .collect());
                }
                PathKind::Decode => return decode_only_logs(bytes),
            };
            Ok(vec![("logs", batch.num_rows())])
        }
        WorkloadKind::Traces => {
            let batch = match path {
                PathKind::Default => transform_traces(bytes, InputFormat::Protobuf)?,
                PathKind::OtapStar => {
                    let TracesOutput::OtapStar(batches) = transform_traces_with_schema(
                        bytes,
                        InputFormat::Protobuf,
                        SchemaOutput::OtapStar,
                    )?
                    else {
                        unreachable!("requested otap-star traces output")
                    };
                    return Ok(batches
                        .iter_named_batches()
                        .map(|(name, batch)| (name, batch.num_rows()))
                        .collect());
                }
                PathKind::Decode => return decode_only_traces(bytes),
            };
            Ok(vec![("traces", batch.num_rows())])
        }
        WorkloadKind::Metrics => {
            let batches = match path {
                PathKind::Default => transform_metrics(bytes, InputFormat::Protobuf)?,
                PathKind::OtapStar => {
                    let MetricsOutput::OtapStar(batches) = transform_metrics_with_schema(
                        bytes,
                        InputFormat::Protobuf,
                        SchemaOutput::OtapStar,
                    )?
                    else {
                        unreachable!("requested otap-star metrics output")
                    };
                    return Ok(batches
                        .iter_named_batches()
                        .map(|(name, batch)| (name, batch.num_rows()))
                        .collect());
                }
                PathKind::Decode => return decode_only_metrics(bytes),
            };
            Ok(metric_batch_rows(batches))
        }
    }
}

fn metric_batch_rows(batches: otlp2records::MetricBatches) -> Vec<(&'static str, usize)> {
    let mut out = Vec::new();
    if let Some(batch) = batches.gauge {
        out.push(("gauge", batch.num_rows()));
    }
    if let Some(batch) = batches.sum {
        out.push(("sum", batch.num_rows()));
    }
    if let Some(batch) = batches.histogram {
        out.push(("histogram", batch.num_rows()));
    }
    if let Some(batch) = batches.exp_histogram {
        out.push(("exp_histogram", batch.num_rows()));
    }
    out
}

fn decode_only_logs(
    bytes: &[u8],
) -> Result<Vec<(&'static str, usize)>, Box<dyn std::error::Error>> {
    let request = ExportLogsServiceRequest::decode(bytes)?;
    let rows = request
        .resource_logs
        .iter()
        .map(|resource_logs| {
            resource_logs
                .scope_logs
                .iter()
                .map(|scope_logs| scope_logs.log_records.len())
                .sum::<usize>()
        })
        .sum();
    black_box(&request);
    Ok(vec![("logs", rows)])
}

fn decode_only_traces(
    bytes: &[u8],
) -> Result<Vec<(&'static str, usize)>, Box<dyn std::error::Error>> {
    let request = ExportTraceServiceRequest::decode(bytes)?;
    let rows = request
        .resource_spans
        .iter()
        .map(|resource_spans| {
            resource_spans
                .scope_spans
                .iter()
                .map(|scope_spans| scope_spans.spans.len())
                .sum::<usize>()
        })
        .sum();
    black_box(&request);
    Ok(vec![("traces", rows)])
}

fn decode_only_metrics(
    bytes: &[u8],
) -> Result<Vec<(&'static str, usize)>, Box<dyn std::error::Error>> {
    let request = ExportMetricsServiceRequest::decode(bytes)?;
    black_box(&request);
    Ok(metric_request_rows(&request))
}

fn metric_request_rows(request: &ExportMetricsServiceRequest) -> Vec<(&'static str, usize)> {
    let mut gauge = 0;
    let mut sum = 0;
    let mut histogram = 0;
    let mut exp_histogram = 0;

    for resource_metrics in &request.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                match &metric.data {
                    Some(Data::Gauge(data)) => {
                        gauge += data
                            .data_points
                            .iter()
                            .filter(|point| metric_value_is_valid(&point.value))
                            .count();
                    }
                    Some(Data::Sum(data)) => {
                        sum += data
                            .data_points
                            .iter()
                            .filter(|point| metric_value_is_valid(&point.value))
                            .count();
                    }
                    Some(Data::Histogram(data)) => histogram += data.data_points.len(),
                    Some(Data::ExponentialHistogram(data)) => {
                        exp_histogram += data.data_points.len();
                    }
                    _ => {}
                }
            }
        }
    }

    let mut out = Vec::new();
    if gauge > 0 {
        out.push(("gauge", gauge));
    }
    if sum > 0 {
        out.push(("sum", sum));
    }
    if histogram > 0 {
        out.push(("histogram", histogram));
    }
    if exp_histogram > 0 {
        out.push(("exp_histogram", exp_histogram));
    }
    out
}

#[derive(Default)]
struct MetricsShape {
    gauge_rows: usize,
    sum_rows: usize,
    histogram_rows: usize,
    exp_histogram_rows: usize,
    hist_bucket_values: usize,
    hist_bound_values: usize,
    exp_positive_bucket_values: usize,
    exp_negative_bucket_values: usize,
    metric_attr_kvs: usize,
    metric_attr_points: usize,
    exemplars: usize,
    exemplar_attr_kvs: usize,
    resource_attr_kv_row_copies: usize,
    scope_attr_kv_row_copies: usize,
    metric_identity_bytes: usize,
}

fn metric_shape(request: &ExportMetricsServiceRequest) -> MetricsShape {
    let mut shape = MetricsShape::default();
    for resource_metrics in &request.resource_metrics {
        let resource_attr_kvs = resource_metrics
            .resource
            .as_ref()
            .map(|resource| attrs_with_values(&resource.attributes))
            .unwrap_or(0);

        for scope_metrics in &resource_metrics.scope_metrics {
            let scope_attr_kvs = scope_metrics
                .scope
                .as_ref()
                .map(|scope| attrs_with_values(&scope.attributes))
                .unwrap_or(0);

            for metric in &scope_metrics.metrics {
                match &metric.data {
                    Some(Data::Gauge(data)) => {
                        for point in &data.data_points {
                            if metric_value_is_valid(&point.value) {
                                shape.gauge_rows += 1;
                                shape.add_common_point(metric, point.attributes.as_slice());
                                shape.add_common_metric_copies(
                                    metric,
                                    resource_attr_kvs,
                                    scope_attr_kvs,
                                );
                                shape.add_exemplars(&point.exemplars);
                            }
                        }
                    }
                    Some(Data::Sum(data)) => {
                        for point in &data.data_points {
                            if metric_value_is_valid(&point.value) {
                                shape.sum_rows += 1;
                                shape.add_common_point(metric, point.attributes.as_slice());
                                shape.add_common_metric_copies(
                                    metric,
                                    resource_attr_kvs,
                                    scope_attr_kvs,
                                );
                                shape.add_exemplars(&point.exemplars);
                            }
                        }
                    }
                    Some(Data::Histogram(data)) => {
                        for point in &data.data_points {
                            shape.histogram_rows += 1;
                            shape.hist_bucket_values += point.bucket_counts.len();
                            shape.hist_bound_values += point.explicit_bounds.len();
                            shape.add_common_point(metric, point.attributes.as_slice());
                            shape.add_common_metric_copies(
                                metric,
                                resource_attr_kvs,
                                scope_attr_kvs,
                            );
                            shape.add_exemplars(&point.exemplars);
                        }
                    }
                    Some(Data::ExponentialHistogram(data)) => {
                        for point in &data.data_points {
                            shape.exp_histogram_rows += 1;
                            if let Some(positive) = &point.positive {
                                shape.exp_positive_bucket_values += positive.bucket_counts.len();
                            }
                            if let Some(negative) = &point.negative {
                                shape.exp_negative_bucket_values += negative.bucket_counts.len();
                            }
                            shape.add_common_point(metric, point.attributes.as_slice());
                            shape.add_common_metric_copies(
                                metric,
                                resource_attr_kvs,
                                scope_attr_kvs,
                            );
                            shape.add_exemplars(&point.exemplars);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    shape
}

impl MetricsShape {
    fn add_common_point(
        &mut self,
        _metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        attrs: &[KeyValue],
    ) {
        let attr_kvs = attrs_with_values(attrs);
        self.metric_attr_kvs += attr_kvs;
        if attr_kvs > 0 {
            self.metric_attr_points += 1;
        }
    }

    fn add_common_metric_copies(
        &mut self,
        metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        resource_attr_kvs: usize,
        scope_attr_kvs: usize,
    ) {
        self.resource_attr_kv_row_copies += resource_attr_kvs;
        self.scope_attr_kv_row_copies += scope_attr_kvs;
        self.metric_identity_bytes +=
            metric.name.len() + metric.description.len() + metric.unit.len();
    }

    fn add_exemplars(&mut self, exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar]) {
        self.exemplars += exemplars.len();
        self.exemplar_attr_kvs += exemplars
            .iter()
            .map(|exemplar| attrs_with_values(&exemplar.filtered_attributes))
            .sum::<usize>();
    }
}

fn attrs_with_values(attrs: &[KeyValue]) -> usize {
    attrs.iter().filter(|kv| kv.value.is_some()).count()
}

fn metric_value_is_valid(value: &Option<number_data_point::Value>) -> bool {
    match value {
        Some(number_data_point::Value::AsInt(_)) => true,
        Some(number_data_point::Value::AsDouble(value)) => value.is_finite(),
        None => false,
    }
}

fn print_measurement(measurement: Measurement, speedup_vs_default: f64) {
    let total_ms = measurement.total.as_secs_f64() * 1000.0;
    let ms_per_iter = total_ms / measurement.iterations as f64;
    let total_rows = measurement.rows_per_iter * measurement.iterations;
    let rows_per_sec = total_rows as f64 / measurement.total.as_secs_f64();
    let total_mib = (measurement.fixture_bytes * measurement.iterations) as f64 / 1024.0 / 1024.0;
    let mib_per_sec = total_mib / measurement.total.as_secs_f64();

    println!(
        "{},{},{},{},{},{},{:.3},{:.3},{:.0},{:.1},{:.2}",
        measurement.workload,
        measurement.path,
        measurement.table,
        measurement.fixture_bytes,
        measurement.rows_per_iter,
        measurement.iterations,
        total_ms,
        ms_per_iter,
        rows_per_sec,
        mib_per_sec,
        speedup_vs_default
    );
}

fn print_metric_shape(workload: &str, shape: &MetricsShape) {
    println!(
        "metric_shape,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        workload,
        shape.gauge_rows,
        shape.sum_rows,
        shape.histogram_rows,
        shape.exp_histogram_rows,
        shape.hist_bucket_values,
        shape.hist_bound_values,
        shape.exp_positive_bucket_values,
        shape.exp_negative_bucket_values,
        shape.metric_attr_kvs,
        shape.metric_attr_points,
        shape.exemplars,
        shape.exemplar_attr_kvs,
        shape.resource_attr_kv_row_copies,
        shape.scope_attr_kv_row_copies,
        shape.metric_identity_bytes
    );
}
