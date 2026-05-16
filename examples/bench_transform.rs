use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    metrics::v1::{metric::Data, number_data_point},
};
use otlp2records::{transform_logs, transform_metrics, transform_traces, InputFormat};
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
    for workload in workloads {
        if !Path::new(workload.fixture).exists() {
            eprintln!("skipping missing fixture {}", workload.fixture);
            continue;
        }

        let bytes = fs::read(workload.fixture)?;
        let default = measure(workload.kind, PathKind::Default, &bytes)?;
        let decode = measure(workload.kind, PathKind::Decode, &bytes)?;

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
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum PathKind {
    Default,
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

fn run_once(
    kind: WorkloadKind,
    path: PathKind,
    bytes: &[u8],
) -> Result<Vec<(&'static str, usize)>, Box<dyn std::error::Error>> {
    match kind {
        WorkloadKind::Logs => {
            let batch = match path {
                PathKind::Default => transform_logs(bytes, InputFormat::Protobuf)?,
                PathKind::Decode => return decode_only_logs(bytes),
            };
            Ok(vec![("logs", batch.num_rows())])
        }
        WorkloadKind::Traces => {
            let batch = match path {
                PathKind::Default => transform_traces(bytes, InputFormat::Protobuf)?,
                PathKind::Decode => return decode_only_traces(bytes),
            };
            Ok(vec![("traces", batch.num_rows())])
        }
        WorkloadKind::Metrics => {
            let batches = match path {
                PathKind::Default => transform_metrics(bytes, InputFormat::Protobuf)?,
                PathKind::Decode => return decode_only_metrics(bytes),
            };
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
            Ok(out)
        }
    }
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

    black_box(&request);
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
    Ok(out)
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
