use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use otlp2records::{transform_logs, transform_metrics, transform_traces, InputFormat};

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
    table: &'static str,
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

    println!("workload,table,rows_per_iter,iterations,total_ms,ms_per_iter,rows_per_sec");
    for workload in workloads {
        if !Path::new(workload.fixture).exists() {
            eprintln!("skipping missing fixture {}", workload.fixture);
            continue;
        }

        let bytes = fs::read(workload.fixture)?;

        for _ in 0..WARMUP_ITERS {
            run_once(workload.kind, &bytes)?;
        }

        let mut last_rows = Vec::new();
        let start = Instant::now();
        for _ in 0..MEASURE_ITERS {
            last_rows = run_once(workload.kind, &bytes)?;
        }
        let total = start.elapsed();

        for (table, rows) in last_rows {
            let measurement = Measurement {
                workload: workload.name,
                table,
                rows_per_iter: rows,
                iterations: MEASURE_ITERS,
                total,
            };
            print_measurement(measurement);
        }
    }

    Ok(())
}

fn run_once(
    kind: WorkloadKind,
    bytes: &[u8],
) -> Result<Vec<(&'static str, usize)>, Box<dyn std::error::Error>> {
    match kind {
        WorkloadKind::Logs => {
            let batch = transform_logs(bytes, InputFormat::Protobuf)?;
            Ok(vec![("logs", batch.num_rows())])
        }
        WorkloadKind::Traces => {
            let batch = transform_traces(bytes, InputFormat::Protobuf)?;
            Ok(vec![("traces", batch.num_rows())])
        }
        WorkloadKind::Metrics => {
            let batches = transform_metrics(bytes, InputFormat::Protobuf)?;
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

fn print_measurement(measurement: Measurement) {
    let total_ms = measurement.total.as_secs_f64() * 1000.0;
    let ms_per_iter = total_ms / measurement.iterations as f64;
    let total_rows = measurement.rows_per_iter * measurement.iterations;
    let rows_per_sec = total_rows as f64 / measurement.total.as_secs_f64();

    println!(
        "{},{},{},{},{:.3},{:.3},{:.0}",
        measurement.workload,
        measurement.table,
        measurement.rows_per_iter,
        measurement.iterations,
        total_ms,
        ms_per_iter,
        rows_per_sec
    );
}
