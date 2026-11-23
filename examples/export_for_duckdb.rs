use std::path::PathBuf;

use arrow_array::RecordBatch;
use otlp2records::{
    to_ipc, to_parquet, transform_logs, transform_metrics, transform_traces, ArrowProfile,
    InputFormat,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = PathBuf::from(
        std::env::args()
            .nth(1)
            .ok_or("usage: export_for_duckdb <dir>")?,
    );
    std::fs::create_dir_all(&out)?;
    let testdata = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata");
    let read = |name: &str| std::fs::read(testdata.join(name));

    let mut batches: Vec<(String, RecordBatch)> = vec![
        (
            "logs".into(),
            transform_logs(&read("logs_large.pb")?, InputFormat::Protobuf)?,
        ),
        (
            "traces".into(),
            transform_traces(&read("traces_large.pb")?, InputFormat::Protobuf)?,
        ),
    ];
    for file in [
        "metrics_mixed.pb",
        "metrics_histogram.pb",
        "metrics_exponential_histogram.pb",
    ] {
        let metrics = transform_metrics(&read(file)?, InputFormat::Protobuf)?;
        for (shape, batch) in [
            ("gauge", metrics.gauge),
            ("sum", metrics.sum),
            ("histogram", metrics.histogram),
            ("exp_histogram", metrics.exp_histogram),
        ] {
            if let Some(batch) = batch {
                if !batches.iter().any(|(name, _)| name == shape) {
                    batches.push((shape.into(), batch));
                }
            }
        }
    }

    for (name, batch) in batches {
        std::fs::write(
            out.join(format!("{name}.canonical.arrows")),
            to_ipc(&batch)?,
        )?;
        let duckdb = ArrowProfile::DuckDb.apply(batch.clone())?;
        std::fs::write(out.join(format!("{name}.duckdb.arrows")), to_ipc(&duckdb)?)?;
        std::fs::write(out.join(format!("{name}.parquet")), to_parquet(&batch)?)?;
        println!("{name}: {} rows", batch.num_rows());
    }
    Ok(())
}
