//! Public transform orchestration for service-partitioned batches.

use crate::{
    api::{transform_logs, transform_metrics, transform_traces},
    arrow::{group_batch_by_service, PartitionedMetrics, ServiceGroupedBatches},
    decode::InputFormat,
    Result,
};

/// Transform OTLP logs with service-based partitioning.
pub fn transform_logs_partitioned(
    bytes: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches> {
    let batch = transform_logs(bytes, format)?;
    Ok(group_batch_by_service(batch))
}

/// Transform OTLP traces with service-based partitioning.
pub fn transform_traces_partitioned(
    bytes: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches> {
    let batch = transform_traces(bytes, format)?;
    Ok(group_batch_by_service(batch))
}

/// Transform OTLP metrics with service-based partitioning.
pub fn transform_metrics_partitioned(
    bytes: &[u8],
    format: InputFormat,
) -> Result<PartitionedMetrics> {
    let batches = transform_metrics(bytes, format)?;

    let gauge = match batches.gauge {
        Some(batch) => group_batch_by_service(batch),
        None => ServiceGroupedBatches::default(),
    };

    let sum = match batches.sum {
        Some(batch) => group_batch_by_service(batch),
        None => ServiceGroupedBatches::default(),
    };

    let histogram = match batches.histogram {
        Some(batch) => group_batch_by_service(batch),
        None => ServiceGroupedBatches::default(),
    };

    let exp_histogram = match batches.exp_histogram {
        Some(batch) => group_batch_by_service(batch),
        None => ServiceGroupedBatches::default(),
    };

    Ok(PartitionedMetrics {
        gauge,
        sum,
        histogram,
        exp_histogram,
        skipped: batches.skipped,
    })
}
