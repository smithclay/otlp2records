//! Arrow layer for otlp2records
//!
//! Provides Arrow helpers for otlp2records:
//! - Schema accessors for logs, traces, and metrics
//! - Partitioning utilities for service-based grouping

mod partition;
mod schema;

pub use partition::{
    extract_min_timestamp_micros, extract_service_name, group_batch_by_service, PartitionedBatch,
    PartitionedMetrics, ServiceGroupedBatches,
};
pub use schema::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, traces_schema,
};
