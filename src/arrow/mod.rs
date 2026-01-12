//! Arrow layer for otlp2records
//!
//! Provides Arrow RecordBatch construction from VRL-transformed values:
//! - Schema accessors for logs, traces, and metrics
//! - RecordBatch builder for converting VRL Values to Arrow arrays
//! - Partitioning utilities for service-based grouping

mod builder;
mod partition;
mod schema;

pub use builder::values_to_arrow;
pub use partition::{
    extract_min_timestamp_micros, extract_service_name, group_batch_by_service, PartitionedBatch,
    PartitionedMetrics, ServiceGroupedBatches,
};
pub use schema::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, traces_schema,
};
