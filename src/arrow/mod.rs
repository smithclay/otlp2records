//! Arrow layer for otlp2records
//!
//! Provides Arrow helpers for service-based grouping.

mod partition;

pub use partition::{
    extract_min_timestamp_micros, extract_service_name, group_batch_by_service, PartitionedBatch,
    PartitionedMetrics, ServiceGroupedBatches,
};
