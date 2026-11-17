//! Arrow layer for otlp2records
//!
//! Provides Arrow helpers for service-based grouping and output type profiles.

mod partition;
mod profile;

pub use partition::{
    extract_min_timestamp_micros, extract_service_name, group_batch_by_service, PartitionedBatch,
    PartitionedMetrics, ServiceGroupedBatches,
};
pub use profile::ArrowProfile;
