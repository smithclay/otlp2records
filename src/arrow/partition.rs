//! Partition utilities for Arrow RecordBatches.
//!
//! Provides service-based partitioning and metadata extraction for
//! storage routing without exposing VRL internals.

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::take;
use arrow::datatypes::{Int64Type, TimestampMillisecondType};
use arrow::record_batch::RecordBatch;
use indexmap::IndexMap;

use crate::decode::SkippedMetrics;

/// A RecordBatch with partition metadata for storage routing.
///
/// Contains the batch data along with pre-extracted metadata needed
/// for partitioned storage paths (service name and timestamp).
#[derive(Debug, Clone)]
pub struct PartitionedBatch {
    /// The Arrow RecordBatch containing signal records
    pub batch: RecordBatch,
    /// Service name extracted from the batch
    pub service_name: Arc<str>,
    /// Minimum timestamp in microseconds (for partition path generation)
    pub min_timestamp_micros: i64,
    /// Number of records in this batch
    pub record_count: usize,
}

/// Multiple batches grouped by service name.
///
/// This is the primary output type when service-based partitioning is needed.
/// Batches are ordered by first occurrence of each service name.
#[derive(Debug, Clone, Default)]
pub struct ServiceGroupedBatches {
    /// Batches keyed by service name, in insertion order
    pub batches: Vec<PartitionedBatch>,
    /// Total record count across all batches
    pub total_records: usize,
}

impl ServiceGroupedBatches {
    /// Iterate over (service_name, batch) pairs
    pub fn iter(&self) -> impl Iterator<Item = (&str, &RecordBatch)> {
        self.batches
            .iter()
            .map(|pb| (pb.service_name.as_ref(), &pb.batch))
    }

    /// Consume and iterate over partitioned batches
    pub fn into_iter(self) -> impl Iterator<Item = PartitionedBatch> {
        self.batches.into_iter()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Number of service groups
    pub fn len(&self) -> usize {
        self.batches.len()
    }
}

/// Partitioned metrics result, with each metric type grouped by service.
#[derive(Debug, Default)]
pub struct PartitionedMetrics {
    /// Gauge metrics grouped by service
    pub gauge: ServiceGroupedBatches,
    /// Sum metrics grouped by service
    pub sum: ServiceGroupedBatches,
    /// Metrics that were skipped during processing
    pub skipped: SkippedMetrics,
}

/// Group a RecordBatch by service_name column.
///
/// Returns batches grouped by service name, preserving insertion order
/// (order in which services first appear in the input batch).
///
/// # Panics
/// Panics if the batch does not contain a "service_name" column of string type.
pub fn group_batch_by_service(batch: RecordBatch) -> ServiceGroupedBatches {
    if batch.num_rows() == 0 {
        return ServiceGroupedBatches::default();
    }

    let service_col = batch
        .column_by_name("service_name")
        .expect("service_name column required");

    let service_array = service_col
        .as_string_opt::<i32>()
        .expect("service_name must be Utf8 string type");

    // Group row indices by service name (preserving insertion order)
    let mut groups: IndexMap<&str, Vec<u32>> = IndexMap::new();
    for (idx, service) in service_array.iter().enumerate() {
        let service = service.unwrap_or("unknown");
        groups.entry(service).or_default().push(idx as u32);
    }

    let total_records = batch.num_rows();
    let schema = batch.schema();

    let batches = groups
        .into_iter()
        .map(|(service, indices)| {
            let indices = arrow::array::UInt32Array::from(indices);
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| take(col.as_ref(), &indices, None).expect("take should succeed"))
                .collect();
            let grouped =
                RecordBatch::try_new(schema.clone(), columns).expect("schema should match");
            let min_ts = extract_min_timestamp_micros(&grouped);

            PartitionedBatch {
                record_count: grouped.num_rows(),
                batch: grouped,
                service_name: Arc::from(service),
                min_timestamp_micros: min_ts,
            }
        })
        .collect();

    ServiceGroupedBatches {
        batches,
        total_records,
    }
}

/// Extract the minimum timestamp from a RecordBatch.
///
/// Looks for a "timestamp" column and returns the minimum value in microseconds.
/// The timestamp column stores milliseconds, so we multiply by 1000.
/// Returns 0 if no valid timestamps found.
pub fn extract_min_timestamp_micros(batch: &RecordBatch) -> i64 {
    if batch.num_rows() == 0 {
        return 0;
    }

    let ts_col = match batch.column_by_name("timestamp") {
        Some(col) => col,
        None => return 0,
    };

    // Timestamp column is TimestampMillisecond
    if let Some(ts_array) = ts_col.as_primitive_opt::<TimestampMillisecondType>() {
        let min_ms = ts_array.iter().flatten().min().unwrap_or(0);
        // Convert milliseconds to microseconds
        min_ms * 1000
    } else if let Some(ts_array) = ts_col.as_primitive_opt::<Int64Type>() {
        // Fallback for Int64 timestamp column
        let min_ms = ts_array.iter().flatten().min().unwrap_or(0);
        min_ms * 1000
    } else {
        0
    }
}

/// Extract the first service name from a RecordBatch.
///
/// Returns "unknown" if no valid service name found.
pub fn extract_service_name(batch: &RecordBatch) -> Arc<str> {
    if batch.num_rows() == 0 {
        return Arc::from("unknown");
    }

    let service_col = match batch.column_by_name("service_name") {
        Some(col) => col,
        None => return Arc::from("unknown"),
    };

    if let Some(service_array) = service_col.as_string_opt::<i32>() {
        service_array
            .iter()
            .flatten()
            .next()
            .map(Arc::from)
            .unwrap_or_else(|| Arc::from("unknown"))
    } else {
        Arc::from("unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringBuilder, TimestampMillisecondBuilder};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc as StdArc;

    fn create_test_batch(services: &[&str], timestamps_ms: &[i64]) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let mut ts_builder = TimestampMillisecondBuilder::new();
        let mut service_builder = StringBuilder::new();
        let mut value_builder = Int32Array::builder(services.len());

        for (i, (service, ts)) in services.iter().zip(timestamps_ms.iter()).enumerate() {
            ts_builder.append_value(*ts);
            service_builder.append_value(service);
            value_builder.append_value(i as i32);
        }

        RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(ts_builder.finish()),
                StdArc::new(service_builder.finish()),
                StdArc::new(value_builder.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_group_batch_by_service_single_service() {
        let batch = create_test_batch(&["svc-a", "svc-a", "svc-a"], &[100, 200, 150]);
        let grouped = group_batch_by_service(batch);

        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped.total_records, 3);
        assert_eq!(grouped.batches[0].service_name.as_ref(), "svc-a");
        assert_eq!(grouped.batches[0].record_count, 3);
        // min timestamp is 100ms = 100_000 micros
        assert_eq!(grouped.batches[0].min_timestamp_micros, 100_000);
    }

    #[test]
    fn test_group_batch_by_service_multiple_services() {
        let batch = create_test_batch(
            &["svc-a", "svc-b", "svc-a", "svc-c", "svc-b"],
            &[100, 200, 150, 300, 250],
        );
        let grouped = group_batch_by_service(batch);

        assert_eq!(grouped.len(), 3);
        assert_eq!(grouped.total_records, 5);

        // Check order is preserved (first occurrence)
        assert_eq!(grouped.batches[0].service_name.as_ref(), "svc-a");
        assert_eq!(grouped.batches[0].record_count, 2);
        assert_eq!(grouped.batches[0].min_timestamp_micros, 100_000); // min(100, 150) * 1000

        assert_eq!(grouped.batches[1].service_name.as_ref(), "svc-b");
        assert_eq!(grouped.batches[1].record_count, 2);
        assert_eq!(grouped.batches[1].min_timestamp_micros, 200_000); // min(200, 250) * 1000

        assert_eq!(grouped.batches[2].service_name.as_ref(), "svc-c");
        assert_eq!(grouped.batches[2].record_count, 1);
        assert_eq!(grouped.batches[2].min_timestamp_micros, 300_000);
    }

    #[test]
    fn test_group_batch_by_service_empty() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let grouped = group_batch_by_service(batch);

        assert!(grouped.is_empty());
        assert_eq!(grouped.total_records, 0);
    }

    #[test]
    fn test_extract_min_timestamp_micros() {
        let batch = create_test_batch(&["svc-a", "svc-a"], &[100, 50]);
        let min_ts = extract_min_timestamp_micros(&batch);
        assert_eq!(min_ts, 50_000); // 50ms * 1000 = 50000 micros
    }

    #[test]
    fn test_extract_service_name() {
        let batch = create_test_batch(&["my-service", "other"], &[100, 200]);
        let service = extract_service_name(&batch);
        assert_eq!(service.as_ref(), "my-service");
    }
}
