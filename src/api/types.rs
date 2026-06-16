//! Public transform result types.

use arrow_array::RecordBatch;

/// Tracks metrics that were skipped during transformation.
#[derive(Debug, Default, Clone)]
pub struct SkippedMetrics {
    /// Count of summary metrics skipped. The normalized output intentionally
    /// drops summary metrics; this records how many were seen.
    pub summaries: usize,
    /// Count of data points skipped due to NaN values.
    pub nan_values: usize,
    /// Count of data points skipped due to Infinity values.
    pub infinity_values: usize,
    /// Count of data points skipped due to missing values.
    pub missing_values: usize,
}

impl SkippedMetrics {
    /// Returns true if any metrics were skipped.
    pub fn has_skipped(&self) -> bool {
        self.summaries > 0
            || self.nan_values > 0
            || self.infinity_values > 0
            || self.missing_values > 0
    }

    /// Returns total count of skipped items.
    pub fn total(&self) -> usize {
        self.summaries + self.nan_values + self.infinity_values + self.missing_values
    }
}

/// Result of transforming OTLP metrics to Arrow RecordBatches.
///
/// Metrics are separated by type because each metric type has a different schema.
/// Each field is `None` if there were no metrics of that type in the input.
///
/// The `skipped` field provides visibility into what data was not processed,
/// including unsupported metric types (summary) and invalid data points
/// (NaN, Infinity, missing values).
#[derive(Debug)]
pub struct MetricBatches {
    /// RecordBatch containing gauge metrics (if any)
    pub gauge: Option<RecordBatch>,
    /// RecordBatch containing sum metrics (if any)
    pub sum: Option<RecordBatch>,
    /// RecordBatch containing histogram metrics (if any)
    pub histogram: Option<RecordBatch>,
    /// RecordBatch containing exponential histogram metrics (if any)
    pub exp_histogram: Option<RecordBatch>,
    /// Metrics that were skipped during processing
    pub skipped: SkippedMetrics,
}

/// Result of transforming OTLP metrics to JSON values.
#[derive(Debug)]
pub struct JsonMetricBatches {
    /// JSON values for gauge metrics
    pub gauge: Vec<serde_json::Value>,
    /// JSON values for sum metrics
    pub sum: Vec<serde_json::Value>,
    /// JSON values for histogram metrics
    pub histogram: Vec<serde_json::Value>,
    /// JSON values for exponential histogram metrics
    pub exp_histogram: Vec<serde_json::Value>,
    /// Metrics that were skipped during processing
    pub skipped: SkippedMetrics,
}
