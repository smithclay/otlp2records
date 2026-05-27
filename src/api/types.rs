//! Public transform result types.

use std::str::FromStr;

use arrow_array::RecordBatch;

use crate::{DecodeError, Error};

/// Public schema output selection for transform APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaOutput {
    /// Current flattened ClickStack-compatible schema.
    #[default]
    Normalized,
    /// Multi-table OTAP Arrow schema modeled after otel-arrow's data model.
    OtapStar,
}

impl FromStr for SchemaOutput {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "default" | "normalized" | "clickstack" | "clickstack-mode" => {
                Ok(Self::Normalized)
            }
            "otap-star" | "otap_star" => Ok(Self::OtapStar),
            other => Err(Error::Decode(DecodeError::Unsupported(format!(
                "unsupported schema output: {other}"
            )))),
        }
    }
}

/// Result of transforming logs with explicit schema selection.
#[derive(Debug)]
pub enum LogsOutput {
    /// Current flattened ClickStack-compatible log batch.
    Normalized(RecordBatch),
    /// OTAP star multi-table log batches.
    OtapStar(OtapLogsBatches),
}

/// Result of transforming traces with explicit schema selection.
///
/// The OTAP variant is boxed to keep the enum small — the OTAP trace tables
/// are ~8× the size of a normalized RecordBatch.
#[derive(Debug)]
pub enum TracesOutput {
    /// Current flattened ClickStack-compatible span batch.
    Normalized(RecordBatch),
    /// OTAP star multi-table trace batches.
    OtapStar(Box<OtapTracesBatches>),
}

/// Result of transforming metrics with explicit schema selection.
///
/// The OTAP variant is boxed (it carries 18 record batches); see
/// `TracesOutput` for the same rationale.
#[derive(Debug)]
pub enum MetricsOutput {
    /// Current flattened ClickStack-compatible metric batches.
    Normalized(MetricBatches),
    /// OTAP star multi-table metric batches.
    OtapStar(Box<OtapMetricsBatches>),
}

/// OTAP star log tables.
#[derive(Debug)]
pub struct OtapLogsBatches {
    pub logs: RecordBatch,
    pub resource_attrs: RecordBatch,
    pub scope_attrs: RecordBatch,
    pub log_attrs: RecordBatch,
}

impl OtapLogsBatches {
    pub fn iter_named_batches(&self) -> impl Iterator<Item = (&'static str, &RecordBatch)> {
        [
            ("logs", &self.logs),
            ("resource_attrs", &self.resource_attrs),
            ("scope_attrs", &self.scope_attrs),
            ("log_attrs", &self.log_attrs),
        ]
        .into_iter()
    }
}

/// OTAP star trace tables.
#[derive(Debug)]
pub struct OtapTracesBatches {
    pub spans: RecordBatch,
    pub resource_attrs: RecordBatch,
    pub scope_attrs: RecordBatch,
    pub span_attrs: RecordBatch,
    pub span_events: RecordBatch,
    pub span_event_attrs: RecordBatch,
    pub span_links: RecordBatch,
    pub span_link_attrs: RecordBatch,
}

impl OtapTracesBatches {
    pub fn iter_named_batches(&self) -> impl Iterator<Item = (&'static str, &RecordBatch)> {
        [
            ("spans", &self.spans),
            ("resource_attrs", &self.resource_attrs),
            ("scope_attrs", &self.scope_attrs),
            ("span_attrs", &self.span_attrs),
            ("span_events", &self.span_events),
            ("span_event_attrs", &self.span_event_attrs),
            ("span_links", &self.span_links),
            ("span_link_attrs", &self.span_link_attrs),
        ]
        .into_iter()
    }
}

/// OTAP star metric tables.
#[derive(Debug)]
pub struct OtapMetricsBatches {
    pub metrics: RecordBatch,
    pub resource_attrs: RecordBatch,
    pub scope_attrs: RecordBatch,
    pub number_data_points: RecordBatch,
    pub number_dp_attrs: RecordBatch,
    pub number_dp_exemplars: RecordBatch,
    pub number_dp_exemplar_attrs: RecordBatch,
    pub summary_data_points: RecordBatch,
    pub quantile: RecordBatch,
    pub summary_dp_attrs: RecordBatch,
    pub histogram_data_points: RecordBatch,
    pub histogram_dp_attrs: RecordBatch,
    pub histogram_dp_exemplars: RecordBatch,
    pub histogram_dp_exemplar_attrs: RecordBatch,
    pub exp_histogram_data_points: RecordBatch,
    pub exp_histogram_dp_attrs: RecordBatch,
    pub exp_histogram_dp_exemplars: RecordBatch,
    pub exp_histogram_dp_exemplar_attrs: RecordBatch,
    pub skipped: SkippedMetrics,
}

/// Number of named tables exposed by [`OtapMetricsBatches::iter_named_batches`].
/// Kept in sync via the `iter_named_batches_covers_all_metric_tables` test;
/// bump this and the iterator together whenever a new table is added.
#[cfg(test)]
const OTAP_METRICS_NAMED_BATCH_COUNT: usize = 18;

impl OtapMetricsBatches {
    pub fn iter_named_batches(&self) -> impl Iterator<Item = (&'static str, &RecordBatch)> {
        [
            ("metrics", &self.metrics),
            ("resource_attrs", &self.resource_attrs),
            ("scope_attrs", &self.scope_attrs),
            ("number_data_points", &self.number_data_points),
            ("number_dp_attrs", &self.number_dp_attrs),
            ("number_dp_exemplars", &self.number_dp_exemplars),
            ("number_dp_exemplar_attrs", &self.number_dp_exemplar_attrs),
            ("summary_data_points", &self.summary_data_points),
            ("quantile", &self.quantile),
            ("summary_dp_attrs", &self.summary_dp_attrs),
            ("histogram_data_points", &self.histogram_data_points),
            ("histogram_dp_attrs", &self.histogram_dp_attrs),
            ("histogram_dp_exemplars", &self.histogram_dp_exemplars),
            (
                "histogram_dp_exemplar_attrs",
                &self.histogram_dp_exemplar_attrs,
            ),
            ("exp_histogram_data_points", &self.exp_histogram_data_points),
            ("exp_histogram_dp_attrs", &self.exp_histogram_dp_attrs),
            (
                "exp_histogram_dp_exemplars",
                &self.exp_histogram_dp_exemplars,
            ),
            (
                "exp_histogram_dp_exemplar_attrs",
                &self.exp_histogram_dp_exemplar_attrs,
            ),
        ]
        .into_iter()
    }
}

/// Tracks metrics that were skipped during transformation.
#[derive(Debug, Default, Clone)]
pub struct SkippedMetrics {
    /// Count of summary metrics skipped (deprecated in OTLP spec).
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use std::sync::Arc;

    fn empty() -> RecordBatch {
        RecordBatch::new_empty(Arc::new(Schema::empty()))
    }

    /// Guards against silent drift between the named-batch iterator and the
    /// struct fields. If a new table is added to `OtapMetricsBatches`, bump
    /// `OTAP_METRICS_NAMED_BATCH_COUNT` and add the corresponding entry to
    /// `iter_named_batches`.
    #[test]
    fn iter_named_batches_covers_all_metric_tables() {
        let batches = OtapMetricsBatches {
            metrics: empty(),
            resource_attrs: empty(),
            scope_attrs: empty(),
            number_data_points: empty(),
            number_dp_attrs: empty(),
            number_dp_exemplars: empty(),
            number_dp_exemplar_attrs: empty(),
            summary_data_points: empty(),
            quantile: empty(),
            summary_dp_attrs: empty(),
            histogram_data_points: empty(),
            histogram_dp_attrs: empty(),
            histogram_dp_exemplars: empty(),
            histogram_dp_exemplar_attrs: empty(),
            exp_histogram_data_points: empty(),
            exp_histogram_dp_attrs: empty(),
            exp_histogram_dp_exemplars: empty(),
            exp_histogram_dp_exemplar_attrs: empty(),
            skipped: SkippedMetrics::default(),
        };
        assert_eq!(
            batches.iter_named_batches().count(),
            OTAP_METRICS_NAMED_BATCH_COUNT
        );
        // Names must be unique (no duplicate or stale entries).
        let names: Vec<_> = batches.iter_named_batches().map(|(n, _)| n).collect();
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            names.len(),
            "duplicate names in iter_named_batches"
        );
    }
}
