//! OTLP metrics decoding - protobuf and JSON
//!
//! Supports Gauge, Sum, Histogram, and ExponentialHistogram metric types.
//! Summary metrics are skipped (deprecated in OTLP spec).
//! Data points with non-finite values (NaN, Infinity) are skipped.

/// Tracks metrics that were skipped during decoding.
///
/// This struct is returned alongside decoded metrics to provide visibility
/// into what data was not processed (unsupported types or invalid values).
#[derive(Debug, Default, Clone)]
pub struct SkippedMetrics {
    /// Count of summary metrics skipped (deprecated in OTLP spec)
    pub summaries: usize,
    /// Count of data points skipped due to NaN values
    pub nan_values: usize,
    /// Count of data points skipped due to Infinity values
    pub infinity_values: usize,
    /// Count of data points skipped due to missing values
    pub missing_values: usize,
}

impl SkippedMetrics {
    /// Returns true if any metrics were skipped
    pub fn has_skipped(&self) -> bool {
        self.summaries > 0
            || self.nan_values > 0
            || self.infinity_values > 0
            || self.missing_values > 0
    }

    /// Returns total count of skipped items
    pub fn total(&self) -> usize {
        self.summaries + self.nan_values + self.infinity_values + self.missing_values
    }
}

use bytes::Bytes;
use const_hex::encode as hex_encode;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
use prost::Message;
use serde::Deserialize;
use std::sync::Arc;
use vrl::value::{ObjectMap, Value as VrlValue};

use super::common::{
    decode_bytes_field, finite_float_to_vrl, for_each_resource_scope, json_attrs_to_value,
    json_resource_to_value, json_scope_to_value, json_timestamp_to_i64, otlp_attributes_to_value,
    otlp_resource_to_value, otlp_scope_to_value, safe_timestamp_conversion, DecodeError,
    JsonInstrumentationScope, JsonKeyValue, JsonNumberOrString, JsonResource,
};

// ============================================================================
// Protobuf decoding
// ============================================================================

/// Result of decoding metrics, including values and tracking of skipped items
pub struct DecodeMetricsResult {
    /// Successfully decoded metric values
    pub values: Vec<VrlValue>,
    /// Metrics that were skipped during decoding
    pub skipped: SkippedMetrics,
}

pub fn decode_protobuf(body: &[u8]) -> Result<DecodeMetricsResult, DecodeError> {
    let request = ExportMetricsServiceRequest::decode(body)?;
    export_metrics_to_vrl_proto(request)
}

/// Shared context for metric metadata to reduce function argument count
struct MetricContext {
    metric_name: Bytes,
    metric_description: Bytes,
    metric_unit: Bytes,
    resource: Arc<VrlValue>,
    scope: Arc<VrlValue>,
}

fn export_metrics_to_vrl_proto(
    request: ExportMetricsServiceRequest,
) -> Result<DecodeMetricsResult, DecodeError> {
    let mut values = preallocate_metric_values(&request.resource_metrics, |rm| {
        rm.scope_metrics
            .iter()
            .flat_map(|sm| sm.metrics.iter())
            .map(count_data_points)
            .sum()
    });
    let mut skipped = SkippedMetrics::default();

    for_each_resource_scope(
        request.resource_metrics,
        |resource_metrics| {
            (
                otlp_resource_to_value(resource_metrics.resource.as_ref()),
                resource_metrics.scope_metrics,
            )
        },
        |scope_metrics| {
            (
                otlp_scope_to_value(scope_metrics.scope.as_ref()),
                scope_metrics.metrics,
            )
        },
        |metrics, resource, scope| {
            for metric in metrics {
                let ctx = MetricContext {
                    metric_name: Bytes::from(metric.name.clone()),
                    metric_description: Bytes::from(metric.description.clone()),
                    metric_unit: Bytes::from(metric.unit.clone()),
                    resource: Arc::clone(&resource),
                    scope: Arc::clone(&scope),
                };

                match metric.data {
                    Some(Data::Gauge(gauge)) => {
                        for point in gauge.data_points {
                            match build_gauge_from_point(&point, &ctx)? {
                                Some(record) => values.push(record),
                                None => track_skipped_value(&point.value, &mut skipped),
                            }
                        }
                    }
                    Some(Data::Sum(sum)) => {
                        let aggregation_temporality = sum.aggregation_temporality as i64;
                        let is_monotonic = sum.is_monotonic;
                        for point in sum.data_points {
                            match build_sum_from_point(
                                &point,
                                &ctx,
                                aggregation_temporality,
                                is_monotonic,
                            )? {
                                Some(record) => values.push(record),
                                None => track_skipped_value(&point.value, &mut skipped),
                            }
                        }
                    }
                    Some(Data::Histogram(h)) => {
                        let aggregation_temporality = h.aggregation_temporality as i64;
                        for point in h.data_points {
                            let record =
                                build_histogram_from_point(&point, &ctx, aggregation_temporality)?;
                            values.push(record);
                        }
                    }
                    Some(Data::ExponentialHistogram(eh)) => {
                        let aggregation_temporality = eh.aggregation_temporality as i64;
                        for point in eh.data_points {
                            let record = build_exp_histogram_from_point(
                                &point,
                                &ctx,
                                aggregation_temporality,
                            )?;
                            values.push(record);
                        }
                    }
                    Some(Data::Summary(s)) => {
                        skipped.summaries += s.data_points.len();
                    }
                    None => {
                        // Metric with no data - nothing to skip
                    }
                }
            }

            Ok::<(), DecodeError>(())
        },
    )?;

    Ok(DecodeMetricsResult { values, skipped })
}

/// Track why a metric value was skipped
fn track_skipped_value(value: &Option<Value>, skipped: &mut SkippedMetrics) {
    match value {
        Some(Value::AsDouble(d)) if d.is_nan() => skipped.nan_values += 1,
        Some(Value::AsDouble(d)) if d.is_infinite() => skipped.infinity_values += 1,
        None => skipped.missing_values += 1,
        _ => {} // Integer values shouldn't be skipped
    }
}

fn count_data_points(metric: &opentelemetry_proto::tonic::metrics::v1::Metric) -> usize {
    match &metric.data {
        Some(Data::Gauge(g)) => g.data_points.len(),
        Some(Data::Sum(s)) => s.data_points.len(),
        Some(Data::Histogram(h)) => h.data_points.len(),
        Some(Data::ExponentialHistogram(eh)) => eh.data_points.len(),
        _ => 0,
    }
}

fn build_gauge_from_point(
    point: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
    ctx: &MetricContext,
) -> Result<Option<VrlValue>, DecodeError> {
    let time_unix_nano = safe_timestamp_conversion(point.time_unix_nano, "gauge.time_unix_nano")?;
    let start_time_unix_nano =
        safe_timestamp_conversion(point.start_time_unix_nano, "gauge.start_time_unix_nano")?;

    // Always produce Float for schema compatibility
    // Skip records with missing or non-finite values (NaN/Infinity)
    let value = match &point.value {
        Some(Value::AsInt(i)) => finite_float_to_vrl(*i as f64),
        Some(Value::AsDouble(d)) => finite_float_to_vrl(*d),
        None => return Ok(None),
    };

    // Skip if value is null (NaN/Infinity was converted to null)
    if matches!(value, VrlValue::Null) {
        return Ok(None);
    }

    let exemplars = build_exemplars(&point.exemplars)?;

    let parts = GaugeRecordParts {
        time_unix_nano,
        start_time_unix_nano,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        value,
        attributes: otlp_attributes_to_value(&point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
    };

    Ok(Some(build_gauge_record(parts)))
}

fn build_sum_from_point(
    point: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
    ctx: &MetricContext,
    aggregation_temporality: i64,
    is_monotonic: bool,
) -> Result<Option<VrlValue>, DecodeError> {
    let time_unix_nano = safe_timestamp_conversion(point.time_unix_nano, "sum.time_unix_nano")?;
    let start_time_unix_nano =
        safe_timestamp_conversion(point.start_time_unix_nano, "sum.start_time_unix_nano")?;

    // Always produce Float for schema compatibility
    // Skip records with missing or non-finite values (NaN/Infinity)
    let value = match &point.value {
        Some(Value::AsInt(i)) => finite_float_to_vrl(*i as f64),
        Some(Value::AsDouble(d)) => finite_float_to_vrl(*d),
        None => return Ok(None),
    };

    // Skip if value is null (NaN/Infinity was converted to null)
    if matches!(value, VrlValue::Null) {
        return Ok(None);
    }

    let exemplars = build_exemplars(&point.exemplars)?;

    let parts = SumRecordParts {
        time_unix_nano,
        start_time_unix_nano,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        value,
        attributes: otlp_attributes_to_value(&point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
        aggregation_temporality,
        is_monotonic,
    };

    Ok(Some(build_sum_record(parts)))
}

fn build_histogram_from_point(
    point: &opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
    ctx: &MetricContext,
    aggregation_temporality: i64,
) -> Result<VrlValue, DecodeError> {
    let time_unix_nano =
        safe_timestamp_conversion(point.time_unix_nano, "histogram.time_unix_nano")?;
    let start_time_unix_nano =
        safe_timestamp_conversion(point.start_time_unix_nano, "histogram.start_time_unix_nano")?;

    let exemplars = build_exemplars(&point.exemplars)?;

    // Convert bucket_counts and explicit_bounds to JSON strings
    let bucket_counts_json =
        serde_json::to_string(&point.bucket_counts).unwrap_or_else(|_| "[]".to_string());
    let explicit_bounds_json =
        serde_json::to_string(&point.explicit_bounds).unwrap_or_else(|_| "[]".to_string());

    let parts = HistogramRecordParts {
        time_unix_nano,
        start_time_unix_nano,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        count: point.count as i64,
        sum: point.sum,
        min: point.min,
        max: point.max,
        bucket_counts: Bytes::from(bucket_counts_json),
        explicit_bounds: Bytes::from(explicit_bounds_json),
        attributes: otlp_attributes_to_value(&point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
        aggregation_temporality,
    };

    Ok(build_histogram_record(parts))
}

fn build_exp_histogram_from_point(
    point: &opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint,
    ctx: &MetricContext,
    aggregation_temporality: i64,
) -> Result<VrlValue, DecodeError> {
    let time_unix_nano =
        safe_timestamp_conversion(point.time_unix_nano, "exp_histogram.time_unix_nano")?;
    let start_time_unix_nano = safe_timestamp_conversion(
        point.start_time_unix_nano,
        "exp_histogram.start_time_unix_nano",
    )?;

    let exemplars = build_exemplars(&point.exemplars)?;

    // Extract positive bucket data
    let (positive_offset, positive_bucket_counts_json) = match &point.positive {
        Some(buckets) => {
            let counts_json =
                serde_json::to_string(&buckets.bucket_counts).unwrap_or_else(|_| "[]".to_string());
            (buckets.offset as i64, Bytes::from(counts_json))
        }
        None => (0, Bytes::from("[]")),
    };

    // Extract negative bucket data
    let (negative_offset, negative_bucket_counts_json) = match &point.negative {
        Some(buckets) => {
            let counts_json =
                serde_json::to_string(&buckets.bucket_counts).unwrap_or_else(|_| "[]".to_string());
            (buckets.offset as i64, Bytes::from(counts_json))
        }
        None => (0, Bytes::from("[]")),
    };

    let parts = ExpHistogramRecordParts {
        time_unix_nano,
        start_time_unix_nano,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        count: point.count as i64,
        sum: point.sum,
        min: point.min,
        max: point.max,
        scale: point.scale as i64,
        zero_count: point.zero_count as i64,
        zero_threshold: point.zero_threshold,
        positive_offset,
        positive_bucket_counts: positive_bucket_counts_json,
        negative_offset,
        negative_bucket_counts: negative_bucket_counts_json,
        attributes: otlp_attributes_to_value(&point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
        aggregation_temporality,
    };

    Ok(build_exp_histogram_record(parts))
}

fn build_exemplars(
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) -> Result<Vec<ExemplarParts>, DecodeError> {
    exemplars
        .iter()
        .map(|e| {
            let time_unix_nano =
                safe_timestamp_conversion(e.time_unix_nano, "exemplar.time_unix_nano")?;

            // Always produce Float for schema compatibility
            let value = match &e.value {
                Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsInt(i)) => {
                    finite_float_to_vrl(*i as f64)
                }
                Some(opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(d)) => {
                    finite_float_to_vrl(*d)
                }
                None => VrlValue::Null,
            };

            Ok(ExemplarParts {
                time_unix_nano,
                value,
                trace_id: Bytes::from(hex_encode(&e.trace_id)),
                span_id: Bytes::from(hex_encode(&e.span_id)),
                filtered_attributes: otlp_attributes_to_value(&e.filtered_attributes),
            })
        })
        .collect()
}

// ============================================================================
// JSON decoding
// ============================================================================

pub fn decode_json(body: &[u8]) -> Result<DecodeMetricsResult, DecodeError> {
    // Normalize JSON to convert enum strings to numbers before parsing
    let normalized = super::normalize::normalize_json_bytes(body)?;
    let request: JsonExportMetricsServiceRequest = serde_json::from_slice(&normalized)?;
    export_metrics_to_vrl_json(request)
}

fn export_metrics_to_vrl_json(
    request: JsonExportMetricsServiceRequest,
) -> Result<DecodeMetricsResult, DecodeError> {
    let mut values = preallocate_metric_values(&request.resource_metrics, |rm| {
        rm.scope_metrics
            .iter()
            .flat_map(|sm| sm.metrics.iter())
            .map(|m| {
                m.gauge.as_ref().map(|g| g.data_points.len()).unwrap_or(0)
                    + m.sum.as_ref().map(|s| s.data_points.len()).unwrap_or(0)
            })
            .sum()
    });
    let mut skipped = SkippedMetrics::default();

    for_each_resource_scope(
        request.resource_metrics,
        |resource_metrics| {
            (
                json_resource_to_value(resource_metrics.resource),
                resource_metrics.scope_metrics,
            )
        },
        |scope_metrics| {
            (
                json_scope_to_value(scope_metrics.scope),
                scope_metrics.metrics,
            )
        },
        |metrics, resource, scope| {
            for metric in metrics {
                let ctx = MetricContext {
                    metric_name: Bytes::from(metric.name),
                    metric_description: Bytes::from(metric.description),
                    metric_unit: Bytes::from(metric.unit),
                    resource: Arc::clone(&resource),
                    scope: Arc::clone(&scope),
                };

                if let Some(gauge) = metric.gauge {
                    for point in gauge.data_points {
                        let as_int = point.as_int.clone();
                        let as_double = point.as_double;
                        match build_gauge_from_json_point(point, &ctx)? {
                            Some(record) => values.push(record),
                            None => track_skipped_json_value(&as_int, &as_double, &mut skipped),
                        }
                    }
                }

                if let Some(sum) = metric.sum {
                    for point in sum.data_points {
                        let as_int = point.as_int.clone();
                        let as_double = point.as_double;
                        match build_sum_from_json_point(
                            point,
                            &ctx,
                            sum.aggregation_temporality,
                            sum.is_monotonic,
                        )? {
                            Some(record) => values.push(record),
                            None => track_skipped_json_value(&as_int, &as_double, &mut skipped),
                        }
                    }
                }

                if let Some(histogram) = metric.histogram {
                    for point in histogram.data_points {
                        let record = build_histogram_from_json_point(
                            point,
                            &ctx,
                            histogram.aggregation_temporality,
                        )?;
                        values.push(record);
                    }
                }

                if let Some(exp_histogram) = metric.exponential_histogram {
                    for point in exp_histogram.data_points {
                        let record = build_exp_histogram_from_json_point(
                            point,
                            &ctx,
                            exp_histogram.aggregation_temporality,
                        )?;
                        values.push(record);
                    }
                }

                // Track skipped summary metrics (deprecated in OTLP spec)
                if let Some(s) = metric.summary {
                    skipped.summaries += s.data_points.len();
                }
            }

            Ok::<(), DecodeError>(())
        },
    )?;

    Ok(DecodeMetricsResult { values, skipped })
}

/// Track why a JSON metric value was skipped
fn track_skipped_json_value(
    as_int: &Option<JsonNumberOrString>,
    as_double: &Option<f64>,
    skipped: &mut SkippedMetrics,
) {
    match (as_int, as_double) {
        (None, None) => skipped.missing_values += 1,
        (_, Some(d)) if d.is_nan() => skipped.nan_values += 1,
        (_, Some(d)) if d.is_infinite() => skipped.infinity_values += 1,
        _ => {} // Shouldn't happen for valid skipped values
    }
}

fn build_gauge_from_json_point(
    point: JsonNumberDataPoint,
    ctx: &MetricContext,
) -> Result<Option<VrlValue>, DecodeError> {
    // Skip records with missing or non-finite values (NaN/Infinity)
    let value = match extract_number_value(&point.as_int, &point.as_double) {
        Some(v) => v,
        None => return Ok(None),
    };

    let exemplars = build_json_exemplars(point.exemplars)?;

    let parts = GaugeRecordParts {
        time_unix_nano: json_timestamp_to_i64(&point.time_unix_nano, "gauge.time_unix_nano")?,
        start_time_unix_nano: json_timestamp_to_i64(
            &point.start_time_unix_nano,
            "gauge.start_time_unix_nano",
        )?,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        value,
        attributes: json_attrs_to_value(point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
    };

    Ok(Some(build_gauge_record(parts)))
}

fn build_sum_from_json_point(
    point: JsonNumberDataPoint,
    ctx: &MetricContext,
    aggregation_temporality: i64,
    is_monotonic: bool,
) -> Result<Option<VrlValue>, DecodeError> {
    // Skip records with missing or non-finite values (NaN/Infinity)
    let value = match extract_number_value(&point.as_int, &point.as_double) {
        Some(v) => v,
        None => return Ok(None),
    };

    let exemplars = build_json_exemplars(point.exemplars)?;

    let parts = SumRecordParts {
        time_unix_nano: json_timestamp_to_i64(&point.time_unix_nano, "sum.time_unix_nano")?,
        start_time_unix_nano: json_timestamp_to_i64(
            &point.start_time_unix_nano,
            "sum.start_time_unix_nano",
        )?,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        value,
        attributes: json_attrs_to_value(point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
        aggregation_temporality,
        is_monotonic,
    };

    Ok(Some(build_sum_record(parts)))
}

fn build_histogram_from_json_point(
    point: JsonHistogramDataPoint,
    ctx: &MetricContext,
    aggregation_temporality: i64,
) -> Result<VrlValue, DecodeError> {
    let exemplars = build_json_exemplars(point.exemplars)?;

    // Convert bucket_counts to JSON string
    let bucket_counts: Vec<u64> = point
        .bucket_counts
        .iter()
        .filter_map(|n| n.as_i64().map(|v| v as u64))
        .collect();
    let bucket_counts_json =
        serde_json::to_string(&bucket_counts).unwrap_or_else(|_| "[]".to_string());

    // Convert explicit_bounds to JSON string
    let explicit_bounds_json =
        serde_json::to_string(&point.explicit_bounds).unwrap_or_else(|_| "[]".to_string());

    let parts = HistogramRecordParts {
        time_unix_nano: json_timestamp_to_i64(&point.time_unix_nano, "histogram.time_unix_nano")?,
        start_time_unix_nano: json_timestamp_to_i64(
            &point.start_time_unix_nano,
            "histogram.start_time_unix_nano",
        )?,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        count: point.count.as_i64().unwrap_or(0),
        sum: point.sum,
        min: point.min,
        max: point.max,
        bucket_counts: Bytes::from(bucket_counts_json),
        explicit_bounds: Bytes::from(explicit_bounds_json),
        attributes: json_attrs_to_value(point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
        aggregation_temporality,
    };

    Ok(build_histogram_record(parts))
}

fn build_exp_histogram_from_json_point(
    point: JsonExpHistogramDataPoint,
    ctx: &MetricContext,
    aggregation_temporality: i64,
) -> Result<VrlValue, DecodeError> {
    let exemplars = build_json_exemplars(point.exemplars)?;

    // Extract positive bucket data
    let (positive_offset, positive_bucket_counts_json) = match &point.positive {
        Some(buckets) => {
            let counts: Vec<u64> = buckets
                .bucket_counts
                .iter()
                .filter_map(|n| n.as_i64().map(|v| v as u64))
                .collect();
            let counts_json = serde_json::to_string(&counts).unwrap_or_else(|_| "[]".to_string());
            (buckets.offset as i64, Bytes::from(counts_json))
        }
        None => (0, Bytes::from("[]")),
    };

    // Extract negative bucket data
    let (negative_offset, negative_bucket_counts_json) = match &point.negative {
        Some(buckets) => {
            let counts: Vec<u64> = buckets
                .bucket_counts
                .iter()
                .filter_map(|n| n.as_i64().map(|v| v as u64))
                .collect();
            let counts_json = serde_json::to_string(&counts).unwrap_or_else(|_| "[]".to_string());
            (buckets.offset as i64, Bytes::from(counts_json))
        }
        None => (0, Bytes::from("[]")),
    };

    let parts = ExpHistogramRecordParts {
        time_unix_nano: json_timestamp_to_i64(
            &point.time_unix_nano,
            "exp_histogram.time_unix_nano",
        )?,
        start_time_unix_nano: json_timestamp_to_i64(
            &point.start_time_unix_nano,
            "exp_histogram.start_time_unix_nano",
        )?,
        metric_name: ctx.metric_name.clone(),
        metric_description: ctx.metric_description.clone(),
        metric_unit: ctx.metric_unit.clone(),
        count: point.count.as_i64().unwrap_or(0),
        sum: point.sum,
        min: point.min,
        max: point.max,
        scale: point.scale as i64,
        zero_count: point.zero_count.as_i64().unwrap_or(0),
        zero_threshold: point.zero_threshold,
        positive_offset,
        positive_bucket_counts: positive_bucket_counts_json,
        negative_offset,
        negative_bucket_counts: negative_bucket_counts_json,
        attributes: json_attrs_to_value(point.attributes),
        resource: Arc::clone(&ctx.resource),
        scope: Arc::clone(&ctx.scope),
        flags: point.flags as i64,
        exemplars,
        aggregation_temporality,
    };

    Ok(build_exp_histogram_record(parts))
}

/// Extract numeric value, always producing Float for schema compatibility
/// Returns None for missing values or non-finite numbers (NaN/Infinity)
fn extract_number_value(
    as_int: &Option<JsonNumberOrString>,
    as_double: &Option<f64>,
) -> Option<VrlValue> {
    if let Some(i) = as_int {
        let value = i.as_i64().map(|n| finite_float_to_vrl(n as f64))?;
        // finite_float_to_vrl returns Null for NaN/Infinity
        if matches!(value, VrlValue::Null) {
            None
        } else {
            Some(value)
        }
    } else if let Some(d) = as_double {
        let value = finite_float_to_vrl(*d);
        // finite_float_to_vrl returns Null for NaN/Infinity
        if matches!(value, VrlValue::Null) {
            None
        } else {
            Some(value)
        }
    } else {
        None
    }
}

fn build_json_exemplars(exemplars: Vec<JsonExemplar>) -> Result<Vec<ExemplarParts>, DecodeError> {
    exemplars
        .into_iter()
        .map(|e| {
            // Exemplar values can be null - they're supplementary metadata
            let value = extract_number_value(&e.as_int, &e.as_double).unwrap_or(VrlValue::Null);
            Ok(ExemplarParts {
                time_unix_nano: json_timestamp_to_i64(
                    &e.time_unix_nano,
                    "exemplar.time_unix_nano",
                )?,
                value,
                trace_id: Bytes::from(decode_bytes_field(&e.trace_id)),
                span_id: Bytes::from(decode_bytes_field(&e.span_id)),
                filtered_attributes: json_attrs_to_value(e.filtered_attributes),
            })
        })
        .collect()
}

// ============================================================================
// JSON struct definitions
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExportMetricsServiceRequest {
    #[serde(default)]
    resource_metrics: Vec<JsonResourceMetrics>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonResourceMetrics {
    #[serde(default)]
    resource: JsonResource,
    #[serde(default)]
    scope_metrics: Vec<JsonScopeMetrics>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonScopeMetrics {
    #[serde(default)]
    scope: JsonInstrumentationScope,
    #[serde(default)]
    metrics: Vec<JsonMetric>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonMetric {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    unit: String,
    #[serde(default)]
    gauge: Option<JsonGauge>,
    #[serde(default)]
    sum: Option<JsonSum>,
    #[serde(default)]
    histogram: Option<JsonHistogram>,
    #[serde(default)]
    exponential_histogram: Option<JsonExponentialHistogram>,
    #[serde(default)]
    summary: Option<JsonSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonGauge {
    #[serde(default)]
    data_points: Vec<JsonNumberDataPoint>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSum {
    #[serde(default)]
    data_points: Vec<JsonNumberDataPoint>,
    #[serde(default)]
    aggregation_temporality: i64,
    #[serde(default)]
    is_monotonic: bool,
}

/// Struct for histogram metrics
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonHistogram {
    #[serde(default)]
    data_points: Vec<JsonHistogramDataPoint>,
    #[serde(default)]
    aggregation_temporality: i64,
}

/// Struct for exponential histogram metrics
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExponentialHistogram {
    #[serde(default)]
    data_points: Vec<JsonExpHistogramDataPoint>,
    #[serde(default)]
    aggregation_temporality: i64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonHistogramDataPoint {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    start_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    count: JsonNumberOrString,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default)]
    bucket_counts: Vec<JsonNumberOrString>,
    #[serde(default)]
    explicit_bounds: Vec<f64>,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    flags: u32,
    #[serde(default)]
    exemplars: Vec<JsonExemplar>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExpHistogramDataPoint {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    start_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    count: JsonNumberOrString,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default)]
    scale: i32,
    #[serde(default)]
    zero_count: JsonNumberOrString,
    #[serde(default)]
    zero_threshold: f64,
    #[serde(default)]
    positive: Option<JsonExpHistogramBuckets>,
    #[serde(default)]
    negative: Option<JsonExpHistogramBuckets>,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    flags: u32,
    #[serde(default)]
    exemplars: Vec<JsonExemplar>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExpHistogramBuckets {
    #[serde(default)]
    offset: i32,
    #[serde(default)]
    bucket_counts: Vec<JsonNumberOrString>,
}

/// Minimal struct for summary metrics (skipped, only count data_points)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSummary {
    #[serde(default)]
    data_points: Vec<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonNumberDataPoint {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    start_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    as_int: Option<JsonNumberOrString>,
    #[serde(default)]
    as_double: Option<f64>,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    flags: u32,
    #[serde(default)]
    exemplars: Vec<JsonExemplar>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExemplar {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    as_int: Option<JsonNumberOrString>,
    #[serde(default)]
    as_double: Option<f64>,
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
    #[serde(default)]
    filtered_attributes: Vec<JsonKeyValue>,
}

// ============================================================================
// Record builder
// ============================================================================

/// Precomputed fields for building a gauge metric record into VRL values
struct GaugeRecordParts {
    time_unix_nano: i64,
    start_time_unix_nano: i64,
    metric_name: Bytes,
    metric_description: Bytes,
    metric_unit: Bytes,
    value: VrlValue,
    attributes: VrlValue,
    resource: Arc<VrlValue>,
    scope: Arc<VrlValue>,
    flags: i64,
    exemplars: Vec<ExemplarParts>,
}

struct ExemplarParts {
    time_unix_nano: i64,
    value: VrlValue,
    trace_id: Bytes,
    span_id: Bytes,
    filtered_attributes: VrlValue,
}

/// Precomputed fields for building a sum metric record into VRL values
struct SumRecordParts {
    time_unix_nano: i64,
    start_time_unix_nano: i64,
    metric_name: Bytes,
    metric_description: Bytes,
    metric_unit: Bytes,
    value: VrlValue,
    attributes: VrlValue,
    resource: Arc<VrlValue>,
    scope: Arc<VrlValue>,
    flags: i64,
    exemplars: Vec<ExemplarParts>,
    aggregation_temporality: i64,
    is_monotonic: bool,
}

/// Precomputed fields for building a histogram metric record into VRL values
struct HistogramRecordParts {
    time_unix_nano: i64,
    start_time_unix_nano: i64,
    metric_name: Bytes,
    metric_description: Bytes,
    metric_unit: Bytes,
    count: i64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    bucket_counts: Bytes,
    explicit_bounds: Bytes,
    attributes: VrlValue,
    resource: Arc<VrlValue>,
    scope: Arc<VrlValue>,
    flags: i64,
    exemplars: Vec<ExemplarParts>,
    aggregation_temporality: i64,
}

/// Precomputed fields for building an exponential histogram metric record into VRL values
struct ExpHistogramRecordParts {
    time_unix_nano: i64,
    start_time_unix_nano: i64,
    metric_name: Bytes,
    metric_description: Bytes,
    metric_unit: Bytes,
    count: i64,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
    scale: i64,
    zero_count: i64,
    zero_threshold: f64,
    positive_offset: i64,
    positive_bucket_counts: Bytes,
    negative_offset: i64,
    negative_bucket_counts: Bytes,
    attributes: VrlValue,
    resource: Arc<VrlValue>,
    scope: Arc<VrlValue>,
    flags: i64,
    exemplars: Vec<ExemplarParts>,
    aggregation_temporality: i64,
}

/// Pre-allocate values Vec for metrics
fn preallocate_metric_values<R, F>(resource_metrics: &[R], count_points: F) -> Vec<VrlValue>
where
    F: Fn(&R) -> usize,
{
    let capacity: usize = resource_metrics.iter().map(&count_points).sum();
    Vec::with_capacity(capacity)
}

/// Helper function to build exemplars array from parts
fn build_exemplars_array(exemplars: Vec<ExemplarParts>) -> VrlValue {
    let exemplars_array: Vec<VrlValue> = exemplars
        .into_iter()
        .map(|e| {
            let mut map = ObjectMap::new();
            map.insert("time_unix_nano".into(), VrlValue::Integer(e.time_unix_nano));
            map.insert("value".into(), e.value);
            map.insert("trace_id".into(), VrlValue::Bytes(e.trace_id));
            map.insert("span_id".into(), VrlValue::Bytes(e.span_id));
            map.insert("filtered_attributes".into(), e.filtered_attributes);
            VrlValue::Object(map)
        })
        .collect();
    VrlValue::Array(exemplars_array)
}

fn build_gauge_record(parts: GaugeRecordParts) -> VrlValue {
    let mut map = ObjectMap::new();
    map.insert(
        "time_unix_nano".into(),
        VrlValue::Integer(parts.time_unix_nano),
    );
    map.insert(
        "start_time_unix_nano".into(),
        VrlValue::Integer(parts.start_time_unix_nano),
    );
    map.insert("metric_name".into(), VrlValue::Bytes(parts.metric_name));
    map.insert(
        "metric_description".into(),
        VrlValue::Bytes(parts.metric_description),
    );
    map.insert("metric_unit".into(), VrlValue::Bytes(parts.metric_unit));
    map.insert("value".into(), parts.value);
    map.insert("attributes".into(), parts.attributes);
    map.insert("resource".into(), (*parts.resource).clone());
    map.insert("scope".into(), (*parts.scope).clone());
    map.insert("flags".into(), VrlValue::Integer(parts.flags));
    map.insert("exemplars".into(), build_exemplars_array(parts.exemplars));
    map.insert("_metric_type".into(), VrlValue::Bytes(Bytes::from("gauge")));
    VrlValue::Object(map)
}

fn build_sum_record(parts: SumRecordParts) -> VrlValue {
    let mut map = ObjectMap::new();
    map.insert(
        "time_unix_nano".into(),
        VrlValue::Integer(parts.time_unix_nano),
    );
    map.insert(
        "start_time_unix_nano".into(),
        VrlValue::Integer(parts.start_time_unix_nano),
    );
    map.insert("metric_name".into(), VrlValue::Bytes(parts.metric_name));
    map.insert(
        "metric_description".into(),
        VrlValue::Bytes(parts.metric_description),
    );
    map.insert("metric_unit".into(), VrlValue::Bytes(parts.metric_unit));
    map.insert("value".into(), parts.value);
    map.insert("attributes".into(), parts.attributes);
    map.insert("resource".into(), (*parts.resource).clone());
    map.insert("scope".into(), (*parts.scope).clone());
    map.insert("flags".into(), VrlValue::Integer(parts.flags));
    map.insert("exemplars".into(), build_exemplars_array(parts.exemplars));
    map.insert(
        "aggregation_temporality".into(),
        VrlValue::Integer(parts.aggregation_temporality),
    );
    map.insert("is_monotonic".into(), VrlValue::Boolean(parts.is_monotonic));
    map.insert("_metric_type".into(), VrlValue::Bytes(Bytes::from("sum")));
    VrlValue::Object(map)
}

fn build_histogram_record(parts: HistogramRecordParts) -> VrlValue {
    let mut map = ObjectMap::new();
    map.insert(
        "time_unix_nano".into(),
        VrlValue::Integer(parts.time_unix_nano),
    );
    map.insert(
        "start_time_unix_nano".into(),
        VrlValue::Integer(parts.start_time_unix_nano),
    );
    map.insert("metric_name".into(), VrlValue::Bytes(parts.metric_name));
    map.insert(
        "metric_description".into(),
        VrlValue::Bytes(parts.metric_description),
    );
    map.insert("metric_unit".into(), VrlValue::Bytes(parts.metric_unit));
    map.insert("count".into(), VrlValue::Integer(parts.count));
    map.insert(
        "sum".into(),
        parts.sum.map(finite_float_to_vrl).unwrap_or(VrlValue::Null),
    );
    map.insert(
        "min".into(),
        parts.min.map(finite_float_to_vrl).unwrap_or(VrlValue::Null),
    );
    map.insert(
        "max".into(),
        parts.max.map(finite_float_to_vrl).unwrap_or(VrlValue::Null),
    );
    map.insert("bucket_counts".into(), VrlValue::Bytes(parts.bucket_counts));
    map.insert(
        "explicit_bounds".into(),
        VrlValue::Bytes(parts.explicit_bounds),
    );
    map.insert("attributes".into(), parts.attributes);
    map.insert("resource".into(), (*parts.resource).clone());
    map.insert("scope".into(), (*parts.scope).clone());
    map.insert("flags".into(), VrlValue::Integer(parts.flags));
    map.insert("exemplars".into(), build_exemplars_array(parts.exemplars));
    map.insert(
        "aggregation_temporality".into(),
        VrlValue::Integer(parts.aggregation_temporality),
    );
    map.insert(
        "_metric_type".into(),
        VrlValue::Bytes(Bytes::from("histogram")),
    );
    VrlValue::Object(map)
}

fn build_exp_histogram_record(parts: ExpHistogramRecordParts) -> VrlValue {
    let mut map = ObjectMap::new();
    map.insert(
        "time_unix_nano".into(),
        VrlValue::Integer(parts.time_unix_nano),
    );
    map.insert(
        "start_time_unix_nano".into(),
        VrlValue::Integer(parts.start_time_unix_nano),
    );
    map.insert("metric_name".into(), VrlValue::Bytes(parts.metric_name));
    map.insert(
        "metric_description".into(),
        VrlValue::Bytes(parts.metric_description),
    );
    map.insert("metric_unit".into(), VrlValue::Bytes(parts.metric_unit));
    map.insert("count".into(), VrlValue::Integer(parts.count));
    map.insert(
        "sum".into(),
        parts.sum.map(finite_float_to_vrl).unwrap_or(VrlValue::Null),
    );
    map.insert(
        "min".into(),
        parts.min.map(finite_float_to_vrl).unwrap_or(VrlValue::Null),
    );
    map.insert(
        "max".into(),
        parts.max.map(finite_float_to_vrl).unwrap_or(VrlValue::Null),
    );
    map.insert("scale".into(), VrlValue::Integer(parts.scale));
    map.insert("zero_count".into(), VrlValue::Integer(parts.zero_count));
    map.insert(
        "zero_threshold".into(),
        finite_float_to_vrl(parts.zero_threshold),
    );
    map.insert(
        "positive_offset".into(),
        VrlValue::Integer(parts.positive_offset),
    );
    map.insert(
        "positive_bucket_counts".into(),
        VrlValue::Bytes(parts.positive_bucket_counts),
    );
    map.insert(
        "negative_offset".into(),
        VrlValue::Integer(parts.negative_offset),
    );
    map.insert(
        "negative_bucket_counts".into(),
        VrlValue::Bytes(parts.negative_bucket_counts),
    );
    map.insert("attributes".into(), parts.attributes);
    map.insert("resource".into(), (*parts.resource).clone());
    map.insert("scope".into(), (*parts.scope).clone());
    map.insert("flags".into(), VrlValue::Integer(parts.flags));
    map.insert("exemplars".into(), build_exemplars_array(parts.exemplars));
    map.insert(
        "aggregation_temporality".into(),
        VrlValue::Integer(parts.aggregation_temporality),
    );
    map.insert(
        "_metric_type".into(),
        VrlValue::Bytes(Bytes::from("exp_histogram")),
    );
    VrlValue::Object(map)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::InstrumentationScope,
        metrics::v1::{Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum},
        resource::v1::Resource,
    };

    #[test]
    fn decodes_gauge_metric() {
        let point = NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            start_time_unix_nano: 900_000_000,
            value: Some(Value::AsDouble(42.5)),
            ..Default::default()
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: "test.gauge".to_string(),
                        description: "A test gauge".to_string(),
                        unit: "1".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![point],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body);

        assert!(result.is_ok());
        let decode_result = result.unwrap();
        assert_eq!(decode_result.values.len(), 1);

        if let VrlValue::Object(map) = &decode_result.values[0] {
            assert_eq!(
                map.get("metric_name"),
                Some(&VrlValue::Bytes(Bytes::from("test.gauge")))
            );
            assert_eq!(
                map.get("_metric_type"),
                Some(&VrlValue::Bytes(Bytes::from("gauge")))
            );
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn decodes_sum_metric() {
        let point = NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            start_time_unix_nano: 900_000_000,
            value: Some(Value::AsInt(100)),
            ..Default::default()
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: "test.sum".to_string(),
                        description: "A test sum".to_string(),
                        unit: "1".to_string(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![point],
                            aggregation_temporality: 2,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body);

        assert!(result.is_ok());
        let decode_result = result.unwrap();
        assert_eq!(decode_result.values.len(), 1);

        if let VrlValue::Object(map) = &decode_result.values[0] {
            assert_eq!(
                map.get("metric_name"),
                Some(&VrlValue::Bytes(Bytes::from("test.sum")))
            );
            assert_eq!(
                map.get("_metric_type"),
                Some(&VrlValue::Bytes(Bytes::from("sum")))
            );
            assert_eq!(
                map.get("aggregation_temporality"),
                Some(&VrlValue::Integer(2))
            );
            assert_eq!(map.get("is_monotonic"), Some(&VrlValue::Boolean(true)));
        } else {
            panic!("expected object");
        }
    }

    fn make_gauge_request(points: Vec<NumberDataPoint>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource::default()),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: "test.gauge".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: points,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn skips_gauge_with_nan_value() {
        let point = NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            value: Some(Value::AsDouble(f64::NAN)),
            ..Default::default()
        };

        let request = make_gauge_request(vec![point]);
        let body = request.encode_to_vec();
        let result = decode_protobuf(&body).unwrap();

        assert_eq!(result.values.len(), 0, "NaN value should be skipped");
        assert_eq!(result.skipped.nan_values, 1, "Should track NaN skip");
    }

    #[test]
    fn skips_gauge_with_infinity_value() {
        let point = NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            value: Some(Value::AsDouble(f64::INFINITY)),
            ..Default::default()
        };

        let request = make_gauge_request(vec![point]);
        let body = request.encode_to_vec();
        let result = decode_protobuf(&body).unwrap();

        assert_eq!(result.values.len(), 0, "Infinity value should be skipped");
        assert_eq!(
            result.skipped.infinity_values, 1,
            "Should track infinity skip"
        );
    }

    #[test]
    fn skips_gauge_with_neg_infinity_value() {
        let point = NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            value: Some(Value::AsDouble(f64::NEG_INFINITY)),
            ..Default::default()
        };

        let request = make_gauge_request(vec![point]);
        let body = request.encode_to_vec();
        let result = decode_protobuf(&body).unwrap();

        assert_eq!(
            result.values.len(),
            0,
            "Negative infinity value should be skipped"
        );
        assert_eq!(
            result.skipped.infinity_values, 1,
            "Should track infinity skip"
        );
    }

    #[test]
    fn skips_gauge_with_missing_value() {
        let point = NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            value: None,
            ..Default::default()
        };

        let request = make_gauge_request(vec![point]);
        let body = request.encode_to_vec();
        let result = decode_protobuf(&body).unwrap();

        assert_eq!(result.values.len(), 0, "Missing value should be skipped");
        assert_eq!(
            result.skipped.missing_values, 1,
            "Should track missing value skip"
        );
    }

    #[test]
    fn keeps_valid_gauge_skips_invalid() {
        let valid_point = NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            value: Some(Value::AsDouble(42.5)),
            ..Default::default()
        };
        let nan_point = NumberDataPoint {
            time_unix_nano: 2_000_000_000,
            value: Some(Value::AsDouble(f64::NAN)),
            ..Default::default()
        };
        let missing_point = NumberDataPoint {
            time_unix_nano: 3_000_000_000,
            value: None,
            ..Default::default()
        };

        let request = make_gauge_request(vec![valid_point, nan_point, missing_point]);
        let body = request.encode_to_vec();
        let result = decode_protobuf(&body).unwrap();

        assert_eq!(result.values.len(), 1, "Only valid point should be kept");
        assert!(result.skipped.has_skipped(), "Should have skipped metrics");
        assert_eq!(result.skipped.nan_values, 1);
        assert_eq!(result.skipped.missing_values, 1);
    }

    #[test]
    fn handles_empty_metrics_request() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body).unwrap();
        assert!(result.values.is_empty());
        assert!(!result.skipped.has_skipped());
    }

    #[test]
    fn handles_empty_json_request() {
        let body = r#"{"resourceMetrics": []}"#;
        let result = decode_json(body.as_bytes()).unwrap();
        assert!(result.values.is_empty());
        assert!(!result.skipped.has_skipped());
    }

    // JSON tests

    fn make_gauge_json(as_double: Option<f64>, as_int: Option<&str>) -> String {
        let value_field = match (as_double, as_int) {
            (Some(d), _) => format!(r#""asDouble": {d}"#),
            (_, Some(i)) => format!(r#""asInt": "{i}""#),
            (None, None) => String::new(),
        };

        format!(
            r#"{{
                "resourceMetrics": [{{
                    "resource": {{}},
                    "scopeMetrics": [{{
                        "scope": {{}},
                        "metrics": [{{
                            "name": "test.gauge",
                            "gauge": {{
                                "dataPoints": [{{
                                    "timeUnixNano": "1000000000"
                                    {}
                                }}]
                            }}
                        }}]
                    }}]
                }}]
            }}"#,
            if value_field.is_empty() {
                String::new()
            } else {
                format!(", {value_field}")
            }
        )
    }

    #[test]
    fn skips_json_gauge_with_missing_value() {
        let json = make_gauge_json(None, None);
        let result = decode_json(json.as_bytes()).unwrap();
        assert_eq!(result.values.len(), 0, "Missing value should be skipped");
        assert_eq!(result.skipped.missing_values, 1);
    }

    #[test]
    fn accepts_json_gauge_with_valid_double() {
        let json = make_gauge_json(Some(42.5), None);
        let result = decode_json(json.as_bytes()).unwrap();
        assert_eq!(result.values.len(), 1, "Valid double should be accepted");
    }

    #[test]
    fn accepts_json_gauge_with_valid_int() {
        let json = make_gauge_json(None, Some("42"));
        let result = decode_json(json.as_bytes()).unwrap();
        assert_eq!(result.values.len(), 1, "Valid int should be accepted");
    }

    #[test]
    fn decodes_json_metrics_payload() {
        let body = r#"{
            "resourceMetrics": [{
                "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "svc" } }]},
                "scopeMetrics": [{
                    "scope": { "name": "lib", "version": "1" },
                    "metrics": [{
                        "name": "test.gauge",
                        "description": "A test gauge",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "timeUnixNano": "1000000000",
                                "asDouble": 42.5
                            }]
                        }
                    }]
                }]
            }]
        }"#;

        let result = decode_json(body.as_bytes()).unwrap();
        assert_eq!(result.values.len(), 1);

        let record = &result.values[0];
        if let VrlValue::Object(map) = record {
            assert_eq!(
                map.get("metric_name"),
                Some(&VrlValue::Bytes(Bytes::from("test.gauge")))
            );
            assert_eq!(
                map.get("_metric_type"),
                Some(&VrlValue::Bytes(Bytes::from("gauge")))
            );
        } else {
            panic!("expected object");
        }
    }
}
