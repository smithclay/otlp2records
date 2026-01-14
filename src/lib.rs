//! otlp2records - Transform OTLP telemetry to Arrow RecordBatches
//!
//! This crate provides synchronous, WASM-compatible transformation of OpenTelemetry
//! Protocol (OTLP) data (logs, traces, metrics) into Arrow RecordBatches.
//!
//! # Design Principles
//!
//! - **No I/O**: Core never touches network or filesystem
//! - **No async**: Pure synchronous transforms
//! - **WASM-first**: All dependencies compile to wasm32
//! - **Arrow-native**: RecordBatch is the canonical output format
//!
//! # High-level API
//!
//! The simplest way to use this crate is with the high-level transform functions:
//!
//! ```ignore
//! use otlp2records::{transform_logs, transform_traces, transform_metrics, InputFormat};
//!
//! // Transform OTLP logs to Arrow RecordBatch
//! let batch = transform_logs(bytes, InputFormat::Protobuf)?;
//!
//! // Transform OTLP traces to Arrow RecordBatch
//! let batch = transform_traces(bytes, InputFormat::Json)?;
//!
//! // Transform OTLP metrics to Arrow RecordBatches (separate gauge and sum)
//! let batches = transform_metrics(bytes, InputFormat::Protobuf)?;
//! if let Some(gauge_batch) = batches.gauge {
//!     // Process gauge metrics
//! }
//! if let Some(sum_batch) = batches.sum {
//!     // Process sum metrics
//! }
//! ```
//!
//! # Lower-level API
//!
//! For more control over individual transformation steps:
//!
//! ```ignore
//! use otlp2records::{decode_logs, apply_log_transform, values_to_arrow, logs_schema, InputFormat};
//!
//! // Step 1: Decode OTLP bytes to VRL Values
//! let values = decode_logs(bytes, InputFormat::Protobuf)?;
//!
//! // Step 2: Apply VRL transformation
//! let transformed = apply_log_transform(values)?;
//!
//! // Step 3: Convert to Arrow RecordBatch
//! let batch = values_to_arrow(&transformed, &logs_schema())?;
//! ```

pub mod arrow;
pub mod convert;
pub mod decode;
pub mod error;
pub mod output;
pub mod schemas;
pub mod transform;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub mod wasm;

#[cfg(feature = "ffi")]
pub mod ffi;

use ::arrow::record_batch::RecordBatch;
use vrl::value::{KeyString, Value};

pub use arrow::{
    exp_histogram_schema, extract_min_timestamp_micros, extract_service_name, gauge_schema,
    group_batch_by_service, histogram_schema, logs_schema, sum_schema, traces_schema,
    values_to_arrow, PartitionedBatch, PartitionedMetrics, ServiceGroupedBatches,
};
pub use decode::{
    count_skipped_metric_data_points, decode_logs, decode_metrics, decode_traces,
    normalise_json_value, normalize_json_bytes, DecodeMetricsResult, InputFormat, MetricSkipCounts,
    SkippedMetrics,
};
pub use error::{Error, Result};
#[cfg(feature = "parquet")]
pub use output::to_parquet;
pub use output::{to_ipc, to_json};
pub use schemas::{schema_def, schema_defs, SchemaDef, SchemaField};
pub use transform::{
    VrlError, VrlTransformer, OTLP_EXP_HISTOGRAM_PROGRAM, OTLP_GAUGE_PROGRAM,
    OTLP_HISTOGRAM_PROGRAM, OTLP_LOGS_PROGRAM, OTLP_SUM_PROGRAM, OTLP_TRACES_PROGRAM,
};

// ============================================================================
// High-level API types
// ============================================================================

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

/// Result of applying VRL transformation to metrics.
///
/// Metrics are partitioned by type because each metric type requires
/// a different VRL transformation program and has a different output schema.
#[derive(Debug, Default)]
pub struct MetricValues {
    /// Transformed gauge metric values
    pub gauge: Vec<Value>,
    /// Transformed sum metric values
    pub sum: Vec<Value>,
    /// Transformed histogram metric values
    pub histogram: Vec<Value>,
    /// Transformed exponential histogram metric values
    pub exp_histogram: Vec<Value>,
}

// ============================================================================
// High-level API functions
// ============================================================================

/// Transform OTLP logs to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP log data to Arrow format.
/// It handles decoding, VRL transformation, and Arrow conversion in one step.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP log data bytes
/// * `format` - The input format (Protobuf or JSON)
///
/// # Returns
///
/// An Arrow RecordBatch containing the transformed log data, or an error.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{transform_logs, InputFormat};
///
/// let batch = transform_logs(otlp_bytes, InputFormat::Protobuf)?;
/// println!("Transformed {} log records", batch.num_rows());
/// ```
pub fn transform_logs(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    // Step 1: Decode OTLP logs
    let values = decode_logs(bytes, format)?;

    // Step 2: Apply VRL transformation
    let transformed = apply_log_transform(values)?;

    // Step 3: Convert to Arrow
    let batch = values_to_arrow(&transformed, &logs_schema())?;

    Ok(batch)
}

/// Transform OTLP logs to JSON values.
pub fn transform_logs_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    let values = decode_logs(bytes, format)?;
    let transformed = apply_log_transform(values)?;
    values_to_json(transformed, "log")
}

/// Transform OTLP traces to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP trace data to Arrow format.
/// It handles decoding, VRL transformation, and Arrow conversion in one step.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP trace data bytes
/// * `format` - The input format (Protobuf or JSON)
///
/// # Returns
///
/// An Arrow RecordBatch containing the transformed trace data, or an error.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{transform_traces, InputFormat};
///
/// let batch = transform_traces(otlp_bytes, InputFormat::Protobuf)?;
/// println!("Transformed {} spans", batch.num_rows());
/// ```
pub fn transform_traces(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    // Step 1: Decode OTLP traces
    let values = decode_traces(bytes, format)?;

    // Step 2: Apply VRL transformation
    let transformed = apply_trace_transform(values)?;

    // Step 3: Convert to Arrow
    let batch = values_to_arrow(&transformed, &traces_schema())?;

    Ok(batch)
}

/// Transform OTLP traces to JSON values.
pub fn transform_traces_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    let values = decode_traces(bytes, format)?;
    let transformed = apply_trace_transform(values)?;
    values_to_json(transformed, "span")
}

/// Transform OTLP metrics to Arrow RecordBatches.
///
/// Returns separate batches for gauge and sum metrics because they have
/// different schemas. Each field in the result is `None` if there were
/// no metrics of that type in the input.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP metric data bytes
/// * `format` - The input format (Protobuf or JSON)
///
/// # Returns
///
/// A `MetricBatches` struct containing optional RecordBatches for gauge
/// and sum metrics, or an error.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{transform_metrics, InputFormat};
///
/// let batches = transform_metrics(otlp_bytes, InputFormat::Protobuf)?;
///
/// if let Some(gauge) = batches.gauge {
///     println!("Transformed {} gauge data points", gauge.num_rows());
/// }
/// if let Some(sum) = batches.sum {
///     println!("Transformed {} sum data points", sum.num_rows());
/// }
/// ```
pub fn transform_metrics(bytes: &[u8], format: InputFormat) -> Result<MetricBatches> {
    // Step 1: Decode OTLP metrics
    let decode_result = decode_metrics(bytes, format)?;

    // Step 2: Apply VRL transformation (partitions by metric type)
    let metric_values = apply_metric_transform(decode_result.values)?;

    // Step 3: Convert each partition to Arrow (if non-empty)
    let gauge = if metric_values.gauge.is_empty() {
        None
    } else {
        Some(values_to_arrow(&metric_values.gauge, &gauge_schema())?)
    };

    let sum = if metric_values.sum.is_empty() {
        None
    } else {
        Some(values_to_arrow(&metric_values.sum, &sum_schema())?)
    };

    let histogram = if metric_values.histogram.is_empty() {
        None
    } else {
        Some(values_to_arrow(
            &metric_values.histogram,
            &histogram_schema(),
        )?)
    };

    let exp_histogram = if metric_values.exp_histogram.is_empty() {
        None
    } else {
        Some(values_to_arrow(
            &metric_values.exp_histogram,
            &exp_histogram_schema(),
        )?)
    };

    Ok(MetricBatches {
        gauge,
        sum,
        histogram,
        exp_histogram,
        skipped: decode_result.skipped,
    })
}

/// Transform OTLP metrics to JSON values.
pub fn transform_metrics_json(bytes: &[u8], format: InputFormat) -> Result<JsonMetricBatches> {
    let decode_result = decode_metrics(bytes, format)?;
    let metric_values = apply_metric_transform(decode_result.values)?;

    Ok(JsonMetricBatches {
        gauge: values_to_json(metric_values.gauge, "gauge metric")?,
        sum: values_to_json(metric_values.sum, "sum metric")?,
        histogram: values_to_json(metric_values.histogram, "histogram metric")?,
        exp_histogram: values_to_json(metric_values.exp_histogram, "exp_histogram metric")?,
        skipped: decode_result.skipped,
    })
}

// ============================================================================
// Partitioned API functions
// ============================================================================

/// Transform OTLP logs with service-based partitioning.
///
/// This function combines decoding, transformation, and service-based grouping
/// into a single call. Returns batches grouped by service name, ready for
/// partitioned storage.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP log data bytes
/// * `format` - The input format (Protobuf or JSON)
///
/// # Returns
///
/// A `ServiceGroupedBatches` containing RecordBatches grouped by service name,
/// with pre-extracted metadata (service_name, min_timestamp_micros) for each batch.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{transform_logs_partitioned, InputFormat};
///
/// let grouped = transform_logs_partitioned(otlp_bytes, InputFormat::Protobuf)?;
/// for batch in grouped.into_iter() {
///     // batch.service_name, batch.min_timestamp_micros, batch.batch are available
///     println!("Service: {}, records: {}", batch.service_name, batch.record_count);
/// }
/// ```
pub fn transform_logs_partitioned(
    bytes: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches> {
    let batch = transform_logs(bytes, format)?;
    Ok(group_batch_by_service(batch))
}

/// Transform OTLP traces with service-based partitioning.
///
/// This function combines decoding, transformation, and service-based grouping
/// into a single call. Returns batches grouped by service name, ready for
/// partitioned storage.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP trace data bytes
/// * `format` - The input format (Protobuf or JSON)
///
/// # Returns
///
/// A `ServiceGroupedBatches` containing RecordBatches grouped by service name,
/// with pre-extracted metadata for each batch.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{transform_traces_partitioned, InputFormat};
///
/// let grouped = transform_traces_partitioned(otlp_bytes, InputFormat::Protobuf)?;
/// for batch in grouped.into_iter() {
///     println!("Service: {}, spans: {}", batch.service_name, batch.record_count);
/// }
/// ```
pub fn transform_traces_partitioned(
    bytes: &[u8],
    format: InputFormat,
) -> Result<ServiceGroupedBatches> {
    let batch = transform_traces(bytes, format)?;
    Ok(group_batch_by_service(batch))
}

/// Transform OTLP metrics with service-based partitioning.
///
/// This function combines decoding, transformation, and service-based grouping
/// into a single call. Returns metrics separated by type (gauge, sum) and
/// grouped by service name.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP metric data bytes
/// * `format` - The input format (Protobuf or JSON)
///
/// # Returns
///
/// A `PartitionedMetrics` containing gauge and sum metrics, each grouped by
/// service name with pre-extracted metadata.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{transform_metrics_partitioned, InputFormat};
///
/// let metrics = transform_metrics_partitioned(otlp_bytes, InputFormat::Protobuf)?;
///
/// for batch in metrics.gauge.into_iter() {
///     println!("Gauge service: {}, points: {}", batch.service_name, batch.record_count);
/// }
/// for batch in metrics.sum.into_iter() {
///     println!("Sum service: {}, points: {}", batch.service_name, batch.record_count);
/// }
/// ```
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

// ============================================================================
// Lower-level API functions
// ============================================================================

/// Apply VRL transformation to decoded log values.
///
/// This function provides finer-grained control over the transformation process.
/// Use this when you need to inspect or modify values between steps, or when
/// you want to handle the Arrow conversion separately.
///
/// # Arguments
///
/// * `values` - Decoded OTLP log values (from `decode_logs`)
///
/// # Returns
///
/// A vector of transformed values ready for Arrow conversion, or an error.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{decode_logs, apply_log_transform, values_to_arrow, logs_schema, InputFormat};
///
/// let decoded = decode_logs(bytes, InputFormat::Protobuf)?;
/// let transformed = apply_log_transform(decoded)?;
/// let batch = values_to_arrow(&transformed, &logs_schema())?;
/// ```
pub fn apply_log_transform(values: Vec<Value>) -> Result<Vec<Value>> {
    let mut transformer = VrlTransformer::new();
    let mut result = Vec::with_capacity(values.len());

    for (idx, value) in values.into_iter().enumerate() {
        let (_table, transformed) = transformer
            .transform(&OTLP_LOGS_PROGRAM, value)
            .map_err(|e| Error::VrlRuntime(format!("log record {}: {}", idx, e.0)))?;
        result.push(transformed);
    }

    Ok(result)
}

/// Apply VRL transformation to decoded trace values.
///
/// This function provides finer-grained control over the transformation process.
/// Use this when you need to inspect or modify values between steps, or when
/// you want to handle the Arrow conversion separately.
///
/// # Arguments
///
/// * `values` - Decoded OTLP trace values (from `decode_traces`)
///
/// # Returns
///
/// A vector of transformed values ready for Arrow conversion, or an error.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{decode_traces, apply_trace_transform, values_to_arrow, traces_schema, InputFormat};
///
/// let decoded = decode_traces(bytes, InputFormat::Protobuf)?;
/// let transformed = apply_trace_transform(decoded)?;
/// let batch = values_to_arrow(&transformed, &traces_schema())?;
/// ```
pub fn apply_trace_transform(values: Vec<Value>) -> Result<Vec<Value>> {
    let mut transformer = VrlTransformer::new();
    let mut result = Vec::with_capacity(values.len());

    for (idx, value) in values.into_iter().enumerate() {
        let (_table, transformed) = transformer
            .transform(&OTLP_TRACES_PROGRAM, value)
            .map_err(|e| Error::VrlRuntime(format!("span {}: {}", idx, e.0)))?;
        result.push(transformed);
    }

    Ok(result)
}

/// Apply VRL transformation to decoded metric values.
///
/// This function partitions metrics by type (gauge vs sum) and applies the
/// appropriate VRL transformation program to each partition.
///
/// # Arguments
///
/// * `values` - Decoded OTLP metric values (from `decode_metrics`)
///
/// # Returns
///
/// A `MetricValues` struct containing transformed gauge and sum values, or an error.
///
/// # Example
///
/// ```ignore
/// use otlp2records::{decode_metrics, apply_metric_transform, values_to_arrow, gauge_schema, sum_schema, InputFormat};
///
/// let decoded = decode_metrics(bytes, InputFormat::Protobuf)?;
/// let transformed = apply_metric_transform(decoded)?;
///
/// if !transformed.gauge.is_empty() {
///     let batch = values_to_arrow(&transformed.gauge, &gauge_schema())?;
/// }
/// if !transformed.sum.is_empty() {
///     let batch = values_to_arrow(&transformed.sum, &sum_schema())?;
/// }
/// ```
pub fn apply_metric_transform(values: Vec<Value>) -> Result<MetricValues> {
    let mut transformer = VrlTransformer::new();
    let mut result = MetricValues::default();

    // Partition metrics by type and transform each with appropriate program
    for (idx, value) in values.into_iter().enumerate() {
        // Extract _metric_type field to determine which program to use
        let metric_type = extract_metric_type(&value);

        match metric_type.as_str() {
            "gauge" => {
                let (_table, transformed) = transformer
                    .transform(&OTLP_GAUGE_PROGRAM, value)
                    .map_err(|e| Error::VrlRuntime(format!("gauge metric {}: {}", idx, e.0)))?;
                result.gauge.push(transformed);
            }
            "sum" => {
                let (_table, transformed) = transformer
                    .transform(&OTLP_SUM_PROGRAM, value)
                    .map_err(|e| Error::VrlRuntime(format!("sum metric {}: {}", idx, e.0)))?;
                result.sum.push(transformed);
            }
            "histogram" => {
                let (_table, transformed) = transformer
                    .transform(&OTLP_HISTOGRAM_PROGRAM, value)
                    .map_err(|e| {
                    Error::VrlRuntime(format!("histogram metric {}: {}", idx, e.0))
                })?;
                result.histogram.push(transformed);
            }
            "exp_histogram" => {
                let (_table, transformed) = transformer
                    .transform(&OTLP_EXP_HISTOGRAM_PROGRAM, value)
                    .map_err(|e| {
                        Error::VrlRuntime(format!("exp_histogram metric {}: {}", idx, e.0))
                    })?;
                result.exp_histogram.push(transformed);
            }
            _ => {
                // Skip unknown metric types (summary - deprecated in OTLP spec)
            }
        }
    }

    Ok(result)
}

fn values_to_json(values: Vec<Value>, label: &str) -> Result<Vec<serde_json::Value>> {
    let mut out = Vec::with_capacity(values.len());

    for (idx, value) in values.into_iter().enumerate() {
        let json = crate::convert::vrl_value_to_json(&value).ok_or_else(|| {
            Error::InvalidInput(format!(
                "{label} record {idx} contains unrepresentable JSON value"
            ))
        })?;
        out.push(json);
    }

    Ok(out)
}

/// Extract the _metric_type field from a decoded metric value.
fn extract_metric_type(value: &Value) -> String {
    let metric_type_key: KeyString = "_metric_type".into();
    if let Value::Object(map) = value {
        if let Some(Value::Bytes(bytes)) = map.get(&metric_type_key) {
            return String::from_utf8_lossy(bytes).to_string();
        }
    }
    String::new()
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        collector::metrics::v1::ExportMetricsServiceRequest,
        collector::trace::v1::ExportTraceServiceRequest,
        common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        },
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;

    // ========================================================================
    // Helper functions for creating test data
    // ========================================================================

    fn create_test_log_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "test-lib".to_string(),
                        version: "1.0.0".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 1_700_000_000_100_000_000,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "Test log message".to_string(),
                            )),
                        }),
                        attributes: vec![KeyValue {
                            key: "log.key".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("log-value".to_string())),
                            }),
                        }],
                        trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                        span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn create_test_trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "test-lib".to_string(),
                        version: "1.0.0".to_string(),
                        ..Default::default()
                    }),
                    spans: vec![Span {
                        trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                        span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                        parent_span_id: vec![],
                        name: "test-span".to_string(),
                        kind: 1, // INTERNAL
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_100_000_000,
                        attributes: vec![KeyValue {
                            key: "span.key".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "span-value".to_string(),
                                )),
                            }),
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn create_test_metrics_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "test-lib".to_string(),
                        version: "1.0.0".to_string(),
                        ..Default::default()
                    }),
                    metrics: vec![
                        // Gauge metric
                        Metric {
                            name: "test.gauge".to_string(),
                            description: "A test gauge".to_string(),
                            unit: "1".to_string(),
                            data: Some(Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: 1_700_000_000_000_000_000,
                                    start_time_unix_nano: 1_699_999_000_000_000_000,
                                    value: Some(
                                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.5),
                                    ),
                                    attributes: vec![KeyValue {
                                        key: "metric.key".to_string(),
                                        value: Some(AnyValue {
                                            value: Some(any_value::Value::StringValue(
                                                "metric-value".to_string(),
                                            )),
                                        }),
                                    }],
                                    ..Default::default()
                                }],
                            })),
                            ..Default::default()
                        },
                        // Sum metric
                        Metric {
                            name: "test.sum".to_string(),
                            description: "A test sum".to_string(),
                            unit: "bytes".to_string(),
                            data: Some(Data::Sum(Sum {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: 1_700_000_000_000_000_000,
                                    start_time_unix_nano: 1_699_999_000_000_000_000,
                                    value: Some(
                                        opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(100.0),
                                    ),
                                    attributes: vec![],
                                    ..Default::default()
                                }],
                                aggregation_temporality: 2, // CUMULATIVE
                                is_monotonic: true,
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn create_gauge_only_metrics_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: "test.gauge".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(
                                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(1.0),
                                ),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn create_sum_only_metrics_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: "test.sum".to_string(),
                        data: Some(Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(
                                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(2.0),
                                ),
                                ..Default::default()
                            }],
                            aggregation_temporality: 1,
                            is_monotonic: false,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    // ========================================================================
    // High-level API tests
    // ========================================================================

    #[test]
    fn test_transform_logs_protobuf() {
        let request = create_test_log_request();
        let bytes = request.encode_to_vec();

        let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert!(batch.num_columns() > 0);

        // Verify some expected columns exist
        let schema = batch.schema();
        assert!(schema.field_with_name("timestamp").is_ok());
        assert!(schema.field_with_name("service_name").is_ok());
        assert!(schema.field_with_name("severity_number").is_ok());
    }

    #[test]
    fn test_transform_logs_json() {
        let json = r#"{
            "resourceLogs": [{
                "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "json-svc" } }]},
                "scopeLogs": [{
                    "scope": { "name": "lib", "version": "1" },
                    "logRecords": [{
                        "timeUnixNano": "1700000000000000000",
                        "observedTimeUnixNano": "1700000000100000000",
                        "severityNumber": 9,
                        "severityText": "INFO",
                        "body": { "stringValue": "JSON log" }
                    }]
                }]
            }]
        }"#;

        let batch = transform_logs(json.as_bytes(), InputFormat::Json).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_transform_logs_empty() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![],
        };
        let bytes = request.encode_to_vec();

        let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_transform_traces_protobuf() {
        let request = create_test_trace_request();
        let bytes = request.encode_to_vec();

        let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Verify some expected columns exist
        let schema = batch.schema();
        assert!(schema.field_with_name("timestamp").is_ok());
        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());
        assert!(schema.field_with_name("span_name").is_ok());
    }

    #[test]
    fn test_transform_traces_json() {
        let json = r#"{
            "resourceSpans": [{
                "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "json-svc" } }]},
                "scopeSpans": [{
                    "scope": { "name": "lib" },
                    "spans": [{
                        "traceId": "00010203040506070809101112131415",
                        "spanId": "0001020304050607",
                        "name": "json-span",
                        "kind": 1,
                        "startTimeUnixNano": "1700000000000000000",
                        "endTimeUnixNano": "1700000000100000000"
                    }]
                }]
            }]
        }"#;

        let batch = transform_traces(json.as_bytes(), InputFormat::Json).unwrap();

        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_transform_traces_empty() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![],
        };
        let bytes = request.encode_to_vec();

        let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_transform_metrics_protobuf() {
        let request = create_test_metrics_request();
        let bytes = request.encode_to_vec();

        let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

        // Should have both gauge and sum
        assert!(batches.gauge.is_some());
        assert!(batches.sum.is_some());

        let gauge = batches.gauge.unwrap();
        let sum = batches.sum.unwrap();

        assert_eq!(gauge.num_rows(), 1);
        assert_eq!(sum.num_rows(), 1);

        // Verify gauge schema
        let gauge_schema = gauge.schema();
        assert!(gauge_schema.field_with_name("metric_name").is_ok());
        assert!(gauge_schema.field_with_name("value").is_ok());

        // Verify sum schema has extra fields
        let sum_schema = sum.schema();
        assert!(sum_schema
            .field_with_name("aggregation_temporality")
            .is_ok());
        assert!(sum_schema.field_with_name("is_monotonic").is_ok());
    }

    #[test]
    fn test_transform_metrics_gauge_only() {
        let request = create_gauge_only_metrics_request();
        let bytes = request.encode_to_vec();

        let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

        assert!(batches.gauge.is_some());
        assert!(batches.sum.is_none());
    }

    #[test]
    fn test_transform_metrics_sum_only() {
        let request = create_sum_only_metrics_request();
        let bytes = request.encode_to_vec();

        let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

        assert!(batches.gauge.is_none());
        assert!(batches.sum.is_some());
    }

    #[test]
    fn test_transform_metrics_empty() {
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        let bytes = request.encode_to_vec();

        let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

        assert!(batches.gauge.is_none());
        assert!(batches.sum.is_none());
    }

    // ========================================================================
    // Lower-level API tests
    // ========================================================================

    #[test]
    fn test_apply_log_transform() {
        let request = create_test_log_request();
        let bytes = request.encode_to_vec();
        let decoded = decode_logs(&bytes, InputFormat::Protobuf).unwrap();

        let transformed = apply_log_transform(decoded).unwrap();

        assert_eq!(transformed.len(), 1);

        // Verify transformation was applied (should have output schema fields)
        if let Value::Object(map) = &transformed[0] {
            let ts_key: KeyString = "timestamp".into();
            let svc_key: KeyString = "service_name".into();
            assert!(map.get(&ts_key).is_some());
            assert!(map.get(&svc_key).is_some());
        } else {
            panic!("Expected object value");
        }
    }

    #[test]
    fn test_apply_trace_transform() {
        let request = create_test_trace_request();
        let bytes = request.encode_to_vec();
        let decoded = decode_traces(&bytes, InputFormat::Protobuf).unwrap();

        let transformed = apply_trace_transform(decoded).unwrap();

        assert_eq!(transformed.len(), 1);

        // Verify transformation was applied
        if let Value::Object(map) = &transformed[0] {
            let ts_key: KeyString = "timestamp".into();
            let span_key: KeyString = "span_name".into();
            assert!(map.get(&ts_key).is_some());
            assert!(map.get(&span_key).is_some());
        } else {
            panic!("Expected object value");
        }
    }

    #[test]
    fn test_apply_metric_transform() {
        let request = create_test_metrics_request();
        let bytes = request.encode_to_vec();
        let decode_result = decode_metrics(&bytes, InputFormat::Protobuf).unwrap();

        let transformed = apply_metric_transform(decode_result.values).unwrap();

        assert_eq!(transformed.gauge.len(), 1);
        assert_eq!(transformed.sum.len(), 1);
    }

    #[test]
    fn test_apply_log_transform_empty() {
        let transformed = apply_log_transform(vec![]).unwrap();
        assert!(transformed.is_empty());
    }

    #[test]
    fn test_apply_trace_transform_empty() {
        let transformed = apply_trace_transform(vec![]).unwrap();
        assert!(transformed.is_empty());
    }

    #[test]
    fn test_apply_metric_transform_empty() {
        let transformed = apply_metric_transform(vec![]).unwrap();
        assert!(transformed.gauge.is_empty());
        assert!(transformed.sum.is_empty());
    }

    // ========================================================================
    // Error handling tests
    // ========================================================================

    #[test]
    fn test_transform_logs_invalid_protobuf() {
        let result = transform_logs(b"not valid protobuf", InputFormat::Protobuf);
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_logs_invalid_json() {
        let result = transform_logs(b"not valid json", InputFormat::Json);
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_traces_invalid_protobuf() {
        let result = transform_traces(b"not valid protobuf", InputFormat::Protobuf);
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_metrics_invalid_protobuf() {
        let result = transform_metrics(b"not valid protobuf", InputFormat::Protobuf);
        assert!(result.is_err());
    }

    // ========================================================================
    // Struct tests
    // ========================================================================

    #[test]
    fn test_metric_batches_debug() {
        let batches = MetricBatches {
            gauge: None,
            sum: None,
            histogram: None,
            exp_histogram: None,
            skipped: SkippedMetrics::default(),
        };
        let debug_str = format!("{batches:?}");
        assert!(debug_str.contains("MetricBatches"));
    }

    #[test]
    fn test_metric_values_default() {
        let values = MetricValues::default();
        assert!(values.gauge.is_empty());
        assert!(values.sum.is_empty());
        assert!(values.histogram.is_empty());
        assert!(values.exp_histogram.is_empty());
    }

    #[test]
    fn test_metric_values_debug() {
        let values = MetricValues::default();
        let debug_str = format!("{values:?}");
        assert!(debug_str.contains("MetricValues"));
    }

    // ========================================================================
    // Integration tests
    // ========================================================================

    #[test]
    fn test_full_pipeline_logs_to_json_output() {
        let request = create_test_log_request();
        let bytes = request.encode_to_vec();

        let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();
        let json_output = to_json(&batch).unwrap();

        // Should be valid NDJSON
        assert!(!json_output.is_empty());
        // Should contain at least one newline or be non-empty
        let json_str = String::from_utf8(json_output).unwrap();
        assert!(json_str.contains('\n') || !json_str.is_empty());
    }

    #[test]
    fn test_full_pipeline_traces_to_ipc_output() {
        let request = create_test_trace_request();
        let bytes = request.encode_to_vec();

        let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();
        let ipc_output = to_ipc(&batch).unwrap();

        // Should produce some bytes
        assert!(!ipc_output.is_empty());
    }

    #[test]
    fn test_full_pipeline_metrics_round_trip() {
        let request = create_test_metrics_request();
        let bytes = request.encode_to_vec();

        // Transform
        let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();

        // Verify we can serialize both
        if let Some(gauge) = &batches.gauge {
            let json = to_json(gauge).unwrap();
            assert!(!json.is_empty());
        }

        if let Some(sum) = &batches.sum {
            let json = to_json(sum).unwrap();
            assert!(!json.is_empty());
        }
    }
}
