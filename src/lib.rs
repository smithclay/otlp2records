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
mod arrow;
mod decode;
mod error;
mod fast;
mod output;
mod schemas;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub mod wasm;

#[cfg(feature = "ffi")]
pub mod ffi;

use ::arrow_array::RecordBatch;

pub use arrow::{
    exp_histogram_schema, extract_min_timestamp_micros, extract_service_name, gauge_schema,
    group_batch_by_service, histogram_schema, logs_schema, sum_schema, traces_schema,
    PartitionedBatch, PartitionedMetrics, ServiceGroupedBatches,
};
use decode::{
    decode_logs_json_request, decode_logs_jsonl_request, decode_metrics_json_request,
    decode_metrics_jsonl_request, decode_traces_json_request, decode_traces_jsonl_request,
    looks_like_json,
};
pub use decode::{DecodeError, InputFormat, SkippedMetrics};
pub use error::{Error, Result};
#[cfg(feature = "parquet")]
pub use output::to_parquet;
pub use output::{to_ipc, to_json};
pub use schemas::{schema_def, schema_defs, FieldType, SchemaDef, SchemaField};

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

// ============================================================================
// High-level API functions
// ============================================================================

/// Transform OTLP logs to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP log data to Arrow format.
/// It handles decoding, transformation, and Arrow conversion in one step.
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
    match format {
        InputFormat::Protobuf => fast::transform_logs_protobuf(bytes),
        InputFormat::Auto => transform_logs_auto(bytes),
        InputFormat::Json | InputFormat::Jsonl => transform_logs_json_arrow(bytes, format),
    }
}

fn transform_logs_json_arrow(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    let request = match format {
        InputFormat::Json => decode_logs_json_request(bytes)?,
        InputFormat::Jsonl => decode_logs_jsonl_request(bytes)?,
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL logs input".to_string(),
            )));
        }
    };
    fast::transform_logs_request(request, bytes.len())
}

fn transform_logs_auto(bytes: &[u8]) -> Result<RecordBatch> {
    if looks_like_json(bytes) {
        match transform_logs_json_arrow(bytes, InputFormat::Json) {
            Ok(batch) => Ok(batch),
            Err(json_err) => match transform_logs_json_arrow(bytes, InputFormat::Jsonl) {
                Ok(batch) => Ok(batch),
                Err(_) => fast::transform_logs_protobuf(bytes).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match fast::transform_logs_protobuf(bytes) {
            Ok(batch) => Ok(batch),
            Err(proto_err) => {
                transform_logs_json_arrow(bytes, InputFormat::Json).map_err(|json_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                    )))
                })
            }
        }
    }
}

/// Transform OTLP logs to JSON values.
pub fn transform_logs_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_logs(bytes, format)?)
}

/// Transform OTLP traces to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP trace data to Arrow format.
/// It handles decoding, transformation, and Arrow conversion in one step.
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
    match format {
        InputFormat::Protobuf => fast::transform_traces_protobuf(bytes),
        InputFormat::Auto => transform_traces_auto(bytes),
        InputFormat::Json | InputFormat::Jsonl => transform_traces_json_arrow(bytes, format),
    }
}

fn transform_traces_json_arrow(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    let request = match format {
        InputFormat::Json => decode_traces_json_request(bytes)?,
        InputFormat::Jsonl => decode_traces_jsonl_request(bytes)?,
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL traces input".to_string(),
            )));
        }
    };
    fast::transform_traces_request(request, bytes.len())
}

fn transform_traces_auto(bytes: &[u8]) -> Result<RecordBatch> {
    if looks_like_json(bytes) {
        match transform_traces_json_arrow(bytes, InputFormat::Json) {
            Ok(batch) => Ok(batch),
            Err(json_err) => match transform_traces_json_arrow(bytes, InputFormat::Jsonl) {
                Ok(batch) => Ok(batch),
                Err(_) => fast::transform_traces_protobuf(bytes).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match fast::transform_traces_protobuf(bytes) {
            Ok(batch) => Ok(batch),
            Err(proto_err) => {
                transform_traces_json_arrow(bytes, InputFormat::Json).map_err(|json_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                    )))
                })
            }
        }
    }
}

/// Transform OTLP traces to JSON values.
pub fn transform_traces_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_traces(bytes, format)?)
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
    match format {
        InputFormat::Protobuf => fast::transform_metrics_protobuf(bytes),
        InputFormat::Auto => transform_metrics_auto(bytes),
        InputFormat::Json | InputFormat::Jsonl => transform_metrics_json_arrow(bytes, format),
    }
}

fn transform_metrics_json_arrow(bytes: &[u8], format: InputFormat) -> Result<MetricBatches> {
    let request = match format {
        InputFormat::Json => decode_metrics_json_request(bytes)?,
        InputFormat::Jsonl => decode_metrics_jsonl_request(bytes)?,
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL metrics input".to_string(),
            )));
        }
    };
    fast::transform_metrics_request(request)
}

fn transform_metrics_auto(bytes: &[u8]) -> Result<MetricBatches> {
    if looks_like_json(bytes) {
        match transform_metrics_json_arrow(bytes, InputFormat::Json) {
            Ok(batches) => Ok(batches),
            Err(json_err) => match transform_metrics_json_arrow(bytes, InputFormat::Jsonl) {
                Ok(batches) => Ok(batches),
                Err(_) => fast::transform_metrics_protobuf(bytes).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match fast::transform_metrics_protobuf(bytes) {
            Ok(batches) => Ok(batches),
            Err(proto_err) => {
                transform_metrics_json_arrow(bytes, InputFormat::Json).map_err(|json_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                    )))
                })
            }
        }
    }
}

/// Transform OTLP metrics to JSON values.
pub fn transform_metrics_json(bytes: &[u8], format: InputFormat) -> Result<JsonMetricBatches> {
    let batches = transform_metrics(bytes, format)?;

    Ok(JsonMetricBatches {
        gauge: optional_batch_to_json_values(batches.gauge.as_ref())?,
        sum: optional_batch_to_json_values(batches.sum.as_ref())?,
        histogram: optional_batch_to_json_values(batches.histogram.as_ref())?,
        exp_histogram: optional_batch_to_json_values(batches.exp_histogram.as_ref())?,
        skipped: batches.skipped,
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

fn optional_batch_to_json_values(batch: Option<&RecordBatch>) -> Result<Vec<serde_json::Value>> {
    match batch {
        Some(batch) => batch_to_json_values(batch),
        None => Ok(Vec::new()),
    }
}

fn batch_to_json_values(batch: &RecordBatch) -> Result<Vec<serde_json::Value>> {
    let bytes = crate::output::to_json(batch)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|err| Error::InvalidInput(format!("JSON output was not UTF-8: {err}")))?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|err| Error::InvalidInput(format!("invalid JSON output row: {err}")))
        })
        .collect()
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

    // ========================================================================
    // Timestamp validation tests - ensure timestamps are not 1970 dates
    // ========================================================================

    #[test]
    fn test_timestamp_not_epoch_traces() {
        // Use a known timestamp: 1703265600000000000 ns = Dec 22, 2023 @ 00:00:00 UTC
        let request = ExportTraceServiceRequest {
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
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![Span {
                        trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                        span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
                        name: "test-span".to_string(),
                        start_time_unix_nano: 1_703_265_600_000_000_000, // Dec 22, 2023
                        end_time_unix_nano: 1_703_265_600_100_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let bytes = request.encode_to_vec();

        let batch = transform_traces(&bytes, InputFormat::Protobuf).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Get the timestamp column and verify it's not 0 (epoch)
        let schema = batch.schema();
        let ts_idx = schema.index_of("timestamp").unwrap();
        let ts_column = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<::arrow_array::TimestampMicrosecondArray>()
            .expect("timestamp should be TimestampMicrosecondArray");

        let ts_value = ts_column.value(0);
        // Expected: 1703265600000000000 ns / 1000 = 1703265600000000 microseconds
        let expected_micros: i64 = 1_703_265_600_000_000;

        assert_eq!(
            ts_value, expected_micros,
            "Timestamp should be Dec 22, 2023, not epoch (1970)"
        );
        // Sanity check: value should be much greater than 0 (year 2023 >> year 1970)
        assert!(
            ts_value > 1_600_000_000_000_000,
            "Timestamp {ts_value} appears to be too small, possibly 1970 date"
        );
    }

    #[test]
    fn test_timestamp_not_epoch_logs() {
        // Use a known timestamp: 1703265600000000000 ns = Dec 22, 2023 @ 00:00:00 UTC
        let request = ExportLogsServiceRequest {
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
                    scope: Some(InstrumentationScope::default()),
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_703_265_600_000_000_000, // Dec 22, 2023
                        observed_time_unix_nano: 1_703_265_600_100_000_000,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("Test log".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let bytes = request.encode_to_vec();

        let batch = transform_logs(&bytes, InputFormat::Protobuf).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Get the timestamp column and verify it's not 0 (epoch)
        let schema = batch.schema();
        let ts_idx = schema.index_of("timestamp").unwrap();
        let ts_column = batch
            .column(ts_idx)
            .as_any()
            .downcast_ref::<::arrow_array::TimestampMicrosecondArray>()
            .expect("timestamp should be TimestampMicrosecondArray");

        let ts_value = ts_column.value(0);
        // Expected: 1703265600000000000 ns / 1000 = 1703265600000000 microseconds
        let expected_micros: i64 = 1_703_265_600_000_000;

        assert_eq!(
            ts_value, expected_micros,
            "Timestamp should be Dec 22, 2023, not epoch (1970)"
        );
        // Sanity check: value should be much greater than 0 (year 2023 >> year 1970)
        assert!(
            ts_value > 1_600_000_000_000_000,
            "Timestamp {ts_value} appears to be too small, possibly 1970 date"
        );
    }

    #[test]
    fn test_timestamp_not_epoch_metrics() {
        // Use a known timestamp: 1703265600000000000 ns = Dec 22, 2023 @ 00:00:00 UTC
        let request = ExportMetricsServiceRequest {
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
                                time_unix_nano: 1_703_265_600_000_000_000, // Dec 22, 2023
                                value: Some(
                                    opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(42.0),
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
        };
        let bytes = request.encode_to_vec();

        let batches = transform_metrics(&bytes, InputFormat::Protobuf).unwrap();
        let gauge = batches.gauge.expect("should have gauge metrics");
        assert_eq!(gauge.num_rows(), 1);

        // Get the timestamp column and verify it's not 0 (epoch)
        let schema = gauge.schema();
        let ts_idx = schema.index_of("timestamp").unwrap();
        let ts_column = gauge
            .column(ts_idx)
            .as_any()
            .downcast_ref::<::arrow_array::TimestampMicrosecondArray>()
            .expect("timestamp should be TimestampMicrosecondArray");

        let ts_value = ts_column.value(0);
        // Expected: 1703265600000000000 ns / 1000 = 1703265600000000 microseconds
        let expected_micros: i64 = 1_703_265_600_000_000;

        assert_eq!(
            ts_value, expected_micros,
            "Timestamp should be Dec 22, 2023, not epoch (1970)"
        );
        // Sanity check: value should be much greater than 0 (year 2023 >> year 1970)
        assert!(
            ts_value > 1_600_000_000_000_000,
            "Timestamp {ts_value} appears to be too small, possibly 1970 date"
        );
    }
}
