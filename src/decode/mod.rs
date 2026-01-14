//! OTLP decode layer - transforms raw bytes into VRL Values
//!
//! This module provides decoders for OTLP logs, traces, and metrics in both
//! protobuf and JSON formats. The output is `Vec<Value>` where each Value
//! represents a single record ready for transformation.
//!
//! # Usage
//!
//! ```ignore
//! use otlp2records::decode::{decode_logs, InputFormat};
//!
//! let records = decode_logs(bytes, InputFormat::Protobuf)?;
//! ```
//!
//! # Simplifications from otlp2pipeline
//!
//! - No Gzip decompression (caller's responsibility)

mod common;
mod logs;
mod metrics;
mod normalize;
mod traces;

pub use common::{looks_like_json, DecodeError};
pub use metrics::{DecodeMetricsResult, SkippedMetrics};
pub use normalize::{
    count_skipped_metric_data_points, normalise_json_value, normalize_json_bytes, MetricSkipCounts,
};
use vrl::value::Value;

/// Input format for OTLP decoding
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InputFormat {
    /// Protocol Buffers binary format
    Protobuf,
    /// JSON format
    Json,
    /// Newline-delimited JSON (JSONL/NDJSON) format
    Jsonl,
    /// Auto-detect JSON vs protobuf, with fallback decoding
    Auto,
}

impl InputFormat {
    /// Infer input format from Content-Type header.
    pub fn from_content_type(content_type: Option<&str>) -> Self {
        let content_type = content_type.map(|v| v.trim().to_ascii_lowercase());

        match content_type.as_deref() {
            Some("application/x-ndjson") | Some("application/jsonl") => InputFormat::Jsonl,
            Some("application/json") | Some("application/otlp+json") => InputFormat::Json,
            Some("application/x-protobuf")
            | Some("application/protobuf")
            | Some("application/otlp") => InputFormat::Protobuf,
            _ => InputFormat::Auto,
        }
    }

    /// Returns the canonical Content-Type string for this format.
    pub fn content_type(&self) -> &'static str {
        match self {
            InputFormat::Protobuf => "application/x-protobuf",
            InputFormat::Json => "application/json",
            InputFormat::Jsonl => "application/x-ndjson",
            InputFormat::Auto => "application/x-protobuf", // Default to protobuf
        }
    }
}

/// Decode OTLP logs from raw bytes into VRL Values.
///
/// Each returned Value represents a single log record with fields:
/// - `time_unix_nano`: i64
/// - `observed_time_unix_nano`: i64
/// - `severity_number`: i64
/// - `severity_text`: string
/// - `body`: any VRL value
/// - `trace_id`: hex string
/// - `span_id`: hex string
/// - `attributes`: object
/// - `resource`: object with `attributes`
/// - `scope`: object with `name`, `version`, `attributes`
pub fn decode_logs(bytes: &[u8], format: InputFormat) -> Result<Vec<Value>, DecodeError> {
    match format {
        InputFormat::Protobuf => logs::decode_protobuf(bytes),
        InputFormat::Json => logs::decode_json(bytes),
        InputFormat::Jsonl => decode_jsonl(bytes, logs::decode_json),
        InputFormat::Auto => {
            if looks_like_json(bytes) {
                match logs::decode_json(bytes) {
                    Ok(values) => Ok(values),
                    Err(json_err) => {
                        // Try JSONL if JSON failed (e.g., multiple JSON objects)
                        match decode_jsonl(bytes, logs::decode_json) {
                            Ok(values) => Ok(values),
                            Err(_jsonl_err) => logs::decode_protobuf(bytes).map_err(|proto_err| {
                                DecodeError::Unsupported(format!(
                                    "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                                ))
                            }),
                        }
                    }
                }
            } else {
                match logs::decode_protobuf(bytes) {
                    Ok(values) => Ok(values),
                    Err(proto_err) => logs::decode_json(bytes).map_err(|json_err| {
                        DecodeError::Unsupported(format!(
                            "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                        ))
                    }),
                }
            }
        }
    }
}

/// Decode OTLP traces from raw bytes into VRL Values.
///
/// Each returned Value represents a single span with fields:
/// - `trace_id`: hex string
/// - `span_id`: hex string
/// - `parent_span_id`: hex string
/// - `trace_state`: string
/// - `name`: string
/// - `kind`: i64 (0-5)
/// - `start_time_unix_nano`: i64
/// - `end_time_unix_nano`: i64
/// - `duration_ns`: i64 (computed)
/// - `attributes`: object
/// - `status_code`: i64 (0-2)
/// - `status_message`: string
/// - `events`: array of event objects
/// - `links`: array of link objects
/// - `resource`: object with `attributes`
/// - `scope`: object with `name`, `version`, `attributes`
/// - `dropped_*_count`: i64
/// - `flags`: i64
pub fn decode_traces(bytes: &[u8], format: InputFormat) -> Result<Vec<Value>, DecodeError> {
    match format {
        InputFormat::Protobuf => traces::decode_protobuf(bytes),
        InputFormat::Json => traces::decode_json(bytes),
        InputFormat::Jsonl => decode_jsonl(bytes, traces::decode_json),
        InputFormat::Auto => {
            if looks_like_json(bytes) {
                match traces::decode_json(bytes) {
                    Ok(values) => Ok(values),
                    Err(json_err) => {
                        // Try JSONL if JSON failed (e.g., multiple JSON objects)
                        match decode_jsonl(bytes, traces::decode_json) {
                            Ok(values) => Ok(values),
                            Err(_jsonl_err) => {
                                traces::decode_protobuf(bytes).map_err(|proto_err| {
                                    DecodeError::Unsupported(format!(
                                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                                    ))
                                })
                            }
                        }
                    }
                }
            } else {
                match traces::decode_protobuf(bytes) {
                    Ok(values) => Ok(values),
                    Err(proto_err) => traces::decode_json(bytes).map_err(|json_err| {
                        DecodeError::Unsupported(format!(
                            "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                        ))
                    }),
                }
            }
        }
    }
}

/// Decode OTLP metrics from raw bytes into VRL Values.
///
/// Each returned Value represents a single metric data point with fields:
/// - `time_unix_nano`: i64
/// - `start_time_unix_nano`: i64
/// - `metric_name`: string
/// - `metric_description`: string
/// - `metric_unit`: string
/// - `value`: float64
/// - `attributes`: object
/// - `resource`: object with `attributes`
/// - `scope`: object with `name`, `version`, `attributes`
/// - `flags`: i64
/// - `exemplars`: array of exemplar objects
/// - `_metric_type`: "gauge" or "sum"
///
/// For sum metrics, additional fields:
/// - `aggregation_temporality`: i64
/// - `is_monotonic`: bool
///
/// # Skipped Metrics
///
/// The following are skipped and tracked in the returned [`DecodeMetricsResult::skipped`]:
/// - Summary metric types (deprecated in OTLP spec)
/// - Data points with non-finite values (NaN, Infinity)
/// - Data points with missing values
///
/// Use [`SkippedMetrics::has_skipped()`] to check if any data was dropped.
pub fn decode_metrics(
    bytes: &[u8],
    format: InputFormat,
) -> Result<DecodeMetricsResult, DecodeError> {
    match format {
        InputFormat::Protobuf => metrics::decode_protobuf(bytes),
        InputFormat::Json => metrics::decode_json(bytes),
        InputFormat::Jsonl => decode_metrics_jsonl(bytes),
        InputFormat::Auto => {
            if looks_like_json(bytes) {
                match metrics::decode_json(bytes) {
                    Ok(values) => Ok(values),
                    Err(json_err) => {
                        // Try JSONL if JSON failed (e.g., multiple JSON objects)
                        match decode_metrics_jsonl(bytes) {
                            Ok(values) => Ok(values),
                            Err(_jsonl_err) => {
                                metrics::decode_protobuf(bytes).map_err(|proto_err| {
                                    DecodeError::Unsupported(format!(
                                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                                    ))
                                })
                            }
                        }
                    }
                }
            } else {
                match metrics::decode_protobuf(bytes) {
                    Ok(values) => Ok(values),
                    Err(proto_err) => metrics::decode_json(bytes).map_err(|json_err| {
                        DecodeError::Unsupported(format!(
                            "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                        ))
                    }),
                }
            }
        }
    }
}

// ============================================================================
// JSONL decoding helpers
// ============================================================================

/// Generic JSONL decoder for logs and traces.
/// Processes each non-empty line as a separate JSON payload and combines results.
fn decode_jsonl<F>(bytes: &[u8], decode_json_fn: F) -> Result<Vec<Value>, DecodeError>
where
    F: Fn(&[u8]) -> Result<Vec<Value>, DecodeError>,
{
    let text = std::str::from_utf8(bytes).map_err(|e| DecodeError::Parse(e.to_string()))?;
    let mut all_values = Vec::new();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;

        let values = decode_json_fn(trimmed.as_bytes())
            .map_err(|e| DecodeError::Parse(format!("line {}: {}", line_num + 1, e)))?;
        all_values.extend(values);
    }

    if !saw_line {
        return Err(DecodeError::Parse(
            "jsonl payload contained no records".to_string(),
        ));
    }

    Ok(all_values)
}

/// JSONL decoder for metrics - handles the special DecodeMetricsResult return type.
fn decode_metrics_jsonl(bytes: &[u8]) -> Result<DecodeMetricsResult, DecodeError> {
    let text = std::str::from_utf8(bytes).map_err(|e| DecodeError::Parse(e.to_string()))?;
    let mut all_values = Vec::new();
    let mut combined_skipped = SkippedMetrics::default();
    let mut saw_line = false;

    for (line_num, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        saw_line = true;

        let result = metrics::decode_json(trimmed.as_bytes())
            .map_err(|e| DecodeError::Parse(format!("line {}: {}", line_num + 1, e)))?;

        all_values.extend(result.values);

        // Merge skipped counts
        combined_skipped.summaries += result.skipped.summaries;
        combined_skipped.nan_values += result.skipped.nan_values;
        combined_skipped.infinity_values += result.skipped.infinity_values;
        combined_skipped.missing_values += result.skipped.missing_values;
    }

    if !saw_line {
        return Err(DecodeError::Parse(
            "jsonl payload contained no records".to_string(),
        ));
    }

    Ok(DecodeMetricsResult {
        values: all_values,
        skipped: combined_skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_format_debug() {
        assert_eq!(format!("{:?}", InputFormat::Protobuf), "Protobuf");
        assert_eq!(format!("{:?}", InputFormat::Json), "Json");
        assert_eq!(format!("{:?}", InputFormat::Jsonl), "Jsonl");
        assert_eq!(format!("{:?}", InputFormat::Auto), "Auto");
    }

    #[test]
    fn input_format_equality() {
        assert_eq!(InputFormat::Protobuf, InputFormat::Protobuf);
        assert_ne!(InputFormat::Protobuf, InputFormat::Json);
        assert_ne!(InputFormat::Json, InputFormat::Jsonl);
        assert_ne!(InputFormat::Jsonl, InputFormat::Auto);
    }

    #[test]
    fn input_format_from_content_type_jsonl() {
        assert_eq!(
            InputFormat::from_content_type(Some("application/x-ndjson")),
            InputFormat::Jsonl
        );
        assert_eq!(
            InputFormat::from_content_type(Some("application/jsonl")),
            InputFormat::Jsonl
        );
    }

    #[test]
    fn input_format_content_type() {
        assert_eq!(
            InputFormat::Protobuf.content_type(),
            "application/x-protobuf"
        );
        assert_eq!(InputFormat::Json.content_type(), "application/json");
        assert_eq!(InputFormat::Jsonl.content_type(), "application/x-ndjson");
        assert_eq!(InputFormat::Auto.content_type(), "application/x-protobuf");
    }

    #[test]
    fn decode_logs_jsonl() {
        let line1 = r#"{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"123","severityNumber":9}]}]}]}"#;
        let line2 = r#"{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"456","severityNumber":13}]}]}]}"#;
        let jsonl = format!("{line1}\n{line2}");

        let result = decode_logs(jsonl.as_bytes(), InputFormat::Jsonl);
        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn decode_logs_jsonl_empty_lines() {
        let line1 = r#"{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"123"}]}]}]}"#;
        let jsonl = format!("\n{line1}\n\n");

        let result = decode_logs(jsonl.as_bytes(), InputFormat::Jsonl);
        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn decode_logs_jsonl_empty_payload() {
        let result = decode_logs(b"", InputFormat::Jsonl);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("no records"));
    }

    #[test]
    fn decode_traces_jsonl() {
        let line1 = r#"{"resourceSpans":[{"resource":{},"scopeSpans":[{"scope":{},"spans":[{"traceId":"00000000000000000000000000000001","spanId":"0000000000000001","name":"span1","startTimeUnixNano":"100","endTimeUnixNano":"200"}]}]}]}"#;
        let jsonl = format!("{line1}\n");

        let result = decode_traces(jsonl.as_bytes(), InputFormat::Jsonl);
        assert!(result.is_ok());
        let values = result.unwrap();
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn decode_metrics_jsonl() {
        let line1 = r#"{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{},"metrics":[{"name":"test","gauge":{"dataPoints":[{"timeUnixNano":"100","asDouble":1.0}]}}]}]}]}"#;
        let line2 = r#"{"resourceMetrics":[{"resource":{},"scopeMetrics":[{"scope":{},"metrics":[{"name":"test2","gauge":{"dataPoints":[{"timeUnixNano":"200","asDouble":2.0}]}}]}]}]}"#;
        let jsonl = format!("{line1}\n{line2}");

        let result = decode_metrics(jsonl.as_bytes(), InputFormat::Jsonl);
        assert!(result.is_ok());
        let decode_result = result.unwrap();
        assert_eq!(decode_result.values.len(), 2);
    }
}
