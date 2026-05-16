//! WASM bindings for otlp2records
//!
//! This module provides WebAssembly bindings for transforming OTLP telemetry data
//! to Arrow IPC format. The functions are designed for use with DuckDB-WASM,
//! arrow-js, or similar browser-based Arrow implementations.
//!
//! # Usage from JavaScript
//!
//! ```javascript
//! import init, { transform_logs_wasm, transform_traces_wasm } from 'otlp2records';
//!
//! // Initialize the WASM module
//! await init();
//!
//! // Transform OTLP logs to Arrow IPC bytes
//! const logBytes = new Uint8Array([...]); // protobuf or JSON bytes
//! const arrowIpc = transform_logs_wasm(logBytes, "protobuf");
//!
//! // Use with arrow-js or DuckDB-WASM
//! const table = arrow.tableFromIPC(arrowIpc);
//! ```
//!
//! # Format Parameter
//!
//! The `format` parameter accepts:
//! - `"protobuf"` or `"proto"` for Protocol Buffers binary format
//! - `"json"` for JSON format
//! - `"auto"` for auto-detection (JSON vs protobuf)

// This module is only compiled when targeting wasm32 with the wasm feature enabled.
// The cfg gate is in lib.rs: #[cfg(all(feature = "wasm", target_arch = "wasm32"))]

use wasm_bindgen::prelude::*;

use crate::arrow::{gauge_schema, sum_schema};
use crate::decode::InputFormat;
use crate::output::to_ipc;
use crate::{transform_logs, transform_metrics, transform_traces};

/// Parse format string to InputFormat enum.
///
/// # Arguments
///
/// * `format` - Format string: "protobuf", "proto", "json", or "auto"
///
/// # Returns
///
/// * `Ok(InputFormat)` - The parsed format
/// * `Err(String)` - If format string is invalid
fn parse_format(format: &str) -> Result<InputFormat, String> {
    match format.to_lowercase().as_str() {
        "protobuf" | "proto" => Ok(InputFormat::Protobuf),
        "json" => Ok(InputFormat::Json),
        "auto" => Ok(InputFormat::Auto),
        _ => Err(format!(
            "Invalid format '{}': expected 'protobuf', 'proto', 'json', or 'auto'",
            format
        )),
    }
}

/// Transform OTLP logs to Arrow IPC bytes (internal implementation).
fn transform_logs_impl(bytes: &[u8], format: &str) -> Result<Vec<u8>, String> {
    let input_format = parse_format(format)?;
    let batch = transform_logs(bytes, input_format).map_err(|e| e.to_string())?;
    to_ipc(&batch).map_err(|e| e.to_string())
}

/// Transform OTLP traces to Arrow IPC bytes (internal implementation).
fn transform_traces_impl(bytes: &[u8], format: &str) -> Result<Vec<u8>, String> {
    let input_format = parse_format(format)?;
    let batch = transform_traces(bytes, input_format).map_err(|e| e.to_string())?;
    to_ipc(&batch).map_err(|e| e.to_string())
}

/// Transform OTLP gauge metrics to Arrow IPC bytes (internal implementation).
fn transform_metrics_gauge_impl(bytes: &[u8], format: &str) -> Result<Vec<u8>, String> {
    use arrow_array::RecordBatch;

    let input_format = parse_format(format)?;
    let batches = transform_metrics(bytes, input_format).map_err(|e| e.to_string())?;

    match batches.gauge {
        Some(batch) => to_ipc(&batch).map_err(|e| e.to_string()),
        None => {
            // Return empty IPC with correct schema for consistency
            let empty_batch = RecordBatch::new_empty(gauge_schema().into());
            to_ipc(&empty_batch).map_err(|e| e.to_string())
        }
    }
}

/// Transform OTLP sum metrics to Arrow IPC bytes (internal implementation).
fn transform_metrics_sum_impl(bytes: &[u8], format: &str) -> Result<Vec<u8>, String> {
    use arrow_array::RecordBatch;

    let input_format = parse_format(format)?;
    let batches = transform_metrics(bytes, input_format).map_err(|e| e.to_string())?;

    match batches.sum {
        Some(batch) => to_ipc(&batch).map_err(|e| e.to_string()),
        None => {
            // Return empty IPC with correct schema for consistency
            let empty_batch = RecordBatch::new_empty(sum_schema().into());
            to_ipc(&empty_batch).map_err(|e| e.to_string())
        }
    }
}

/// Initialize the WASM module.
///
/// # Example
///
/// ```javascript
/// import init from 'otlp2records';
///
/// // init() automatically calls this function
/// await init();
/// ```
#[wasm_bindgen(start)]
pub fn init() {}

/// Transform OTLP logs to Arrow IPC bytes.
///
/// Decodes OTLP log data, transforms it, and serializes to Arrow IPC format.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP log data (protobuf or JSON bytes)
/// * `format` - Input format: "protobuf", "proto", "json", or "auto"
///
/// # Returns
///
/// * `Ok(Vec<u8>)` - Arrow IPC bytes that can be read by arrow-js or DuckDB-WASM
/// * `Err(JsError)` - If decoding, transformation, or serialization fails
///
/// # Example
///
/// ```javascript
/// const logBytes = new Uint8Array([...]); // OTLP protobuf
/// const arrowIpc = transform_logs_wasm(logBytes, "protobuf");
/// const table = arrow.tableFromIPC(arrowIpc);
/// ```
#[wasm_bindgen]
pub fn transform_logs_wasm(bytes: &[u8], format: &str) -> Result<Vec<u8>, JsError> {
    transform_logs_impl(bytes, format).map_err(|e| JsError::new(&e))
}

/// Transform OTLP traces to Arrow IPC bytes.
///
/// Decodes OTLP trace data, transforms it, and serializes to Arrow IPC format.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP trace data (protobuf or JSON bytes)
/// * `format` - Input format: "protobuf", "proto", "json", or "auto"
///
/// # Returns
///
/// * `Ok(Vec<u8>)` - Arrow IPC bytes that can be read by arrow-js or DuckDB-WASM
/// * `Err(JsError)` - If decoding, transformation, or serialization fails
///
/// # Example
///
/// ```javascript
/// const traceBytes = new Uint8Array([...]); // OTLP protobuf
/// const arrowIpc = transform_traces_wasm(traceBytes, "protobuf");
/// const table = arrow.tableFromIPC(arrowIpc);
/// ```
#[wasm_bindgen]
pub fn transform_traces_wasm(bytes: &[u8], format: &str) -> Result<Vec<u8>, JsError> {
    transform_traces_impl(bytes, format).map_err(|e| JsError::new(&e))
}

/// Transform OTLP gauge metrics to Arrow IPC bytes.
///
/// Decodes OTLP metric data, filters for gauge metrics, transforms it,
/// and serializes to Arrow IPC format.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP metric data (protobuf or JSON bytes)
/// * `format` - Input format: "protobuf", "proto", "json", or "auto"
///
/// # Returns
///
/// * `Ok(Vec<u8>)` - Arrow IPC bytes containing gauge metrics (empty if no gauges)
/// * `Err(JsError)` - If decoding, transformation, or serialization fails
///
/// # Example
///
/// ```javascript
/// const metricBytes = new Uint8Array([...]); // OTLP protobuf
/// const arrowIpc = transform_metrics_gauge_wasm(metricBytes, "protobuf");
/// if (arrowIpc.length > 0) {
///     const table = arrow.tableFromIPC(arrowIpc);
/// }
/// ```
#[wasm_bindgen]
pub fn transform_metrics_gauge_wasm(bytes: &[u8], format: &str) -> Result<Vec<u8>, JsError> {
    transform_metrics_gauge_impl(bytes, format).map_err(|e| JsError::new(&e))
}

/// Transform OTLP sum metrics to Arrow IPC bytes.
///
/// Decodes OTLP metric data, filters for sum (counter) metrics, transforms it,
/// and serializes to Arrow IPC format.
///
/// # Arguments
///
/// * `bytes` - Raw OTLP metric data (protobuf or JSON bytes)
/// * `format` - Input format: "protobuf", "proto", "json", or "auto"
///
/// # Returns
///
/// * `Ok(Vec<u8>)` - Arrow IPC bytes containing sum metrics (empty if no sums)
/// * `Err(JsError)` - If decoding, transformation, or serialization fails
///
/// # Example
///
/// ```javascript
/// const metricBytes = new Uint8Array([...]); // OTLP protobuf
/// const arrowIpc = transform_metrics_sum_wasm(metricBytes, "protobuf");
/// if (arrowIpc.length > 0) {
///     const table = arrow.tableFromIPC(arrowIpc);
/// }
/// ```
#[wasm_bindgen]
pub fn transform_metrics_sum_wasm(bytes: &[u8], format: &str) -> Result<Vec<u8>, JsError> {
    transform_metrics_sum_impl(bytes, format).map_err(|e| JsError::new(&e))
}

// ============================================================================
// Tests
// ============================================================================
// Note: These tests are included for documentation purposes but won't run
// since this module is only compiled on wasm32. For actual testing, use
// wasm-bindgen-test or test the underlying functions in lib.rs tests.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_format_protobuf() {
        assert!(matches!(
            parse_format("protobuf"),
            Ok(InputFormat::Protobuf)
        ));
        assert!(matches!(
            parse_format("PROTOBUF"),
            Ok(InputFormat::Protobuf)
        ));
        assert!(matches!(
            parse_format("Protobuf"),
            Ok(InputFormat::Protobuf)
        ));
    }

    #[test]
    fn test_parse_format_proto() {
        assert!(matches!(parse_format("proto"), Ok(InputFormat::Protobuf)));
        assert!(matches!(parse_format("PROTO"), Ok(InputFormat::Protobuf)));
    }

    #[test]
    fn test_parse_format_json() {
        assert!(matches!(parse_format("json"), Ok(InputFormat::Json)));
        assert!(matches!(parse_format("JSON"), Ok(InputFormat::Json)));
        assert!(matches!(parse_format("Json"), Ok(InputFormat::Json)));
    }

    #[test]
    fn test_parse_format_auto() {
        assert!(matches!(parse_format("auto"), Ok(InputFormat::Auto)));
        assert!(matches!(parse_format("AUTO"), Ok(InputFormat::Auto)));
        assert!(matches!(parse_format("Auto"), Ok(InputFormat::Auto)));
    }

    #[test]
    fn test_parse_format_invalid() {
        let result = parse_format("xml");
        assert!(result.is_err());

        let result = parse_format("");
        assert!(result.is_err());

        let result = parse_format("binary");
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_logs_impl_invalid_format() {
        let result = transform_logs_impl(b"test", "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_traces_impl_invalid_format() {
        let result = transform_traces_impl(b"test", "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_metrics_gauge_impl_invalid_format() {
        let result = transform_metrics_gauge_impl(b"test", "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_metrics_sum_impl_invalid_format() {
        let result = transform_metrics_sum_impl(b"test", "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_transform_logs_impl_empty_protobuf() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use prost::Message;

        let request = ExportLogsServiceRequest {
            resource_logs: vec![],
        };
        let bytes = request.encode_to_vec();

        let result = transform_logs_impl(&bytes, "protobuf");
        assert!(result.is_ok());

        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());
    }

    #[test]
    fn test_transform_traces_impl_empty_protobuf() {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
        use prost::Message;

        let request = ExportTraceServiceRequest {
            resource_spans: vec![],
        };
        let bytes = request.encode_to_vec();

        let result = transform_traces_impl(&bytes, "protobuf");
        assert!(result.is_ok());

        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());
    }

    #[test]
    fn test_transform_metrics_gauge_impl_empty_protobuf() {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use prost::Message;

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        let bytes = request.encode_to_vec();

        let result = transform_metrics_gauge_impl(&bytes, "protobuf");
        assert!(result.is_ok());

        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());
    }

    #[test]
    fn test_transform_metrics_sum_impl_empty_protobuf() {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use prost::Message;

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        let bytes = request.encode_to_vec();

        let result = transform_metrics_sum_impl(&bytes, "protobuf");
        assert!(result.is_ok());

        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());
    }

    #[test]
    fn test_transform_logs_impl_with_data() {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::{
            any_value, AnyValue, InstrumentationScope, KeyValue,
        };
        use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use prost::Message;

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
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let bytes = request.encode_to_vec();

        let result = transform_logs_impl(&bytes, "protobuf");
        assert!(result.is_ok());

        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());

        // Verify we can read back the IPC
        use arrow_ipc::reader::StreamReader;
        use std::io::Cursor;

        let cursor = Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn test_transform_logs_impl_json_format() {
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

        let result = transform_logs_impl(json.as_bytes(), "json");
        assert!(result.is_ok());

        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());

        // Verify we can read back the IPC
        use arrow_ipc::reader::StreamReader;
        use std::io::Cursor;

        let cursor = Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<_> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}
