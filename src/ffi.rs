//! C FFI bindings for otlp2records
//!
//! Exposes streaming OTLP parser via Arrow C Data Interface.
//!
//! # Safety
//!
//! All functions use `catch_unwind` to prevent Rust panics crossing FFI boundary.
//! Pointer validation is performed before dereferencing.
//!
//! # Memory Ownership
//!
//! - Input data: Caller owns, Rust borrows during function call
//! - Parser handle: Rust allocates, caller must call `otlp_parser_destroy()`
//! - ArrowSchema/ArrowArray: Rust allocates, caller must call `release()` callback
//! - Error strings: Rust owns, valid until next FFI call on same handle

use std::ffi::{c_char, c_int, CString};
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::Schema;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

use crate::decode::InputFormat;
use crate::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, traces_schema,
    transform_logs, transform_metrics, transform_traces,
};

// ============================================================================
// C-compatible enums
// ============================================================================

/// OTLP signal types supported by the parser.
///
/// C names: OTLP_SIGNAL_LOGS, OTLP_SIGNAL_TRACES, etc.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpSignalType {
    /// Log records (C: OTLP_SIGNAL_LOGS)
    Logs = 0,
    /// Trace spans (C: OTLP_SIGNAL_TRACES)
    Traces = 1,
    /// Gauge metrics (C: OTLP_SIGNAL_METRICS_GAUGE)
    MetricsGauge = 2,
    /// Sum/counter metrics (C: OTLP_SIGNAL_METRICS_SUM)
    MetricsSum = 3,
    /// Histogram metrics (C: OTLP_SIGNAL_METRICS_HISTOGRAM)
    MetricsHistogram = 4,
    /// Exponential histogram metrics (C: OTLP_SIGNAL_METRICS_EXP_HISTOGRAM)
    MetricsExpHistogram = 5,
}

/// Input format for OTLP data.
///
/// C names: OTLP_FORMAT_AUTO, OTLP_FORMAT_PROTOBUF, etc.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpInputFormat {
    /// Auto-detect format from content (C: OTLP_FORMAT_AUTO)
    Auto = 0,
    /// Binary protobuf format (C: OTLP_FORMAT_PROTOBUF)
    Protobuf = 1,
    /// JSON format (C: OTLP_FORMAT_JSON)
    Json = 2,
    /// Newline-delimited JSON (C: OTLP_FORMAT_JSONL)
    Jsonl = 3,
}

impl From<OtlpInputFormat> for InputFormat {
    fn from(f: OtlpInputFormat) -> Self {
        match f {
            OtlpInputFormat::Auto => InputFormat::Auto,
            OtlpInputFormat::Protobuf => InputFormat::Protobuf,
            OtlpInputFormat::Json => InputFormat::Json,
            OtlpInputFormat::Jsonl => InputFormat::Jsonl,
        }
    }
}

/// Status codes returned by FFI functions.
///
/// C names: OTLP_OK, OTLP_ERROR_INVALID_ARGUMENT, etc.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpStatus {
    /// Success (C: OTLP_OK)
    Ok = 0,
    /// Invalid argument (C: OTLP_ERROR_INVALID_ARGUMENT)
    InvalidArgument = 1,
    /// Parse failed (C: OTLP_ERROR_PARSE_FAILED)
    ParseFailed = 2,
    /// Invalid format (C: OTLP_ERROR_INVALID_FORMAT)
    InvalidFormat = 3,
    /// Out of memory (C: OTLP_ERROR_OUT_OF_MEMORY)
    OutOfMemory = 4,
    /// Internal error (C: OTLP_ERROR_INTERNAL)
    Internal = 5,
}

// ============================================================================
// Parser Handle
// ============================================================================

/// Opaque parser handle for streaming OTLP data.
///
/// This handle maintains state for parsing OTLP data and producing Arrow batches.
/// It is NOT thread-safe - use one handle per thread.
pub struct OtlpParserHandle {
    signal_type: OtlpSignalType,
    format: InputFormat,
    buffer: Vec<u8>,
    batches: Vec<RecordBatch>,
    last_error: Option<CString>,
}

impl OtlpParserHandle {
    fn new(signal_type: OtlpSignalType, format: OtlpInputFormat) -> Self {
        Self {
            signal_type,
            format: format.into(),
            buffer: Vec::new(),
            batches: Vec::new(),
            last_error: None,
        }
    }

    fn set_error(&mut self, msg: &str) {
        self.last_error = CString::new(msg).ok();
    }

    fn clear_error(&mut self) {
        self.last_error = None;
    }

    fn push(&mut self, data: &[u8], is_final: bool) -> OtlpStatus {
        self.clear_error();
        self.buffer.extend_from_slice(data);

        if !is_final {
            // For streaming: could parse complete lines for JSONL here
            // For now, wait until is_final
            return OtlpStatus::Ok;
        }

        // Parse the complete buffer
        let result = match self.signal_type {
            OtlpSignalType::Logs => transform_logs(&self.buffer, self.format).map(Some),
            OtlpSignalType::Traces => transform_traces(&self.buffer, self.format).map(Some),
            OtlpSignalType::MetricsGauge => {
                transform_metrics(&self.buffer, self.format).map(|m| m.gauge)
            }
            OtlpSignalType::MetricsSum => {
                transform_metrics(&self.buffer, self.format).map(|m| m.sum)
            }
            OtlpSignalType::MetricsHistogram => {
                transform_metrics(&self.buffer, self.format).map(|m| m.histogram)
            }
            OtlpSignalType::MetricsExpHistogram => {
                transform_metrics(&self.buffer, self.format).map(|m| m.exp_histogram)
            }
        };

        match result {
            Ok(Some(batch)) => {
                self.batches.push(batch);
                self.buffer.clear();
                OtlpStatus::Ok
            }
            Ok(None) => {
                // No data of this type (e.g., no gauge metrics in input)
                self.buffer.clear();
                OtlpStatus::Ok
            }
            Err(e) => {
                self.set_error(&e.to_string());
                self.buffer.clear();
                OtlpStatus::ParseFailed
            }
        }
    }

    fn get_schema(&self) -> Arc<Schema> {
        match self.signal_type {
            OtlpSignalType::Logs => Arc::new(logs_schema()),
            OtlpSignalType::Traces => Arc::new(traces_schema()),
            OtlpSignalType::MetricsGauge => Arc::new(gauge_schema()),
            OtlpSignalType::MetricsSum => Arc::new(sum_schema()),
            OtlpSignalType::MetricsHistogram => Arc::new(histogram_schema()),
            OtlpSignalType::MetricsExpHistogram => Arc::new(exp_histogram_schema()),
        }
    }
}

// ============================================================================
// FFI Functions - Parser Lifecycle
// ============================================================================

/// Create a new streaming parser for the given signal type.
///
/// # Safety
///
/// - `out_handle` must be a valid, non-null pointer to a pointer
/// - On success, caller owns the handle and must call `otlp_parser_destroy()`
///
/// # Returns
///
/// `OTLP_OK` on success, error code otherwise.
#[no_mangle]
pub unsafe extern "C" fn otlp_parser_create(
    signal_type: OtlpSignalType,
    format: OtlpInputFormat,
    out_handle: *mut *mut OtlpParserHandle,
) -> OtlpStatus {
    if out_handle.is_null() {
        return OtlpStatus::InvalidArgument;
    }

    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let handle = Box::new(OtlpParserHandle::new(signal_type, format));
        *out_handle = Box::into_raw(handle);
        OtlpStatus::Ok
    }))
    .unwrap_or(OtlpStatus::Internal)
}

/// Destroy a parser handle and release all resources.
///
/// # Safety
///
/// - `handle` may be null (no-op in that case)
/// - After this call, the handle pointer is invalid
#[no_mangle]
pub unsafe extern "C" fn otlp_parser_destroy(handle: *mut OtlpParserHandle) {
    if !handle.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(Box::from_raw(handle));
        }));
    }
}

// ============================================================================
// FFI Functions - Streaming Interface
// ============================================================================

/// Push input bytes to the parser.
///
/// # Safety
///
/// - `handle` must be a valid parser handle
/// - `data` must be valid for `len` bytes (or null if len is 0)
/// - Caller retains ownership of `data` buffer
///
/// # Arguments
///
/// - `handle`: Parser handle
/// - `data`: Pointer to input bytes (JSON or protobuf)
/// - `len`: Length of input bytes
/// - `is_final`: Non-zero if this is the last chunk (triggers parsing)
///
/// # Returns
///
/// `OTLP_OK` on success, error code otherwise.
#[no_mangle]
pub unsafe extern "C" fn otlp_parser_push(
    handle: *mut OtlpParserHandle,
    data: *const u8,
    len: usize,
    is_final: c_int,
) -> OtlpStatus {
    if handle.is_null() {
        return OtlpStatus::InvalidArgument;
    }
    if data.is_null() && len > 0 {
        return OtlpStatus::InvalidArgument;
    }

    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let handle = &mut *handle;
        let slice = if len > 0 && !data.is_null() {
            std::slice::from_raw_parts(data, len)
        } else {
            &[]
        };

        handle.push(slice, is_final != 0)
    }))
    .unwrap_or_else(|_| {
        if let Some(handle) = handle.as_mut() {
            handle.set_error("Internal panic in parser");
        }
        OtlpStatus::Internal
    })
}

/// Export available batches as an ArrowArrayStream.
///
/// # Safety
///
/// - `handle` must be a valid parser handle
/// - `out_stream` must be a valid pointer to FFI_ArrowArrayStream
/// - Caller must call `out_stream->release()` when done
/// - Stream is valid until next `push()` or `destroy()` on handle
///
/// # Returns
///
/// `OTLP_OK` on success, error code otherwise.
/// Stream may yield 0 batches if no data was parsed.
#[no_mangle]
pub unsafe extern "C" fn otlp_parser_drain(
    handle: *mut OtlpParserHandle,
    out_stream: *mut FFI_ArrowArrayStream,
) -> OtlpStatus {
    if handle.is_null() || out_stream.is_null() {
        return OtlpStatus::InvalidArgument;
    }

    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let handle = &mut *handle;
        handle.clear_error();

        // Take batches from handle
        let batches = std::mem::take(&mut handle.batches);
        let schema = handle.get_schema();

        // Create a RecordBatchReader from the batches
        let reader =
            arrow::record_batch::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        // Export to FFI stream
        let stream = arrow::ffi_stream::FFI_ArrowArrayStream::new(Box::new(reader));
        std::ptr::write(out_stream, stream);
        OtlpStatus::Ok
    }))
    .unwrap_or_else(|_| {
        if let Some(handle) = handle.as_mut() {
            handle.set_error("Internal panic during drain");
        }
        OtlpStatus::Internal
    })
}

// ============================================================================
// FFI Functions - Schema Access
// ============================================================================

/// Get the Arrow schema for a signal type.
///
/// # Safety
///
/// - `out_schema` must be a valid pointer to FFI_ArrowSchema
/// - Caller must call `out_schema->release()` when done
///
/// # Returns
///
/// `OTLP_OK` on success, error code otherwise.
#[no_mangle]
pub unsafe extern "C" fn otlp_get_schema(
    signal_type: OtlpSignalType,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    if out_schema.is_null() {
        return OtlpStatus::InvalidArgument;
    }

    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let schema = match signal_type {
            OtlpSignalType::Logs => logs_schema(),
            OtlpSignalType::Traces => traces_schema(),
            OtlpSignalType::MetricsGauge => gauge_schema(),
            OtlpSignalType::MetricsSum => sum_schema(),
            OtlpSignalType::MetricsHistogram => histogram_schema(),
            OtlpSignalType::MetricsExpHistogram => exp_histogram_schema(),
        };

        match FFI_ArrowSchema::try_from(&schema) {
            Ok(ffi_schema) => {
                std::ptr::write(out_schema, ffi_schema);
                OtlpStatus::Ok
            }
            Err(_) => OtlpStatus::Internal,
        }
    }))
    .unwrap_or(OtlpStatus::Internal)
}

// ============================================================================
// FFI Functions - One-Shot API
// ============================================================================

/// Transform OTLP bytes to Arrow in one call (non-streaming).
///
/// # Safety
///
/// - `data` must be valid for `len` bytes
/// - `out_array` and `out_schema` must be valid pointers
/// - Caller must call `release()` on both out_array and out_schema
///
/// # Returns
///
/// `OTLP_OK` on success, error code otherwise.
#[no_mangle]
pub unsafe extern "C" fn otlp_transform(
    signal_type: OtlpSignalType,
    format: OtlpInputFormat,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    if data.is_null() || out_array.is_null() || out_schema.is_null() {
        return OtlpStatus::InvalidArgument;
    }

    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let slice = std::slice::from_raw_parts(data, len);
        let format: InputFormat = format.into();

        let batch_result = match signal_type {
            OtlpSignalType::Logs => transform_logs(slice, format).map(Some),
            OtlpSignalType::Traces => transform_traces(slice, format).map(Some),
            OtlpSignalType::MetricsGauge => transform_metrics(slice, format).map(|m| m.gauge),
            OtlpSignalType::MetricsSum => transform_metrics(slice, format).map(|m| m.sum),
            OtlpSignalType::MetricsHistogram => {
                transform_metrics(slice, format).map(|m| m.histogram)
            }
            OtlpSignalType::MetricsExpHistogram => {
                transform_metrics(slice, format).map(|m| m.exp_histogram)
            }
        };

        match batch_result {
            Ok(Some(batch)) => {
                // Get schema
                let schema = batch.schema();

                // Export schema
                let ffi_schema = match FFI_ArrowSchema::try_from(schema.as_ref()) {
                    Ok(s) => s,
                    Err(_) => return OtlpStatus::Internal,
                };

                // Convert RecordBatch to StructArray for FFI export
                let struct_array: arrow::array::StructArray = batch.into();
                let array_data = struct_array.into_data();

                // Export array
                let ffi_array = FFI_ArrowArray::new(&array_data);

                std::ptr::write(out_schema, ffi_schema);
                std::ptr::write(out_array, ffi_array);

                OtlpStatus::Ok
            }
            Ok(None) => {
                // No data of this type - create empty batch
                let schema = match signal_type {
                    OtlpSignalType::Logs => logs_schema(),
                    OtlpSignalType::Traces => traces_schema(),
                    OtlpSignalType::MetricsGauge => gauge_schema(),
                    OtlpSignalType::MetricsSum => sum_schema(),
                    OtlpSignalType::MetricsHistogram => histogram_schema(),
                    OtlpSignalType::MetricsExpHistogram => exp_histogram_schema(),
                };

                let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
                let ffi_schema = match FFI_ArrowSchema::try_from(&schema) {
                    Ok(s) => s,
                    Err(_) => return OtlpStatus::Internal,
                };

                let struct_array: arrow::array::StructArray = empty_batch.into();
                let array_data = struct_array.into_data();
                let ffi_array = FFI_ArrowArray::new(&array_data);

                std::ptr::write(out_schema, ffi_schema);
                std::ptr::write(out_array, ffi_array);

                OtlpStatus::Ok
            }
            Err(_) => OtlpStatus::ParseFailed,
        }
    }))
    .unwrap_or(OtlpStatus::Internal)
}

// ============================================================================
// FFI Functions - Error Handling
// ============================================================================

/// Get the last error message for a parser handle.
///
/// # Safety
///
/// - `handle` may be null (returns null in that case)
/// - Returned string is valid until next FFI call on same handle
///
/// # Returns
///
/// Error message string, or null if no error.
#[no_mangle]
pub unsafe extern "C" fn otlp_parser_last_error(handle: *const OtlpParserHandle) -> *const c_char {
    if handle.is_null() {
        return ptr::null();
    }

    match &(*handle).last_error {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    }
}

/// Get a static message for a status code.
///
/// # Returns
///
/// Static string describing the status (never null).
#[no_mangle]
pub extern "C" fn otlp_status_message(status: OtlpStatus) -> *const c_char {
    // Static strings with null terminators
    static OK: &[u8] = b"Success\0";
    static INVALID_ARG: &[u8] = b"Invalid argument\0";
    static PARSE_FAILED: &[u8] = b"Parse failed\0";
    static INVALID_FORMAT: &[u8] = b"Invalid format\0";
    static OOM: &[u8] = b"Out of memory\0";
    static INTERNAL: &[u8] = b"Internal error\0";

    let msg = match status {
        OtlpStatus::Ok => OK,
        OtlpStatus::InvalidArgument => INVALID_ARG,
        OtlpStatus::ParseFailed => PARSE_FAILED,
        OtlpStatus::InvalidFormat => INVALID_FORMAT,
        OtlpStatus::OutOfMemory => OOM,
        OtlpStatus::Internal => INTERNAL,
    };

    msg.as_ptr() as *const c_char
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use opentelemetry_proto::tonic::{
        collector::logs::v1::ExportLogsServiceRequest,
        common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use prost::Message;

    fn create_test_log_bytes() -> Vec<u8> {
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
        request.encode_to_vec()
    }

    #[test]
    fn test_parser_create_destroy() {
        unsafe {
            let mut handle: *mut OtlpParserHandle = ptr::null_mut();
            let status =
                otlp_parser_create(OtlpSignalType::Logs, OtlpInputFormat::Protobuf, &mut handle);
            assert_eq!(status, OtlpStatus::Ok);
            assert!(!handle.is_null());

            otlp_parser_destroy(handle);
        }
    }

    #[test]
    fn test_parser_create_null_handle() {
        unsafe {
            let status = otlp_parser_create(
                OtlpSignalType::Logs,
                OtlpInputFormat::Protobuf,
                ptr::null_mut(),
            );
            assert_eq!(status, OtlpStatus::InvalidArgument);
        }
    }

    #[test]
    fn test_parser_destroy_null() {
        unsafe {
            // Should not panic
            otlp_parser_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn test_parser_push_and_drain() {
        unsafe {
            let mut handle: *mut OtlpParserHandle = ptr::null_mut();
            let status =
                otlp_parser_create(OtlpSignalType::Logs, OtlpInputFormat::Protobuf, &mut handle);
            assert_eq!(status, OtlpStatus::Ok);

            let bytes = create_test_log_bytes();
            let status = otlp_parser_push(handle, bytes.as_ptr(), bytes.len(), 1);
            assert_eq!(status, OtlpStatus::Ok);

            let mut stream = std::mem::MaybeUninit::<FFI_ArrowArrayStream>::uninit();
            let status = otlp_parser_drain(handle, stream.as_mut_ptr());
            assert_eq!(status, OtlpStatus::Ok);

            let stream = stream.assume_init();

            // Read batches from stream
            let reader = ArrowArrayStreamReader::try_new(stream).unwrap();
            let batches: Vec<_> = reader.collect();
            assert_eq!(batches.len(), 1);
            assert!(batches[0].is_ok());
            assert_eq!(batches[0].as_ref().unwrap().num_rows(), 1);

            otlp_parser_destroy(handle);
        }
    }

    #[test]
    fn test_get_schema() {
        unsafe {
            let mut ffi_schema = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
            let status = otlp_get_schema(OtlpSignalType::Logs, ffi_schema.as_mut_ptr());
            assert_eq!(status, OtlpStatus::Ok);

            let ffi_schema = ffi_schema.assume_init();

            // Convert back to Rust Schema to verify
            let schema =
                arrow::datatypes::Schema::try_from(&ffi_schema).expect("Failed to convert schema");

            // Verify schema has columns (logs schema has 14+ columns)
            assert!(!schema.fields().is_empty());
            assert!(schema.field_with_name("timestamp").is_ok());
            assert!(schema.field_with_name("service_name").is_ok());

            // Schema is automatically released when dropped via try_from
        }
    }

    #[test]
    fn test_one_shot_transform() {
        unsafe {
            let bytes = create_test_log_bytes();

            let mut ffi_array = std::mem::MaybeUninit::<FFI_ArrowArray>::uninit();
            let mut ffi_schema = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();

            let status = otlp_transform(
                OtlpSignalType::Logs,
                OtlpInputFormat::Protobuf,
                bytes.as_ptr(),
                bytes.len(),
                ffi_array.as_mut_ptr(),
                ffi_schema.as_mut_ptr(),
            );
            assert_eq!(status, OtlpStatus::Ok);

            let ffi_array = ffi_array.assume_init();
            let ffi_schema = ffi_schema.assume_init();

            // Import back to Rust types to verify
            let array_data =
                arrow::ffi::from_ffi(ffi_array, &ffi_schema).expect("Failed to import array");

            // Verify we got data (should be a struct array with 1 row)
            assert_eq!(array_data.len(), 1);
        }
    }

    #[test]
    fn test_status_message() {
        let msg = otlp_status_message(OtlpStatus::Ok);
        assert!(!msg.is_null());

        unsafe {
            let c_str = std::ffi::CStr::from_ptr(msg);
            assert_eq!(c_str.to_str().unwrap(), "Success");
        }
    }

    #[test]
    fn test_parser_push_null_handle() {
        unsafe {
            let status = otlp_parser_push(ptr::null_mut(), ptr::null(), 0, 1);
            assert_eq!(status, OtlpStatus::InvalidArgument);
        }
    }

    #[test]
    fn test_parser_push_null_data_with_len() {
        unsafe {
            let mut handle: *mut OtlpParserHandle = ptr::null_mut();
            otlp_parser_create(OtlpSignalType::Logs, OtlpInputFormat::Auto, &mut handle);

            // Null data with len > 0 should fail
            let status = otlp_parser_push(handle, ptr::null(), 10, 1);
            assert_eq!(status, OtlpStatus::InvalidArgument);

            otlp_parser_destroy(handle);
        }
    }

    #[test]
    fn test_parser_error_message() {
        unsafe {
            let mut handle: *mut OtlpParserHandle = ptr::null_mut();
            otlp_parser_create(OtlpSignalType::Logs, OtlpInputFormat::Protobuf, &mut handle);

            // Push invalid data
            let invalid = b"not valid protobuf";
            let status = otlp_parser_push(handle, invalid.as_ptr(), invalid.len(), 1);
            assert_eq!(status, OtlpStatus::ParseFailed);

            // Should have error message
            let err = otlp_parser_last_error(handle);
            assert!(!err.is_null());

            otlp_parser_destroy(handle);
        }
    }
}
