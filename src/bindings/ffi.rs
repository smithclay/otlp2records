//! C FFI bindings for otlp2records
//!
//! Exposes one-shot OTLP transforms via Arrow C Data Interface.
//!
//! # Safety
//!
//! All functions use `catch_unwind` to prevent Rust panics crossing FFI boundary.
//! Pointer validation is performed before dereferencing.
//!
//! # Memory Ownership
//!
//! - Input data: Caller owns, Rust borrows during function call
//! - ArrowSchema/ArrowArray: Rust allocates, caller must call `release()` callback

use std::ffi::{c_char, c_int};
use std::sync::Arc;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch};

use crate::decode::InputFormat;
use crate::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, traces_schema,
    transform_logs, transform_metrics, transform_traces,
};

// ============================================================================
// C-compatible enums
// ============================================================================

/// OTLP signal types supported by the transformer.
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
    /// Internal error (C: OTLP_ERROR_INTERNAL)
    Internal = 3,
}

/// One optional Arrow batch returned by `otlp_transform_metrics_all`.
///
/// If `present` is 0, `array` and `schema` are not initialized and must not be
/// released by the caller. If `present` is non-zero, the caller owns both and
/// must call their Arrow C Data release callbacks.
#[repr(C)]
pub struct OtlpArrowBatch {
    pub array: FFI_ArrowArray,
    pub schema: FFI_ArrowSchema,
    pub present: c_int,
}

/// Output batches for all normalized metric shapes.
#[repr(C)]
pub struct OtlpMetricsArrowBatches {
    pub gauge: OtlpArrowBatch,
    pub sum: OtlpArrowBatch,
    pub histogram: OtlpArrowBatch,
    pub exp_histogram: OtlpArrowBatch,
    pub skipped_summaries: u64,
    pub skipped_nan_values: u64,
    pub skipped_infinity_values: u64,
    pub skipped_missing_values: u64,
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

unsafe fn export_record_batch(
    batch: RecordBatch,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> Result<(), OtlpStatus> {
    let schema = batch.schema();
    let ffi_schema =
        FFI_ArrowSchema::try_from(schema.as_ref()).map_err(|_| OtlpStatus::Internal)?;

    let struct_array: arrow_array::StructArray = batch.into();
    let array_data = struct_array.into_data();
    let ffi_array = FFI_ArrowArray::new(&array_data);

    // Both FFI structs are fully built above; the only remaining work is two infallible
    // `ptr::write`s. Write the array first, then the schema: the Arrow C Data Interface
    // convention is array-before-schema, and keeping the final write infallible means no
    // fallible call can land between them. The C++ contract is "OK => both out-params written;
    // non-OK => nothing to release" (callers treat any non-OK status as no batch present).
    std::ptr::write(out_array, ffi_array);
    std::ptr::write(out_schema, ffi_schema);

    Ok(())
}

unsafe fn export_optional_metric_batch(
    batch: Option<RecordBatch>,
    out_batch: *mut OtlpArrowBatch,
) -> Result<(), OtlpStatus> {
    (*out_batch).present = 0;
    if let Some(batch) = batch {
        export_record_batch(
            batch,
            std::ptr::addr_of_mut!((*out_batch).array),
            std::ptr::addr_of_mut!((*out_batch).schema),
        )?;
        (*out_batch).present = 1;
    }
    Ok(())
}

/// Release an already-exported batch, restoring the "nothing to release" invariant.
///
/// If `present` is non-zero, the batch's `array`/`schema` were written by
/// `export_optional_metric_batch` and own C Data Interface resources. We swap each field for an
/// `empty()` FFI struct (whose `release` callback is null) and drop the old value, which invokes
/// its release callback — the same way the `arrow` crate frees an exported
/// `FFI_ArrowArray`/`FFI_ArrowSchema` anywhere else. Swapping (rather than reading and leaving
/// stale bytes) means a later drop of the field is a harmless no-op, so there is no risk of a
/// double-release. `present` is reset to 0 so the caller never observes a released batch.
unsafe fn release_exported_metric_batch(out_batch: *mut OtlpArrowBatch) {
    if (*out_batch).present != 0 {
        let old_array = std::ptr::replace(
            std::ptr::addr_of_mut!((*out_batch).array),
            FFI_ArrowArray::empty(),
        );
        let old_schema = std::ptr::replace(
            std::ptr::addr_of_mut!((*out_batch).schema),
            FFI_ArrowSchema::empty(),
        );
        (*out_batch).present = 0;
        drop(old_array);
        drop(old_schema);
    }
}

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

        // otlp_transform is the canonical verb for single-shape signals (Logs/Traces) only.
        // Metric signals have four shape-specific schemas from a single parse; callers must use
        // otlp_transform_metrics_all (which returns all shapes from one parse) instead of having
        // this function parse-and-discard 3 of 4 shapes. Reject metric signals here.
        let batch_result = match signal_type {
            OtlpSignalType::Logs => transform_logs(slice, format).map(Some),
            OtlpSignalType::Traces => transform_traces(slice, format).map(Some),
            OtlpSignalType::MetricsGauge
            | OtlpSignalType::MetricsSum
            | OtlpSignalType::MetricsHistogram
            | OtlpSignalType::MetricsExpHistogram => {
                return OtlpStatus::InvalidArgument;
            }
        };

        match batch_result {
            Ok(Some(batch)) => export_record_batch(batch, out_array, out_schema)
                .map_or_else(|status| status, |_| OtlpStatus::Ok),
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
                export_record_batch(empty_batch, out_array, out_schema)
                    .map_or_else(|status| status, |_| OtlpStatus::Ok)
            }
            Err(_) => OtlpStatus::ParseFailed,
        }
    }))
    .unwrap_or(OtlpStatus::Internal)
}

/// Transform OTLP metric bytes to all metric Arrow batches in one parse.
///
/// # Safety
///
/// - `data` must be valid for `len` bytes
/// - `out_batches` must be a valid pointer to `OtlpMetricsArrowBatches`
/// - Caller must release array/schema for every output whose `present` is non-zero
///
/// # Returns
///
/// `OTLP_OK` on success, error code otherwise.
#[no_mangle]
pub unsafe extern "C" fn otlp_transform_metrics_all(
    format: OtlpInputFormat,
    data: *const u8,
    len: usize,
    out_batches: *mut OtlpMetricsArrowBatches,
) -> OtlpStatus {
    if data.is_null() || out_batches.is_null() {
        return OtlpStatus::InvalidArgument;
    }

    // Make every output safely inspectable even when parsing/export fails before a
    // specific metric shape is written.
    (*out_batches).gauge.present = 0;
    (*out_batches).sum.present = 0;
    (*out_batches).histogram.present = 0;
    (*out_batches).exp_histogram.present = 0;
    (*out_batches).skipped_summaries = 0;
    (*out_batches).skipped_nan_values = 0;
    (*out_batches).skipped_infinity_values = 0;
    (*out_batches).skipped_missing_values = 0;

    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let slice = std::slice::from_raw_parts(data, len);
        let format: InputFormat = format.into();
        let batches = match transform_metrics(slice, format) {
            Ok(batches) => batches,
            Err(_) => return OtlpStatus::ParseFailed,
        };
        let skipped = batches.skipped;

        // Export each shape in order. If any export fails mid-sequence, release every batch
        // already exported (present == 1) so we honor the C contract: "non-OK => no batch is
        // present; nothing to release." Without this, an earlier success (e.g. gauge) would
        // leak its array/schema private_data when a later shape (e.g. sum) fails, because the
        // C++ caller trusts that contract and does not inspect present flags on failure.
        let gauge_ptr = std::ptr::addr_of_mut!((*out_batches).gauge);
        let sum_ptr = std::ptr::addr_of_mut!((*out_batches).sum);
        let histogram_ptr = std::ptr::addr_of_mut!((*out_batches).histogram);
        let exp_histogram_ptr = std::ptr::addr_of_mut!((*out_batches).exp_histogram);

        let export_result = export_optional_metric_batch(batches.gauge, gauge_ptr)
            .and_then(|_| export_optional_metric_batch(batches.sum, sum_ptr))
            .and_then(|_| export_optional_metric_batch(batches.histogram, histogram_ptr))
            .and_then(|_| export_optional_metric_batch(batches.exp_histogram, exp_histogram_ptr));

        if export_result.is_err() {
            release_exported_metric_batch(gauge_ptr);
            release_exported_metric_batch(sum_ptr);
            release_exported_metric_batch(histogram_ptr);
            release_exported_metric_batch(exp_histogram_ptr);
            return OtlpStatus::Internal;
        }

        (*out_batches).skipped_summaries = skipped.summaries as u64;
        (*out_batches).skipped_nan_values = skipped.nan_values as u64;
        (*out_batches).skipped_infinity_values = skipped.infinity_values as u64;
        (*out_batches).skipped_missing_values = skipped.missing_values as u64;

        OtlpStatus::Ok
    }))
    .unwrap_or(OtlpStatus::Internal)
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
    static INTERNAL: &[u8] = b"Internal error\0";

    let msg = match status {
        OtlpStatus::Ok => OK,
        OtlpStatus::InvalidArgument => INVALID_ARG,
        OtlpStatus::ParseFailed => PARSE_FAILED,
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
    use opentelemetry_proto::tonic::{
        collector::{logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest},
        common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            metric, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics,
            ScopeMetrics, Summary, SummaryDataPoint,
        },
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
    fn test_get_schema() {
        unsafe {
            let mut ffi_schema = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
            let status = otlp_get_schema(OtlpSignalType::Logs, ffi_schema.as_mut_ptr());
            assert_eq!(status, OtlpStatus::Ok);

            let ffi_schema = ffi_schema.assume_init();

            // Convert back to Rust Schema to verify
            let schema =
                arrow_schema::Schema::try_from(&ffi_schema).expect("Failed to convert schema");

            // Verify schema has columns (logs schema has 14+ columns)
            assert!(!schema.fields().is_empty());
            assert!(schema.field_with_name("time_unix_nano").is_ok());
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
                arrow_array::ffi::from_ffi(ffi_array, &ffi_schema).expect("Failed to import array");

            // Verify we got data (should be a struct array with 1 row)
            assert_eq!(array_data.len(), 1);
        }
    }

    #[test]
    fn test_metrics_all_reports_skipped_counters() {
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
                    scope: Some(InstrumentationScope {
                        name: "test-lib".to_string(),
                        ..Default::default()
                    }),
                    metrics: vec![
                        Metric {
                            name: "invalid_gauge".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![
                                    NumberDataPoint {
                                        value: Some(number_data_point::Value::AsDouble(f64::NAN)),
                                        ..Default::default()
                                    },
                                    NumberDataPoint {
                                        value: Some(number_data_point::Value::AsDouble(
                                            f64::INFINITY,
                                        )),
                                        ..Default::default()
                                    },
                                    NumberDataPoint {
                                        value: None,
                                        ..Default::default()
                                    },
                                ],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "summary".to_string(),
                            data: Some(metric::Data::Summary(Summary {
                                data_points: vec![
                                    SummaryDataPoint {
                                        time_unix_nano: 1,
                                        ..Default::default()
                                    },
                                    SummaryDataPoint {
                                        time_unix_nano: 2,
                                        ..Default::default()
                                    },
                                ],
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let bytes = request.encode_to_vec();

        unsafe {
            let mut batches = std::mem::zeroed::<OtlpMetricsArrowBatches>();
            let status = otlp_transform_metrics_all(
                OtlpInputFormat::Protobuf,
                bytes.as_ptr(),
                bytes.len(),
                &mut batches,
            );
            assert_eq!(status, OtlpStatus::Ok);
            assert_eq!(batches.gauge.present, 0);
            assert_eq!(batches.sum.present, 0);
            assert_eq!(batches.histogram.present, 0);
            assert_eq!(batches.exp_histogram.present, 0);
            assert_eq!(batches.skipped_summaries, 2);
            assert_eq!(batches.skipped_nan_values, 1);
            assert_eq!(batches.skipped_infinity_values, 1);
            assert_eq!(batches.skipped_missing_values, 1);
        }
    }

    fn create_test_gauge_bytes() -> Vec<u8> {
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
                    scope: Some(InstrumentationScope {
                        name: "test-lib".to_string(),
                        ..Default::default()
                    }),
                    metrics: vec![Metric {
                        name: "valid_gauge".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(42.0)),
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
        request.encode_to_vec()
    }

    /// C1 regression: `release_exported_metric_batch` must invoke the C Data Interface
    /// release callbacks of an exported batch and clear `present`, restoring the
    /// "nothing to release" invariant. This is the path taken when a later metric shape's
    /// export fails after an earlier shape was already exported.
    #[test]
    fn test_release_exported_metric_batch_releases_and_clears() {
        let bytes = create_test_gauge_bytes();

        unsafe {
            let mut batches = std::mem::zeroed::<OtlpMetricsArrowBatches>();
            let status = otlp_transform_metrics_all(
                OtlpInputFormat::Protobuf,
                bytes.as_ptr(),
                bytes.len(),
                &mut batches,
            );
            assert_eq!(status, OtlpStatus::Ok);
            // Valid gauge data must produce a present gauge batch with live FFI resources.
            assert_eq!(batches.gauge.present, 1);

            // Releasing the present batch invokes the release callbacks (drop) and clears present.
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.gauge));
            assert_eq!(batches.gauge.present, 0);

            // Idempotent: releasing an already-released (or never-present) batch is a no-op and
            // must not double-free. The other shapes were absent for this payload.
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.gauge));
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.sum));
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.histogram));
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.exp_histogram));
            assert_eq!(batches.gauge.present, 0);
            assert_eq!(batches.sum.present, 0);
            assert_eq!(batches.histogram.present, 0);
            assert_eq!(batches.exp_histogram.present, 0);
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
}
