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
    transform_logs, transform_metrics, transform_traces, ArrowProfile,
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

/// Arrow type profile for FFI output. See [`ArrowProfile`].
///
/// C names: OTLP_ARROW_PROFILE_CANONICAL, OTLP_ARROW_PROFILE_DUCKDB.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpArrowProfile {
    /// Canonical schema, identical to the profile-less entry points
    /// (C: OTLP_ARROW_PROFILE_CANONICAL)
    Canonical = 0,
    /// Types DuckDB's stock Arrow importer maps to the duckdb-otlp column types
    /// (C: OTLP_ARROW_PROFILE_DUCKDB)
    DuckDb = 1,
}

impl From<OtlpArrowProfile> for ArrowProfile {
    fn from(p: OtlpArrowProfile) -> Self {
        match p {
            OtlpArrowProfile::Canonical => ArrowProfile::Canonical,
            OtlpArrowProfile::DuckDb => ArrowProfile::DuckDb,
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
    otlp_get_schema_with_profile(signal_type, OtlpArrowProfile::Canonical, out_schema)
}

/// Get the Arrow schema for a signal type under an output profile.
///
/// `OTLP_ARROW_PROFILE_CANONICAL` is identical to `otlp_get_schema`.
///
/// # Safety
///
/// Same contract as `otlp_get_schema`.
#[no_mangle]
pub unsafe extern "C" fn otlp_get_schema_with_profile(
    signal_type: OtlpSignalType,
    profile: OtlpArrowProfile,
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
        let schema = ArrowProfile::from(profile).schema(&schema);

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

/// Export the four normalized metric shapes into `out_batches` and copy the
/// skipped-metric counters.
///
/// Honors the C1 rollback invariant: if any shape's export fails mid-sequence,
/// every already-exported batch is released so a non-OK return leaves nothing
/// `present` to release. Callers must zero-initialize `out_batches` and invoke
/// this inside `catch_unwind`. Shared by the one-shot (`transform_metrics`) and
/// streaming OTAP (`decode_metrics`) paths so the invariant has one home.
///
/// # Safety
///
/// `out_batches` must be a valid, writable pointer to a zero-initialized
/// `OtlpMetricsArrowBatches`.
unsafe fn export_metric_batches(
    batches: crate::MetricBatches,
    profile: ArrowProfile,
    out_batches: *mut OtlpMetricsArrowBatches,
) -> OtlpStatus {
    // Re-type before anything is exported, so a profile error leaves nothing present.
    let batches = match profile.apply_metrics(batches) {
        Ok(batches) => batches,
        Err(_) => return OtlpStatus::ParseFailed,
    };
    let skipped = batches.skipped;

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
    otlp_transform_with_profile(
        signal_type,
        format,
        OtlpArrowProfile::Canonical,
        data,
        len,
        out_array,
        out_schema,
    )
}

/// Transform OTLP bytes to Arrow in one call, under an output profile.
///
/// `OTLP_ARROW_PROFILE_CANONICAL` is identical to `otlp_transform`. Under
/// `OTLP_ARROW_PROFILE_DUCKDB`, a value the profile cannot represent (an
/// unsigned integer above the signed maximum) returns `OTLP_ERROR_PARSE_FAILED`.
///
/// # Safety
///
/// Same contract as `otlp_transform`.
#[no_mangle]
pub unsafe extern "C" fn otlp_transform_with_profile(
    signal_type: OtlpSignalType,
    format: OtlpInputFormat,
    profile: OtlpArrowProfile,
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

        let profile = ArrowProfile::from(profile);
        let batch_result =
            batch_result.and_then(|batch| batch.map(|b| profile.apply(b)).transpose());
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

                let empty_batch = RecordBatch::new_empty(Arc::new(profile.schema(&schema)));
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
    otlp_transform_metrics_all_with_profile(
        format,
        OtlpArrowProfile::Canonical,
        data,
        len,
        out_batches,
    )
}

/// Transform OTLP metric bytes to all metric Arrow batches in one parse, under
/// an output profile.
///
/// `OTLP_ARROW_PROFILE_CANONICAL` is identical to `otlp_transform_metrics_all`.
/// Under `OTLP_ARROW_PROFILE_DUCKDB`, a value the profile cannot represent
/// returns `OTLP_ERROR_PARSE_FAILED` with no batch present.
///
/// # Safety
///
/// Same contract as `otlp_transform_metrics_all`.
#[no_mangle]
pub unsafe extern "C" fn otlp_transform_metrics_all_with_profile(
    format: OtlpInputFormat,
    profile: OtlpArrowProfile,
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
        export_metric_batches(batches, profile.into(), out_batches)
    }))
    .unwrap_or(OtlpStatus::Internal)
}

// ============================================================================
// FFI Functions - Native OTAP Streaming API
// ============================================================================

/// Opaque, heap-owned handle wrapping a stateful OTAP decoder.
///
/// Created by `otlp_otap_decoder_new`, destroyed by `otlp_otap_decoder_free`.
/// NOT thread-safe and NOT re-entrant: keep one decoder per OTAP stream, driven
/// by a single thread at a time (mirrors `OtapDecoder`'s `&mut self` methods).
///
/// On a non-OK decode the decoder's internal stream state may be partially
/// advanced. The safe contract is that the stream is then no longer
/// trustworthy: free the decoder and start a new one rather than feeding it
/// more messages. Do not attempt mid-stream recovery.
pub struct OtlpOtapDecoder {
    inner: crate::otap::OtapDecoder,
    profile: ArrowProfile,
}

/// Create a new stateful OTAP decoder.
///
/// # Returns
///
/// A non-null handle owned by the caller, who must release it with
/// `otlp_otap_decoder_free`.
#[no_mangle]
pub extern "C" fn otlp_otap_decoder_new() -> *mut OtlpOtapDecoder {
    otlp_otap_decoder_new_with_profile(OtlpArrowProfile::Canonical)
}

/// Create a new stateful OTAP decoder whose output uses `profile`.
///
/// Every batch the decoder returns (logs, traces, and each metric shape) is
/// re-typed for `profile`. `OTLP_ARROW_PROFILE_CANONICAL` is identical to
/// `otlp_otap_decoder_new`. Under `OTLP_ARROW_PROFILE_DUCKDB`, a value the
/// profile cannot represent fails the decode with `OTLP_ERROR_PARSE_FAILED`.
///
/// # Returns
///
/// A non-null handle owned by the caller, who must release it with
/// `otlp_otap_decoder_free`.
#[no_mangle]
pub extern "C" fn otlp_otap_decoder_new_with_profile(
    profile: OtlpArrowProfile,
) -> *mut OtlpOtapDecoder {
    Box::into_raw(Box::new(OtlpOtapDecoder {
        inner: crate::otap::OtapDecoder::new(),
        profile: profile.into(),
    }))
}

/// Free an OTAP decoder created by `otlp_otap_decoder_new`.
///
/// # Safety
///
/// `decoder` must be a pointer returned by `otlp_otap_decoder_new` that has not
/// already been freed, or null. Passing null is a safe no-op.
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decoder_free(decoder: *mut OtlpOtapDecoder) {
    if !decoder.is_null() {
        drop(Box::from_raw(decoder));
    }
}

/// Shared body for the single-batch OTAP decode entry points (logs and traces),
/// which each return exactly one `RecordBatch` on success.
///
/// # Safety
///
/// Same contract as the public entry points: `decoder` from
/// `otlp_otap_decoder_new`; `data` valid for `len` bytes; `out_array`/
/// `out_schema` valid and writable. On `OTLP_OK` the caller owns and must
/// release both out-params; on non-OK nothing is written.
unsafe fn otap_decode_batch(
    decoder: *mut OtlpOtapDecoder,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    decode: fn(&mut crate::otap::OtapDecoder, &[u8]) -> crate::Result<RecordBatch>,
) -> OtlpStatus {
    if decoder.is_null() || data.is_null() || out_array.is_null() || out_schema.is_null() {
        return OtlpStatus::InvalidArgument;
    }

    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let slice = std::slice::from_raw_parts(data, len);
        let profile = (*decoder).profile;
        match decode(&mut (*decoder).inner, slice).and_then(|batch| profile.apply(batch)) {
            Ok(batch) => export_record_batch(batch, out_array, out_schema)
                .map_or_else(|status| status, |_| OtlpStatus::Ok),
            Err(_) => OtlpStatus::ParseFailed,
        }
    }))
    .unwrap_or(OtlpStatus::Internal)
}

/// Decode one canonical OTAP `BatchArrowRecords` message into a normalized logs
/// batch, reusing schema/dictionary state established by earlier messages on the
/// same decoder.
///
/// # Safety
///
/// - `decoder` must come from `otlp_otap_decoder_new` and not be freed
/// - `data` must be valid for `len` bytes
/// - `out_array` and `out_schema` must be valid, writable pointers
/// - On `OTLP_OK` the caller owns and must release both out_array and out_schema
/// - On any non-OK status nothing is written and the stream should be discarded
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decode_logs(
    decoder: *mut OtlpOtapDecoder,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    otap_decode_batch(
        decoder,
        data,
        len,
        out_array,
        out_schema,
        crate::otap::OtapDecoder::decode_logs,
    )
}

/// Decode one canonical OTAP `BatchArrowRecords` message into a normalized
/// traces batch, reusing schema/dictionary state established by earlier messages
/// on the same decoder.
///
/// # Safety
///
/// Identical contract to `otlp_otap_decode_logs`.
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decode_traces(
    decoder: *mut OtlpOtapDecoder,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    otap_decode_batch(
        decoder,
        data,
        len,
        out_array,
        out_schema,
        crate::otap::OtapDecoder::decode_traces,
    )
}

/// Decode one canonical OTAP `BatchArrowRecords` message into the normalized
/// metric batches, reusing schema/dictionary state established by earlier
/// messages on the same decoder.
///
/// # Safety
///
/// - `decoder` must come from `otlp_otap_decoder_new` and not be freed
/// - `data` must be valid for `len` bytes
/// - `out_batches` must be a valid pointer to `OtlpMetricsArrowBatches`
/// - Caller should zero-initialize `out_batches` before calling
/// - For each output with `present != 0`, caller must release array and schema;
///   outputs with `present == 0` must not be released
/// - On any non-OK status no batch is present and the stream should be discarded
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decode_metrics(
    decoder: *mut OtlpOtapDecoder,
    data: *const u8,
    len: usize,
    out_batches: *mut OtlpMetricsArrowBatches,
) -> OtlpStatus {
    if decoder.is_null() || data.is_null() || out_batches.is_null() {
        return OtlpStatus::InvalidArgument;
    }

    // Make every output safely inspectable even when decoding/export fails
    // before a specific metric shape is written.
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
        let batches = match (*decoder).inner.decode_metrics(slice) {
            Ok(batches) => batches,
            Err(_) => return OtlpStatus::ParseFailed,
        };
        export_metric_batches(batches, (*decoder).profile, out_batches)
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

    const OTAP_LOGS_INITIAL: &[u8] = include_bytes!("../../tests/fixtures/otap/logs-initial.bar");
    const OTAP_LOGS_REUSE: &[u8] = include_bytes!("../../tests/fixtures/otap/logs-reuse.bar");
    const OTAP_TRACES_INITIAL: &[u8] =
        include_bytes!("../../tests/fixtures/otap/traces-initial.bar");
    const OTAP_METRICS_INITIAL: &[u8] =
        include_bytes!("../../tests/fixtures/otap/metrics-initial.bar");
    #[cfg(feature = "otap-zstd")]
    const OTAP_LOGS_ZSTD: &[u8] = include_bytes!("../../tests/fixtures/otap/logs-zstd.bar");

    /// Decode one single-batch OTAP signal through the FFI, import the exported
    /// Arrow array back into Rust (which also releases the FFI resources on
    /// drop), and return its row count. Asserts `OTLP_OK`.
    unsafe fn ffi_decode_rows(
        decoder: *mut OtlpOtapDecoder,
        bytes: &[u8],
        decode: unsafe extern "C" fn(
            *mut OtlpOtapDecoder,
            *const u8,
            usize,
            *mut FFI_ArrowArray,
            *mut FFI_ArrowSchema,
        ) -> OtlpStatus,
    ) -> usize {
        let mut ffi_array = std::mem::MaybeUninit::<FFI_ArrowArray>::uninit();
        let mut ffi_schema = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
        let status = decode(
            decoder,
            bytes.as_ptr(),
            bytes.len(),
            ffi_array.as_mut_ptr(),
            ffi_schema.as_mut_ptr(),
        );
        assert_eq!(status, OtlpStatus::Ok);
        let array = arrow_array::ffi::from_ffi(ffi_array.assume_init(), &ffi_schema.assume_init())
            .expect("import exported array");
        array.len()
    }

    fn create_test_log_bytes() -> Vec<u8> {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        key_strindex: 0,
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
                        key_strindex: 0,
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
                        key_strindex: 0,
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
    fn otap_decode_logs_and_traces_happy_path() {
        unsafe {
            let decoder = otlp_otap_decoder_new();
            assert!(!decoder.is_null());
            assert!(ffi_decode_rows(decoder, OTAP_LOGS_INITIAL, otlp_otap_decode_logs) > 0);
            otlp_otap_decoder_free(decoder);

            let decoder = otlp_otap_decoder_new();
            assert!(ffi_decode_rows(decoder, OTAP_TRACES_INITIAL, otlp_otap_decode_traces) > 0);
            otlp_otap_decoder_free(decoder);
        }
    }

    #[test]
    fn otap_decode_metrics_reports_present_and_skipped() {
        unsafe {
            // Ground truth from the safe decoder on the same bytes; the FFI path
            // must agree on which shapes are present and the skipped counters.
            let expected = crate::otap::OtapDecoder::new()
                .decode_metrics(OTAP_METRICS_INITIAL)
                .expect("safe decode of metrics fixture");

            let decoder = otlp_otap_decoder_new();
            let mut batches = std::mem::zeroed::<OtlpMetricsArrowBatches>();
            let status = otlp_otap_decode_metrics(
                decoder,
                OTAP_METRICS_INITIAL.as_ptr(),
                OTAP_METRICS_INITIAL.len(),
                &mut batches,
            );
            assert_eq!(status, OtlpStatus::Ok);
            assert_eq!(batches.gauge.present, expected.gauge.is_some() as c_int);
            assert_eq!(batches.sum.present, expected.sum.is_some() as c_int);
            assert_eq!(
                batches.histogram.present,
                expected.histogram.is_some() as c_int
            );
            assert_eq!(
                batches.exp_histogram.present,
                expected.exp_histogram.is_some() as c_int
            );
            assert_eq!(batches.skipped_summaries, expected.skipped.summaries as u64);
            assert_eq!(
                batches.skipped_nan_values,
                expected.skipped.nan_values as u64
            );
            assert_eq!(
                batches.skipped_infinity_values,
                expected.skipped.infinity_values as u64
            );
            assert_eq!(
                batches.skipped_missing_values,
                expected.skipped.missing_values as u64
            );
            // The canonical fixture decodes at least one metric shape.
            assert_ne!(
                batches.gauge.present
                    | batches.sum.present
                    | batches.histogram.present
                    | batches.exp_histogram.present,
                0
            );

            // Honor the ownership contract: release every present batch.
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.gauge));
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.sum));
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.histogram));
            release_exported_metric_batch(std::ptr::addr_of_mut!(batches.exp_histogram));
            otlp_otap_decoder_free(decoder);
        }
    }

    #[test]
    fn otap_dictionary_reuse_persists_across_ffi_calls() {
        unsafe {
            let decoder = otlp_otap_decoder_new();
            assert!(ffi_decode_rows(decoder, OTAP_LOGS_INITIAL, otlp_otap_decode_logs) > 0);
            // The reuse message omits the Arrow schema/dictionary; it only decodes
            // because the decoder retained state from the initial message.
            assert!(ffi_decode_rows(decoder, OTAP_LOGS_REUSE, otlp_otap_decode_logs) > 0);
            otlp_otap_decoder_free(decoder);
        }
    }

    #[test]
    fn otap_reuse_on_fresh_decoder_fails() {
        unsafe {
            // The documented "one decoder per stream" guarantee: a reuse message
            // has no schema/dictionary, so a fresh decoder cannot decode it.
            let decoder = otlp_otap_decoder_new();
            let mut ffi_array = std::mem::MaybeUninit::<FFI_ArrowArray>::uninit();
            let mut ffi_schema = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
            let status = otlp_otap_decode_logs(
                decoder,
                OTAP_LOGS_REUSE.as_ptr(),
                OTAP_LOGS_REUSE.len(),
                ffi_array.as_mut_ptr(),
                ffi_schema.as_mut_ptr(),
            );
            assert_eq!(status, OtlpStatus::ParseFailed);
            otlp_otap_decoder_free(decoder);
        }
    }

    #[test]
    fn otap_null_args_rejected() {
        unsafe {
            let decoder = otlp_otap_decoder_new();
            let mut arr = std::mem::MaybeUninit::<FFI_ArrowArray>::uninit();
            let mut sch = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
            let (ptr, len) = (OTAP_LOGS_INITIAL.as_ptr(), OTAP_LOGS_INITIAL.len());

            // Each null argument is rejected before any decoding happens, so the
            // decoder's stream state is never advanced.
            assert_eq!(
                otlp_otap_decode_logs(
                    std::ptr::null_mut(),
                    ptr,
                    len,
                    arr.as_mut_ptr(),
                    sch.as_mut_ptr()
                ),
                OtlpStatus::InvalidArgument
            );
            assert_eq!(
                otlp_otap_decode_logs(
                    decoder,
                    std::ptr::null(),
                    0,
                    arr.as_mut_ptr(),
                    sch.as_mut_ptr()
                ),
                OtlpStatus::InvalidArgument
            );
            assert_eq!(
                otlp_otap_decode_logs(decoder, ptr, len, std::ptr::null_mut(), sch.as_mut_ptr()),
                OtlpStatus::InvalidArgument
            );
            assert_eq!(
                otlp_otap_decode_logs(decoder, ptr, len, arr.as_mut_ptr(), std::ptr::null_mut()),
                OtlpStatus::InvalidArgument
            );
            assert_eq!(
                otlp_otap_decode_metrics(std::ptr::null_mut(), ptr, len, std::ptr::null_mut()),
                OtlpStatus::InvalidArgument
            );
            assert_eq!(
                otlp_otap_decode_metrics(decoder, ptr, len, std::ptr::null_mut()),
                OtlpStatus::InvalidArgument
            );

            otlp_otap_decoder_free(decoder);
            // Freeing null must be a safe no-op.
            otlp_otap_decoder_free(std::ptr::null_mut());
        }
    }

    #[cfg(feature = "otap-zstd")]
    #[test]
    fn otap_decode_zstd_logs() {
        unsafe {
            let decoder = otlp_otap_decoder_new();
            assert!(ffi_decode_rows(decoder, OTAP_LOGS_ZSTD, otlp_otap_decode_logs) > 0);
            otlp_otap_decoder_free(decoder);
        }
    }

    const TRACES_PB: &[u8] = include_bytes!("../../testdata/traces_large.pb");
    const METRICS_MIXED_PB: &[u8] = include_bytes!("../../testdata/metrics_mixed.pb");

    const ALL_SIGNALS: [OtlpSignalType; 6] = [
        OtlpSignalType::Logs,
        OtlpSignalType::Traces,
        OtlpSignalType::MetricsGauge,
        OtlpSignalType::MetricsSum,
        OtlpSignalType::MetricsHistogram,
        OtlpSignalType::MetricsExpHistogram,
    ];

    /// Every Arrow C format string in `schema`'s children, recursively
    /// (list element formats included), as `(name, format)`.
    fn child_formats(schema: &FFI_ArrowSchema) -> Vec<(String, String)> {
        fn walk(schema: &FFI_ArrowSchema, out: &mut Vec<(String, String)>) {
            for child in schema.children() {
                out.push((
                    child.name().unwrap_or_default().to_string(),
                    child.format().to_string(),
                ));
                walk(child, out);
            }
        }
        let mut out = Vec::new();
        walk(schema, &mut out);
        out
    }

    /// No format string DuckDB's stock importer maps differently from the
    /// duckdb-otlp column types: FixedSizeBinary (`w:N`), Duration (`tD*`),
    /// unsigned ints (`C`/`S`/`I`/`L`).
    fn assert_duckdb_formats(schema: &FFI_ArrowSchema) {
        let formats = child_formats(schema);
        assert!(!formats.is_empty());
        for (name, format) in formats {
            assert!(
                !format.starts_with("w:")
                    && !format.starts_with("tD")
                    && !matches!(format.as_str(), "C" | "S" | "I" | "L"),
                "{name}: {format}"
            );
        }
    }

    unsafe fn get_schema(signal: OtlpSignalType, profile: OtlpArrowProfile) -> FFI_ArrowSchema {
        let mut out = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
        assert_eq!(
            otlp_get_schema_with_profile(signal, profile, out.as_mut_ptr()),
            OtlpStatus::Ok
        );
        out.assume_init()
    }

    unsafe fn transform(
        signal: OtlpSignalType,
        profile: Option<OtlpArrowProfile>,
        bytes: &[u8],
    ) -> Result<(arrow_array::ArrayRef, FFI_ArrowSchema), OtlpStatus> {
        let mut arr = std::mem::MaybeUninit::<FFI_ArrowArray>::uninit();
        let mut sch = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
        let (ptr, len) = (bytes.as_ptr(), bytes.len());
        let status = match profile {
            None => otlp_transform(
                signal,
                OtlpInputFormat::Protobuf,
                ptr,
                len,
                arr.as_mut_ptr(),
                sch.as_mut_ptr(),
            ),
            Some(p) => otlp_transform_with_profile(
                signal,
                OtlpInputFormat::Protobuf,
                p,
                ptr,
                len,
                arr.as_mut_ptr(),
                sch.as_mut_ptr(),
            ),
        };
        if status != OtlpStatus::Ok {
            return Err(status);
        }
        let schema = sch.assume_init();
        let data = arrow_array::ffi::from_ffi(arr.assume_init(), &schema).expect("import");
        Ok((arrow_array::make_array(data), schema))
    }

    #[test]
    fn get_schema_canonical_profile_matches_plain_entry_point() {
        unsafe {
            for signal in ALL_SIGNALS {
                let mut plain = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
                assert_eq!(otlp_get_schema(signal, plain.as_mut_ptr()), OtlpStatus::Ok);
                let plain = arrow_schema::Schema::try_from(&plain.assume_init()).unwrap();
                let canonical = arrow_schema::Schema::try_from(&get_schema(
                    signal,
                    OtlpArrowProfile::Canonical,
                ))
                .unwrap();
                assert_eq!(plain, canonical, "{signal:?}");
            }
        }
    }

    #[test]
    fn get_schema_duckdb_profile_has_no_remapped_formats() {
        unsafe {
            for signal in ALL_SIGNALS {
                assert_duckdb_formats(&get_schema(signal, OtlpArrowProfile::DuckDb));
            }
            // The canonical schema still carries them (default unchanged).
            let canonical = child_formats(&get_schema(
                OtlpSignalType::Traces,
                OtlpArrowProfile::Canonical,
            ));
            assert!(canonical.contains(&("trace_id".into(), "w:16".into())));
            assert!(canonical.contains(&("duration_time_unix_nano".into(), "tDn".into())));
            assert!(canonical.contains(&("flags".into(), "I".into())));
        }
    }

    #[test]
    fn transform_canonical_profile_is_identical_to_plain_transform() {
        unsafe {
            let (plain, plain_schema) = transform(OtlpSignalType::Traces, None, TRACES_PB).unwrap();
            let (canonical, canonical_schema) = transform(
                OtlpSignalType::Traces,
                Some(OtlpArrowProfile::Canonical),
                TRACES_PB,
            )
            .unwrap();
            assert_eq!(plain.to_data(), canonical.to_data());
            assert_eq!(
                arrow_schema::Schema::try_from(&plain_schema).unwrap(),
                arrow_schema::Schema::try_from(&canonical_schema).unwrap()
            );
        }
    }

    #[test]
    fn transform_duckdb_profile_matches_its_schema() {
        unsafe {
            for (signal, bytes) in [
                (OtlpSignalType::Traces, TRACES_PB),
                (OtlpSignalType::Logs, &create_test_log_bytes()[..]),
            ] {
                let (array, schema) =
                    transform(signal, Some(OtlpArrowProfile::DuckDb), bytes).unwrap();
                assert!(!array.is_empty());
                assert_duckdb_formats(&schema);
                // Bind-time schema == scan-time schema.
                assert_eq!(
                    arrow_schema::Schema::try_from(&schema).unwrap(),
                    arrow_schema::Schema::try_from(&get_schema(signal, OtlpArrowProfile::DuckDb))
                        .unwrap()
                );
            }
            // An empty request still yields the profile's (empty) schema.
            let (array, schema) =
                transform(OtlpSignalType::Traces, Some(OtlpArrowProfile::DuckDb), &[]).unwrap();
            assert_eq!(array.len(), 0);
            assert_duckdb_formats(&schema);
        }
    }

    #[test]
    fn transform_duckdb_profile_rejects_unrepresentable_unsigned() {
        // dropped_attributes_count is a proto uint32; above i32::MAX it has no
        // INTEGER representation, so the profile fails rather than wrapping.
        let mut request = ExportLogsServiceRequest::decode(&create_test_log_bytes()[..]).unwrap();
        request.resource_logs[0].scope_logs[0].log_records[0].dropped_attributes_count = u32::MAX;
        let bytes = request.encode_to_vec();
        unsafe {
            assert!(transform(OtlpSignalType::Logs, None, &bytes).is_ok());
            assert_eq!(
                transform(OtlpSignalType::Logs, Some(OtlpArrowProfile::DuckDb), &bytes)
                    .unwrap_err(),
                OtlpStatus::ParseFailed
            );
        }
    }

    unsafe fn present_batches(batches: &mut OtlpMetricsArrowBatches) -> Vec<FFI_ArrowSchema> {
        let mut out = Vec::new();
        for batch in [
            &mut batches.gauge,
            &mut batches.sum,
            &mut batches.histogram,
            &mut batches.exp_histogram,
        ] {
            if batch.present != 0 {
                batch.present = 0;
                let array = std::ptr::replace(&mut batch.array, FFI_ArrowArray::empty());
                let schema = std::ptr::replace(&mut batch.schema, FFI_ArrowSchema::empty());
                // Import (and so release) the array; keep the schema to inspect.
                arrow_array::ffi::from_ffi(array, &schema).expect("import metric batch");
                out.push(schema);
            }
        }
        out
    }

    #[test]
    fn metrics_all_duckdb_profile_retypes_every_shape() {
        unsafe {
            let mut plain = std::mem::zeroed::<OtlpMetricsArrowBatches>();
            assert_eq!(
                otlp_transform_metrics_all(
                    OtlpInputFormat::Protobuf,
                    METRICS_MIXED_PB.as_ptr(),
                    METRICS_MIXED_PB.len(),
                    &mut plain,
                ),
                OtlpStatus::Ok
            );
            let mut duck = std::mem::zeroed::<OtlpMetricsArrowBatches>();
            assert_eq!(
                otlp_transform_metrics_all_with_profile(
                    OtlpInputFormat::Protobuf,
                    OtlpArrowProfile::DuckDb,
                    METRICS_MIXED_PB.as_ptr(),
                    METRICS_MIXED_PB.len(),
                    &mut duck,
                ),
                OtlpStatus::Ok
            );
            assert_eq!(plain.skipped_summaries, duck.skipped_summaries);
            let plain = present_batches(&mut plain);
            let duck = present_batches(&mut duck);
            assert_eq!(plain.len(), duck.len());
            assert!(!duck.is_empty());
            for schema in &duck {
                assert_duckdb_formats(schema);
            }
        }
    }

    #[test]
    fn otap_decoder_duckdb_profile_retypes_every_signal() {
        unsafe {
            for (bytes, decode) in [
                (
                    OTAP_LOGS_INITIAL,
                    otlp_otap_decode_logs as unsafe extern "C" fn(_, _, _, _, _) -> _,
                ),
                (OTAP_TRACES_INITIAL, otlp_otap_decode_traces),
            ] {
                let decoder = otlp_otap_decoder_new_with_profile(OtlpArrowProfile::DuckDb);
                let mut arr = std::mem::MaybeUninit::<FFI_ArrowArray>::uninit();
                let mut sch = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
                assert_eq!(
                    decode(
                        decoder,
                        bytes.as_ptr(),
                        bytes.len(),
                        arr.as_mut_ptr(),
                        sch.as_mut_ptr()
                    ),
                    OtlpStatus::Ok
                );
                let schema = sch.assume_init();
                let data = arrow_array::ffi::from_ffi(arr.assume_init(), &schema).unwrap();
                assert!(!data.is_empty());
                assert_duckdb_formats(&schema);
                otlp_otap_decoder_free(decoder);
            }

            let decoder = otlp_otap_decoder_new_with_profile(OtlpArrowProfile::DuckDb);
            let mut batches = std::mem::zeroed::<OtlpMetricsArrowBatches>();
            assert_eq!(
                otlp_otap_decode_metrics(
                    decoder,
                    OTAP_METRICS_INITIAL.as_ptr(),
                    OTAP_METRICS_INITIAL.len(),
                    &mut batches,
                ),
                OtlpStatus::Ok
            );
            let schemas = present_batches(&mut batches);
            assert!(!schemas.is_empty());
            for schema in &schemas {
                assert_duckdb_formats(schema);
            }
            otlp_otap_decoder_free(decoder);

            // The profile-less constructor still yields canonical ids.
            let decoder = otlp_otap_decoder_new();
            let mut arr = std::mem::MaybeUninit::<FFI_ArrowArray>::uninit();
            let mut sch = std::mem::MaybeUninit::<FFI_ArrowSchema>::uninit();
            assert_eq!(
                otlp_otap_decode_traces(
                    decoder,
                    OTAP_TRACES_INITIAL.as_ptr(),
                    OTAP_TRACES_INITIAL.len(),
                    arr.as_mut_ptr(),
                    sch.as_mut_ptr()
                ),
                OtlpStatus::Ok
            );
            let schema = sch.assume_init();
            arrow_array::ffi::from_ffi(arr.assume_init(), &schema).unwrap();
            assert!(child_formats(&schema).contains(&("trace_id".into(), "w:16".into())));
            otlp_otap_decoder_free(decoder);
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
