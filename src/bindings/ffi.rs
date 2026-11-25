//! C ABI for the one-shot transforms and the stateful OTAP decoder.

use std::ffi::{c_char, c_void};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

use crate::decode::InputFormat;
use crate::ffi_export::{ffi_boundary, ExportError, ExportedBatch, ExportedMetrics};
pub use crate::ffi_export::{OtlpArrowBatch, OtlpMetricsArrowBatches};
use crate::{
    schema::{logs_schema_arc, traces_schema_arc},
    transform_logs, transform_metrics, transform_traces, ArrowProfile, Error, MetricShape,
    TransformOptions, Transformer,
};

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

impl OtlpSignalType {
    fn schema(self) -> Schema {
        self.schema_ref().as_ref().clone()
    }

    fn schema_ref(self) -> SchemaRef {
        match self {
            OtlpSignalType::Logs => logs_schema_arc(),
            OtlpSignalType::Traces => traces_schema_arc(),
            OtlpSignalType::MetricsGauge => MetricShape::Gauge.schema_ref(),
            OtlpSignalType::MetricsSum => MetricShape::Sum.schema_ref(),
            OtlpSignalType::MetricsHistogram => MetricShape::Histogram.schema_ref(),
            OtlpSignalType::MetricsExpHistogram => MetricShape::ExponentialHistogram.schema_ref(),
        }
    }
}

/// An inclusive nanosecond range on one timestamp column; see
/// [`TransformOptions::with_time_range`].
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OtlpTimeRange {
    pub column: u32,
    pub min: i64,
    pub max: i64,
}

/// Decides whether a resource's rows are kept, from its emitted
/// `service_name` (`len` UTF-8 bytes, not NUL-terminated). Called once per
/// resource, on the calling thread, during the transform.
pub type OtlpServiceFilter =
    unsafe extern "C" fn(user_data: *mut c_void, service_name: *const u8, len: usize) -> bool;

/// What one `*_with_options` call builds; the C form of
/// [`TransformOptions`]. Zero-initialized, it builds every column of every row.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OtlpReadOptions {
    /// The columns to build and return, in this order, by index in the
    /// signal's schema. NULL: every column in schema order. A non-NULL pointer
    /// with length 0 returns no columns, only the row count.
    pub columns: *const u32,
    pub columns_len: usize,
    /// Keep only the rows of resources this accepts. NULL: every resource.
    pub keep_service: Option<OtlpServiceFilter>,
    pub keep_service_data: *mut c_void,
    /// Keep only rows within every range. NULL (or length 0): no ranges.
    pub time_ranges: *const OtlpTimeRange,
    pub time_ranges_len: usize,
}

/// The host's service filter, callable from the transform.
struct ServiceCallback {
    filter: OtlpServiceFilter,
    user_data: *mut c_void,
}

impl ServiceCallback {
    fn keeps(&self, name: &str) -> bool {
        // SAFETY: the host promised a callback that is safe to call during the
        // `*_with_options` call it was passed to.
        unsafe { (self.filter)(self.user_data, name.as_ptr(), name.len()) }
    }
}

// SAFETY: the callback only runs inside the `*_with_options` call that was
// given it, on the calling thread; the options never outlive that call.
unsafe impl Send for ServiceCallback {}
unsafe impl Sync for ServiceCallback {}

/// Reads `options` (NULL means every column of every row).
unsafe fn read_options(options: *const OtlpReadOptions) -> TransformOptions {
    let mut result = TransformOptions::new();
    let Some(options) = options.as_ref() else {
        return result;
    };
    if !options.columns.is_null() {
        let columns = std::slice::from_raw_parts(options.columns, options.columns_len);
        result = result.with_columns(columns.iter().map(|&column| column as usize));
    }
    if let Some(filter) = options.keep_service {
        let callback = ServiceCallback {
            filter,
            user_data: options.keep_service_data,
        };
        result = result.with_service_filter(move |name| callback.keeps(name));
    }
    if !options.time_ranges.is_null() {
        let ranges = std::slice::from_raw_parts(options.time_ranges, options.time_ranges_len);
        for range in ranges {
            result = result.with_time_range(range.column as usize, range.min, range.max);
        }
    }
    result
}

/// The status of a failed `*_with_options` call.
fn options_status(error: &Error) -> OtlpStatus {
    match error {
        Error::InvalidOptions(_) => OtlpStatus::InvalidArgument,
        _ => OtlpStatus::ParseFailed,
    }
}

/// The metric shape a signal type names, if it names one.
fn metric_shape(signal: OtlpSignalType) -> Option<MetricShape> {
    match signal {
        OtlpSignalType::Logs | OtlpSignalType::Traces => None,
        OtlpSignalType::MetricsGauge => Some(MetricShape::Gauge),
        OtlpSignalType::MetricsSum => Some(MetricShape::Sum),
        OtlpSignalType::MetricsHistogram => Some(MetricShape::Histogram),
        OtlpSignalType::MetricsExpHistogram => Some(MetricShape::ExponentialHistogram),
    }
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

impl From<ExportError> for OtlpStatus {
    fn from(error: ExportError) -> Self {
        match error {
            ExportError::Unrepresentable => OtlpStatus::ParseFailed,
            ExportError::SchemaNotExportable => OtlpStatus::Internal,
        }
    }
}

unsafe fn export_batch(
    profile: ArrowProfile,
    batch: RecordBatch,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    match ExportedBatch::new(profile, batch) {
        Ok(exported) => {
            exported.write_to(out_array, out_schema);
            OtlpStatus::Ok
        }
        Err(error) => error.into(),
    }
}

/// Get the Arrow schema for a signal type.
///
/// # Safety
///
/// - `out_schema` must be a valid pointer to FFI_ArrowSchema
/// - Caller must call `out_schema->release()` when done
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
    ffi_boundary(OtlpStatus::Internal, || {
        if out_schema.is_null() {
            return OtlpStatus::InvalidArgument;
        }
        let schema = ArrowProfile::from(profile).schema(&signal_type.schema());
        match FFI_ArrowSchema::try_from(&schema) {
            Ok(ffi_schema) => {
                std::ptr::write(out_schema, ffi_schema);
                OtlpStatus::Ok
            }
            Err(_) => OtlpStatus::Internal,
        }
    })
}

/// Transform OTLP bytes to Arrow in one call (non-streaming).
///
/// # Safety
///
/// - `data` must be valid for `len` bytes
/// - `out_array` and `out_schema` must be valid pointers
/// - Caller must call `release()` on both out_array and out_schema
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
/// Only single-shape signals (logs, traces) are accepted; metric signals return
/// `OTLP_ERROR_INVALID_ARGUMENT` because `otlp_transform_metrics_all` returns
/// every shape from one parse. Under `OTLP_ARROW_PROFILE_DUCKDB`, a value the
/// profile cannot represent returns `OTLP_ERROR_PARSE_FAILED`.
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
    ffi_boundary(OtlpStatus::Internal, || {
        if data.is_null() || out_array.is_null() || out_schema.is_null() {
            return OtlpStatus::InvalidArgument;
        }
        let input = std::slice::from_raw_parts(data, len);
        let format = InputFormat::from(format);
        let transformed = match signal_type {
            OtlpSignalType::Logs => transform_logs(input, format),
            OtlpSignalType::Traces => transform_traces(input, format),
            OtlpSignalType::MetricsGauge
            | OtlpSignalType::MetricsSum
            | OtlpSignalType::MetricsHistogram
            | OtlpSignalType::MetricsExpHistogram => return OtlpStatus::InvalidArgument,
        };
        let Ok(batch) = transformed else {
            return OtlpStatus::ParseFailed;
        };
        export_batch(profile.into(), batch, out_array, out_schema)
    })
}

/// Transforms one signal (for metrics, one shape), building and returning
/// only the columns and rows `options` asks for. A metric shape the input
/// does not contain yields an empty batch.
///
/// # Safety
///
/// `data` must point to `len` readable bytes, `options` must be NULL or point
/// to a valid [`OtlpReadOptions`] whose arrays are readable for their lengths
/// and whose callback is safe to call during this call, and
/// `out_array`/`out_schema` must be writable. On `OTLP_OK` the caller owns
/// both outputs.
#[no_mangle]
pub unsafe extern "C" fn otlp_transform_with_options(
    signal_type: OtlpSignalType,
    format: OtlpInputFormat,
    profile: OtlpArrowProfile,
    options: *const OtlpReadOptions,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    ffi_boundary(OtlpStatus::Internal, || {
        if data.is_null() || out_array.is_null() || out_schema.is_null() {
            return OtlpStatus::InvalidArgument;
        }
        let input = std::slice::from_raw_parts(data, len);
        let mut transformer =
            Transformer::new(InputFormat::from(format)).options(read_options(options));
        let transformed = match metric_shape(signal_type) {
            Some(shape) => transformer
                .metric_shape(shape, input)
                .map(|shape| shape.batch),
            None if signal_type == OtlpSignalType::Logs => transformer.logs(input),
            None => transformer.traces(input),
        };
        match transformed {
            Ok(batch) => export_batch(profile.into(), batch, out_array, out_schema),
            Err(error) => options_status(&error),
        }
    })
}

/// Transform OTLP metric bytes to all metric Arrow batches in one parse.
///
/// # Safety
///
/// - `data` must be valid for `len` bytes
/// - `out_batches` must be a valid pointer to `OtlpMetricsArrowBatches`
/// - Caller must release array/schema for every output whose `present` is non-zero
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
    if out_batches.is_null() {
        return OtlpStatus::InvalidArgument;
    }
    std::ptr::write(out_batches, OtlpMetricsArrowBatches::absent());
    ffi_boundary(OtlpStatus::Internal, || {
        if data.is_null() {
            return OtlpStatus::InvalidArgument;
        }
        let input = std::slice::from_raw_parts(data, len);
        let Ok(batches) = transform_metrics(input, format.into()) else {
            return OtlpStatus::ParseFailed;
        };
        write_metrics(profile.into(), batches, out_batches)
    })
}

unsafe fn write_metrics(
    profile: ArrowProfile,
    batches: crate::MetricBatches,
    out_batches: *mut OtlpMetricsArrowBatches,
) -> OtlpStatus {
    match ExportedMetrics::new(profile, batches) {
        Ok(exported) => {
            exported.write_to(out_batches);
            OtlpStatus::Ok
        }
        Err(error) => error.into(),
    }
}

/// Opaque, heap-owned handle wrapping a stateful OTAP decoder.
///
/// Created by `otlp_otap_decoder_new`, destroyed by `otlp_otap_decoder_free`.
/// NOT thread-safe and NOT re-entrant: keep one decoder per OTAP stream, driven
/// by a single thread at a time. Any non-OK decode poisons it: every later
/// decode returns `OTLP_ERROR_PARSE_FAILED`.
pub struct OtlpOtapDecoder {
    inner: crate::otap::OtapDecoder,
    profile: ArrowProfile,
}

/// Create a new stateful OTAP decoder.
///
/// Returns a non-null handle owned by the caller, who must release it with
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
#[no_mangle]
pub extern "C" fn otlp_otap_decoder_new_with_profile(
    profile: OtlpArrowProfile,
) -> *mut OtlpOtapDecoder {
    ffi_boundary(std::ptr::null_mut(), || {
        Box::into_raw(Box::new(OtlpOtapDecoder {
            inner: crate::otap::OtapDecoder::new(),
            profile: profile.into(),
        }))
    })
}

/// Free an OTAP decoder created by `otlp_otap_decoder_new`.
///
/// # Safety
///
/// `decoder` must be a pointer returned by `otlp_otap_decoder_new` that has not
/// already been freed, or null. Passing null is a safe no-op.
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decoder_free(decoder: *mut OtlpOtapDecoder) {
    ffi_boundary((), || {
        if !decoder.is_null() {
            drop(Box::from_raw(decoder));
        }
    })
}

type DecodeOne = fn(&mut crate::otap::OtapDecoder, &[u8]) -> crate::Result<RecordBatch>;

unsafe fn otap_decode_batch(
    decoder: *mut OtlpOtapDecoder,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
    decode: DecodeOne,
) -> OtlpStatus {
    if decoder.is_null() || data.is_null() || out_array.is_null() || out_schema.is_null() {
        return OtlpStatus::InvalidArgument;
    }
    let decoder = &mut *decoder;
    let Ok(batch) = decode(&mut decoder.inner, std::slice::from_raw_parts(data, len)) else {
        return OtlpStatus::ParseFailed;
    };
    export_batch(decoder.profile, batch, out_array, out_schema)
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
/// - On any non-OK status nothing is written
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decode_logs(
    decoder: *mut OtlpOtapDecoder,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    ffi_boundary(OtlpStatus::Internal, || {
        otap_decode_batch(
            decoder,
            data,
            len,
            out_array,
            out_schema,
            crate::otap::OtapDecoder::decode_logs,
        )
    })
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
    ffi_boundary(OtlpStatus::Internal, || {
        otap_decode_batch(
            decoder,
            data,
            len,
            out_array,
            out_schema,
            crate::otap::OtapDecoder::decode_traces,
        )
    })
}

/// Decode one canonical OTAP `BatchArrowRecords` message into the normalized
/// metric batches, reusing schema/dictionary state established by earlier
/// messages on the same decoder.
///
/// # Safety
///
/// - `decoder` must come from `otlp_otap_decoder_new` and not be freed
/// - `data` must be valid for `len` bytes
/// - `out_batches` must be a valid pointer to `OtlpMetricsArrowBatches`; it is
///   fully overwritten, so it need not be initialized
/// - For each output with `present != 0`, caller must release array and schema;
///   outputs with `present == 0` must not be released
/// - On any non-OK status no batch is present
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decode_metrics(
    decoder: *mut OtlpOtapDecoder,
    data: *const u8,
    len: usize,
    out_batches: *mut OtlpMetricsArrowBatches,
) -> OtlpStatus {
    if out_batches.is_null() {
        return OtlpStatus::InvalidArgument;
    }
    std::ptr::write(out_batches, OtlpMetricsArrowBatches::absent());
    ffi_boundary(OtlpStatus::Internal, || {
        if decoder.is_null() || data.is_null() {
            return OtlpStatus::InvalidArgument;
        }
        let decoder = &mut *decoder;
        let Ok(batches) = decoder
            .inner
            .decode_metrics(std::slice::from_raw_parts(data, len))
        else {
            return OtlpStatus::ParseFailed;
        };
        write_metrics(decoder.profile, batches, out_batches)
    })
}

/// Decodes one OTAP message of `signal_type` (for metrics, one shape),
/// building only the columns and rows `options` asks for. The whole message
/// is decoded either way, so the decoder's stream state stays complete.
///
/// # Safety
///
/// As [`otlp_transform_with_options`], and `decoder` must come from
/// `otlp_otap_decoder_new*`.
#[no_mangle]
pub unsafe extern "C" fn otlp_otap_decode_with_options(
    decoder: *mut OtlpOtapDecoder,
    signal_type: OtlpSignalType,
    options: *const OtlpReadOptions,
    data: *const u8,
    len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> OtlpStatus {
    ffi_boundary(OtlpStatus::Internal, || {
        if decoder.is_null() || data.is_null() || out_array.is_null() || out_schema.is_null() {
            return OtlpStatus::InvalidArgument;
        }
        let options = read_options(options);
        let decoder = &mut *decoder;
        let input = std::slice::from_raw_parts(data, len);
        let decoded = match metric_shape(signal_type) {
            Some(shape) => decoder
                .inner
                .decode_metric_shape(input, shape, &options)
                .map(|shape| shape.batch),
            None if signal_type == OtlpSignalType::Logs => {
                decoder.inner.decode_logs_with_options(input, &options)
            }
            None => decoder.inner.decode_traces_with_options(input, &options),
        };
        match decoded {
            Ok(batch) => export_batch(decoder.profile, batch, out_array, out_schema),
            Err(error) => options_status(&error),
        }
    })
}

/// Get a static, never-null, NUL-terminated message for a status code.
#[no_mangle]
pub extern "C" fn otlp_status_message(status: OtlpStatus) -> *const c_char {
    ffi_boundary(c"Internal error".as_ptr(), || {
        match status {
            OtlpStatus::Ok => c"Success",
            OtlpStatus::InvalidArgument => c"Invalid argument",
            OtlpStatus::ParseFailed => c"Parse failed",
            OtlpStatus::Internal => c"Internal error",
        }
        .as_ptr()
    })
}

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
    use std::ffi::c_int;

    const OTAP_LOGS_INITIAL: &[u8] = include_bytes!("../../tests/fixtures/otap/logs-initial.bar");
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
            assert_ne!(
                batches.gauge.present
                    | batches.sum.present
                    | batches.histogram.present
                    | batches.exp_histogram.present,
                0
            );

            present_batches(&mut batches);
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
}
