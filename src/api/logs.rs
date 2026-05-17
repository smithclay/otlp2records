//! Public log transform orchestration.

use std::time::Instant;

use arrow_array::RecordBatch;
#[cfg(feature = "bench-internals")]
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use crate::{
    api::batch_to_json_values,
    batch::{self, TransformObserver, TransformPhase, TransformSignal},
    decode::{
        decode_logs_json_request, decode_logs_jsonl_request, looks_like_json, DecodeError,
        InputFormat,
    },
    Error, Result,
};

/// Transform OTLP logs to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP log data to Arrow format.
/// It handles decoding, transformation, and Arrow conversion in one step.
pub fn transform_logs(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    match format {
        InputFormat::Protobuf => batch::transform_logs_protobuf(bytes),
        InputFormat::Auto => transform_logs_auto(bytes),
        InputFormat::Json | InputFormat::Jsonl => transform_logs_json_arrow(bytes, format),
    }
}

/// Transform OTLP logs while reporting phase timings to an observer.
pub fn transform_logs_with_observer(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut dyn TransformObserver,
) -> Result<RecordBatch> {
    let mut observer = Some(observer);
    transform_logs_observed(bytes, format, &mut observer)
}

#[doc(hidden)]
#[cfg(feature = "bench-internals")]
pub fn transform_logs_decoded_for_bench(
    request: ExportLogsServiceRequest,
    input_bytes: usize,
) -> Result<RecordBatch> {
    batch::transform_logs_request(request, input_bytes)
}

fn transform_logs_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    match format {
        InputFormat::Protobuf => batch::transform_logs_protobuf_observed(bytes, observer),
        InputFormat::Auto => transform_logs_auto_observed(bytes, observer),
        InputFormat::Json | InputFormat::Jsonl => {
            transform_logs_json_arrow_observed(bytes, format, observer)
        }
    }
}

fn transform_logs_json_arrow(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    let mut observer = None;
    transform_logs_json_arrow_observed(bytes, format, &mut observer)
}

fn transform_logs_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    let start = Instant::now();
    let request = match format {
        InputFormat::Json => {
            let request = decode_logs_json_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Logs,
                TransformPhase::JsonDecode,
                start.elapsed(),
            );
            request
        }
        InputFormat::Jsonl => {
            let request = decode_logs_jsonl_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Logs,
                TransformPhase::JsonlDecode,
                start.elapsed(),
            );
            request
        }
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL logs input".to_string(),
            )));
        }
    };
    batch::transform_logs_request_observed(request, bytes.len(), observer)
}

fn transform_logs_auto(bytes: &[u8]) -> Result<RecordBatch> {
    if looks_like_json(bytes) {
        match transform_logs_json_arrow(bytes, InputFormat::Json) {
            Ok(batch) => Ok(batch),
            Err(json_err) => match transform_logs_json_arrow(bytes, InputFormat::Jsonl) {
                Ok(batch) => Ok(batch),
                Err(_) => batch::transform_logs_protobuf(bytes).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match batch::transform_logs_protobuf(bytes) {
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

fn transform_logs_auto_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    if looks_like_json(bytes) {
        match transform_logs_json_arrow_observed(bytes, InputFormat::Json, observer) {
            Ok(batch) => Ok(batch),
            Err(json_err) => {
                match transform_logs_json_arrow_observed(bytes, InputFormat::Jsonl, observer) {
                    Ok(batch) => Ok(batch),
                    Err(_) => batch::transform_logs_protobuf_observed(bytes, observer).map_err(
                        |proto_err| {
                            Error::Decode(DecodeError::Unsupported(format!(
                            "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                        )))
                        },
                    ),
                }
            }
        }
    } else {
        match batch::transform_logs_protobuf_observed(bytes, observer) {
            Ok(batch) => Ok(batch),
            Err(proto_err) => {
                transform_logs_json_arrow_observed(bytes, InputFormat::Json, observer).map_err(
                    |json_err| {
                        Error::Decode(DecodeError::Unsupported(format!(
                            "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                        )))
                    },
                )
            }
        }
    }
}

/// Transform OTLP logs to JSON values.
pub fn transform_logs_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_logs(bytes, format)?)
}
