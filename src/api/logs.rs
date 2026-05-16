//! Public log transform orchestration.

use arrow_array::RecordBatch;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use crate::{
    api::batch_to_json_values,
    batch,
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

#[doc(hidden)]
pub fn transform_logs_decoded_for_bench(
    request: ExportLogsServiceRequest,
    input_bytes: usize,
) -> Result<RecordBatch> {
    batch::transform_logs_request(request, input_bytes)
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
    batch::transform_logs_request(request, bytes.len())
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

/// Transform OTLP logs to JSON values.
pub fn transform_logs_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_logs(bytes, format)?)
}
