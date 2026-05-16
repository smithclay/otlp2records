//! Public trace transform orchestration.

use arrow_array::RecordBatch;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use crate::{
    api::batch_to_json_values,
    batch,
    decode::{
        decode_traces_json_request, decode_traces_jsonl_request, looks_like_json, DecodeError,
        InputFormat,
    },
    Error, Result,
};

/// Transform OTLP traces to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP trace data to Arrow format.
/// It handles decoding, transformation, and Arrow conversion in one step.
pub fn transform_traces(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    match format {
        InputFormat::Protobuf => batch::transform_traces_protobuf(bytes),
        InputFormat::Auto => transform_traces_auto(bytes),
        InputFormat::Json | InputFormat::Jsonl => transform_traces_json_arrow(bytes, format),
    }
}

#[doc(hidden)]
pub fn transform_traces_decoded_for_bench(
    request: ExportTraceServiceRequest,
    input_bytes: usize,
) -> Result<RecordBatch> {
    batch::transform_traces_request(request, input_bytes)
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
    batch::transform_traces_request(request, bytes.len())
}

fn transform_traces_auto(bytes: &[u8]) -> Result<RecordBatch> {
    if looks_like_json(bytes) {
        match transform_traces_json_arrow(bytes, InputFormat::Json) {
            Ok(batch) => Ok(batch),
            Err(json_err) => match transform_traces_json_arrow(bytes, InputFormat::Jsonl) {
                Ok(batch) => Ok(batch),
                Err(_) => batch::transform_traces_protobuf(bytes).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match batch::transform_traces_protobuf(bytes) {
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
