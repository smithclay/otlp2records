//! Public trace transform orchestration.

use arrow_array::RecordBatch;

use crate::{
    api::{
        auto::{auto_dispatch, decode_json_observed},
        batch_to_json_values,
    },
    batch::{self, TransformObserver, TransformSignal},
    decode::{decode_traces_json_request, decode_traces_jsonl_request, InputFormat},
    options::Plan,
    schema::traces_schema_arc,
    Result,
};

/// Transform OTLP traces to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP trace data to Arrow format.
/// It handles decoding, transformation, and Arrow conversion in one step.
/// [`crate::Transformer`] adds options and an observer.
pub fn transform_traces(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    transform_traces_observed(bytes, format, &mut None, &Plan::all(&traces_schema_arc()))
}

pub(super) fn transform_traces_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    plan: &Plan,
) -> Result<RecordBatch> {
    match format {
        InputFormat::Protobuf => batch::transform_traces_protobuf_observed(bytes, observer, plan),
        InputFormat::Auto => auto_dispatch(
            bytes,
            observer,
            |b, obs| transform_traces_json_arrow_observed(b, InputFormat::Json, obs, plan),
            |b, obs| transform_traces_json_arrow_observed(b, InputFormat::Jsonl, obs, plan),
            |b, obs| batch::transform_traces_protobuf_observed(b, obs, plan),
        ),
        InputFormat::Json | InputFormat::Jsonl => {
            transform_traces_json_arrow_observed(bytes, format, observer, plan)
        }
    }
}

fn transform_traces_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    plan: &Plan,
) -> Result<RecordBatch> {
    let request = decode_json_observed(
        bytes,
        format,
        TransformSignal::Traces,
        observer,
        decode_traces_json_request,
        decode_traces_jsonl_request,
        "traces",
    )?;
    batch::transform_traces_request_observed(request, bytes.len(), observer, plan)
}

/// Transform OTLP traces to JSON values.
pub fn transform_traces_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_traces(bytes, format)?)
}
