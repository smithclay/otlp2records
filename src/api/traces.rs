//! Public trace transform orchestration.

use arrow_array::RecordBatch;

use crate::{
    api::{
        auto::{auto_dispatch, decode_json_observed},
        batch_to_json_values,
    },
    batch::{self, TransformObserver, TransformSignal},
    decode::{decode_traces_json_request, decode_traces_jsonl_request, InputFormat},
    options::{self, TransformOptions},
    Result,
};

/// Transform OTLP traces to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP trace data to Arrow format.
/// It handles decoding, transformation, and Arrow conversion in one step.
pub fn transform_traces(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    transform_traces_observed(bytes, format, &mut None, &options::ALL)
}

/// Transform OTLP traces, building only the columns and rows `options` asks
/// for. See [`TransformOptions`].
pub fn transform_traces_with_options(
    bytes: &[u8],
    format: InputFormat,
    options: &TransformOptions,
) -> Result<RecordBatch> {
    // Reject bad options before format detection can mistake the error for a
    // decode failure and retry another format.
    options.plan(&crate::traces_schema())?;
    transform_traces_observed(bytes, format, &mut None, options)
}

/// Transform OTLP traces while reporting phase timings to an observer.
pub fn transform_traces_with_observer(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut dyn TransformObserver,
) -> Result<RecordBatch> {
    let mut observer = Some(observer);
    transform_traces_observed(bytes, format, &mut observer, &options::ALL)
}

fn transform_traces_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    options: &TransformOptions,
) -> Result<RecordBatch> {
    match format {
        InputFormat::Protobuf => {
            batch::transform_traces_protobuf_observed(bytes, observer, options)
        }
        InputFormat::Auto => auto_dispatch(
            bytes,
            observer,
            |b, obs| transform_traces_json_arrow_observed(b, InputFormat::Json, obs, options),
            |b, obs| transform_traces_json_arrow_observed(b, InputFormat::Jsonl, obs, options),
            |b, obs| batch::transform_traces_protobuf_observed(b, obs, options),
        ),
        InputFormat::Json | InputFormat::Jsonl => {
            transform_traces_json_arrow_observed(bytes, format, observer, options)
        }
    }
}

fn transform_traces_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    options: &TransformOptions,
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
    batch::transform_traces_request_observed(request, bytes.len(), observer, options)
}

/// Transform OTLP traces to JSON values.
pub fn transform_traces_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_traces(bytes, format)?)
}
