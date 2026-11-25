//! Public log transform orchestration.

use arrow_array::RecordBatch;

use crate::{
    api::{
        auto::{auto_dispatch, decode_json_observed},
        batch_to_json_values,
    },
    batch::{self, TransformObserver, TransformSignal},
    decode::{decode_logs_json_request, decode_logs_jsonl_request, InputFormat},
    options::{Plan, TransformOptions},
    schema::logs_schema_arc,
    Result,
};

/// Transform OTLP logs to Arrow RecordBatch.
///
/// This is the simplest way to convert OTLP log data to Arrow format.
/// It handles decoding, transformation, and Arrow conversion in one step.
pub fn transform_logs(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    transform_logs_planned(bytes, format, &Plan::all(&logs_schema_arc()))
}

/// Transform OTLP logs, building only the columns and rows `options` asks
/// for. See [`TransformOptions`].
pub fn transform_logs_with_options(
    bytes: &[u8],
    format: InputFormat,
    options: &TransformOptions,
) -> Result<RecordBatch> {
    transform_logs_planned(bytes, format, &options.plan(&logs_schema_arc())?)
}

/// The transform behind the public entry points, for options already planned
/// against the logs schema. Planning first keeps an options error from
/// being mistaken for a decode failure and retried as another format.
pub(crate) fn transform_logs_planned(
    bytes: &[u8],
    format: InputFormat,
    plan: &Plan,
) -> Result<RecordBatch> {
    transform_logs_observed(bytes, format, &mut None, plan)
}

/// Transform OTLP logs while reporting phase timings to an observer.
pub fn transform_logs_with_observer(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut dyn TransformObserver,
) -> Result<RecordBatch> {
    let mut observer = Some(observer);
    transform_logs_observed(bytes, format, &mut observer, &Plan::all(&logs_schema_arc()))
}

fn transform_logs_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    plan: &Plan,
) -> Result<RecordBatch> {
    match format {
        InputFormat::Protobuf => batch::transform_logs_protobuf_observed(bytes, observer, plan),
        InputFormat::Auto => auto_dispatch(
            bytes,
            observer,
            |b, obs| transform_logs_json_arrow_observed(b, InputFormat::Json, obs, plan),
            |b, obs| transform_logs_json_arrow_observed(b, InputFormat::Jsonl, obs, plan),
            |b, obs| batch::transform_logs_protobuf_observed(b, obs, plan),
        ),
        InputFormat::Json | InputFormat::Jsonl => {
            transform_logs_json_arrow_observed(bytes, format, observer, plan)
        }
    }
}

fn transform_logs_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    plan: &Plan,
) -> Result<RecordBatch> {
    let request = decode_json_observed(
        bytes,
        format,
        TransformSignal::Logs,
        observer,
        decode_logs_json_request,
        decode_logs_jsonl_request,
        "logs",
    )?;
    batch::transform_logs_request_observed(request, bytes.len(), observer, plan)
}

/// Transform OTLP logs to JSON values.
pub fn transform_logs_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_logs(bytes, format)?)
}
