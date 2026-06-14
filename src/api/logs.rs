//! Public log transform orchestration.

use arrow_array::RecordBatch;

use crate::{
    api::{
        auto::{auto_dispatch, decode_json_observed},
        batch_to_json_values, LogsOutput, SchemaOutput,
    },
    batch::{self, TransformObserver, TransformSignal},
    decode::{decode_logs_json_request, decode_logs_jsonl_request, InputFormat},
    Result,
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

/// Transform OTLP logs using an explicit schema output.
pub fn transform_logs_with_schema(
    bytes: &[u8],
    format: InputFormat,
    schema_output: SchemaOutput,
) -> Result<LogsOutput> {
    match schema_output {
        SchemaOutput::Normalized => transform_logs(bytes, format).map(LogsOutput::Normalized),
        SchemaOutput::OtapStar => transform_logs_otap(bytes, format).map(LogsOutput::OtapStar),
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

/// Transform OTLP logs with both explicit schema selection and observer
/// instrumentation. Routes through the normalized or OTAP star pipeline based
/// on `schema_output`; both paths emit phase timings and counters via
/// `observer`. OTAP currently surfaces `OutputRows` plus the resource/scope
/// dedup hit/miss counters (driven by the in-request fingerprint map).
pub fn transform_logs_with_schema_and_observer(
    bytes: &[u8],
    format: InputFormat,
    schema_output: SchemaOutput,
    observer: &mut dyn TransformObserver,
) -> Result<LogsOutput> {
    let mut observer = Some(observer);
    match schema_output {
        SchemaOutput::Normalized => {
            transform_logs_observed(bytes, format, &mut observer).map(LogsOutput::Normalized)
        }
        SchemaOutput::OtapStar => {
            transform_logs_otap_observed(bytes, format, &mut observer).map(LogsOutput::OtapStar)
        }
    }
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

fn transform_logs_otap(bytes: &[u8], format: InputFormat) -> Result<crate::api::OtapLogsBatches> {
    match format {
        InputFormat::Protobuf => batch::transform_logs_protobuf_otap(bytes),
        InputFormat::Auto => transform_logs_otap_auto(bytes),
        InputFormat::Json => batch::transform_logs_request_otap(decode_logs_json_request(bytes)?),
        InputFormat::Jsonl => batch::transform_logs_request_otap(decode_logs_jsonl_request(bytes)?),
    }
}

fn transform_logs_otap_auto(bytes: &[u8]) -> Result<crate::api::OtapLogsBatches> {
    auto_dispatch(
        bytes,
        &mut (),
        |b, _| transform_logs_otap(b, InputFormat::Json),
        |b, _| transform_logs_otap(b, InputFormat::Jsonl),
        |b, _| batch::transform_logs_protobuf_otap(b),
    )
}

fn transform_logs_otap_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<crate::api::OtapLogsBatches> {
    match format {
        InputFormat::Protobuf => batch::transform_logs_protobuf_otap_observed(bytes, observer),
        InputFormat::Auto => transform_logs_otap_auto_observed(bytes, observer),
        InputFormat::Json => transform_logs_otap_json_observed(bytes, InputFormat::Json, observer),
        InputFormat::Jsonl => {
            transform_logs_otap_json_observed(bytes, InputFormat::Jsonl, observer)
        }
    }
}

fn transform_logs_otap_auto_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<crate::api::OtapLogsBatches> {
    auto_dispatch(
        bytes,
        observer,
        |b, obs| transform_logs_otap_json_observed(b, InputFormat::Json, obs),
        |b, obs| transform_logs_otap_json_observed(b, InputFormat::Jsonl, obs),
        |b, obs| batch::transform_logs_protobuf_otap_observed(b, obs),
    )
}

fn transform_logs_otap_json_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<crate::api::OtapLogsBatches> {
    let request = decode_json_observed(
        bytes,
        format,
        TransformSignal::Logs,
        observer,
        decode_logs_json_request,
        decode_logs_jsonl_request,
        "logs",
    )?;
    batch::transform_logs_request_otap_observed(request, observer)
}

fn transform_logs_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
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
    batch::transform_logs_request_observed(request, bytes.len(), observer)
}

fn transform_logs_auto(bytes: &[u8]) -> Result<RecordBatch> {
    auto_dispatch(
        bytes,
        &mut (),
        |b, _| transform_logs_json_arrow(b, InputFormat::Json),
        |b, _| transform_logs_json_arrow(b, InputFormat::Jsonl),
        |b, _| batch::transform_logs_protobuf(b),
    )
}

fn transform_logs_auto_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    auto_dispatch(
        bytes,
        observer,
        |b, obs| transform_logs_json_arrow_observed(b, InputFormat::Json, obs),
        |b, obs| transform_logs_json_arrow_observed(b, InputFormat::Jsonl, obs),
        |b, obs| batch::transform_logs_protobuf_observed(b, obs),
    )
}

/// Transform OTLP logs to JSON values.
pub fn transform_logs_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_logs(bytes, format)?)
}
