//! Public trace transform orchestration.

use std::time::Instant;

use arrow_array::RecordBatch;

use crate::{
    api::{auto::auto_dispatch, batch_to_json_values, SchemaOutput, TracesOutput},
    batch::{self, TransformObserver, TransformPhase, TransformSignal},
    decode::{decode_traces_json_request, decode_traces_jsonl_request, DecodeError, InputFormat},
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

/// Transform OTLP traces using an explicit schema output.
pub fn transform_traces_with_schema(
    bytes: &[u8],
    format: InputFormat,
    schema_output: SchemaOutput,
) -> Result<TracesOutput> {
    match schema_output {
        SchemaOutput::Normalized => transform_traces(bytes, format).map(TracesOutput::Normalized),
        SchemaOutput::OtapStar => transform_traces_otap(bytes, format)
            .map(|batches| TracesOutput::OtapStar(Box::new(batches))),
    }
}

/// Transform OTLP traces while reporting phase timings to an observer.
pub fn transform_traces_with_observer(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut dyn TransformObserver,
) -> Result<RecordBatch> {
    let mut observer = Some(observer);
    transform_traces_observed(bytes, format, &mut observer)
}

/// Transform OTLP traces with both explicit schema selection and observer
/// instrumentation. Routes through the normalized or OTAP star pipeline based
/// on `schema_output`; both paths emit phase timings and counters via
/// `observer`. OTAP currently surfaces `OutputRows` plus the resource/scope
/// dedup hit/miss counters.
pub fn transform_traces_with_schema_and_observer(
    bytes: &[u8],
    format: InputFormat,
    schema_output: SchemaOutput,
    observer: &mut dyn TransformObserver,
) -> Result<TracesOutput> {
    let mut observer = Some(observer);
    match schema_output {
        SchemaOutput::Normalized => {
            transform_traces_observed(bytes, format, &mut observer).map(TracesOutput::Normalized)
        }
        SchemaOutput::OtapStar => transform_traces_otap_observed(bytes, format, &mut observer)
            .map(|batches| TracesOutput::OtapStar(Box::new(batches))),
    }
}

fn transform_traces_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    match format {
        InputFormat::Protobuf => batch::transform_traces_protobuf_observed(bytes, observer),
        InputFormat::Auto => transform_traces_auto_observed(bytes, observer),
        InputFormat::Json | InputFormat::Jsonl => {
            transform_traces_json_arrow_observed(bytes, format, observer)
        }
    }
}

fn transform_traces_json_arrow(bytes: &[u8], format: InputFormat) -> Result<RecordBatch> {
    let mut observer = None;
    transform_traces_json_arrow_observed(bytes, format, &mut observer)
}

fn transform_traces_otap(
    bytes: &[u8],
    format: InputFormat,
) -> Result<crate::api::OtapTracesBatches> {
    match format {
        InputFormat::Protobuf => batch::transform_traces_protobuf_otap(bytes),
        InputFormat::Auto => transform_traces_otap_auto(bytes),
        InputFormat::Json => {
            batch::transform_traces_request_otap(decode_traces_json_request(bytes)?)
        }
        InputFormat::Jsonl => {
            batch::transform_traces_request_otap(decode_traces_jsonl_request(bytes)?)
        }
    }
}

fn transform_traces_otap_auto(bytes: &[u8]) -> Result<crate::api::OtapTracesBatches> {
    auto_dispatch(
        bytes,
        &mut (),
        |b, _| transform_traces_otap(b, InputFormat::Json),
        |b, _| transform_traces_otap(b, InputFormat::Jsonl),
        |b, _| batch::transform_traces_protobuf_otap(b),
    )
}

fn transform_traces_otap_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<crate::api::OtapTracesBatches> {
    match format {
        InputFormat::Protobuf => batch::transform_traces_protobuf_otap_observed(bytes, observer),
        InputFormat::Auto => transform_traces_otap_auto_observed(bytes, observer),
        InputFormat::Json => {
            transform_traces_otap_json_observed(bytes, InputFormat::Json, observer)
        }
        InputFormat::Jsonl => {
            transform_traces_otap_json_observed(bytes, InputFormat::Jsonl, observer)
        }
    }
}

fn transform_traces_otap_auto_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<crate::api::OtapTracesBatches> {
    auto_dispatch(
        bytes,
        observer,
        |b, obs| transform_traces_otap_json_observed(b, InputFormat::Json, obs),
        |b, obs| transform_traces_otap_json_observed(b, InputFormat::Jsonl, obs),
        |b, obs| batch::transform_traces_protobuf_otap_observed(b, obs),
    )
}

fn transform_traces_otap_json_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<crate::api::OtapTracesBatches> {
    let start = Instant::now();
    let request = match format {
        InputFormat::Json => {
            let request = decode_traces_json_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Traces,
                TransformPhase::JsonDecode,
                start.elapsed(),
            );
            request
        }
        InputFormat::Jsonl => {
            let request = decode_traces_jsonl_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Traces,
                TransformPhase::JsonlDecode,
                start.elapsed(),
            );
            request
        }
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL traces input".to_string(),
            )));
        }
    };
    batch::transform_traces_request_otap_observed(request, observer)
}

fn transform_traces_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    let start = Instant::now();
    let request = match format {
        InputFormat::Json => {
            let request = decode_traces_json_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Traces,
                TransformPhase::JsonDecode,
                start.elapsed(),
            );
            request
        }
        InputFormat::Jsonl => {
            let request = decode_traces_jsonl_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Traces,
                TransformPhase::JsonlDecode,
                start.elapsed(),
            );
            request
        }
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL traces input".to_string(),
            )));
        }
    };
    batch::transform_traces_request_observed(request, bytes.len(), observer)
}

fn transform_traces_auto(bytes: &[u8]) -> Result<RecordBatch> {
    auto_dispatch(
        bytes,
        &mut (),
        |b, _| transform_traces_json_arrow(b, InputFormat::Json),
        |b, _| transform_traces_json_arrow(b, InputFormat::Jsonl),
        |b, _| batch::transform_traces_protobuf(b),
    )
}

fn transform_traces_auto_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<RecordBatch> {
    auto_dispatch(
        bytes,
        observer,
        |b, obs| transform_traces_json_arrow_observed(b, InputFormat::Json, obs),
        |b, obs| transform_traces_json_arrow_observed(b, InputFormat::Jsonl, obs),
        |b, obs| batch::transform_traces_protobuf_observed(b, obs),
    )
}

/// Transform OTLP traces to JSON values.
pub fn transform_traces_json(bytes: &[u8], format: InputFormat) -> Result<Vec<serde_json::Value>> {
    batch_to_json_values(&transform_traces(bytes, format)?)
}
