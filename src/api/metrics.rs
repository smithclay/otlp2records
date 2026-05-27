//! Public metric transform orchestration.

use std::time::Instant;

use crate::{
    api::{
        auto::auto_dispatch, optional_batch_to_json_values, JsonMetricBatches, MetricBatches,
        MetricsOutput, SchemaOutput,
    },
    batch::{self, TransformObserver, TransformPhase, TransformSignal},
    decode::{decode_metrics_json_request, decode_metrics_jsonl_request, DecodeError, InputFormat},
    Error, Result,
};

/// Transform OTLP metrics to Arrow RecordBatches.
///
/// Returns separate batches for gauge and sum metrics because they have
/// different schemas. Each field in the result is `None` if there were
/// no metrics of that type in the input.
pub fn transform_metrics(bytes: &[u8], format: InputFormat) -> Result<MetricBatches> {
    match format {
        InputFormat::Protobuf => batch::transform_metrics_protobuf(bytes),
        InputFormat::Auto => transform_metrics_auto(bytes),
        InputFormat::Json | InputFormat::Jsonl => transform_metrics_json_arrow(bytes, format),
    }
}

/// Transform OTLP metrics using an explicit schema output.
pub fn transform_metrics_with_schema(
    bytes: &[u8],
    format: InputFormat,
    schema_output: SchemaOutput,
) -> Result<MetricsOutput> {
    match schema_output {
        SchemaOutput::Normalized => transform_metrics(bytes, format).map(MetricsOutput::Normalized),
        SchemaOutput::OtapStar => transform_metrics_otap(bytes, format)
            .map(|batches| MetricsOutput::OtapStar(Box::new(batches))),
    }
}

/// Transform OTLP metrics while reporting phase timings to an observer.
pub fn transform_metrics_with_observer(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut dyn TransformObserver,
) -> Result<MetricBatches> {
    let mut observer = Some(observer);
    transform_metrics_observed(bytes, format, &mut observer)
}

fn transform_metrics_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<MetricBatches> {
    match format {
        InputFormat::Protobuf => batch::transform_metrics_protobuf_observed(bytes, observer),
        InputFormat::Auto => transform_metrics_auto_observed(bytes, observer),
        InputFormat::Json | InputFormat::Jsonl => {
            transform_metrics_json_arrow_observed(bytes, format, observer)
        }
    }
}

fn transform_metrics_json_arrow(bytes: &[u8], format: InputFormat) -> Result<MetricBatches> {
    let mut observer = None;
    transform_metrics_json_arrow_observed(bytes, format, &mut observer)
}

fn transform_metrics_otap(
    bytes: &[u8],
    format: InputFormat,
) -> Result<crate::api::OtapMetricsBatches> {
    match format {
        InputFormat::Protobuf => batch::transform_metrics_protobuf_otap(bytes),
        InputFormat::Auto => transform_metrics_otap_auto(bytes),
        InputFormat::Json => {
            batch::transform_metrics_request_otap(decode_metrics_json_request(bytes)?)
        }
        InputFormat::Jsonl => {
            batch::transform_metrics_request_otap(decode_metrics_jsonl_request(bytes)?)
        }
    }
}

fn transform_metrics_otap_auto(bytes: &[u8]) -> Result<crate::api::OtapMetricsBatches> {
    auto_dispatch(
        bytes,
        &mut (),
        |b, _| transform_metrics_otap(b, InputFormat::Json),
        |b, _| transform_metrics_otap(b, InputFormat::Jsonl),
        |b, _| batch::transform_metrics_protobuf_otap(b),
    )
}

fn transform_metrics_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<MetricBatches> {
    let start = Instant::now();
    let request = match format {
        InputFormat::Json => {
            let request = decode_metrics_json_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Metrics,
                TransformPhase::JsonDecode,
                start.elapsed(),
            );
            request
        }
        InputFormat::Jsonl => {
            let request = decode_metrics_jsonl_request(bytes)?;
            batch::observe_phase(
                observer,
                TransformSignal::Metrics,
                TransformPhase::JsonlDecode,
                start.elapsed(),
            );
            request
        }
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL metrics input".to_string(),
            )));
        }
    };
    batch::transform_metrics_request_observed(request, observer)
}

fn transform_metrics_auto(bytes: &[u8]) -> Result<MetricBatches> {
    auto_dispatch(
        bytes,
        &mut (),
        |b, _| transform_metrics_json_arrow(b, InputFormat::Json),
        |b, _| transform_metrics_json_arrow(b, InputFormat::Jsonl),
        |b, _| batch::transform_metrics_protobuf(b),
    )
}

fn transform_metrics_auto_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<MetricBatches> {
    auto_dispatch(
        bytes,
        observer,
        |b, obs| transform_metrics_json_arrow_observed(b, InputFormat::Json, obs),
        |b, obs| transform_metrics_json_arrow_observed(b, InputFormat::Jsonl, obs),
        |b, obs| batch::transform_metrics_protobuf_observed(b, obs),
    )
}

/// Transform OTLP metrics to JSON values.
pub fn transform_metrics_json(bytes: &[u8], format: InputFormat) -> Result<JsonMetricBatches> {
    let batches = transform_metrics(bytes, format)?;

    Ok(JsonMetricBatches {
        gauge: optional_batch_to_json_values(batches.gauge.as_ref())?,
        sum: optional_batch_to_json_values(batches.sum.as_ref())?,
        histogram: optional_batch_to_json_values(batches.histogram.as_ref())?,
        exp_histogram: optional_batch_to_json_values(batches.exp_histogram.as_ref())?,
        skipped: batches.skipped,
    })
}
