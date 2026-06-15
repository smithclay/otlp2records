//! Public metric transform orchestration.

use crate::{
    api::{
        auto::{auto_dispatch, decode_json_observed},
        optional_batch_to_json_values, JsonMetricBatches, MetricBatches,
    },
    batch::{self, TransformObserver, TransformSignal},
    decode::{decode_metrics_json_request, decode_metrics_jsonl_request, InputFormat},
    Result,
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

fn transform_metrics_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<MetricBatches> {
    let request = decode_json_observed(
        bytes,
        format,
        TransformSignal::Metrics,
        observer,
        decode_metrics_json_request,
        decode_metrics_jsonl_request,
        "metrics",
    )?;
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
