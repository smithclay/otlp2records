//! Public metric transform orchestration.

use crate::{
    api::{
        auto::{auto_dispatch, decode_json_observed},
        optional_batch_to_json_values, JsonMetricBatches, MetricBatches,
    },
    batch::{self, MetricsSelection, TransformObserver, TransformSignal},
    decode::{decode_metrics_json_request, decode_metrics_jsonl_request, InputFormat},
    Result,
};

/// Transform OTLP metrics to Arrow RecordBatches.
///
/// Returns separate batches for gauge and sum metrics because they have
/// different schemas. Each field in the result is `None` if there were
/// no metrics of that type in the input.
/// [`crate::Transformer`] adds options and an observer.
pub fn transform_metrics(bytes: &[u8], format: InputFormat) -> Result<MetricBatches> {
    transform_metrics_observed(bytes, format, &mut None, MetricsSelection::All)
}

pub(super) fn transform_metrics_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    selection: MetricsSelection<'_>,
) -> Result<MetricBatches> {
    match format {
        InputFormat::Protobuf => {
            batch::transform_metrics_protobuf_observed(bytes, observer, selection)
        }
        InputFormat::Auto => auto_dispatch(
            bytes,
            observer,
            |b, obs| transform_metrics_json_arrow_observed(b, InputFormat::Json, obs, selection),
            |b, obs| transform_metrics_json_arrow_observed(b, InputFormat::Jsonl, obs, selection),
            |b, obs| batch::transform_metrics_protobuf_observed(b, obs, selection),
        ),
        InputFormat::Json | InputFormat::Jsonl => {
            transform_metrics_json_arrow_observed(bytes, format, observer, selection)
        }
    }
}

fn transform_metrics_json_arrow_observed(
    bytes: &[u8],
    format: InputFormat,
    observer: &mut Option<&mut dyn TransformObserver>,
    selection: MetricsSelection<'_>,
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
    batch::transform_metrics_request_observed(request, observer, selection)
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
