//! Public metric transform orchestration.

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;

use crate::{
    api::{optional_batch_to_json_values, JsonMetricBatches, MetricBatches},
    batch,
    decode::{
        decode_metrics_json_request, decode_metrics_jsonl_request, looks_like_json, DecodeError,
        InputFormat,
    },
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

#[doc(hidden)]
pub fn transform_metrics_decoded_for_bench(
    request: ExportMetricsServiceRequest,
) -> Result<MetricBatches> {
    batch::transform_metrics_request(request)
}

fn transform_metrics_json_arrow(bytes: &[u8], format: InputFormat) -> Result<MetricBatches> {
    let request = match format {
        InputFormat::Json => decode_metrics_json_request(bytes)?,
        InputFormat::Jsonl => decode_metrics_jsonl_request(bytes)?,
        _ => {
            return Err(Error::Decode(DecodeError::Unsupported(
                "expected JSON or JSONL metrics input".to_string(),
            )));
        }
    };
    batch::transform_metrics_request(request)
}

fn transform_metrics_auto(bytes: &[u8]) -> Result<MetricBatches> {
    if looks_like_json(bytes) {
        match transform_metrics_json_arrow(bytes, InputFormat::Json) {
            Ok(batches) => Ok(batches),
            Err(json_err) => match transform_metrics_json_arrow(bytes, InputFormat::Jsonl) {
                Ok(batches) => Ok(batches),
                Err(_) => batch::transform_metrics_protobuf(bytes).map_err(|proto_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "json decode failed: {json_err}; protobuf fallback failed: {proto_err}"
                    )))
                }),
            },
        }
    } else {
        match batch::transform_metrics_protobuf(bytes) {
            Ok(batches) => Ok(batches),
            Err(proto_err) => {
                transform_metrics_json_arrow(bytes, InputFormat::Json).map_err(|json_err| {
                    Error::Decode(DecodeError::Unsupported(format!(
                        "protobuf decode failed: {proto_err}; json fallback failed: {json_err}"
                    )))
                })
            }
        }
    }
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
