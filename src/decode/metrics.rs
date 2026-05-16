//! OTLP metric JSON decoding into protobuf request structs.

/// Tracks metrics that were skipped during transformation.
#[derive(Debug, Default, Clone)]
pub struct SkippedMetrics {
    /// Count of summary metrics skipped (deprecated in OTLP spec).
    pub summaries: usize,
    /// Count of data points skipped due to NaN values.
    pub nan_values: usize,
    /// Count of data points skipped due to Infinity values.
    pub infinity_values: usize,
    /// Count of data points skipped due to missing values.
    pub missing_values: usize,
}

impl SkippedMetrics {
    /// Returns true if any metrics were skipped.
    pub fn has_skipped(&self) -> bool {
        self.summaries > 0
            || self.nan_values > 0
            || self.infinity_values > 0
            || self.missing_values > 0
    }

    /// Returns total count of skipped items.
    pub fn total(&self) -> usize {
        self.summaries + self.nan_values + self.infinity_values + self.missing_values
    }
}

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    metrics::v1::{
        exemplar, exponential_histogram_data_point, metric::Data, number_data_point::Value,
        Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary,
        SummaryDataPoint,
    },
};
use serde::Deserialize;

use super::common::{
    decode_bytes_field, json_attrs_to_otlp, json_resource_to_otlp, json_scope_to_otlp,
    json_timestamp_to_u64, DecodeError, JsonInstrumentationScope, JsonKeyValue, JsonNumberOrString,
    JsonResource,
};

pub fn decode_json_request(body: &[u8]) -> Result<ExportMetricsServiceRequest, DecodeError> {
    let normalized = super::normalize::normalize_json_bytes(body)?;
    let request: JsonExportMetricsServiceRequest = serde_json::from_slice(&normalized)?;
    export_metrics_json_to_request(request)
}

fn export_metrics_json_to_request(
    request: JsonExportMetricsServiceRequest,
) -> Result<ExportMetricsServiceRequest, DecodeError> {
    let mut resource_metrics = Vec::with_capacity(request.resource_metrics.len());

    for resource in request.resource_metrics {
        let mut scope_metrics = Vec::with_capacity(resource.scope_metrics.len());
        for scope in resource.scope_metrics {
            let mut metrics = Vec::with_capacity(scope.metrics.len());
            for metric in scope.metrics {
                let data = if let Some(gauge) = metric.gauge {
                    Some(Data::Gauge(Gauge {
                        data_points: json_number_points_to_otlp(gauge.data_points)?,
                    }))
                } else if let Some(sum) = metric.sum {
                    Some(Data::Sum(Sum {
                        data_points: json_number_points_to_otlp(sum.data_points)?,
                        aggregation_temporality: sum.aggregation_temporality as i32,
                        is_monotonic: sum.is_monotonic,
                    }))
                } else if let Some(histogram) = metric.histogram {
                    Some(Data::Histogram(Histogram {
                        data_points: json_histogram_points_to_otlp(histogram.data_points)?,
                        aggregation_temporality: histogram.aggregation_temporality as i32,
                    }))
                } else if let Some(histogram) = metric.exponential_histogram {
                    Some(Data::ExponentialHistogram(ExponentialHistogram {
                        data_points: json_exp_histogram_points_to_otlp(histogram.data_points)?,
                        aggregation_temporality: histogram.aggregation_temporality as i32,
                    }))
                } else {
                    metric.summary.map(|summary| {
                        Data::Summary(Summary {
                            data_points: vec![
                                SummaryDataPoint::default();
                                summary.data_points.len()
                            ],
                        })
                    })
                };

                metrics.push(Metric {
                    name: metric.name,
                    description: metric.description,
                    unit: metric.unit,
                    data,
                    metadata: Vec::new(),
                });
            }
            scope_metrics.push(ScopeMetrics {
                scope: Some(json_scope_to_otlp(scope.scope)),
                metrics,
                schema_url: String::new(),
            });
        }
        resource_metrics.push(ResourceMetrics {
            resource: Some(json_resource_to_otlp(resource.resource)),
            scope_metrics,
            schema_url: String::new(),
        });
    }

    Ok(ExportMetricsServiceRequest { resource_metrics })
}

fn json_number_points_to_otlp(
    points: Vec<JsonNumberDataPoint>,
) -> Result<Vec<NumberDataPoint>, DecodeError> {
    points
        .into_iter()
        .map(|point| {
            Ok(NumberDataPoint {
                attributes: json_attrs_to_otlp(point.attributes),
                start_time_unix_nano: json_timestamp_to_u64(
                    &point.start_time_unix_nano,
                    "metric.start_time_unix_nano",
                )?,
                time_unix_nano: json_timestamp_to_u64(
                    &point.time_unix_nano,
                    "metric.time_unix_nano",
                )?,
                exemplars: json_exemplars_to_otlp(point.exemplars)?,
                flags: point.flags,
                value: json_number_value_to_otlp(point.as_int, point.as_double),
            })
        })
        .collect()
}

fn json_histogram_points_to_otlp(
    points: Vec<JsonHistogramDataPoint>,
) -> Result<Vec<HistogramDataPoint>, DecodeError> {
    points
        .into_iter()
        .map(|point| {
            Ok(HistogramDataPoint {
                attributes: json_attrs_to_otlp(point.attributes),
                start_time_unix_nano: json_timestamp_to_u64(
                    &point.start_time_unix_nano,
                    "histogram.start_time_unix_nano",
                )?,
                time_unix_nano: json_timestamp_to_u64(
                    &point.time_unix_nano,
                    "histogram.time_unix_nano",
                )?,
                count: json_number_to_u64_lossy(&point.count),
                sum: point.sum,
                bucket_counts: point
                    .bucket_counts
                    .iter()
                    .map(json_number_to_u64_lossy)
                    .collect(),
                explicit_bounds: point.explicit_bounds,
                exemplars: json_exemplars_to_otlp(point.exemplars)?,
                flags: point.flags,
                min: point.min,
                max: point.max,
            })
        })
        .collect()
}

fn json_exp_histogram_points_to_otlp(
    points: Vec<JsonExpHistogramDataPoint>,
) -> Result<Vec<ExponentialHistogramDataPoint>, DecodeError> {
    points
        .into_iter()
        .map(|point| {
            Ok(ExponentialHistogramDataPoint {
                attributes: json_attrs_to_otlp(point.attributes),
                start_time_unix_nano: json_timestamp_to_u64(
                    &point.start_time_unix_nano,
                    "exp_histogram.start_time_unix_nano",
                )?,
                time_unix_nano: json_timestamp_to_u64(
                    &point.time_unix_nano,
                    "exp_histogram.time_unix_nano",
                )?,
                count: json_number_to_u64_lossy(&point.count),
                sum: point.sum,
                scale: point.scale,
                zero_count: json_number_to_u64_lossy(&point.zero_count),
                positive: point.positive.map(json_exp_buckets_to_otlp),
                negative: point.negative.map(json_exp_buckets_to_otlp),
                flags: point.flags,
                exemplars: json_exemplars_to_otlp(point.exemplars)?,
                min: point.min,
                max: point.max,
                zero_threshold: point.zero_threshold,
            })
        })
        .collect()
}

fn json_exp_buckets_to_otlp(
    buckets: JsonExpHistogramBuckets,
) -> exponential_histogram_data_point::Buckets {
    exponential_histogram_data_point::Buckets {
        offset: buckets.offset,
        bucket_counts: buckets
            .bucket_counts
            .iter()
            .map(json_number_to_u64_lossy)
            .collect(),
    }
}

fn json_exemplars_to_otlp(exemplars: Vec<JsonExemplar>) -> Result<Vec<Exemplar>, DecodeError> {
    exemplars
        .into_iter()
        .map(|exemplar| {
            Ok(Exemplar {
                filtered_attributes: json_attrs_to_otlp(exemplar.filtered_attributes),
                time_unix_nano: json_timestamp_to_u64(
                    &exemplar.time_unix_nano,
                    "exemplar.time_unix_nano",
                )?,
                span_id: decode_bytes_field(&exemplar.span_id),
                trace_id: decode_bytes_field(&exemplar.trace_id),
                value: json_exemplar_value_to_otlp(exemplar.as_int, exemplar.as_double),
            })
        })
        .collect()
}

fn json_number_value_to_otlp(
    as_int: Option<JsonNumberOrString>,
    as_double: Option<f64>,
) -> Option<Value> {
    if let Some(value) = as_int {
        value.as_i64().map(Value::AsInt)
    } else {
        as_double.map(Value::AsDouble)
    }
}

fn json_exemplar_value_to_otlp(
    as_int: Option<JsonNumberOrString>,
    as_double: Option<f64>,
) -> Option<exemplar::Value> {
    if let Some(value) = as_int {
        value.as_i64().map(exemplar::Value::AsInt)
    } else {
        as_double.map(exemplar::Value::AsDouble)
    }
}

fn json_number_to_u64_lossy(value: &JsonNumberOrString) -> u64 {
    match value {
        JsonNumberOrString::String(value) => {
            value.parse::<i64>().map(|value| value as u64).unwrap_or(0)
        }
        JsonNumberOrString::Number(value) => value
            .as_i64()
            .map(|value| value as u64)
            .or_else(|| value.as_u64())
            .unwrap_or(0),
        JsonNumberOrString::Missing => 0,
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExportMetricsServiceRequest {
    #[serde(default)]
    resource_metrics: Vec<JsonResourceMetrics>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonResourceMetrics {
    #[serde(default)]
    resource: JsonResource,
    #[serde(default)]
    scope_metrics: Vec<JsonScopeMetrics>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonScopeMetrics {
    #[serde(default)]
    scope: JsonInstrumentationScope,
    #[serde(default)]
    metrics: Vec<JsonMetric>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonMetric {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    unit: String,
    #[serde(default)]
    gauge: Option<JsonGauge>,
    #[serde(default)]
    sum: Option<JsonSum>,
    #[serde(default)]
    histogram: Option<JsonHistogram>,
    #[serde(default)]
    exponential_histogram: Option<JsonExponentialHistogram>,
    #[serde(default)]
    summary: Option<JsonSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonGauge {
    #[serde(default)]
    data_points: Vec<JsonNumberDataPoint>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSum {
    #[serde(default)]
    data_points: Vec<JsonNumberDataPoint>,
    #[serde(default)]
    aggregation_temporality: i64,
    #[serde(default)]
    is_monotonic: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonHistogram {
    #[serde(default)]
    data_points: Vec<JsonHistogramDataPoint>,
    #[serde(default)]
    aggregation_temporality: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExponentialHistogram {
    #[serde(default)]
    data_points: Vec<JsonExpHistogramDataPoint>,
    #[serde(default)]
    aggregation_temporality: i64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonHistogramDataPoint {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    start_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    count: JsonNumberOrString,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default)]
    bucket_counts: Vec<JsonNumberOrString>,
    #[serde(default)]
    explicit_bounds: Vec<f64>,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    flags: u32,
    #[serde(default)]
    exemplars: Vec<JsonExemplar>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExpHistogramDataPoint {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    start_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    count: JsonNumberOrString,
    #[serde(default)]
    sum: Option<f64>,
    #[serde(default)]
    min: Option<f64>,
    #[serde(default)]
    max: Option<f64>,
    #[serde(default)]
    scale: i32,
    #[serde(default)]
    zero_count: JsonNumberOrString,
    #[serde(default)]
    zero_threshold: f64,
    #[serde(default)]
    positive: Option<JsonExpHistogramBuckets>,
    #[serde(default)]
    negative: Option<JsonExpHistogramBuckets>,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    flags: u32,
    #[serde(default)]
    exemplars: Vec<JsonExemplar>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExpHistogramBuckets {
    #[serde(default)]
    offset: i32,
    #[serde(default)]
    bucket_counts: Vec<JsonNumberOrString>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSummary {
    #[serde(default)]
    data_points: Vec<serde_json::Value>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonNumberDataPoint {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    start_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    as_int: Option<JsonNumberOrString>,
    #[serde(default)]
    as_double: Option<f64>,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    flags: u32,
    #[serde(default)]
    exemplars: Vec<JsonExemplar>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExemplar {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    as_int: Option<JsonNumberOrString>,
    #[serde(default)]
    as_double: Option<f64>,
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
    #[serde(default)]
    filtered_attributes: Vec<JsonKeyValue>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_json_gauge_request() {
        let body = r#"{
            "resourceMetrics": [{
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "test.gauge",
                        "gauge": {
                            "dataPoints": [{
                                "timeUnixNano": "1000000000",
                                "asDouble": 42.5
                            }]
                        }
                    }]
                }]
            }]
        }"#;

        let request = decode_json_request(body.as_bytes()).unwrap();
        let metric = &request.resource_metrics[0].scope_metrics[0].metrics[0];
        assert_eq!(metric.name, "test.gauge");
    }
}
