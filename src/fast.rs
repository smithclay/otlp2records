//! Direct protobuf-to-Arrow hot paths.

use std::{io, sync::Arc};

use arrow_array::{
    builder::{
        BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    },
    ArrayRef, RecordBatch,
};
use const_hex::{encode as hex_encode, encode_to_str};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::LogRecord,
    metrics::v1::{
        exemplar, metric::Data, number_data_point, Exemplar, ExponentialHistogramDataPoint,
        HistogramDataPoint, NumberDataPoint,
    },
    trace::v1::{span, Span},
};
use prost::Message;
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Serialize, Serializer,
};

use crate::{
    arrow::{
        exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema,
        traces_schema,
    },
    decode::{DecodeError, SkippedMetrics},
    Error, MetricBatches, Result,
};

pub fn transform_logs_protobuf(bytes: &[u8]) -> Result<RecordBatch> {
    let request = ExportLogsServiceRequest::decode(bytes)?;
    let rows = request
        .resource_logs
        .iter()
        .map(|rl| {
            rl.scope_logs
                .iter()
                .map(|sl| sl.log_records.len())
                .sum::<usize>()
        })
        .sum();
    let mut builders = LogBuilders::with_capacity(rows, bytes.len());

    for resource_logs in request.resource_logs {
        let resource = ResourceContext::from_attrs(
            resource_logs
                .resource
                .as_ref()
                .map(|r| r.attributes.as_slice())
                .unwrap_or(&[]),
        );

        for scope_logs in resource_logs.scope_logs {
            let scope = ScopeContext::new(
                scope_logs.scope.as_ref().map(|s| s.name.as_str()),
                scope_logs.scope.as_ref().map(|s| s.version.as_str()),
                scope_logs
                    .scope
                    .as_ref()
                    .map(|s| s.attributes.as_slice())
                    .unwrap_or(&[]),
            );

            for record in scope_logs.log_records {
                builders.append(&record, &resource, &scope)?;
            }
        }
    }

    builders.finish()
}

pub fn transform_traces_protobuf(bytes: &[u8]) -> Result<RecordBatch> {
    let request = ExportTraceServiceRequest::decode(bytes)?;
    let rows = request
        .resource_spans
        .iter()
        .map(|rs| {
            rs.scope_spans
                .iter()
                .map(|ss| ss.spans.len())
                .sum::<usize>()
        })
        .sum();
    let mut builders = TraceBuilders::with_capacity(rows, bytes.len());

    for resource_spans in request.resource_spans {
        let resource = ResourceContext::from_attrs(
            resource_spans
                .resource
                .as_ref()
                .map(|r| r.attributes.as_slice())
                .unwrap_or(&[]),
        );

        for scope_spans in resource_spans.scope_spans {
            let scope = ScopeContext::new(
                scope_spans.scope.as_ref().map(|s| s.name.as_str()),
                scope_spans.scope.as_ref().map(|s| s.version.as_str()),
                scope_spans
                    .scope
                    .as_ref()
                    .map(|s| s.attributes.as_slice())
                    .unwrap_or(&[]),
            );

            for span in scope_spans.spans {
                builders.append(&span, &resource, &scope)?;
            }
        }
    }

    builders.finish()
}

pub fn transform_metrics_protobuf(bytes: &[u8]) -> Result<MetricBatches> {
    let request = ExportMetricsServiceRequest::decode(bytes)?;
    let mut capacities = MetricCapacities::default();
    for resource_metrics in &request.resource_metrics {
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                match &metric.data {
                    Some(Data::Gauge(gauge)) => capacities.gauge += gauge.data_points.len(),
                    Some(Data::Sum(sum)) => capacities.sum += sum.data_points.len(),
                    Some(Data::Histogram(histogram)) => {
                        capacities.histogram += histogram.data_points.len();
                    }
                    Some(Data::ExponentialHistogram(histogram)) => {
                        capacities.exp_histogram += histogram.data_points.len();
                    }
                    _ => {}
                }
            }
        }
    }

    let mut gauge = GaugeBuilders::with_capacity(capacities.gauge);
    let mut sum = SumBuilders::with_capacity(capacities.sum);
    let mut histogram = HistogramBuilders::with_capacity(capacities.histogram);
    let mut exp_histogram = ExpHistogramBuilders::with_capacity(capacities.exp_histogram);
    let mut skipped = SkippedMetrics::default();

    for resource_metrics in request.resource_metrics {
        let resource = ResourceContext::from_attrs(
            resource_metrics
                .resource
                .as_ref()
                .map(|r| r.attributes.as_slice())
                .unwrap_or(&[]),
        );

        for scope_metrics in resource_metrics.scope_metrics {
            let scope = ScopeContext::new(
                scope_metrics.scope.as_ref().map(|s| s.name.as_str()),
                scope_metrics.scope.as_ref().map(|s| s.version.as_str()),
                scope_metrics
                    .scope
                    .as_ref()
                    .map(|s| s.attributes.as_slice())
                    .unwrap_or(&[]),
            );

            for metric in scope_metrics.metrics {
                let metric_name = metric.name.as_str();
                let metric_description = metric.description.as_str();
                let metric_unit = metric.unit.as_str();

                match metric.data {
                    Some(Data::Gauge(data)) => {
                        for point in data.data_points {
                            if let Some(value) = metric_point_value(&point.value, &mut skipped) {
                                gauge.append(
                                    &point,
                                    value,
                                    MetricMeta {
                                        name: metric_name,
                                        description: metric_description,
                                        unit: metric_unit,
                                    },
                                    &resource,
                                    &scope,
                                )?;
                            }
                        }
                    }
                    Some(Data::Sum(data)) => {
                        for point in data.data_points {
                            if let Some(value) = metric_point_value(&point.value, &mut skipped) {
                                sum.append(
                                    &point,
                                    value,
                                    data.aggregation_temporality,
                                    data.is_monotonic,
                                    MetricMeta {
                                        name: metric_name,
                                        description: metric_description,
                                        unit: metric_unit,
                                    },
                                    &resource,
                                    &scope,
                                )?;
                            }
                        }
                    }
                    Some(Data::Histogram(data)) => {
                        for point in data.data_points {
                            histogram.append(
                                &point,
                                data.aggregation_temporality,
                                MetricMeta {
                                    name: metric_name,
                                    description: metric_description,
                                    unit: metric_unit,
                                },
                                &resource,
                                &scope,
                            )?;
                        }
                    }
                    Some(Data::ExponentialHistogram(data)) => {
                        for point in data.data_points {
                            exp_histogram.append(
                                &point,
                                data.aggregation_temporality,
                                MetricMeta {
                                    name: metric_name,
                                    description: metric_description,
                                    unit: metric_unit,
                                },
                                &resource,
                                &scope,
                            )?;
                        }
                    }
                    Some(Data::Summary(summary)) => {
                        skipped.summaries += summary.data_points.len();
                    }
                    None => {}
                }
            }
        }
    }

    Ok(MetricBatches {
        gauge: gauge.finish_if_non_empty()?,
        sum: sum.finish_if_non_empty()?,
        histogram: histogram.finish_if_non_empty()?,
        exp_histogram: exp_histogram.finish_if_non_empty()?,
        skipped,
    })
}

struct ResourceContext {
    service_name: Option<String>,
    service_namespace: Option<String>,
    service_instance_id: Option<String>,
    attributes_json: Option<String>,
}

impl ResourceContext {
    fn from_attrs(attrs: &[KeyValue]) -> Self {
        Self {
            service_name: attr_field(attrs, "service.name"),
            service_namespace: attr_field(attrs, "service.namespace"),
            service_instance_id: attr_field(attrs, "service.instance.id"),
            attributes_json: attrs_json(attrs),
        }
    }
}

struct ScopeContext {
    name: Option<String>,
    version: Option<String>,
    attributes_json: Option<String>,
}

impl ScopeContext {
    fn new(name: Option<&str>, version: Option<&str>, attrs: &[KeyValue]) -> Self {
        Self {
            name: non_empty_str(name),
            version: non_empty_str(version),
            attributes_json: attrs_json(attrs),
        }
    }
}

#[derive(Clone, Copy)]
struct MetricMeta<'a> {
    name: &'a str,
    description: &'a str,
    unit: &'a str,
}

#[derive(Default)]
struct MetricCapacities {
    gauge: usize,
    sum: usize,
    histogram: usize,
    exp_histogram: usize,
}

struct LogBuilders {
    timestamp: TimestampMicrosecondBuilder,
    observed_timestamp: Int64Builder,
    trace_id: StringBuilder,
    span_id: StringBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    severity_number: Int32Builder,
    severity_text: StringBuilder,
    body: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    log_attributes: StringBuilder,
}

impl LogBuilders {
    fn with_capacity(rows: usize, input_bytes: usize) -> Self {
        Self {
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            observed_timestamp: Int64Builder::with_capacity(rows),
            trace_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            span_id: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_name: string_builder_bytes(rows, rows.saturating_mul(24)),
            service_namespace: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_instance_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            severity_number: Int32Builder::with_capacity(rows),
            severity_text: string_builder_bytes(rows, rows.saturating_mul(8)),
            body: string_builder_bytes(rows, input_bytes),
            resource_attributes: string_builder_bytes(rows, input_bytes),
            scope_name: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_version: string_builder_bytes(rows, rows.saturating_mul(8)),
            scope_attributes: string_builder_bytes(rows, input_bytes / 4),
            log_attributes: string_builder_bytes(rows, input_bytes),
        }
    }

    fn append(
        &mut self,
        record: &LogRecord,
        resource: &ResourceContext,
        scope: &ScopeContext,
    ) -> Result<()> {
        self.timestamp
            .append_value(u64_to_i64(record.time_unix_nano, "log.time_unix_nano")? / 1_000);
        self.observed_timestamp.append_value(
            u64_to_i64(
                record.observed_time_unix_nano,
                "log.observed_time_unix_nano",
            )? / 1_000,
        );
        append_hex_or_null(&mut self.trace_id, &record.trace_id);
        append_hex_or_null(&mut self.span_id, &record.span_id);
        append_required_service_name(&mut self.service_name, resource.service_name.as_deref());
        append_opt(
            &mut self.service_namespace,
            resource.service_namespace.as_deref(),
        );
        append_opt(
            &mut self.service_instance_id,
            resource.service_instance_id.as_deref(),
        );
        self.severity_number.append_value(record.severity_number);
        self.severity_text.append_value(&record.severity_text);
        append_log_body(&mut self.body, record.body.as_ref())?;
        append_opt(
            &mut self.resource_attributes,
            resource.attributes_json.as_deref(),
        );
        append_opt(&mut self.scope_name, scope.name.as_deref());
        append_opt(&mut self.scope_version, scope.version.as_deref());
        append_opt(&mut self.scope_attributes, scope.attributes_json.as_deref());
        append_attrs_json(&mut self.log_attributes, &record.attributes)?;
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            logs_schema(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.observed_timestamp.finish()),
                Arc::new(self.trace_id.finish()),
                Arc::new(self.span_id.finish()),
                Arc::new(self.service_name.finish()),
                Arc::new(self.service_namespace.finish()),
                Arc::new(self.service_instance_id.finish()),
                Arc::new(self.severity_number.finish()),
                Arc::new(self.severity_text.finish()),
                Arc::new(self.body.finish()),
                Arc::new(self.resource_attributes.finish()),
                Arc::new(self.scope_name.finish()),
                Arc::new(self.scope_version.finish()),
                Arc::new(self.scope_attributes.finish()),
                Arc::new(self.log_attributes.finish()),
            ],
        )
    }
}

struct TraceBuilders {
    timestamp: TimestampMicrosecondBuilder,
    end_timestamp: Int64Builder,
    duration: Int64Builder,
    trace_id: StringBuilder,
    span_id: StringBuilder,
    parent_span_id: StringBuilder,
    trace_state: StringBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    span_name: StringBuilder,
    span_kind: Int32Builder,
    status_code: Int32Builder,
    status_message: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    span_attributes: StringBuilder,
    events_json: StringBuilder,
    links_json: StringBuilder,
    dropped_attributes_count: Int32Builder,
    dropped_events_count: Int32Builder,
    dropped_links_count: Int32Builder,
    flags: Int32Builder,
}

impl TraceBuilders {
    fn with_capacity(rows: usize, input_bytes: usize) -> Self {
        Self {
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            end_timestamp: Int64Builder::with_capacity(rows),
            duration: Int64Builder::with_capacity(rows),
            trace_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            span_id: string_builder_bytes(rows, rows.saturating_mul(16)),
            parent_span_id: string_builder_bytes(rows, rows.saturating_mul(16)),
            trace_state: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_name: string_builder_bytes(rows, rows.saturating_mul(24)),
            service_namespace: string_builder_bytes(rows, rows.saturating_mul(16)),
            service_instance_id: string_builder_bytes(rows, rows.saturating_mul(32)),
            span_name: string_builder_bytes(rows, rows.saturating_mul(48)),
            span_kind: Int32Builder::with_capacity(rows),
            status_code: Int32Builder::with_capacity(rows),
            status_message: string_builder_bytes(rows, rows.saturating_mul(16)),
            resource_attributes: string_builder_bytes(rows, input_bytes),
            scope_name: string_builder_bytes(rows, rows.saturating_mul(16)),
            scope_version: string_builder_bytes(rows, rows.saturating_mul(8)),
            scope_attributes: string_builder_bytes(rows, input_bytes / 4),
            span_attributes: string_builder_bytes(rows, input_bytes),
            events_json: string_builder_bytes(rows, input_bytes),
            links_json: string_builder_bytes(rows, input_bytes / 2),
            dropped_attributes_count: Int32Builder::with_capacity(rows),
            dropped_events_count: Int32Builder::with_capacity(rows),
            dropped_links_count: Int32Builder::with_capacity(rows),
            flags: Int32Builder::with_capacity(rows),
        }
    }

    fn append(
        &mut self,
        span: &Span,
        resource: &ResourceContext,
        scope: &ScopeContext,
    ) -> Result<()> {
        let start = u64_to_i64(span.start_time_unix_nano, "span.start_time_unix_nano")?;
        let end = u64_to_i64(span.end_time_unix_nano, "span.end_time_unix_nano")?;
        self.timestamp.append_value(start / 1_000);
        self.end_timestamp.append_value(end / 1_000_000);
        self.duration
            .append_value(end.saturating_sub(start) / 1_000_000);
        append_hex_or_null(&mut self.trace_id, &span.trace_id);
        append_hex_or_null(&mut self.span_id, &span.span_id);
        append_hex_or_null(&mut self.parent_span_id, &span.parent_span_id);
        append_empty_as_null(&mut self.trace_state, &span.trace_state);
        append_required_service_name(&mut self.service_name, resource.service_name.as_deref());
        append_opt(
            &mut self.service_namespace,
            resource.service_namespace.as_deref(),
        );
        append_opt(
            &mut self.service_instance_id,
            resource.service_instance_id.as_deref(),
        );
        self.span_name.append_value(&span.name);
        self.span_kind.append_value(span.kind);
        let (status_code, status_message) = span
            .status
            .as_ref()
            .map(|status| (status.code, status.message.as_str()))
            .unwrap_or((0, ""));
        self.status_code.append_value(status_code);
        append_empty_as_null(&mut self.status_message, status_message);
        append_opt(
            &mut self.resource_attributes,
            resource.attributes_json.as_deref(),
        );
        append_opt(&mut self.scope_name, scope.name.as_deref());
        append_opt(&mut self.scope_version, scope.version.as_deref());
        append_opt(&mut self.scope_attributes, scope.attributes_json.as_deref());
        append_attrs_json(&mut self.span_attributes, &span.attributes)?;
        append_span_events_json(&mut self.events_json, &span.events)?;
        append_span_links_json(&mut self.links_json, &span.links)?;
        self.dropped_attributes_count
            .append_value(span.dropped_attributes_count as i32);
        self.dropped_events_count
            .append_value(span.dropped_events_count as i32);
        self.dropped_links_count
            .append_value(span.dropped_links_count as i32);
        self.flags.append_value(span.flags as i32);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            traces_schema(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.end_timestamp.finish()),
                Arc::new(self.duration.finish()),
                Arc::new(self.trace_id.finish()),
                Arc::new(self.span_id.finish()),
                Arc::new(self.parent_span_id.finish()),
                Arc::new(self.trace_state.finish()),
                Arc::new(self.service_name.finish()),
                Arc::new(self.service_namespace.finish()),
                Arc::new(self.service_instance_id.finish()),
                Arc::new(self.span_name.finish()),
                Arc::new(self.span_kind.finish()),
                Arc::new(self.status_code.finish()),
                Arc::new(self.status_message.finish()),
                Arc::new(self.resource_attributes.finish()),
                Arc::new(self.scope_name.finish()),
                Arc::new(self.scope_version.finish()),
                Arc::new(self.scope_attributes.finish()),
                Arc::new(self.span_attributes.finish()),
                Arc::new(self.events_json.finish()),
                Arc::new(self.links_json.finish()),
                Arc::new(self.dropped_attributes_count.finish()),
                Arc::new(self.dropped_events_count.finish()),
                Arc::new(self.dropped_links_count.finish()),
                Arc::new(self.flags.finish()),
            ],
        )
    }
}

struct GaugeBuilders {
    rows: usize,
    timestamp: TimestampMicrosecondBuilder,
    start_timestamp: Int64Builder,
    metric_name: StringBuilder,
    metric_description: StringBuilder,
    metric_unit: StringBuilder,
    value: Float64Builder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: Int32Builder,
    exemplars_json: StringBuilder,
}

impl GaugeBuilders {
    fn with_capacity(rows: usize) -> Self {
        Self {
            rows: 0,
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            start_timestamp: Int64Builder::with_capacity(rows),
            metric_name: string_builder(rows),
            metric_description: string_builder(rows),
            metric_unit: string_builder(rows),
            value: Float64Builder::with_capacity(rows),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: Int32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
        }
    }

    fn append(
        &mut self,
        point: &NumberDataPoint,
        value: f64,
        meta: MetricMeta<'_>,
        resource: &ResourceContext,
        scope: &ScopeContext,
    ) -> Result<()> {
        append_metric_common(self, point, value, meta, resource, scope)?;
        self.rows += 1;
        Ok(())
    }

    fn finish_if_non_empty(mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        record_batch(
            gauge_schema(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.start_timestamp.finish()),
                Arc::new(self.metric_name.finish()),
                Arc::new(self.metric_description.finish()),
                Arc::new(self.metric_unit.finish()),
                Arc::new(self.value.finish()),
                Arc::new(self.service_name.finish()),
                Arc::new(self.service_namespace.finish()),
                Arc::new(self.service_instance_id.finish()),
                Arc::new(self.resource_attributes.finish()),
                Arc::new(self.scope_name.finish()),
                Arc::new(self.scope_version.finish()),
                Arc::new(self.scope_attributes.finish()),
                Arc::new(self.metric_attributes.finish()),
                Arc::new(self.flags.finish()),
                Arc::new(self.exemplars_json.finish()),
            ],
        )
        .map(Some)
    }
}

struct SumBuilders {
    rows: usize,
    timestamp: TimestampMicrosecondBuilder,
    start_timestamp: Int64Builder,
    metric_name: StringBuilder,
    metric_description: StringBuilder,
    metric_unit: StringBuilder,
    value: Float64Builder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: Int32Builder,
    exemplars_json: StringBuilder,
    aggregation_temporality: Int32Builder,
    is_monotonic: BooleanBuilder,
}

impl SumBuilders {
    fn with_capacity(rows: usize) -> Self {
        Self {
            rows: 0,
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            start_timestamp: Int64Builder::with_capacity(rows),
            metric_name: string_builder(rows),
            metric_description: string_builder(rows),
            metric_unit: string_builder(rows),
            value: Float64Builder::with_capacity(rows),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: Int32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
            aggregation_temporality: Int32Builder::with_capacity(rows),
            is_monotonic: BooleanBuilder::with_capacity(rows),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        point: &NumberDataPoint,
        value: f64,
        aggregation_temporality: i32,
        is_monotonic: bool,
        meta: MetricMeta<'_>,
        resource: &ResourceContext,
        scope: &ScopeContext,
    ) -> Result<()> {
        append_metric_common(self, point, value, meta, resource, scope)?;
        self.aggregation_temporality
            .append_value(aggregation_temporality);
        self.is_monotonic.append_value(is_monotonic);
        self.rows += 1;
        Ok(())
    }

    fn finish_if_non_empty(mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        record_batch(
            sum_schema(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.start_timestamp.finish()),
                Arc::new(self.metric_name.finish()),
                Arc::new(self.metric_description.finish()),
                Arc::new(self.metric_unit.finish()),
                Arc::new(self.value.finish()),
                Arc::new(self.service_name.finish()),
                Arc::new(self.service_namespace.finish()),
                Arc::new(self.service_instance_id.finish()),
                Arc::new(self.resource_attributes.finish()),
                Arc::new(self.scope_name.finish()),
                Arc::new(self.scope_version.finish()),
                Arc::new(self.scope_attributes.finish()),
                Arc::new(self.metric_attributes.finish()),
                Arc::new(self.flags.finish()),
                Arc::new(self.exemplars_json.finish()),
                Arc::new(self.aggregation_temporality.finish()),
                Arc::new(self.is_monotonic.finish()),
            ],
        )
        .map(Some)
    }
}

trait NumberMetricBuilders {
    fn timestamp(&mut self) -> &mut TimestampMicrosecondBuilder;
    fn start_timestamp(&mut self) -> &mut Int64Builder;
    fn metric_name(&mut self) -> &mut StringBuilder;
    fn metric_description(&mut self) -> &mut StringBuilder;
    fn metric_unit(&mut self) -> &mut StringBuilder;
    fn value(&mut self) -> &mut Float64Builder;
    fn service_name(&mut self) -> &mut StringBuilder;
    fn service_namespace(&mut self) -> &mut StringBuilder;
    fn service_instance_id(&mut self) -> &mut StringBuilder;
    fn resource_attributes(&mut self) -> &mut StringBuilder;
    fn scope_name(&mut self) -> &mut StringBuilder;
    fn scope_version(&mut self) -> &mut StringBuilder;
    fn scope_attributes(&mut self) -> &mut StringBuilder;
    fn metric_attributes(&mut self) -> &mut StringBuilder;
    fn flags(&mut self) -> &mut Int32Builder;
    fn exemplars_json(&mut self) -> &mut StringBuilder;
}

impl NumberMetricBuilders for GaugeBuilders {
    fn timestamp(&mut self) -> &mut TimestampMicrosecondBuilder {
        &mut self.timestamp
    }
    fn start_timestamp(&mut self) -> &mut Int64Builder {
        &mut self.start_timestamp
    }
    fn metric_name(&mut self) -> &mut StringBuilder {
        &mut self.metric_name
    }
    fn metric_description(&mut self) -> &mut StringBuilder {
        &mut self.metric_description
    }
    fn metric_unit(&mut self) -> &mut StringBuilder {
        &mut self.metric_unit
    }
    fn value(&mut self) -> &mut Float64Builder {
        &mut self.value
    }
    fn service_name(&mut self) -> &mut StringBuilder {
        &mut self.service_name
    }
    fn service_namespace(&mut self) -> &mut StringBuilder {
        &mut self.service_namespace
    }
    fn service_instance_id(&mut self) -> &mut StringBuilder {
        &mut self.service_instance_id
    }
    fn resource_attributes(&mut self) -> &mut StringBuilder {
        &mut self.resource_attributes
    }
    fn scope_name(&mut self) -> &mut StringBuilder {
        &mut self.scope_name
    }
    fn scope_version(&mut self) -> &mut StringBuilder {
        &mut self.scope_version
    }
    fn scope_attributes(&mut self) -> &mut StringBuilder {
        &mut self.scope_attributes
    }
    fn metric_attributes(&mut self) -> &mut StringBuilder {
        &mut self.metric_attributes
    }
    fn flags(&mut self) -> &mut Int32Builder {
        &mut self.flags
    }
    fn exemplars_json(&mut self) -> &mut StringBuilder {
        &mut self.exemplars_json
    }
}

impl NumberMetricBuilders for SumBuilders {
    fn timestamp(&mut self) -> &mut TimestampMicrosecondBuilder {
        &mut self.timestamp
    }
    fn start_timestamp(&mut self) -> &mut Int64Builder {
        &mut self.start_timestamp
    }
    fn metric_name(&mut self) -> &mut StringBuilder {
        &mut self.metric_name
    }
    fn metric_description(&mut self) -> &mut StringBuilder {
        &mut self.metric_description
    }
    fn metric_unit(&mut self) -> &mut StringBuilder {
        &mut self.metric_unit
    }
    fn value(&mut self) -> &mut Float64Builder {
        &mut self.value
    }
    fn service_name(&mut self) -> &mut StringBuilder {
        &mut self.service_name
    }
    fn service_namespace(&mut self) -> &mut StringBuilder {
        &mut self.service_namespace
    }
    fn service_instance_id(&mut self) -> &mut StringBuilder {
        &mut self.service_instance_id
    }
    fn resource_attributes(&mut self) -> &mut StringBuilder {
        &mut self.resource_attributes
    }
    fn scope_name(&mut self) -> &mut StringBuilder {
        &mut self.scope_name
    }
    fn scope_version(&mut self) -> &mut StringBuilder {
        &mut self.scope_version
    }
    fn scope_attributes(&mut self) -> &mut StringBuilder {
        &mut self.scope_attributes
    }
    fn metric_attributes(&mut self) -> &mut StringBuilder {
        &mut self.metric_attributes
    }
    fn flags(&mut self) -> &mut Int32Builder {
        &mut self.flags
    }
    fn exemplars_json(&mut self) -> &mut StringBuilder {
        &mut self.exemplars_json
    }
}

fn append_metric_common<B: NumberMetricBuilders>(
    builders: &mut B,
    point: &NumberDataPoint,
    value: f64,
    meta: MetricMeta<'_>,
    resource: &ResourceContext,
    scope: &ScopeContext,
) -> Result<()> {
    builders
        .timestamp()
        .append_value(u64_to_i64(point.time_unix_nano, "metric.time_unix_nano")? / 1_000);
    builders.start_timestamp().append_value(
        u64_to_i64(point.start_time_unix_nano, "metric.start_time_unix_nano")? / 1_000_000,
    );
    builders.metric_name().append_value(meta.name);
    builders.metric_description().append_value(meta.description);
    builders.metric_unit().append_value(meta.unit);
    builders.value().append_value(value);
    append_required_service_name(builders.service_name(), resource.service_name.as_deref());
    append_opt(
        builders.service_namespace(),
        resource.service_namespace.as_deref(),
    );
    append_opt(
        builders.service_instance_id(),
        resource.service_instance_id.as_deref(),
    );
    append_opt(
        builders.resource_attributes(),
        resource.attributes_json.as_deref(),
    );
    append_opt(builders.scope_name(), scope.name.as_deref());
    append_opt(builders.scope_version(), scope.version.as_deref());
    append_opt(
        builders.scope_attributes(),
        scope.attributes_json.as_deref(),
    );
    append_attrs_json(builders.metric_attributes(), &point.attributes)?;
    builders.flags().append_value(point.flags as i32);
    append_exemplars_json(builders.exemplars_json(), &point.exemplars)?;
    Ok(())
}

struct HistogramBuilders {
    rows: usize,
    timestamp: TimestampMicrosecondBuilder,
    start_timestamp: Int64Builder,
    metric_name: StringBuilder,
    metric_description: StringBuilder,
    metric_unit: StringBuilder,
    count: Int64Builder,
    sum: Float64Builder,
    min: Float64Builder,
    max: Float64Builder,
    bucket_counts: StringBuilder,
    explicit_bounds: StringBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: Int32Builder,
    exemplars_json: StringBuilder,
    aggregation_temporality: Int32Builder,
}

impl HistogramBuilders {
    fn with_capacity(rows: usize) -> Self {
        Self {
            rows: 0,
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            start_timestamp: Int64Builder::with_capacity(rows),
            metric_name: string_builder(rows),
            metric_description: string_builder(rows),
            metric_unit: string_builder(rows),
            count: Int64Builder::with_capacity(rows),
            sum: Float64Builder::with_capacity(rows),
            min: Float64Builder::with_capacity(rows),
            max: Float64Builder::with_capacity(rows),
            bucket_counts: string_builder(rows),
            explicit_bounds: string_builder(rows),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: Int32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
            aggregation_temporality: Int32Builder::with_capacity(rows),
        }
    }

    fn append(
        &mut self,
        point: &HistogramDataPoint,
        aggregation_temporality: i32,
        meta: MetricMeta<'_>,
        resource: &ResourceContext,
        scope: &ScopeContext,
    ) -> Result<()> {
        self.timestamp
            .append_value(u64_to_i64(point.time_unix_nano, "histogram.time_unix_nano")? / 1_000);
        self.start_timestamp.append_value(
            u64_to_i64(point.start_time_unix_nano, "histogram.start_time_unix_nano")? / 1_000_000,
        );
        self.metric_name.append_value(meta.name);
        self.metric_description.append_value(meta.description);
        self.metric_unit.append_value(meta.unit);
        self.count.append_value(point.count as i64);
        append_finite_opt(&mut self.sum, point.sum);
        append_finite_opt(&mut self.min, point.min);
        append_finite_opt(&mut self.max, point.max);
        self.bucket_counts
            .append_value(json_string_or_empty_array(&point.bucket_counts));
        self.explicit_bounds
            .append_value(json_string_or_empty_array(&point.explicit_bounds));
        append_metric_resource_scope(
            &mut self.service_name,
            &mut self.service_namespace,
            &mut self.service_instance_id,
            &mut self.resource_attributes,
            &mut self.scope_name,
            &mut self.scope_version,
            &mut self.scope_attributes,
            resource,
            scope,
        );
        append_attrs_json(&mut self.metric_attributes, &point.attributes)?;
        self.flags.append_value(point.flags as i32);
        append_exemplars_json(&mut self.exemplars_json, &point.exemplars)?;
        self.aggregation_temporality
            .append_value(aggregation_temporality);
        self.rows += 1;
        Ok(())
    }

    fn finish_if_non_empty(mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        record_batch(
            histogram_schema(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.start_timestamp.finish()),
                Arc::new(self.metric_name.finish()),
                Arc::new(self.metric_description.finish()),
                Arc::new(self.metric_unit.finish()),
                Arc::new(self.count.finish()),
                Arc::new(self.sum.finish()),
                Arc::new(self.min.finish()),
                Arc::new(self.max.finish()),
                Arc::new(self.bucket_counts.finish()),
                Arc::new(self.explicit_bounds.finish()),
                Arc::new(self.service_name.finish()),
                Arc::new(self.service_namespace.finish()),
                Arc::new(self.service_instance_id.finish()),
                Arc::new(self.resource_attributes.finish()),
                Arc::new(self.scope_name.finish()),
                Arc::new(self.scope_version.finish()),
                Arc::new(self.scope_attributes.finish()),
                Arc::new(self.metric_attributes.finish()),
                Arc::new(self.flags.finish()),
                Arc::new(self.exemplars_json.finish()),
                Arc::new(self.aggregation_temporality.finish()),
            ],
        )
        .map(Some)
    }
}

struct ExpHistogramBuilders {
    rows: usize,
    timestamp: TimestampMicrosecondBuilder,
    start_timestamp: Int64Builder,
    metric_name: StringBuilder,
    metric_description: StringBuilder,
    metric_unit: StringBuilder,
    count: Int64Builder,
    sum: Float64Builder,
    min: Float64Builder,
    max: Float64Builder,
    scale: Int32Builder,
    zero_count: Int64Builder,
    zero_threshold: Float64Builder,
    positive_offset: Int32Builder,
    positive_bucket_counts: StringBuilder,
    negative_offset: Int32Builder,
    negative_bucket_counts: StringBuilder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: Int32Builder,
    exemplars_json: StringBuilder,
    aggregation_temporality: Int32Builder,
}

impl ExpHistogramBuilders {
    fn with_capacity(rows: usize) -> Self {
        Self {
            rows: 0,
            timestamp: TimestampMicrosecondBuilder::with_capacity(rows),
            start_timestamp: Int64Builder::with_capacity(rows),
            metric_name: string_builder(rows),
            metric_description: string_builder(rows),
            metric_unit: string_builder(rows),
            count: Int64Builder::with_capacity(rows),
            sum: Float64Builder::with_capacity(rows),
            min: Float64Builder::with_capacity(rows),
            max: Float64Builder::with_capacity(rows),
            scale: Int32Builder::with_capacity(rows),
            zero_count: Int64Builder::with_capacity(rows),
            zero_threshold: Float64Builder::with_capacity(rows),
            positive_offset: Int32Builder::with_capacity(rows),
            positive_bucket_counts: string_builder(rows),
            negative_offset: Int32Builder::with_capacity(rows),
            negative_bucket_counts: string_builder(rows),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: Int32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
            aggregation_temporality: Int32Builder::with_capacity(rows),
        }
    }

    fn append(
        &mut self,
        point: &ExponentialHistogramDataPoint,
        aggregation_temporality: i32,
        meta: MetricMeta<'_>,
        resource: &ResourceContext,
        scope: &ScopeContext,
    ) -> Result<()> {
        self.timestamp.append_value(
            u64_to_i64(point.time_unix_nano, "exp_histogram.time_unix_nano")? / 1_000,
        );
        self.start_timestamp.append_value(
            u64_to_i64(
                point.start_time_unix_nano,
                "exp_histogram.start_time_unix_nano",
            )? / 1_000_000,
        );
        self.metric_name.append_value(meta.name);
        self.metric_description.append_value(meta.description);
        self.metric_unit.append_value(meta.unit);
        self.count.append_value(point.count as i64);
        append_finite_opt(&mut self.sum, point.sum);
        append_finite_opt(&mut self.min, point.min);
        append_finite_opt(&mut self.max, point.max);
        self.scale.append_value(point.scale);
        self.zero_count.append_value(point.zero_count as i64);
        append_finite(&mut self.zero_threshold, point.zero_threshold);
        if let Some(positive) = &point.positive {
            self.positive_offset.append_value(positive.offset);
            self.positive_bucket_counts
                .append_value(json_string_or_empty_array(&positive.bucket_counts));
        } else {
            self.positive_offset.append_value(0);
            self.positive_bucket_counts.append_value("[]");
        }
        if let Some(negative) = &point.negative {
            self.negative_offset.append_value(negative.offset);
            self.negative_bucket_counts
                .append_value(json_string_or_empty_array(&negative.bucket_counts));
        } else {
            self.negative_offset.append_value(0);
            self.negative_bucket_counts.append_value("[]");
        }
        append_metric_resource_scope(
            &mut self.service_name,
            &mut self.service_namespace,
            &mut self.service_instance_id,
            &mut self.resource_attributes,
            &mut self.scope_name,
            &mut self.scope_version,
            &mut self.scope_attributes,
            resource,
            scope,
        );
        append_attrs_json(&mut self.metric_attributes, &point.attributes)?;
        self.flags.append_value(point.flags as i32);
        append_exemplars_json(&mut self.exemplars_json, &point.exemplars)?;
        self.aggregation_temporality
            .append_value(aggregation_temporality);
        self.rows += 1;
        Ok(())
    }

    fn finish_if_non_empty(mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        record_batch(
            exp_histogram_schema(),
            vec![
                Arc::new(self.timestamp.finish()),
                Arc::new(self.start_timestamp.finish()),
                Arc::new(self.metric_name.finish()),
                Arc::new(self.metric_description.finish()),
                Arc::new(self.metric_unit.finish()),
                Arc::new(self.count.finish()),
                Arc::new(self.sum.finish()),
                Arc::new(self.min.finish()),
                Arc::new(self.max.finish()),
                Arc::new(self.scale.finish()),
                Arc::new(self.zero_count.finish()),
                Arc::new(self.zero_threshold.finish()),
                Arc::new(self.positive_offset.finish()),
                Arc::new(self.positive_bucket_counts.finish()),
                Arc::new(self.negative_offset.finish()),
                Arc::new(self.negative_bucket_counts.finish()),
                Arc::new(self.service_name.finish()),
                Arc::new(self.service_namespace.finish()),
                Arc::new(self.service_instance_id.finish()),
                Arc::new(self.resource_attributes.finish()),
                Arc::new(self.scope_name.finish()),
                Arc::new(self.scope_version.finish()),
                Arc::new(self.scope_attributes.finish()),
                Arc::new(self.metric_attributes.finish()),
                Arc::new(self.flags.finish()),
                Arc::new(self.exemplars_json.finish()),
                Arc::new(self.aggregation_temporality.finish()),
            ],
        )
        .map(Some)
    }
}

#[allow(clippy::too_many_arguments)]
fn append_metric_resource_scope(
    service_name: &mut StringBuilder,
    service_namespace: &mut StringBuilder,
    service_instance_id: &mut StringBuilder,
    resource_attributes: &mut StringBuilder,
    scope_name: &mut StringBuilder,
    scope_version: &mut StringBuilder,
    scope_attributes: &mut StringBuilder,
    resource: &ResourceContext,
    scope: &ScopeContext,
) {
    append_required_service_name(service_name, resource.service_name.as_deref());
    append_opt(service_namespace, resource.service_namespace.as_deref());
    append_opt(service_instance_id, resource.service_instance_id.as_deref());
    append_opt(resource_attributes, resource.attributes_json.as_deref());
    append_opt(scope_name, scope.name.as_deref());
    append_opt(scope_version, scope.version.as_deref());
    append_opt(scope_attributes, scope.attributes_json.as_deref());
}

#[inline]
fn metric_point_value(
    value: &Option<number_data_point::Value>,
    skipped: &mut SkippedMetrics,
) -> Option<f64> {
    match value {
        Some(number_data_point::Value::AsInt(value)) => Some(*value as f64),
        Some(number_data_point::Value::AsDouble(value)) if value.is_nan() => {
            skipped.nan_values += 1;
            None
        }
        Some(number_data_point::Value::AsDouble(value)) if value.is_infinite() => {
            skipped.infinity_values += 1;
            None
        }
        Some(number_data_point::Value::AsDouble(value)) => Some(*value),
        None => {
            skipped.missing_values += 1;
            None
        }
    }
}

fn append_exemplars_json(builder: &mut StringBuilder, exemplars: &[Exemplar]) -> Result<()> {
    if exemplars.is_empty() {
        builder.append_null();
        return Ok(());
    }
    write_str_builder(builder, "[")?;
    for (idx, exemplar) in exemplars.iter().enumerate() {
        if idx > 0 {
            write_str_builder(builder, ",")?;
        }
        write_str_builder(builder, "{\"filtered_attributes\":")?;
        write_attrs_object(builder, &exemplar.filtered_attributes)?;
        write_str_builder(builder, ",\"span_id\":")?;
        write_hex_json(builder, &exemplar.span_id)?;
        write_str_builder(builder, ",\"time_unix_nano\":")?;
        write_i64_builder(
            builder,
            u64_to_i64(exemplar.time_unix_nano, "exemplar.time_unix_nano")?,
        )?;
        write_str_builder(builder, ",\"trace_id\":")?;
        write_hex_json(builder, &exemplar.trace_id)?;
        if exemplar_value_is_finite(&exemplar.value) {
            write_str_builder(builder, ",\"value\":")?;
            write_exemplar_value_json(builder, &exemplar.value)?;
        }
        write_str_builder(builder, "}")?;
    }
    write_str_builder(builder, "]")?;
    builder.append_value("");
    Ok(())
}

fn append_span_events_json(builder: &mut StringBuilder, events: &[span::Event]) -> Result<()> {
    if events.is_empty() {
        builder.append_null();
        return Ok(());
    }
    write_str_builder(builder, "[")?;
    for (idx, event) in events.iter().enumerate() {
        if idx > 0 {
            write_str_builder(builder, ",")?;
        }
        write_str_builder(builder, "{\"attributes\":")?;
        write_attrs_object(builder, &event.attributes)?;
        write_str_builder(builder, ",\"name\":")?;
        write_json_string(builder, &event.name)?;
        write_str_builder(builder, ",\"time_unix_nano\":")?;
        write_i64_builder(
            builder,
            u64_to_i64(event.time_unix_nano, "event.time_unix_nano")?,
        )?;
        write_str_builder(builder, "}")?;
    }
    write_str_builder(builder, "]")?;
    builder.append_value("");
    Ok(())
}

fn append_span_links_json(builder: &mut StringBuilder, links: &[span::Link]) -> Result<()> {
    if links.is_empty() {
        builder.append_null();
        return Ok(());
    }
    write_str_builder(builder, "[")?;
    for (idx, link) in links.iter().enumerate() {
        if idx > 0 {
            write_str_builder(builder, ",")?;
        }
        write_str_builder(builder, "{\"attributes\":")?;
        write_attrs_object(builder, &link.attributes)?;
        write_str_builder(builder, ",\"span_id\":")?;
        write_hex_json(builder, &link.span_id)?;
        write_str_builder(builder, ",\"trace_id\":")?;
        write_hex_json(builder, &link.trace_id)?;
        write_str_builder(builder, ",\"trace_state\":")?;
        write_json_string(builder, &link.trace_state)?;
        write_str_builder(builder, "}")?;
    }
    write_str_builder(builder, "]")?;
    builder.append_value("");
    Ok(())
}

#[inline]
fn append_log_body(builder: &mut StringBuilder, value: Option<&AnyValue>) -> Result<()> {
    let Some(value) = value.and_then(|value| value.value.as_ref()) else {
        builder.append_null();
        return Ok(());
    };

    match value {
        any_value::Value::StringValue(value) if value.is_empty() => builder.append_null(),
        any_value::Value::StringValue(value) => builder.append_value(value),
        any_value::Value::BoolValue(value) => {
            write_str_builder(builder, if *value { "true" } else { "false" })?;
            builder.append_value("");
        }
        any_value::Value::IntValue(value) => {
            write_i64_builder(builder, *value)?;
            builder.append_value("");
        }
        any_value::Value::DoubleValue(value) if value.is_finite() => {
            serde_json::to_writer(BuilderWriter(builder), value)?;
            builder.append_value("");
        }
        any_value::Value::DoubleValue(_) => builder.append_null(),
        any_value::Value::BytesValue(value) if value.is_empty() => builder.append_null(),
        any_value::Value::BytesValue(value) => {
            let value = String::from_utf8_lossy(value);
            builder.append_value(&value);
        }
        any_value::Value::ArrayValue(_) | any_value::Value::KvlistValue(_) => {
            write_any_json(builder, value)?;
            builder.append_value("");
        }
    }
    Ok(())
}

#[inline]
fn append_attrs_json(builder: &mut StringBuilder, attrs: &[KeyValue]) -> Result<()> {
    if !attrs.iter().any(|kv| kv.value.is_some()) {
        builder.append_null();
        return Ok(());
    }
    write_attrs_object(builder, attrs)?;
    builder.append_value("");
    Ok(())
}

#[inline]
fn attrs_json(attrs: &[KeyValue]) -> Option<String> {
    if !attrs.iter().any(|kv| kv.value.is_some()) {
        None
    } else {
        Some(serde_json::to_string(&AttrsJson(attrs)).expect("attributes serialize to JSON"))
    }
}

#[inline]
fn attr_field(attrs: &[KeyValue], key: &str) -> Option<String> {
    attrs
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref())
        .and_then(any_to_arrow_string)
        .filter(|value| !value.is_empty())
}

#[inline]
fn any_to_arrow_string(value: &AnyValue) -> Option<String> {
    match value.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(value.clone()),
        any_value::Value::BoolValue(value) => Some(value.to_string()),
        any_value::Value::IntValue(value) => Some(value.to_string()),
        any_value::Value::DoubleValue(value) if value.is_finite() => Some(value.to_string()),
        any_value::Value::DoubleValue(_) => None,
        any_value::Value::BytesValue(value) => Some(String::from_utf8_lossy(value).into_owned()),
        any_value::Value::ArrayValue(_) | any_value::Value::KvlistValue(_) => {
            Some(any_json_string(value.value.as_ref().unwrap()))
        }
    }
}

#[inline]
fn any_json_string(value: &any_value::Value) -> String {
    serde_json::to_string(&AnyJson(value)).expect("OTLP AnyValue serializes to JSON")
}

#[inline]
fn write_any_json(builder: &mut StringBuilder, value: &any_value::Value) -> Result<()> {
    match value {
        any_value::Value::StringValue(value) => write_json_string(builder, value),
        any_value::Value::BoolValue(value) => {
            write_str_builder(builder, if *value { "true" } else { "false" })
        }
        any_value::Value::IntValue(value) => write_i64_builder(builder, *value),
        any_value::Value::DoubleValue(value) if value.is_finite() => {
            serde_json::to_writer(BuilderWriter(builder), value)?;
            Ok(())
        }
        any_value::Value::DoubleValue(_) => write_str_builder(builder, "null"),
        any_value::Value::BytesValue(value) => {
            let value = String::from_utf8_lossy(value);
            write_json_string(builder, &value)
        }
        any_value::Value::ArrayValue(value) => {
            write_str_builder(builder, "[")?;
            for (idx, item) in value.values.iter().enumerate() {
                if idx > 0 {
                    write_str_builder(builder, ",")?;
                }
                if let Some(value) = item.value.as_ref() {
                    write_any_json(builder, value)?;
                } else {
                    write_str_builder(builder, "null")?;
                }
            }
            write_str_builder(builder, "]")
        }
        any_value::Value::KvlistValue(value) => write_attrs_object(builder, &value.values),
    }
}

#[inline]
fn write_attrs_object(builder: &mut StringBuilder, attrs: &[KeyValue]) -> Result<()> {
    if attrs_are_sorted_unique(attrs) {
        write_str_builder(builder, "{")?;
        let mut written = false;
        for kv in attrs {
            if let Some(value) = kv.value.as_ref().and_then(|value| value.value.as_ref()) {
                if any_omits_object_field(value) {
                    continue;
                }
                if written {
                    write_str_builder(builder, ",")?;
                }
                write_json_string(builder, &kv.key)?;
                write_str_builder(builder, ":")?;
                write_any_json(builder, value)?;
                written = true;
            }
        }
        write_str_builder(builder, "}")?;
        return Ok(());
    }

    let mut values: Vec<_> = attrs
        .iter()
        .filter_map(|kv| {
            kv.value
                .as_ref()
                .and_then(|value| value.value.as_ref())
                .map(|value| (kv.key.as_str(), value))
        })
        .collect();
    values.sort_unstable_by(|left, right| left.0.cmp(right.0));

    write_str_builder(builder, "{")?;
    let mut idx = 0;
    let mut written = false;
    while idx < values.len() {
        let key = values[idx].0;
        let mut next = idx + 1;
        while next < values.len() && values[next].0 == key {
            next += 1;
        }
        let value = values[next - 1].1;
        if !any_omits_object_field(value) {
            if written {
                write_str_builder(builder, ",")?;
            }
            write_json_string(builder, key)?;
            write_str_builder(builder, ":")?;
            write_any_json(builder, value)?;
            written = true;
        }
        idx = next;
    }
    write_str_builder(builder, "}")?;
    Ok(())
}

#[inline]
fn write_json_string(builder: &mut StringBuilder, value: &str) -> Result<()> {
    if !value
        .as_bytes()
        .iter()
        .any(|byte| *byte < 0x20 || *byte == b'"' || *byte == b'\\')
    {
        write_str_builder(builder, "\"")?;
        write_str_builder(builder, value)?;
        write_str_builder(builder, "\"")?;
        return Ok(());
    }

    serde_json::to_writer(BuilderWriter(builder), value)?;
    Ok(())
}

#[inline]
fn write_i64_builder(builder: &mut StringBuilder, value: i64) -> Result<()> {
    std::fmt::Write::write_fmt(builder, format_args!("{value}")).map_err(|_| {
        Error::InvalidInput("failed to write integer to Arrow string builder".to_string())
    })
}

#[inline]
fn write_exemplar_value_json(
    builder: &mut StringBuilder,
    value: &Option<exemplar::Value>,
) -> Result<()> {
    match value {
        Some(exemplar::Value::AsInt(value)) => {
            serde_json::to_writer(BuilderWriter(builder), &(*value as f64))?;
            Ok(())
        }
        Some(exemplar::Value::AsDouble(value)) if value.is_finite() => {
            serde_json::to_writer(BuilderWriter(builder), value)?;
            Ok(())
        }
        _ => write_str_builder(builder, "null"),
    }
}

#[inline]
fn write_hex_json(builder: &mut StringBuilder, bytes: &[u8]) -> Result<()> {
    write_str_builder(builder, "\"")?;
    if bytes.len() <= 32 {
        let mut buf = [0_u8; 64];
        let hex = encode_to_str(bytes, &mut buf[..bytes.len() * 2])
            .expect("hex buffer length is exactly input length * 2");
        write_str_builder(builder, hex)?;
    } else {
        write_str_builder(builder, &hex_encode(bytes))?;
    }
    write_str_builder(builder, "\"")
}

#[inline]
fn write_str_builder(builder: &mut StringBuilder, value: &str) -> Result<()> {
    std::fmt::Write::write_str(builder, value).map_err(|_| {
        Error::InvalidInput("failed to write string to Arrow string builder".to_string())
    })
}

struct BuilderWriter<'a>(&'a mut StringBuilder);

impl io::Write for BuilderWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let text = std::str::from_utf8(buf).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("JSON serializer wrote invalid UTF-8: {err}"),
            )
        })?;
        std::fmt::Write::write_str(self.0, text).map_err(io::Error::other)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct AttrsJson<'a>(&'a [KeyValue]);

impl Serialize for AttrsJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        if attrs_are_sorted_unique(self.0) {
            let mut map = serializer.serialize_map(None)?;
            for kv in self.0 {
                if let Some(value) = kv.value.as_ref().and_then(|value| value.value.as_ref()) {
                    if !any_omits_object_field(value) {
                        map.serialize_entry(kv.key.as_str(), &AnyJson(value))?;
                    }
                }
            }
            return map.end();
        }

        let mut values: Vec<_> = self
            .0
            .iter()
            .filter_map(|kv| {
                kv.value
                    .as_ref()
                    .and_then(|value| value.value.as_ref())
                    .map(|value| (kv.key.as_str(), value))
            })
            .collect();
        values.sort_unstable_by(|left, right| left.0.cmp(right.0));

        let mut map = serializer.serialize_map(None)?;
        let mut idx = 0;
        while idx < values.len() {
            let key = values[idx].0;
            let mut next = idx + 1;
            while next < values.len() && values[next].0 == key {
                next += 1;
            }
            let value = values[next - 1].1;
            if !any_omits_object_field(value) {
                map.serialize_entry(key, &AnyJson(value))?;
            }
            idx = next;
        }
        map.end()
    }
}

#[inline]
fn attrs_are_sorted_unique(attrs: &[KeyValue]) -> bool {
    let mut last_key = None;
    for kv in attrs {
        if kv.value.is_none() {
            continue;
        }
        if let Some(last_key) = last_key {
            if last_key >= kv.key.as_str() {
                return false;
            }
        }
        last_key = Some(kv.key.as_str());
    }
    true
}

struct AnyJson<'a>(&'a any_value::Value);

impl Serialize for AnyJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self.0 {
            any_value::Value::StringValue(value) => serializer.serialize_str(value),
            any_value::Value::BoolValue(value) => serializer.serialize_bool(*value),
            any_value::Value::IntValue(value) => serializer.serialize_i64(*value),
            any_value::Value::DoubleValue(value) if value.is_finite() => {
                serializer.serialize_f64(*value)
            }
            any_value::Value::DoubleValue(_) => serializer.serialize_unit(),
            any_value::Value::BytesValue(value) => {
                let value = String::from_utf8_lossy(value);
                serializer.serialize_str(&value)
            }
            any_value::Value::ArrayValue(value) => {
                let mut seq = serializer.serialize_seq(Some(value.values.len()))?;
                for item in &value.values {
                    match item.value.as_ref() {
                        Some(value) => seq.serialize_element(&AnyJson(value))?,
                        None => seq.serialize_element(&None::<()>)?,
                    }
                }
                seq.end()
            }
            any_value::Value::KvlistValue(value) => AttrsJson(&value.values).serialize(serializer),
        }
    }
}

#[inline]
fn any_omits_object_field(value: &any_value::Value) -> bool {
    matches!(value, any_value::Value::DoubleValue(value) if !value.is_finite())
}

#[inline]
fn exemplar_value_is_finite(value: &Option<exemplar::Value>) -> bool {
    match value {
        Some(exemplar::Value::AsInt(_)) => true,
        Some(exemplar::Value::AsDouble(value)) => value.is_finite(),
        None => false,
    }
}

#[inline]
fn non_empty_str(value: Option<&str>) -> Option<String> {
    value
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[inline]
fn append_opt(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

#[inline]
fn append_required_service_name(builder: &mut StringBuilder, value: Option<&str>) {
    builder.append_value(value.unwrap_or("unknown"));
}

#[inline]
fn append_empty_as_null(builder: &mut StringBuilder, value: &str) {
    if value.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(value);
    }
}

#[inline]
fn append_hex_or_null(builder: &mut StringBuilder, bytes: &[u8]) {
    if bytes.is_empty() {
        builder.append_null();
    } else if bytes.len() <= 32 {
        let mut buf = [0_u8; 64];
        let hex = encode_to_str(bytes, &mut buf[..bytes.len() * 2])
            .expect("hex buffer length is exactly input length * 2");
        builder.append_value(hex);
    } else {
        builder.append_value(hex_encode(bytes));
    }
}

#[inline]
fn append_finite(builder: &mut Float64Builder, value: f64) {
    if value.is_finite() {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

#[inline]
fn append_finite_opt(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(value) => append_finite(builder, value),
        None => builder.append_null(),
    }
}

fn json_string_or_empty_array<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "[]".to_string())
}

#[inline]
fn string_builder(rows: usize) -> StringBuilder {
    string_builder_bytes(rows, rows.saturating_mul(32))
}

#[inline]
fn string_builder_bytes(rows: usize, data_capacity: usize) -> StringBuilder {
    StringBuilder::with_capacity(rows, data_capacity)
}

#[inline]
fn record_batch(schema: arrow_schema::Schema, arrays: Vec<ArrayRef>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
}

#[inline]
fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        Error::Decode(DecodeError::Unsupported(format!(
            "timestamp overflow: {field} value {value} exceeds i64::MAX (year 2262)"
        )))
    })
}
