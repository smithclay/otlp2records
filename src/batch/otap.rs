//! OTAP star request-to-RecordBatch conversion.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use arrow_array::{
    builder::{
        BinaryBuilder, BooleanBuilder, DurationNanosecondBuilder, FixedSizeBinaryBuilder,
        Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    },
    RecordBatch,
};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value, AnyValue, KeyValue},
    logs::v1::LogRecord,
    metrics::v1::{
        exemplar, metric::Data, number_data_point, Exemplar, ExponentialHistogramDataPoint,
        HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
    },
    trace::v1::Span,
};
use prost::Message;

use crate::{
    api::{OtapLogsBatches, OtapMetricsBatches, OtapTracesBatches, SkippedMetrics},
    otap::{
        VALUE_BOOL, VALUE_BYTES, VALUE_EMPTY, VALUE_F64, VALUE_I64, VALUE_MAP, VALUE_SLICE,
        VALUE_STR,
    },
    schema::{
        otap_attrs_u16_schema_arc, otap_attrs_u32_schema_arc, otap_exemplars_schema_arc,
        otap_exp_histogram_data_points_schema_arc, otap_histogram_data_points_schema_arc,
        otap_logs_schema_arc, otap_metrics_schema_arc, otap_number_data_points_schema_arc,
        otap_quantile_schema_arc, otap_span_events_schema_arc, otap_span_links_schema_arc,
        otap_spans_schema_arc, otap_summary_data_points_schema_arc,
    },
    DecodeError, Error, Result,
};

use super::context::hash_attrs;
use super::profile::{
    finish_phase, observe_counter, observe_phase, phase_start, TransformCounter, TransformObserver,
    TransformPhase, TransformSignal,
};
use super::util::{
    append_f64_list, append_finite_opt, append_fixed_or_null, append_fixed_required,
    append_opt_ts_ns, append_required_ts_ns, append_u64_list, array, record_batch, u64_to_i64,
};

const METRIC_GAUGE: u8 = 1;
const METRIC_SUM: u8 = 2;
const METRIC_HISTOGRAM: u8 = 3;
const METRIC_EXP_HISTOGRAM: u8 = 4;
const METRIC_SUMMARY: u8 = 5;

pub fn transform_logs_request_otap(request: ExportLogsServiceRequest) -> Result<OtapLogsBatches> {
    transform_logs_request_otap_observed(request, &mut None)
}

pub fn transform_logs_request_otap_observed(
    request: ExportLogsServiceRequest,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<OtapLogsBatches> {
    let signal = TransformSignal::Logs;
    let init_start = phase_start(observer);
    let mut builders = LogsBuilders::default();
    finish_phase(observer, signal, TransformPhase::BuilderInit, init_start);

    for resource_logs in request.resource_logs {
        let res_start = phase_start(observer);
        let resource_attrs = resource_logs
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        let resource_schema_url = empty_to_none(&resource_logs.schema_url);
        let resource_dropped = resource_logs
            .resource
            .as_ref()
            .map(|r| r.dropped_attributes_count);
        let (resource_id, resource_is_new) =
            builders.resource_id(resource_attrs, resource_schema_url, resource_dropped)?;
        observe_dedup(observer, signal, DedupKind::Resource, resource_is_new);
        if resource_is_new {
            builders
                .resource_attrs
                .append_attrs(resource_id, resource_attrs)?;
        }

        for scope_logs in resource_logs.scope_logs {
            let scope_start = phase_start(observer);
            let scope = scope_logs.scope.as_ref();
            let scope_attrs = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
            let scope_name = scope.and_then(|s| empty_to_none(&s.name));
            let scope_version = scope.and_then(|s| empty_to_none(&s.version));
            let scope_dropped = scope.map(|s| s.dropped_attributes_count);
            let schema_url = empty_to_none(&scope_logs.schema_url);
            let (scope_id, scope_is_new) = builders.scope_id(
                scope_name,
                scope_version,
                scope_attrs,
                schema_url,
                scope_dropped,
            )?;
            observe_dedup(observer, signal, DedupKind::Scope, scope_is_new);
            if scope_is_new {
                builders.scope_attrs.append_attrs(scope_id, scope_attrs)?;
            }

            for record in scope_logs.log_records {
                let rec_start = phase_start(observer);
                let id = builders.next_log_id()?;
                builders.logs.append(
                    id,
                    resource_id,
                    resource_schema_url,
                    resource_dropped,
                    scope_id,
                    scope_name,
                    scope_version,
                    scope_dropped,
                    schema_url,
                    &record,
                )?;
                builders.log_attrs.append_attrs(id, &record.attributes)?;
                finish_phase(observer, signal, TransformPhase::LogRecordBuild, rec_start);
            }
            finish_phase(
                observer,
                signal,
                TransformPhase::ScopeLogsBuild,
                scope_start,
            );
        }
        finish_phase(
            observer,
            signal,
            TransformPhase::ResourceLogsBuild,
            res_start,
        );
    }

    let finalize_start = phase_start(observer);
    let batches = builders.finish()?;
    finish_phase(
        observer,
        signal,
        TransformPhase::ArrowFinalize,
        finalize_start,
    );
    observe_counter(
        observer,
        signal,
        TransformCounter::OutputRows,
        batches.logs.num_rows() as u64,
    );
    Ok(batches)
}

pub fn transform_logs_protobuf_otap(bytes: &[u8]) -> Result<OtapLogsBatches> {
    transform_logs_protobuf_otap_observed(bytes, &mut None)
}

pub fn transform_logs_protobuf_otap_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<OtapLogsBatches> {
    let start = Instant::now();
    let request = ExportLogsServiceRequest::decode(bytes)?;
    observe_phase(
        observer,
        TransformSignal::Logs,
        TransformPhase::ProtobufDecode,
        start.elapsed(),
    );
    transform_logs_request_otap_observed(request, observer)
}

pub fn transform_traces_request_otap(
    request: ExportTraceServiceRequest,
) -> Result<OtapTracesBatches> {
    transform_traces_request_otap_observed(request, &mut None)
}

pub fn transform_traces_request_otap_observed(
    request: ExportTraceServiceRequest,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<OtapTracesBatches> {
    let signal = TransformSignal::Traces;
    let init_start = phase_start(observer);
    let mut builders = TracesBuilders::default();
    finish_phase(observer, signal, TransformPhase::BuilderInit, init_start);

    for resource_spans in request.resource_spans {
        let res_start = phase_start(observer);
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        let resource_schema_url = empty_to_none(&resource_spans.schema_url);
        let resource_dropped = resource_spans
            .resource
            .as_ref()
            .map(|r| r.dropped_attributes_count);
        let (resource_id, resource_is_new) =
            builders.resource_id(resource_attrs, resource_schema_url, resource_dropped)?;
        observe_dedup(observer, signal, DedupKind::Resource, resource_is_new);
        if resource_is_new {
            builders
                .resource_attrs
                .append_attrs(resource_id, resource_attrs)?;
        }

        for scope_spans in resource_spans.scope_spans {
            let scope_start = phase_start(observer);
            let scope = scope_spans.scope.as_ref();
            let scope_attrs = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
            let scope_name = scope.and_then(|s| empty_to_none(&s.name));
            let scope_version = scope.and_then(|s| empty_to_none(&s.version));
            let scope_dropped = scope.map(|s| s.dropped_attributes_count);
            let schema_url = empty_to_none(&scope_spans.schema_url);
            let (scope_id, scope_is_new) = builders.scope_id(
                scope_name,
                scope_version,
                scope_attrs,
                schema_url,
                scope_dropped,
            )?;
            observe_dedup(observer, signal, DedupKind::Scope, scope_is_new);
            if scope_is_new {
                builders.scope_attrs.append_attrs(scope_id, scope_attrs)?;
            }

            for span in scope_spans.spans {
                let span_start = phase_start(observer);
                let id = builders.next_span_id()?;
                builders.spans.append(
                    id,
                    resource_id,
                    resource_schema_url,
                    resource_dropped,
                    scope_id,
                    scope_name,
                    scope_version,
                    scope_dropped,
                    schema_url,
                    &span,
                )?;
                builders.span_attrs.append_attrs(id, &span.attributes)?;

                for event in &span.events {
                    let event_id = builders.next_event_id()?;
                    builders.span_events.append(event_id, id, event)?;
                    builders
                        .span_event_attrs
                        .append_attrs(event_id, &event.attributes)?;
                }
                for link in &span.links {
                    let link_id = builders.next_link_id()?;
                    builders.span_links.append(link_id, id, link)?;
                    builders
                        .span_link_attrs
                        .append_attrs(link_id, &link.attributes)?;
                }
                finish_phase(observer, signal, TransformPhase::SpanBuild, span_start);
            }
            finish_phase(
                observer,
                signal,
                TransformPhase::ScopeSpansBuild,
                scope_start,
            );
        }
        finish_phase(
            observer,
            signal,
            TransformPhase::ResourceSpansBuild,
            res_start,
        );
    }

    let finalize_start = phase_start(observer);
    let batches = builders.finish()?;
    finish_phase(
        observer,
        signal,
        TransformPhase::ArrowFinalize,
        finalize_start,
    );
    observe_counter(
        observer,
        signal,
        TransformCounter::OutputRows,
        batches.spans.num_rows() as u64,
    );
    Ok(batches)
}

pub fn transform_traces_protobuf_otap(bytes: &[u8]) -> Result<OtapTracesBatches> {
    transform_traces_protobuf_otap_observed(bytes, &mut None)
}

pub fn transform_traces_protobuf_otap_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<OtapTracesBatches> {
    let start = Instant::now();
    let request = ExportTraceServiceRequest::decode(bytes)?;
    observe_phase(
        observer,
        TransformSignal::Traces,
        TransformPhase::ProtobufDecode,
        start.elapsed(),
    );
    transform_traces_request_otap_observed(request, observer)
}

pub fn transform_metrics_request_otap(
    request: ExportMetricsServiceRequest,
) -> Result<OtapMetricsBatches> {
    transform_metrics_request_otap_observed(request, &mut None)
}

pub fn transform_metrics_request_otap_observed(
    request: ExportMetricsServiceRequest,
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<OtapMetricsBatches> {
    let signal = TransformSignal::Metrics;
    let init_start = phase_start(observer);
    let mut builders = MetricsBuilders::default();
    finish_phase(observer, signal, TransformPhase::BuilderInit, init_start);

    for resource_metrics in request.resource_metrics {
        let res_start = phase_start(observer);
        let resource_attrs = resource_metrics
            .resource
            .as_ref()
            .map(|r| r.attributes.as_slice())
            .unwrap_or(&[]);
        let resource_schema_url = empty_to_none(&resource_metrics.schema_url);
        let resource_dropped = resource_metrics
            .resource
            .as_ref()
            .map(|r| r.dropped_attributes_count);
        let (resource_id, resource_is_new) =
            builders.resource_id(resource_attrs, resource_schema_url, resource_dropped)?;
        observe_dedup(observer, signal, DedupKind::Resource, resource_is_new);
        if resource_is_new {
            builders
                .resource_attrs
                .append_attrs(resource_id, resource_attrs)?;
        }

        for scope_metrics in resource_metrics.scope_metrics {
            let scope_start = phase_start(observer);
            let scope = scope_metrics.scope.as_ref();
            let scope_attrs = scope.map(|s| s.attributes.as_slice()).unwrap_or(&[]);
            let scope_name = scope.and_then(|s| empty_to_none(&s.name));
            let scope_version = scope.and_then(|s| empty_to_none(&s.version));
            let scope_dropped = scope.map(|s| s.dropped_attributes_count);
            let schema_url = empty_to_none(&scope_metrics.schema_url);
            let (scope_id, scope_is_new) = builders.scope_id(
                scope_name,
                scope_version,
                scope_attrs,
                schema_url,
                scope_dropped,
            )?;
            observe_dedup(observer, signal, DedupKind::Scope, scope_is_new);
            if scope_is_new {
                builders.scope_attrs.append_attrs(scope_id, scope_attrs)?;
            }

            for metric in scope_metrics.metrics {
                let metric_start = phase_start(observer);
                // OTLP allows a Metric to carry no data variant. We skip the
                // row entirely rather than emit one with metric_type=0 (which
                // is not a valid OTAP type code). See B6.
                let Some(metric_type) = metric_type(&metric.data) else {
                    continue;
                };
                let id = builders.next_metric_id()?;
                let aggregation_temporality = aggregation_temporality(&metric.data);
                let is_monotonic = is_monotonic(&metric.data);
                builders.metrics.append(
                    id,
                    resource_id,
                    resource_schema_url,
                    resource_dropped,
                    scope_id,
                    scope_name,
                    scope_version,
                    scope_dropped,
                    schema_url,
                    metric_type,
                    &metric.name,
                    empty_to_none(&metric.description),
                    empty_to_none(&metric.unit),
                    aggregation_temporality,
                    is_monotonic,
                );

                match metric.data {
                    Some(Data::Gauge(data)) => {
                        for point in data.data_points {
                            append_number_point(&mut builders, id, &point)?;
                        }
                    }
                    Some(Data::Sum(data)) => {
                        for point in data.data_points {
                            append_number_point(&mut builders, id, &point)?;
                        }
                    }
                    Some(Data::Histogram(data)) => {
                        for point in data.data_points {
                            let dp_id = builders.next_histogram_dp_id()?;
                            builders.histogram_data_points.append(dp_id, id, &point)?;
                            builders
                                .histogram_dp_attrs
                                .append_attrs(dp_id, &point.attributes)?;
                            append_exemplars(
                                &mut builders.histogram_dp_exemplars,
                                &mut builders.histogram_dp_exemplar_attrs,
                                &mut builders.next_histogram_exemplar_id,
                                dp_id,
                                &point.exemplars,
                            )?;
                        }
                    }
                    Some(Data::ExponentialHistogram(data)) => {
                        for point in data.data_points {
                            let dp_id = builders.next_exp_histogram_dp_id()?;
                            builders
                                .exp_histogram_data_points
                                .append(dp_id, id, &point)?;
                            builders
                                .exp_histogram_dp_attrs
                                .append_attrs(dp_id, &point.attributes)?;
                            append_exemplars(
                                &mut builders.exp_histogram_dp_exemplars,
                                &mut builders.exp_histogram_dp_exemplar_attrs,
                                &mut builders.next_exp_histogram_exemplar_id,
                                dp_id,
                                &point.exemplars,
                            )?;
                        }
                    }
                    Some(Data::Summary(data)) => {
                        for point in data.data_points {
                            let dp_id = builders.next_summary_dp_id()?;
                            builders.summary_data_points.append(dp_id, id, &point)?;
                            builders
                                .summary_dp_attrs
                                .append_attrs(dp_id, &point.attributes)?;
                            for quantile in &point.quantile_values {
                                builders
                                    .quantile
                                    .append(dp_id, quantile.quantile, quantile.value);
                            }
                        }
                    }
                    None => {}
                }
                finish_phase(observer, signal, TransformPhase::MetricBuild, metric_start);
            }
            finish_phase(
                observer,
                signal,
                TransformPhase::ScopeMetricsBuild,
                scope_start,
            );
        }
        finish_phase(
            observer,
            signal,
            TransformPhase::ResourceMetricsBuild,
            res_start,
        );
    }

    let finalize_start = phase_start(observer);
    let batches = builders.finish()?;
    finish_phase(
        observer,
        signal,
        TransformPhase::ArrowFinalize,
        finalize_start,
    );
    observe_counter(
        observer,
        signal,
        TransformCounter::OutputRows,
        batches.metrics.num_rows() as u64,
    );
    Ok(batches)
}

pub fn transform_metrics_protobuf_otap(bytes: &[u8]) -> Result<OtapMetricsBatches> {
    transform_metrics_protobuf_otap_observed(bytes, &mut None)
}

pub fn transform_metrics_protobuf_otap_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
) -> Result<OtapMetricsBatches> {
    let start = Instant::now();
    let request = ExportMetricsServiceRequest::decode(bytes)?;
    observe_phase(
        observer,
        TransformSignal::Metrics,
        TransformPhase::ProtobufDecode,
        start.elapsed(),
    );
    transform_metrics_request_otap_observed(request, observer)
}

fn append_number_point(
    builders: &mut MetricsBuilders,
    metric_id: u16,
    point: &NumberDataPoint,
) -> Result<()> {
    // Reject NaN / Infinity / missing values, bumping the SkippedMetrics
    // counters that the normalized path also tracks (B2). Skipped points do
    // not allocate a dp_id or emit any child rows.
    if !validate_number_value(&point.value, &mut builders.skipped) {
        return Ok(());
    }
    let dp_id = builders.next_number_dp_id()?;
    builders
        .number_data_points
        .append(dp_id, metric_id, point)?;
    builders
        .number_dp_attrs
        .append_attrs(dp_id, &point.attributes)?;
    append_exemplars(
        &mut builders.number_dp_exemplars,
        &mut builders.number_dp_exemplar_attrs,
        &mut builders.next_number_exemplar_id,
        dp_id,
        &point.exemplars,
    )
}

fn validate_number_value(
    value: &Option<number_data_point::Value>,
    skipped: &mut SkippedMetrics,
) -> bool {
    match value {
        Some(number_data_point::Value::AsInt(_)) => true,
        Some(number_data_point::Value::AsDouble(v)) if v.is_nan() => {
            skipped.nan_values += 1;
            false
        }
        Some(number_data_point::Value::AsDouble(v)) if v.is_infinite() => {
            skipped.infinity_values += 1;
            false
        }
        Some(number_data_point::Value::AsDouble(_)) => true,
        None => {
            skipped.missing_values += 1;
            false
        }
    }
}

fn append_exemplars(
    exemplars: &mut ExemplarBuilders,
    attrs: &mut AttrBuildersU32,
    next_id: &mut u32,
    parent_id: u32,
    values: &[Exemplar],
) -> Result<()> {
    for exemplar in values {
        let id = next_u32(next_id, "exemplar.id")?;
        exemplars.append(id, parent_id, exemplar)?;
        attrs.append_attrs(id, &exemplar.filtered_attributes)?;
    }
    Ok(())
}

#[derive(Default)]
struct LogsBuilders {
    next_resource_id: u32,
    next_scope_id: u32,
    next_id: u32,
    resource_ids: ContextIdMap,
    scope_ids: ContextIdMap,
    logs: LogTableBuilder,
    resource_attrs: AttrBuildersU16,
    scope_attrs: AttrBuildersU16,
    log_attrs: AttrBuildersU16,
}

impl LogsBuilders {
    fn resource_id(
        &mut self,
        attrs: &[KeyValue],
        schema_url: Option<&str>,
        dropped: Option<u32>,
    ) -> Result<(u16, bool)> {
        let fp = resource_fingerprint(attrs, schema_url, dropped);
        self.resource_ids
            .lookup_or_insert(fp, &mut self.next_resource_id, "resource.id")
    }

    fn scope_id(
        &mut self,
        name: Option<&str>,
        version: Option<&str>,
        attrs: &[KeyValue],
        schema_url: Option<&str>,
        dropped: Option<u32>,
    ) -> Result<(u16, bool)> {
        let fp = scope_fingerprint(name, version, attrs, schema_url, dropped);
        self.scope_ids
            .lookup_or_insert(fp, &mut self.next_scope_id, "scope.id")
    }

    fn next_log_id(&mut self) -> Result<u16> {
        next_u16(&mut self.next_id, "log.id")
    }

    fn finish(self) -> Result<OtapLogsBatches> {
        Ok(OtapLogsBatches {
            logs: self.logs.finish()?,
            resource_attrs: self.resource_attrs.finish_u16()?,
            scope_attrs: self.scope_attrs.finish_u16()?,
            log_attrs: self.log_attrs.finish_u16()?,
        })
    }
}

#[derive(Default)]
struct TracesBuilders {
    next_resource_id: u32,
    next_scope_id: u32,
    next_span_id: u32,
    next_event_id: u32,
    next_link_id: u32,
    resource_ids: ContextIdMap,
    scope_ids: ContextIdMap,
    spans: SpanTableBuilder,
    resource_attrs: AttrBuildersU16,
    scope_attrs: AttrBuildersU16,
    span_attrs: AttrBuildersU16,
    span_events: SpanEventBuilders,
    span_event_attrs: AttrBuildersU32,
    span_links: SpanLinkBuilders,
    span_link_attrs: AttrBuildersU32,
}

impl TracesBuilders {
    fn resource_id(
        &mut self,
        attrs: &[KeyValue],
        schema_url: Option<&str>,
        dropped: Option<u32>,
    ) -> Result<(u16, bool)> {
        let fp = resource_fingerprint(attrs, schema_url, dropped);
        self.resource_ids
            .lookup_or_insert(fp, &mut self.next_resource_id, "resource.id")
    }
    fn scope_id(
        &mut self,
        name: Option<&str>,
        version: Option<&str>,
        attrs: &[KeyValue],
        schema_url: Option<&str>,
        dropped: Option<u32>,
    ) -> Result<(u16, bool)> {
        let fp = scope_fingerprint(name, version, attrs, schema_url, dropped);
        self.scope_ids
            .lookup_or_insert(fp, &mut self.next_scope_id, "scope.id")
    }
    fn next_span_id(&mut self) -> Result<u16> {
        next_u16(&mut self.next_span_id, "span.id")
    }
    fn next_event_id(&mut self) -> Result<u32> {
        next_u32(&mut self.next_event_id, "span_event.id")
    }
    fn next_link_id(&mut self) -> Result<u32> {
        next_u32(&mut self.next_link_id, "span_link.id")
    }
    fn finish(self) -> Result<OtapTracesBatches> {
        Ok(OtapTracesBatches {
            spans: self.spans.finish()?,
            resource_attrs: self.resource_attrs.finish_u16()?,
            scope_attrs: self.scope_attrs.finish_u16()?,
            span_attrs: self.span_attrs.finish_u16()?,
            span_events: self.span_events.finish()?,
            span_event_attrs: self.span_event_attrs.finish_u32()?,
            span_links: self.span_links.finish()?,
            span_link_attrs: self.span_link_attrs.finish_u32()?,
        })
    }
}

#[derive(Default)]
struct MetricsBuilders {
    next_resource_id: u32,
    next_scope_id: u32,
    next_metric_id: u32,
    next_number_dp_id: u32,
    next_number_exemplar_id: u32,
    next_summary_dp_id: u32,
    next_histogram_dp_id: u32,
    next_histogram_exemplar_id: u32,
    next_exp_histogram_dp_id: u32,
    next_exp_histogram_exemplar_id: u32,
    resource_ids: ContextIdMap,
    scope_ids: ContextIdMap,
    metrics: MetricTableBuilder,
    resource_attrs: AttrBuildersU16,
    scope_attrs: AttrBuildersU16,
    number_data_points: NumberDataPointBuilders,
    number_dp_attrs: AttrBuildersU32,
    number_dp_exemplars: ExemplarBuilders,
    number_dp_exemplar_attrs: AttrBuildersU32,
    summary_data_points: SummaryDataPointBuilders,
    quantile: QuantileBuilders,
    summary_dp_attrs: AttrBuildersU32,
    histogram_data_points: HistogramDataPointBuilders,
    histogram_dp_attrs: AttrBuildersU32,
    histogram_dp_exemplars: ExemplarBuilders,
    histogram_dp_exemplar_attrs: AttrBuildersU32,
    exp_histogram_data_points: ExpHistogramDataPointBuilders,
    exp_histogram_dp_attrs: AttrBuildersU32,
    exp_histogram_dp_exemplars: ExemplarBuilders,
    exp_histogram_dp_exemplar_attrs: AttrBuildersU32,
    skipped: SkippedMetrics,
}

impl MetricsBuilders {
    fn resource_id(
        &mut self,
        attrs: &[KeyValue],
        schema_url: Option<&str>,
        dropped: Option<u32>,
    ) -> Result<(u16, bool)> {
        let fp = resource_fingerprint(attrs, schema_url, dropped);
        self.resource_ids
            .lookup_or_insert(fp, &mut self.next_resource_id, "resource.id")
    }
    fn scope_id(
        &mut self,
        name: Option<&str>,
        version: Option<&str>,
        attrs: &[KeyValue],
        schema_url: Option<&str>,
        dropped: Option<u32>,
    ) -> Result<(u16, bool)> {
        let fp = scope_fingerprint(name, version, attrs, schema_url, dropped);
        self.scope_ids
            .lookup_or_insert(fp, &mut self.next_scope_id, "scope.id")
    }
    fn next_metric_id(&mut self) -> Result<u16> {
        next_u16(&mut self.next_metric_id, "metric.id")
    }
    fn next_number_dp_id(&mut self) -> Result<u32> {
        next_u32(&mut self.next_number_dp_id, "number_data_point.id")
    }
    fn next_summary_dp_id(&mut self) -> Result<u32> {
        next_u32(&mut self.next_summary_dp_id, "summary_data_point.id")
    }
    fn next_histogram_dp_id(&mut self) -> Result<u32> {
        next_u32(&mut self.next_histogram_dp_id, "histogram_data_point.id")
    }
    fn next_exp_histogram_dp_id(&mut self) -> Result<u32> {
        next_u32(
            &mut self.next_exp_histogram_dp_id,
            "exp_histogram_data_point.id",
        )
    }
    fn finish(self) -> Result<OtapMetricsBatches> {
        Ok(OtapMetricsBatches {
            metrics: self.metrics.finish()?,
            resource_attrs: self.resource_attrs.finish_u16()?,
            scope_attrs: self.scope_attrs.finish_u16()?,
            number_data_points: self.number_data_points.finish()?,
            number_dp_attrs: self.number_dp_attrs.finish_u32()?,
            number_dp_exemplars: self.number_dp_exemplars.finish()?,
            number_dp_exemplar_attrs: self.number_dp_exemplar_attrs.finish_u32()?,
            summary_data_points: self.summary_data_points.finish()?,
            quantile: self.quantile.finish()?,
            summary_dp_attrs: self.summary_dp_attrs.finish_u32()?,
            histogram_data_points: self.histogram_data_points.finish()?,
            histogram_dp_attrs: self.histogram_dp_attrs.finish_u32()?,
            histogram_dp_exemplars: self.histogram_dp_exemplars.finish()?,
            histogram_dp_exemplar_attrs: self.histogram_dp_exemplar_attrs.finish_u32()?,
            exp_histogram_data_points: self.exp_histogram_data_points.finish()?,
            exp_histogram_dp_attrs: self.exp_histogram_dp_attrs.finish_u32()?,
            exp_histogram_dp_exemplars: self.exp_histogram_dp_exemplars.finish()?,
            exp_histogram_dp_exemplar_attrs: self.exp_histogram_dp_exemplar_attrs.finish_u32()?,
            skipped: self.skipped,
        })
    }
}

struct LogTableBuilder {
    id: UInt16Builder,
    resource_id: UInt16Builder,
    resource_schema_url: StringBuilder,
    resource_dropped_attributes_count: UInt32Builder,
    scope_id: UInt16Builder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_dropped_attributes_count: UInt32Builder,
    schema_url: StringBuilder,
    time_unix_nano: TimestampNanosecondBuilder,
    observed_time_unix_nano: TimestampNanosecondBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    severity_number: Int32Builder,
    severity_text: StringBuilder,
    event_name: StringBuilder,
    body: AnyValueBuilders,
    dropped_attributes_count: UInt32Builder,
    flags: UInt32Builder,
}

impl Default for LogTableBuilder {
    fn default() -> Self {
        Self {
            id: UInt16Builder::new(),
            resource_id: UInt16Builder::new(),
            resource_schema_url: StringBuilder::new(),
            resource_dropped_attributes_count: UInt32Builder::new(),
            scope_id: UInt16Builder::new(),
            scope_name: StringBuilder::new(),
            scope_version: StringBuilder::new(),
            scope_dropped_attributes_count: UInt32Builder::new(),
            schema_url: StringBuilder::new(),
            time_unix_nano: TimestampNanosecondBuilder::new(),
            observed_time_unix_nano: TimestampNanosecondBuilder::new(),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            severity_number: Int32Builder::new(),
            severity_text: StringBuilder::new(),
            event_name: StringBuilder::new(),
            body: AnyValueBuilders::new(),
            dropped_attributes_count: UInt32Builder::new(),
            flags: UInt32Builder::new(),
        }
    }
}

impl LogTableBuilder {
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        id: u16,
        resource_id: u16,
        resource_schema_url: Option<&str>,
        resource_dropped: Option<u32>,
        scope_id: u16,
        scope_name: Option<&str>,
        scope_version: Option<&str>,
        scope_dropped: Option<u32>,
        schema_url: Option<&str>,
        record: &LogRecord,
    ) -> Result<()> {
        self.id.append_value(id);
        self.resource_id.append_value(resource_id);
        append_opt_str(&mut self.resource_schema_url, resource_schema_url);
        append_opt_u32(
            &mut self.resource_dropped_attributes_count,
            resource_dropped,
        );
        self.scope_id.append_value(scope_id);
        append_opt_str(&mut self.scope_name, scope_name);
        append_opt_str(&mut self.scope_version, scope_version);
        append_opt_u32(&mut self.scope_dropped_attributes_count, scope_dropped);
        append_opt_str(&mut self.schema_url, schema_url);
        append_opt_ts_ns(
            &mut self.time_unix_nano,
            record.time_unix_nano,
            "log.time_unix_nano",
        )?;
        append_opt_ts_ns(
            &mut self.observed_time_unix_nano,
            record.observed_time_unix_nano,
            "log.observed_time_unix_nano",
        )?;
        append_fixed_or_null(&mut self.trace_id, &record.trace_id, 16)?;
        append_fixed_or_null(&mut self.span_id, &record.span_id, 8)?;
        self.severity_number.append_value(record.severity_number);
        append_opt_str(
            &mut self.severity_text,
            empty_to_none(&record.severity_text),
        );
        append_opt_str(&mut self.event_name, empty_to_none(&record.event_name));
        self.body.append_any(record.body.as_ref())?;
        self.dropped_attributes_count
            .append_value(record.dropped_attributes_count);
        self.flags.append_value(record.flags);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_logs_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.resource_id.finish()),
                array(self.resource_schema_url.finish()),
                array(self.resource_dropped_attributes_count.finish()),
                array(self.scope_id.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_dropped_attributes_count.finish()),
                array(self.schema_url.finish()),
                array(self.time_unix_nano.finish()),
                array(self.observed_time_unix_nano.finish()),
                array(self.trace_id.finish()),
                array(self.span_id.finish()),
                array(self.severity_number.finish()),
                array(self.severity_text.finish()),
                array(self.event_name.finish()),
                array(self.body.type_.finish()),
                array(self.body.str.finish()),
                array(self.body.int.finish()),
                array(self.body.double.finish()),
                array(self.body.bool.finish()),
                array(self.body.bytes.finish()),
                array(self.body.ser.finish()),
                array(self.dropped_attributes_count.finish()),
                array(self.flags.finish()),
            ],
        )
    }
}

struct SpanTableBuilder {
    id: UInt16Builder,
    resource_id: UInt16Builder,
    resource_schema_url: StringBuilder,
    resource_dropped_attributes_count: UInt32Builder,
    scope_id: UInt16Builder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_dropped_attributes_count: UInt32Builder,
    schema_url: StringBuilder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    duration_time_unix_nano: DurationNanosecondBuilder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    trace_state: StringBuilder,
    parent_span_id: FixedSizeBinaryBuilder,
    name: StringBuilder,
    kind: Int32Builder,
    dropped_attributes_count: UInt32Builder,
    dropped_events_count: UInt32Builder,
    dropped_links_count: UInt32Builder,
    status_code: Int32Builder,
    status_status_message: StringBuilder,
    flags: UInt32Builder,
}

impl Default for SpanTableBuilder {
    fn default() -> Self {
        Self {
            id: UInt16Builder::new(),
            resource_id: UInt16Builder::new(),
            resource_schema_url: StringBuilder::new(),
            resource_dropped_attributes_count: UInt32Builder::new(),
            scope_id: UInt16Builder::new(),
            scope_name: StringBuilder::new(),
            scope_version: StringBuilder::new(),
            scope_dropped_attributes_count: UInt32Builder::new(),
            schema_url: StringBuilder::new(),
            start_time_unix_nano: TimestampNanosecondBuilder::new(),
            duration_time_unix_nano: DurationNanosecondBuilder::new(),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            trace_state: StringBuilder::new(),
            parent_span_id: FixedSizeBinaryBuilder::new(8),
            name: StringBuilder::new(),
            kind: Int32Builder::new(),
            dropped_attributes_count: UInt32Builder::new(),
            dropped_events_count: UInt32Builder::new(),
            dropped_links_count: UInt32Builder::new(),
            status_code: Int32Builder::new(),
            status_status_message: StringBuilder::new(),
            flags: UInt32Builder::new(),
        }
    }
}

impl SpanTableBuilder {
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        id: u16,
        resource_id: u16,
        resource_schema_url: Option<&str>,
        resource_dropped: Option<u32>,
        scope_id: u16,
        scope_name: Option<&str>,
        scope_version: Option<&str>,
        scope_dropped: Option<u32>,
        schema_url: Option<&str>,
        span: &Span,
    ) -> Result<()> {
        // Compute duration in u64 first so malformed `end < start` spans yield 0
        // rather than a negative Duration value. Matches the normalized path.
        let duration = span
            .end_time_unix_nano
            .saturating_sub(span.start_time_unix_nano);
        self.id.append_value(id);
        self.resource_id.append_value(resource_id);
        append_opt_str(&mut self.resource_schema_url, resource_schema_url);
        append_opt_u32(
            &mut self.resource_dropped_attributes_count,
            resource_dropped,
        );
        self.scope_id.append_value(scope_id);
        append_opt_str(&mut self.scope_name, scope_name);
        append_opt_str(&mut self.scope_version, scope_version);
        append_opt_u32(&mut self.scope_dropped_attributes_count, scope_dropped);
        append_opt_str(&mut self.schema_url, schema_url);
        append_required_ts_ns(
            &mut self.start_time_unix_nano,
            span.start_time_unix_nano,
            "span.start_time_unix_nano",
        )?;
        self.duration_time_unix_nano
            .append_value(u64_to_i64(duration, "span.duration_time_unix_nano")?);
        append_fixed_required(&mut self.trace_id, &span.trace_id, 16, "span.trace_id")?;
        append_fixed_required(&mut self.span_id, &span.span_id, 8, "span.span_id")?;
        append_opt_str(&mut self.trace_state, empty_to_none(&span.trace_state));
        append_fixed_or_null(&mut self.parent_span_id, &span.parent_span_id, 8)?;
        self.name.append_value(&span.name);
        self.kind.append_value(span.kind);
        self.dropped_attributes_count
            .append_value(span.dropped_attributes_count);
        self.dropped_events_count
            .append_value(span.dropped_events_count);
        self.dropped_links_count
            .append_value(span.dropped_links_count);
        if let Some(status) = &span.status {
            self.status_code.append_value(status.code);
            append_opt_str(
                &mut self.status_status_message,
                empty_to_none(&status.message),
            );
        } else {
            self.status_code.append_null();
            self.status_status_message.append_null();
        }
        self.flags.append_value(span.flags);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_spans_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.resource_id.finish()),
                array(self.resource_schema_url.finish()),
                array(self.resource_dropped_attributes_count.finish()),
                array(self.scope_id.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_dropped_attributes_count.finish()),
                array(self.schema_url.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.duration_time_unix_nano.finish()),
                array(self.trace_id.finish()),
                array(self.span_id.finish()),
                array(self.trace_state.finish()),
                array(self.parent_span_id.finish()),
                array(self.name.finish()),
                array(self.kind.finish()),
                array(self.dropped_attributes_count.finish()),
                array(self.dropped_events_count.finish()),
                array(self.dropped_links_count.finish()),
                array(self.status_code.finish()),
                array(self.status_status_message.finish()),
                array(self.flags.finish()),
            ],
        )
    }
}

struct MetricTableBuilder {
    id: UInt16Builder,
    resource_id: UInt16Builder,
    resource_schema_url: StringBuilder,
    resource_dropped_attributes_count: UInt32Builder,
    scope_id: UInt16Builder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_dropped_attributes_count: UInt32Builder,
    schema_url: StringBuilder,
    metric_type: UInt8Builder,
    name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    aggregation_temporality: Int32Builder,
    is_monotonic: BooleanBuilder,
}

impl Default for MetricTableBuilder {
    fn default() -> Self {
        Self {
            id: UInt16Builder::new(),
            resource_id: UInt16Builder::new(),
            resource_schema_url: StringBuilder::new(),
            resource_dropped_attributes_count: UInt32Builder::new(),
            scope_id: UInt16Builder::new(),
            scope_name: StringBuilder::new(),
            scope_version: StringBuilder::new(),
            scope_dropped_attributes_count: UInt32Builder::new(),
            schema_url: StringBuilder::new(),
            metric_type: UInt8Builder::new(),
            name: StringBuilder::new(),
            description: StringBuilder::new(),
            unit: StringBuilder::new(),
            aggregation_temporality: Int32Builder::new(),
            is_monotonic: BooleanBuilder::new(),
        }
    }
}

impl MetricTableBuilder {
    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        id: u16,
        resource_id: u16,
        resource_schema_url: Option<&str>,
        resource_dropped: Option<u32>,
        scope_id: u16,
        scope_name: Option<&str>,
        scope_version: Option<&str>,
        scope_dropped: Option<u32>,
        schema_url: Option<&str>,
        metric_type: u8,
        name: &str,
        description: Option<&str>,
        unit: Option<&str>,
        aggregation_temporality: Option<i32>,
        is_monotonic: Option<bool>,
    ) {
        self.id.append_value(id);
        self.resource_id.append_value(resource_id);
        append_opt_str(&mut self.resource_schema_url, resource_schema_url);
        append_opt_u32(
            &mut self.resource_dropped_attributes_count,
            resource_dropped,
        );
        self.scope_id.append_value(scope_id);
        append_opt_str(&mut self.scope_name, scope_name);
        append_opt_str(&mut self.scope_version, scope_version);
        append_opt_u32(&mut self.scope_dropped_attributes_count, scope_dropped);
        append_opt_str(&mut self.schema_url, schema_url);
        self.metric_type.append_value(metric_type);
        self.name.append_value(name);
        append_opt_str(&mut self.description, description);
        append_opt_str(&mut self.unit, unit);
        append_opt_i32(&mut self.aggregation_temporality, aggregation_temporality);
        append_opt_bool(&mut self.is_monotonic, is_monotonic);
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_metrics_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.resource_id.finish()),
                array(self.resource_schema_url.finish()),
                array(self.resource_dropped_attributes_count.finish()),
                array(self.scope_id.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_dropped_attributes_count.finish()),
                array(self.schema_url.finish()),
                array(self.metric_type.finish()),
                array(self.name.finish()),
                array(self.description.finish()),
                array(self.unit.finish()),
                array(self.aggregation_temporality.finish()),
                array(self.is_monotonic.finish()),
            ],
        )
    }
}

struct AttrBuildersU16 {
    parent_id: UInt16Builder,
    any: AttrAnyValueBuilders,
}

impl Default for AttrBuildersU16 {
    fn default() -> Self {
        Self {
            parent_id: UInt16Builder::new(),
            any: AttrAnyValueBuilders::default(),
        }
    }
}

impl AttrBuildersU16 {
    fn append_attrs(&mut self, parent_id: u16, attrs: &[KeyValue]) -> Result<()> {
        for attr in attrs.iter().filter(|attr| attr.value.is_some()) {
            self.parent_id.append_value(parent_id);
            self.any.append_key_value(attr)?;
        }
        Ok(())
    }

    fn finish_u16(mut self) -> Result<RecordBatch> {
        let any = self.any.finish();
        record_batch(
            otap_attrs_u16_schema_arc(),
            vec![
                array(self.parent_id.finish()),
                any.0,
                any.1,
                any.2,
                any.3,
                any.4,
                any.5,
                any.6,
                any.7,
            ],
        )
    }
}

struct AttrBuildersU32 {
    parent_id: UInt32Builder,
    any: AttrAnyValueBuilders,
}

impl Default for AttrBuildersU32 {
    fn default() -> Self {
        Self {
            parent_id: UInt32Builder::new(),
            any: AttrAnyValueBuilders::default(),
        }
    }
}

impl AttrBuildersU32 {
    fn append_attrs(&mut self, parent_id: u32, attrs: &[KeyValue]) -> Result<()> {
        for attr in attrs.iter().filter(|attr| attr.value.is_some()) {
            self.parent_id.append_value(parent_id);
            self.any.append_key_value(attr)?;
        }
        Ok(())
    }

    fn finish_u32(mut self) -> Result<RecordBatch> {
        let any = self.any.finish();
        record_batch(
            otap_attrs_u32_schema_arc(),
            vec![
                array(self.parent_id.finish()),
                any.0,
                any.1,
                any.2,
                any.3,
                any.4,
                any.5,
                any.6,
                any.7,
            ],
        )
    }
}

#[derive(Default)]
struct AttrAnyValueBuilders {
    key: StringBuilder,
    value: AnyValueBuilders,
}

impl AttrAnyValueBuilders {
    fn append_key_value(&mut self, attr: &KeyValue) -> Result<()> {
        self.key.append_value(&attr.key);
        self.value.append_any(attr.value.as_ref())
    }

    fn finish(
        mut self,
    ) -> (
        arrow_array::ArrayRef,
        arrow_array::ArrayRef,
        arrow_array::ArrayRef,
        arrow_array::ArrayRef,
        arrow_array::ArrayRef,
        arrow_array::ArrayRef,
        arrow_array::ArrayRef,
        arrow_array::ArrayRef,
    ) {
        (
            array(self.key.finish()),
            array(self.value.type_.finish()),
            array(self.value.str.finish()),
            array(self.value.int.finish()),
            array(self.value.double.finish()),
            array(self.value.bool.finish()),
            array(self.value.bytes.finish()),
            array(self.value.ser.finish()),
        )
    }
}

struct AnyValueBuilders {
    type_: UInt8Builder,
    str: StringBuilder,
    int: Int64Builder,
    double: Float64Builder,
    bool: BooleanBuilder,
    bytes: BinaryBuilder,
    ser: BinaryBuilder,
}

impl Default for AnyValueBuilders {
    fn default() -> Self {
        Self::new()
    }
}

impl AnyValueBuilders {
    fn new() -> Self {
        Self {
            type_: UInt8Builder::new(),
            str: StringBuilder::new(),
            int: Int64Builder::new(),
            double: Float64Builder::new(),
            bool: BooleanBuilder::new(),
            bytes: BinaryBuilder::new(),
            ser: BinaryBuilder::new(),
        }
    }

    fn append_any(&mut self, value: Option<&AnyValue>) -> Result<()> {
        let Some(any) = value.and_then(|value| value.value.as_ref()) else {
            self.append_empty();
            return Ok(());
        };

        match any {
            any_value::Value::StringValue(value) => {
                self.type_.append_value(VALUE_STR);
                self.str.append_value(value);
                self.int.append_null();
                self.double.append_null();
                self.bool.append_null();
                self.bytes.append_null();
                self.ser.append_null();
            }
            any_value::Value::IntValue(value) => {
                self.type_.append_value(VALUE_I64);
                self.str.append_null();
                self.int.append_value(*value);
                self.double.append_null();
                self.bool.append_null();
                self.bytes.append_null();
                self.ser.append_null();
            }
            any_value::Value::DoubleValue(value) => {
                self.type_.append_value(VALUE_F64);
                self.str.append_null();
                self.int.append_null();
                self.double.append_value(*value);
                self.bool.append_null();
                self.bytes.append_null();
                self.ser.append_null();
            }
            any_value::Value::BoolValue(value) => {
                self.type_.append_value(VALUE_BOOL);
                self.str.append_null();
                self.int.append_null();
                self.double.append_null();
                self.bool.append_value(*value);
                self.bytes.append_null();
                self.ser.append_null();
            }
            any_value::Value::BytesValue(value) => {
                self.type_.append_value(VALUE_BYTES);
                self.str.append_null();
                self.int.append_null();
                self.double.append_null();
                self.bool.append_null();
                self.bytes.append_value(value);
                self.ser.append_null();
            }
            any_value::Value::ArrayValue(value) => {
                let bytes = cbor_array(&value.values);
                self.append_ser(VALUE_SLICE, bytes.as_slice());
            }
            any_value::Value::KvlistValue(value) => {
                let bytes = cbor_kvlist(&value.values);
                self.append_ser(VALUE_MAP, bytes.as_slice());
            }
        }
        Ok(())
    }

    fn append_empty(&mut self) {
        self.type_.append_value(VALUE_EMPTY);
        self.str.append_null();
        self.int.append_null();
        self.double.append_null();
        self.bool.append_null();
        self.bytes.append_null();
        self.ser.append_null();
    }

    fn append_ser(&mut self, type_: u8, bytes: &[u8]) {
        self.type_.append_value(type_);
        self.str.append_null();
        self.int.append_null();
        self.double.append_null();
        self.bool.append_null();
        self.bytes.append_null();
        self.ser.append_value(bytes);
    }
}

// Minimal CBOR encoder per RFC 8949. We only need a deterministic, no-deps
// serializer for the `body_ser` and `attrs.ser` columns when an AnyValue is a
// map or array. Major types used here:
//   0 = unsigned integer
//   1 = negative integer (encoded as -1 - n)
//   2 = byte string
//   3 = text string (UTF-8)
//   4 = array
//   5 = map
//   7 = simple/float values: 0xf4=false, 0xf5=true, 0xf6=null, 0xfb=f64
fn cbor_array(values: &[AnyValue]) -> Vec<u8> {
    let mut out = Vec::new();
    cbor_len(&mut out, 4, values.len() as u64);
    for value in values {
        cbor_any(&mut out, value.value.as_ref());
    }
    out
}

fn cbor_kvlist(values: &[KeyValue]) -> Vec<u8> {
    let mut out = Vec::new();
    cbor_len(&mut out, 5, values.len() as u64);
    for value in values {
        cbor_text(&mut out, &value.key);
        cbor_any(
            &mut out,
            value.value.as_ref().and_then(|value| value.value.as_ref()),
        );
    }
    out
}

fn cbor_any(out: &mut Vec<u8>, value: Option<&any_value::Value>) {
    match value {
        Some(any_value::Value::StringValue(value)) => cbor_text(out, value),
        Some(any_value::Value::IntValue(value)) => cbor_i64(out, *value),
        Some(any_value::Value::DoubleValue(value)) => {
            out.push(0xfb);
            out.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        Some(any_value::Value::BoolValue(value)) => out.push(if *value { 0xf5 } else { 0xf4 }),
        Some(any_value::Value::BytesValue(value)) => {
            cbor_len(out, 2, value.len() as u64);
            out.extend_from_slice(value);
        }
        Some(any_value::Value::ArrayValue(value)) => {
            cbor_len(out, 4, value.values.len() as u64);
            for value in &value.values {
                cbor_any(out, value.value.as_ref());
            }
        }
        Some(any_value::Value::KvlistValue(value)) => {
            cbor_len(out, 5, value.values.len() as u64);
            for value in &value.values {
                cbor_text(out, &value.key);
                cbor_any(
                    out,
                    value.value.as_ref().and_then(|value| value.value.as_ref()),
                );
            }
        }
        None => out.push(0xf6),
    }
}

fn cbor_text(out: &mut Vec<u8>, value: &str) {
    cbor_len(out, 3, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn cbor_i64(out: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        cbor_len(out, 0, value as u64);
    } else {
        cbor_len(out, 1, (-1 - value) as u64);
    }
}

fn cbor_len(out: &mut Vec<u8>, major: u8, value: u64) {
    let prefix = major << 5;
    match value {
        0..=23 => out.push(prefix | value as u8),
        24..=0xff => out.extend_from_slice(&[prefix | 24, value as u8]),
        0x100..=0xffff => {
            out.push(prefix | 25);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(prefix | 26);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            out.push(prefix | 27);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}

#[derive(Default)]
struct NumberDataPointBuilders {
    id: UInt32Builder,
    parent_id: UInt16Builder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    time_unix_nano: TimestampNanosecondBuilder,
    int_value: Int64Builder,
    double_value: Float64Builder,
    flags: UInt32Builder,
}

impl NumberDataPointBuilders {
    fn append(&mut self, id: u32, parent_id: u16, point: &NumberDataPoint) -> Result<()> {
        self.id.append_value(id);
        self.parent_id.append_value(parent_id);
        append_opt_ts_ns(
            &mut self.start_time_unix_nano,
            point.start_time_unix_nano,
            "number_dp.start_time_unix_nano",
        )?;
        append_required_ts_ns(
            &mut self.time_unix_nano,
            point.time_unix_nano,
            "number_dp.time_unix_nano",
        )?;
        match point.value {
            Some(number_data_point::Value::AsInt(value)) => {
                self.int_value.append_value(value);
                self.double_value.append_null();
            }
            Some(number_data_point::Value::AsDouble(value)) => {
                self.int_value.append_null();
                append_finite_opt(&mut self.double_value, Some(value));
            }
            None => {
                self.int_value.append_null();
                self.double_value.append_null();
            }
        }
        self.flags.append_value(point.flags);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_number_data_points_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.parent_id.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.time_unix_nano.finish()),
                array(self.int_value.finish()),
                array(self.double_value.finish()),
                array(self.flags.finish()),
            ],
        )
    }
}

struct ExemplarBuilders {
    id: UInt32Builder,
    parent_id: UInt32Builder,
    time_unix_nano: TimestampNanosecondBuilder,
    int_value: Int64Builder,
    double_value: Float64Builder,
    span_id: FixedSizeBinaryBuilder,
    trace_id: FixedSizeBinaryBuilder,
}

impl ExemplarBuilders {
    fn append(&mut self, id: u32, parent_id: u32, exemplar: &Exemplar) -> Result<()> {
        self.id.append_value(id);
        self.parent_id.append_value(parent_id);
        append_required_ts_ns(
            &mut self.time_unix_nano,
            exemplar.time_unix_nano,
            "exemplar.time_unix_nano",
        )?;
        match exemplar.value {
            Some(exemplar::Value::AsInt(value)) => {
                self.int_value.append_value(value);
                self.double_value.append_null();
            }
            Some(exemplar::Value::AsDouble(value)) => {
                self.int_value.append_null();
                append_finite_opt(&mut self.double_value, Some(value));
            }
            None => {
                self.int_value.append_null();
                self.double_value.append_null();
            }
        }
        append_fixed_or_null(&mut self.span_id, &exemplar.span_id, 8)?;
        append_fixed_or_null(&mut self.trace_id, &exemplar.trace_id, 16)?;
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_exemplars_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.parent_id.finish()),
                array(self.time_unix_nano.finish()),
                array(self.int_value.finish()),
                array(self.double_value.finish()),
                array(self.span_id.finish()),
                array(self.trace_id.finish()),
            ],
        )
    }
}

impl Default for ExemplarBuilders {
    fn default() -> Self {
        Self {
            id: UInt32Builder::new(),
            parent_id: UInt32Builder::new(),
            time_unix_nano: TimestampNanosecondBuilder::new(),
            int_value: Int64Builder::new(),
            double_value: Float64Builder::new(),
            span_id: FixedSizeBinaryBuilder::new(8),
            trace_id: FixedSizeBinaryBuilder::new(16),
        }
    }
}

// Quantile values are emitted into the separate `quantile` child table rather
// than embedded as a List<Struct> inside this row. The two representations are
// not interchangeable; we pick the relational form for consistency with the
// other star-schema child tables (B3).
#[derive(Default)]
struct SummaryDataPointBuilders {
    id: UInt32Builder,
    parent_id: UInt16Builder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    time_unix_nano: TimestampNanosecondBuilder,
    count: UInt64Builder,
    sum: Float64Builder,
    flags: UInt32Builder,
}

impl SummaryDataPointBuilders {
    fn append(&mut self, id: u32, parent_id: u16, point: &SummaryDataPoint) -> Result<()> {
        self.id.append_value(id);
        self.parent_id.append_value(parent_id);
        append_opt_ts_ns(
            &mut self.start_time_unix_nano,
            point.start_time_unix_nano,
            "summary.start_time_unix_nano",
        )?;
        append_required_ts_ns(
            &mut self.time_unix_nano,
            point.time_unix_nano,
            "summary.time_unix_nano",
        )?;
        self.count.append_value(point.count);
        append_finite_opt(&mut self.sum, Some(point.sum));
        self.flags.append_value(point.flags);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_summary_data_points_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.parent_id.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.time_unix_nano.finish()),
                array(self.count.finish()),
                array(self.sum.finish()),
                array(self.flags.finish()),
            ],
        )
    }
}

#[derive(Default)]
struct QuantileBuilders {
    parent_id: UInt32Builder,
    quantile: Float64Builder,
    value: Float64Builder,
}

impl QuantileBuilders {
    fn append(&mut self, parent_id: u32, quantile: f64, value: f64) {
        self.parent_id.append_value(parent_id);
        append_finite_opt(&mut self.quantile, Some(quantile));
        append_finite_opt(&mut self.value, Some(value));
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_quantile_schema_arc(),
            vec![
                array(self.parent_id.finish()),
                array(self.quantile.finish()),
                array(self.value.finish()),
            ],
        )
    }
}

#[derive(Default)]
struct HistogramDataPointBuilders {
    id: UInt32Builder,
    parent_id: UInt16Builder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    time_unix_nano: TimestampNanosecondBuilder,
    count: UInt64Builder,
    sum: Float64Builder,
    bucket_counts: ListBuilder<UInt64Builder>,
    explicit_bounds: ListBuilder<Float64Builder>,
    flags: UInt32Builder,
    min: Float64Builder,
    max: Float64Builder,
}

impl HistogramDataPointBuilders {
    fn append(&mut self, id: u32, parent_id: u16, point: &HistogramDataPoint) -> Result<()> {
        self.id.append_value(id);
        self.parent_id.append_value(parent_id);
        append_opt_ts_ns(
            &mut self.start_time_unix_nano,
            point.start_time_unix_nano,
            "histogram.start_time_unix_nano",
        )?;
        append_required_ts_ns(
            &mut self.time_unix_nano,
            point.time_unix_nano,
            "histogram.time_unix_nano",
        )?;
        self.count.append_value(point.count);
        append_finite_opt(&mut self.sum, point.sum);
        append_u64_list(&mut self.bucket_counts, &point.bucket_counts);
        append_f64_list(&mut self.explicit_bounds, &point.explicit_bounds);
        self.flags.append_value(point.flags);
        append_finite_opt(&mut self.min, point.min);
        append_finite_opt(&mut self.max, point.max);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_histogram_data_points_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.parent_id.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.time_unix_nano.finish()),
                array(self.count.finish()),
                array(self.sum.finish()),
                array(self.bucket_counts.finish()),
                array(self.explicit_bounds.finish()),
                array(self.flags.finish()),
                array(self.min.finish()),
                array(self.max.finish()),
            ],
        )
    }
}

#[derive(Default)]
struct ExpHistogramDataPointBuilders {
    id: UInt32Builder,
    parent_id: UInt16Builder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    time_unix_nano: TimestampNanosecondBuilder,
    count: UInt64Builder,
    sum: Float64Builder,
    scale: Int32Builder,
    zero_count: UInt64Builder,
    positive_offset: Int32Builder,
    positive_bucket_counts: ListBuilder<UInt64Builder>,
    negative_offset: Int32Builder,
    negative_bucket_counts: ListBuilder<UInt64Builder>,
    flags: UInt32Builder,
    min: Float64Builder,
    max: Float64Builder,
    zero_threshold: Float64Builder,
}

impl ExpHistogramDataPointBuilders {
    fn append(
        &mut self,
        id: u32,
        parent_id: u16,
        point: &ExponentialHistogramDataPoint,
    ) -> Result<()> {
        self.id.append_value(id);
        self.parent_id.append_value(parent_id);
        append_opt_ts_ns(
            &mut self.start_time_unix_nano,
            point.start_time_unix_nano,
            "exp_histogram.start_time_unix_nano",
        )?;
        append_required_ts_ns(
            &mut self.time_unix_nano,
            point.time_unix_nano,
            "exp_histogram.time_unix_nano",
        )?;
        self.count.append_value(point.count);
        append_finite_opt(&mut self.sum, point.sum);
        self.scale.append_value(point.scale);
        self.zero_count.append_value(point.zero_count);
        if let Some(positive) = &point.positive {
            self.positive_offset.append_value(positive.offset);
            append_u64_list(&mut self.positive_bucket_counts, &positive.bucket_counts);
        } else {
            self.positive_offset.append_null();
            self.positive_bucket_counts.append(false);
        }
        if let Some(negative) = &point.negative {
            self.negative_offset.append_value(negative.offset);
            append_u64_list(&mut self.negative_bucket_counts, &negative.bucket_counts);
        } else {
            self.negative_offset.append_null();
            self.negative_bucket_counts.append(false);
        }
        self.flags.append_value(point.flags);
        append_finite_opt(&mut self.min, point.min);
        append_finite_opt(&mut self.max, point.max);
        append_finite_opt(&mut self.zero_threshold, Some(point.zero_threshold));
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_exp_histogram_data_points_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.parent_id.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.time_unix_nano.finish()),
                array(self.count.finish()),
                array(self.sum.finish()),
                array(self.scale.finish()),
                array(self.zero_count.finish()),
                array(self.positive_offset.finish()),
                array(self.positive_bucket_counts.finish()),
                array(self.negative_offset.finish()),
                array(self.negative_bucket_counts.finish()),
                array(self.flags.finish()),
                array(self.min.finish()),
                array(self.max.finish()),
                array(self.zero_threshold.finish()),
            ],
        )
    }
}

#[derive(Default)]
struct SpanEventBuilders {
    id: UInt32Builder,
    parent_id: UInt16Builder,
    time_unix_nano: TimestampNanosecondBuilder,
    name: StringBuilder,
    dropped_attributes_count: UInt32Builder,
}

impl SpanEventBuilders {
    fn append(
        &mut self,
        id: u32,
        parent_id: u16,
        event: &opentelemetry_proto::tonic::trace::v1::span::Event,
    ) -> Result<()> {
        self.id.append_value(id);
        self.parent_id.append_value(parent_id);
        append_opt_ts_ns(
            &mut self.time_unix_nano,
            event.time_unix_nano,
            "span_event.time_unix_nano",
        )?;
        self.name.append_value(&event.name);
        self.dropped_attributes_count
            .append_value(event.dropped_attributes_count);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_span_events_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.parent_id.finish()),
                array(self.time_unix_nano.finish()),
                array(self.name.finish()),
                array(self.dropped_attributes_count.finish()),
            ],
        )
    }
}

struct SpanLinkBuilders {
    id: UInt32Builder,
    parent_id: UInt16Builder,
    trace_id: FixedSizeBinaryBuilder,
    span_id: FixedSizeBinaryBuilder,
    trace_state: StringBuilder,
    dropped_attributes_count: UInt32Builder,
    flags: UInt32Builder,
}

impl SpanLinkBuilders {
    fn append(
        &mut self,
        id: u32,
        parent_id: u16,
        link: &opentelemetry_proto::tonic::trace::v1::span::Link,
    ) -> Result<()> {
        self.id.append_value(id);
        self.parent_id.append_value(parent_id);
        append_fixed_required(&mut self.trace_id, &link.trace_id, 16, "span_link.trace_id")?;
        append_fixed_required(&mut self.span_id, &link.span_id, 8, "span_link.span_id")?;
        append_opt_str(&mut self.trace_state, empty_to_none(&link.trace_state));
        self.dropped_attributes_count
            .append_value(link.dropped_attributes_count);
        self.flags.append_value(link.flags);
        Ok(())
    }

    fn finish(mut self) -> Result<RecordBatch> {
        record_batch(
            otap_span_links_schema_arc(),
            vec![
                array(self.id.finish()),
                array(self.parent_id.finish()),
                array(self.trace_id.finish()),
                array(self.span_id.finish()),
                array(self.trace_state.finish()),
                array(self.dropped_attributes_count.finish()),
                array(self.flags.finish()),
            ],
        )
    }
}

impl Default for SpanLinkBuilders {
    fn default() -> Self {
        Self {
            id: UInt32Builder::new(),
            parent_id: UInt16Builder::new(),
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            trace_state: StringBuilder::new(),
            dropped_attributes_count: UInt32Builder::new(),
            flags: UInt32Builder::new(),
        }
    }
}

fn append_opt_str(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_opt_u32(builder: &mut UInt32Builder, value: Option<u32>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_opt_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_opt_bool(builder: &mut BooleanBuilder, value: Option<bool>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

#[derive(Clone, Copy)]
enum DedupKind {
    Resource,
    Scope,
}

/// Emit the appropriate `*DuplicateHit` / `*DuplicateMiss` counter for a
/// resource or scope lookup. `is_new` mirrors the second tuple element returned
/// by [`ContextIdMap::lookup_or_insert`] (true → first time we saw this
/// context, i.e. a miss; false → reuse, i.e. a hit).
fn observe_dedup(
    observer: &mut Option<&mut dyn TransformObserver>,
    signal: TransformSignal,
    kind: DedupKind,
    is_new: bool,
) {
    let counter = match (kind, is_new) {
        (DedupKind::Resource, true) => TransformCounter::ResourceContextDuplicateMiss,
        (DedupKind::Resource, false) => TransformCounter::ResourceContextDuplicateHit,
        (DedupKind::Scope, true) => TransformCounter::ScopeContextDuplicateMiss,
        (DedupKind::Scope, false) => TransformCounter::ScopeContextDuplicateHit,
    };
    observe_counter(observer, signal, counter, 1);
}

/// Maps a resource or scope fingerprint to its previously assigned u16 id, so
/// identical contexts across `resource_*` / `scope_*` blocks within a single
/// request share the same id and emit their attribute rows only once (B4).
#[derive(Default)]
struct ContextIdMap {
    map: HashMap<u64, u16>,
}

impl ContextIdMap {
    /// Returns `(id, is_new)`. When `is_new` is true the caller must emit the
    /// child rows (resource_attrs / scope_attrs) for this id; otherwise the
    /// previously emitted ones are reused.
    fn lookup_or_insert(
        &mut self,
        fingerprint: u64,
        next: &mut u32,
        field: &str,
    ) -> Result<(u16, bool)> {
        if let Some(&id) = self.map.get(&fingerprint) {
            return Ok((id, false));
        }
        let id = next_u16(next, field)?;
        self.map.insert(fingerprint, id);
        Ok((id, true))
    }
}

fn resource_fingerprint(attrs: &[KeyValue], schema_url: Option<&str>, dropped: Option<u32>) -> u64 {
    let mut hasher = DefaultHasher::new();
    schema_url.hash(&mut hasher);
    dropped.hash(&mut hasher);
    hash_attrs(attrs, &mut hasher);
    hasher.finish()
}

fn scope_fingerprint(
    name: Option<&str>,
    version: Option<&str>,
    attrs: &[KeyValue],
    schema_url: Option<&str>,
    dropped: Option<u32>,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    version.hash(&mut hasher);
    schema_url.hash(&mut hasher);
    dropped.hash(&mut hasher);
    hash_attrs(attrs, &mut hasher);
    hasher.finish()
}

fn next_u16(next: &mut u32, field: &str) -> Result<u16> {
    let value = u16::try_from(*next).map_err(|_| {
        Error::Decode(DecodeError::Unsupported(format!(
            "{field} exceeds UInt16 capacity"
        )))
    })?;
    // Defensive: the try_from gate keeps *next ≤ u16::MAX so this can't
    // overflow in practice, but use checked_add to stay symmetric with
    // next_u32 and to crash loudly if that invariant ever changes.
    *next = next.checked_add(1).ok_or_else(|| {
        Error::Decode(DecodeError::Unsupported(format!(
            "{field} id counter overflow"
        )))
    })?;
    Ok(value)
}

fn next_u32(next: &mut u32, field: &str) -> Result<u32> {
    let value = *next;
    *next = next.checked_add(1).ok_or_else(|| {
        Error::Decode(DecodeError::Unsupported(format!(
            "{field} exceeds UInt32 capacity"
        )))
    })?;
    Ok(value)
}

fn empty_to_none(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

fn metric_type(data: &Option<Data>) -> Option<u8> {
    match data {
        Some(Data::Gauge(_)) => Some(METRIC_GAUGE),
        Some(Data::Sum(_)) => Some(METRIC_SUM),
        Some(Data::Histogram(_)) => Some(METRIC_HISTOGRAM),
        Some(Data::ExponentialHistogram(_)) => Some(METRIC_EXP_HISTOGRAM),
        Some(Data::Summary(_)) => Some(METRIC_SUMMARY),
        None => None,
    }
}

fn aggregation_temporality(data: &Option<Data>) -> Option<i32> {
    match data {
        Some(Data::Sum(data)) => Some(data.aggregation_temporality),
        Some(Data::Histogram(data)) => Some(data.aggregation_temporality),
        Some(Data::ExponentialHistogram(data)) => Some(data.aggregation_temporality),
        _ => None,
    }
}

fn is_monotonic(data: &Option<Data>) -> Option<bool> {
    match data {
        Some(Data::Sum(data)) => Some(data.is_monotonic),
        _ => None,
    }
}
