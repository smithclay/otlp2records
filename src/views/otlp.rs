// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Zero-allocation OTLP Prost adapters for the semantic logs views.

use std::{marker::PhantomData, slice};

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    collector::metrics::v1::ExportMetricsServiceRequest,
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        exemplar, exponential_histogram_data_point, metric, number_data_point,
        summary_data_point::ValueAtQuantile, Exemplar, ExponentialHistogram,
        ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
    },
    resource::v1::Resource,
    trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status},
};

use super::pdata::{
    AggregationTemporality, AnyValueView, AttributeView, BucketsView, DataPointFlags, DataType,
    DataView, EventView, ExemplarView, ExponentialHistogramDataPointView, ExponentialHistogramView,
    GaugeView, HistogramDataPointView, HistogramView, InstrumentationScopeView, LinkView,
    LogRecordView, LogsDataView, MetricView, MetricsView, NumberDataPointView, ResourceLogsView,
    ResourceMetricsView, ResourceSpansView, ResourceView, ScopeLogsView, ScopeMetricsView,
    ScopeSpansView, SpanId, SpanView, StatusView, Str, SumView, SummaryDataPointView, SummaryView,
    TraceId, TracesView, Value, ValueAtQuantileView, ValueType,
};

#[derive(Clone)]
pub struct Obj<'a, T> {
    inner: &'a T,
}

impl<'a, T> Obj<'a, T> {
    fn new(inner: &'a T) -> Self {
        Self { inner }
    }
}

pub struct ObjIter<'a, T, O> {
    iter: slice::Iter<'a, T>,
    outer: PhantomData<O>,
}

impl<'a, T, O> ObjIter<'a, T, O> {
    fn new(iter: slice::Iter<'a, T>) -> Self {
        Self {
            iter,
            outer: PhantomData,
        }
    }
}

impl<'a, T, O> Iterator for ObjIter<'a, T, O>
where
    O: From<&'a T>,
{
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Into::into)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a, T> From<&'a T> for Obj<'a, T> {
    fn from(inner: &'a T) -> Self {
        Self::new(inner)
    }
}

#[derive(Clone)]
pub struct ObjAttribute<'a> {
    key: &'a str,
    value: Option<Obj<'a, AnyValue>>,
}

#[derive(Clone)]
pub struct AttributeIter<'a> {
    iter: slice::Iter<'a, KeyValue>,
}

impl<'a> AttributeIter<'a> {
    fn new(iter: slice::Iter<'a, KeyValue>) -> Self {
        Self { iter }
    }
}

impl<'a> Iterator for AttributeIter<'a> {
    type Item = ObjAttribute<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|attribute| ObjAttribute {
            key: &attribute.key,
            value: attribute.value.as_ref().map(Obj::new),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

#[derive(Clone)]
pub struct AnyValueIter<'a> {
    iter: slice::Iter<'a, AnyValue>,
}

impl<'a> Iterator for AnyValueIter<'a> {
    type Item = Obj<'a, AnyValue>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(Obj::new)
    }
}

impl LogsDataView for ExportLogsServiceRequest {
    type ResourceLogs<'a>
        = Obj<'a, ResourceLogs>
    where
        Self: 'a;
    type ResourcesIter<'a>
        = ObjIter<'a, ResourceLogs, Self::ResourceLogs<'a>>
    where
        Self: 'a;

    fn resources(&self) -> Self::ResourcesIter<'_> {
        ObjIter::new(self.resource_logs.iter())
    }
}

impl ResourceLogsView for Obj<'_, ResourceLogs> {
    type Resource<'a>
        = Obj<'a, Resource>
    where
        Self: 'a;
    type ScopeLogs<'a>
        = Obj<'a, ScopeLogs>
    where
        Self: 'a;
    type ScopesIter<'a>
        = ObjIter<'a, ScopeLogs, Self::ScopeLogs<'a>>
    where
        Self: 'a;

    fn resource(&self) -> Option<Self::Resource<'_>> {
        self.inner.resource.as_ref().map(Obj::new)
    }

    fn scopes(&self) -> Self::ScopesIter<'_> {
        ObjIter::new(self.inner.scope_logs.iter())
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        non_empty(self.inner.schema_url.as_bytes())
    }
}

impl ScopeLogsView for Obj<'_, ScopeLogs> {
    type Scope<'a>
        = Obj<'a, InstrumentationScope>
    where
        Self: 'a;
    type LogRecord<'a>
        = Obj<'a, LogRecord>
    where
        Self: 'a;
    type LogRecordsIter<'a>
        = ObjIter<'a, LogRecord, Self::LogRecord<'a>>
    where
        Self: 'a;

    fn scope(&self) -> Option<Self::Scope<'_>> {
        self.inner.scope.as_ref().map(Obj::new)
    }

    fn log_records(&self) -> Self::LogRecordsIter<'_> {
        ObjIter::new(self.inner.log_records.iter())
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        non_empty(self.inner.schema_url.as_bytes())
    }
}

impl LogRecordView for Obj<'_, LogRecord> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;
    type Body<'a>
        = Obj<'a, AnyValue>
    where
        Self: 'a;

    fn time_unix_nano(&self) -> Option<u64> {
        (self.inner.time_unix_nano != 0).then_some(self.inner.time_unix_nano)
    }

    fn observed_time_unix_nano(&self) -> Option<u64> {
        (self.inner.observed_time_unix_nano != 0).then_some(self.inner.observed_time_unix_nano)
    }

    fn severity_number(&self) -> Option<i32> {
        (self.inner.severity_number != 0).then_some(self.inner.severity_number)
    }

    fn severity_text(&self) -> Option<Str<'_>> {
        non_empty(self.inner.severity_text.as_bytes())
    }

    fn body(&self) -> Option<Self::Body<'_>> {
        self.inner.body.as_ref().map(Obj::new)
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }

    fn dropped_attributes_count(&self) -> u32 {
        self.inner.dropped_attributes_count
    }

    fn flags(&self) -> Option<u32> {
        (self.inner.flags != 0).then_some(self.inner.flags)
    }

    fn trace_id(&self) -> Option<&TraceId> {
        parse_id(&self.inner.trace_id)
    }

    fn span_id(&self) -> Option<&SpanId> {
        parse_id(&self.inner.span_id)
    }

    fn event_name(&self) -> Option<Str<'_>> {
        non_empty(self.inner.event_name.as_bytes())
    }
}

impl TracesView for ExportTraceServiceRequest {
    type ResourceSpans<'a>
        = Obj<'a, ResourceSpans>
    where
        Self: 'a;
    type ResourcesIter<'a>
        = ObjIter<'a, ResourceSpans, Self::ResourceSpans<'a>>
    where
        Self: 'a;

    fn resources(&self) -> Self::ResourcesIter<'_> {
        ObjIter::new(self.resource_spans.iter())
    }
}

impl ResourceSpansView for Obj<'_, ResourceSpans> {
    type Resource<'a>
        = Obj<'a, Resource>
    where
        Self: 'a;
    type ScopeSpans<'a>
        = Obj<'a, ScopeSpans>
    where
        Self: 'a;
    type ScopesIter<'a>
        = ObjIter<'a, ScopeSpans, Self::ScopeSpans<'a>>
    where
        Self: 'a;

    fn resource(&self) -> Option<Self::Resource<'_>> {
        self.inner.resource.as_ref().map(Obj::new)
    }

    fn scopes(&self) -> Self::ScopesIter<'_> {
        ObjIter::new(self.inner.scope_spans.iter())
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        non_empty(self.inner.schema_url.as_bytes())
    }
}

impl ScopeSpansView for Obj<'_, ScopeSpans> {
    type Scope<'a>
        = Obj<'a, InstrumentationScope>
    where
        Self: 'a;
    type Span<'a>
        = Obj<'a, Span>
    where
        Self: 'a;
    type SpanIter<'a>
        = ObjIter<'a, Span, Self::Span<'a>>
    where
        Self: 'a;

    fn scope(&self) -> Option<Self::Scope<'_>> {
        self.inner.scope.as_ref().map(Obj::new)
    }

    fn spans(&self) -> Self::SpanIter<'_> {
        ObjIter::new(self.inner.spans.iter())
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        non_empty(self.inner.schema_url.as_bytes())
    }
}

impl SpanView for Obj<'_, Span> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;
    type Event<'a>
        = Obj<'a, span::Event>
    where
        Self: 'a;
    type EventsIter<'a>
        = ObjIter<'a, span::Event, Self::Event<'a>>
    where
        Self: 'a;
    type Link<'a>
        = Obj<'a, span::Link>
    where
        Self: 'a;
    type LinksIter<'a>
        = ObjIter<'a, span::Link, Self::Link<'a>>
    where
        Self: 'a;
    type Status<'a>
        = Obj<'a, Status>
    where
        Self: 'a;

    fn trace_id(&self) -> Option<&TraceId> {
        parse_id(&self.inner.trace_id)
    }

    fn span_id(&self) -> Option<&SpanId> {
        parse_id(&self.inner.span_id)
    }

    fn trace_state(&self) -> Option<Str<'_>> {
        non_empty(self.inner.trace_state.as_bytes())
    }

    fn parent_span_id(&self) -> Option<&SpanId> {
        parse_id(&self.inner.parent_span_id)
    }

    fn flags(&self) -> Option<u32> {
        Some(self.inner.flags)
    }

    fn name(&self) -> Option<Str<'_>> {
        Some(self.inner.name.as_bytes())
    }

    fn kind(&self) -> i32 {
        self.inner.kind
    }

    fn start_time_unix_nano(&self) -> Option<u64> {
        Some(self.inner.start_time_unix_nano)
    }

    fn end_time_unix_nano(&self) -> Option<u64> {
        Some(self.inner.end_time_unix_nano)
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }

    fn dropped_attributes_count(&self) -> u32 {
        self.inner.dropped_attributes_count
    }

    fn events(&self) -> Self::EventsIter<'_> {
        ObjIter::new(self.inner.events.iter())
    }

    fn dropped_events_count(&self) -> u32 {
        self.inner.dropped_events_count
    }

    fn links(&self) -> Self::LinksIter<'_> {
        ObjIter::new(self.inner.links.iter())
    }

    fn dropped_links_count(&self) -> u32 {
        self.inner.dropped_links_count
    }

    fn status(&self) -> Option<Self::Status<'_>> {
        self.inner.status.as_ref().map(Obj::new)
    }
}

impl EventView for Obj<'_, span::Event> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;

    fn time_unix_nano(&self) -> Option<u64> {
        Some(self.inner.time_unix_nano)
    }

    fn name(&self) -> Option<Str<'_>> {
        Some(self.inner.name.as_bytes())
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }

    fn dropped_attributes_count(&self) -> u32 {
        self.inner.dropped_attributes_count
    }
}

impl LinkView for Obj<'_, span::Link> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;

    fn trace_id(&self) -> Option<&TraceId> {
        parse_id(&self.inner.trace_id)
    }

    fn span_id(&self) -> Option<&SpanId> {
        parse_id(&self.inner.span_id)
    }

    fn trace_state(&self) -> Option<Str<'_>> {
        Some(self.inner.trace_state.as_bytes())
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }

    fn dropped_attributes_count(&self) -> u32 {
        self.inner.dropped_attributes_count
    }

    fn flags(&self) -> Option<u32> {
        Some(self.inner.flags)
    }
}

impl StatusView for Obj<'_, Status> {
    fn message(&self) -> Option<Str<'_>> {
        Some(self.inner.message.as_bytes())
    }

    fn status_code(&self) -> i32 {
        self.inner.code
    }
}

impl MetricsView for ExportMetricsServiceRequest {
    type ResourceMetrics<'a>
        = Obj<'a, ResourceMetrics>
    where
        Self: 'a;
    type ResourceMetricsIter<'a>
        = ObjIter<'a, ResourceMetrics, Self::ResourceMetrics<'a>>
    where
        Self: 'a;

    fn resources(&self) -> Self::ResourceMetricsIter<'_> {
        ObjIter::new(self.resource_metrics.iter())
    }
}

impl ResourceMetricsView for Obj<'_, ResourceMetrics> {
    type Resource<'a>
        = Obj<'a, Resource>
    where
        Self: 'a;
    type ScopeMetrics<'a>
        = Obj<'a, ScopeMetrics>
    where
        Self: 'a;
    type ScopesIter<'a>
        = ObjIter<'a, ScopeMetrics, Self::ScopeMetrics<'a>>
    where
        Self: 'a;

    fn resource(&self) -> Option<Self::Resource<'_>> {
        self.inner.resource.as_ref().map(Obj::new)
    }

    fn scopes(&self) -> Self::ScopesIter<'_> {
        ObjIter::new(self.inner.scope_metrics.iter())
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        non_empty(self.inner.schema_url.as_bytes())
    }
}

impl ScopeMetricsView for Obj<'_, ScopeMetrics> {
    type Scope<'a>
        = Obj<'a, InstrumentationScope>
    where
        Self: 'a;
    type Metric<'a>
        = Obj<'a, Metric>
    where
        Self: 'a;
    type MetricIter<'a>
        = ObjIter<'a, Metric, Self::Metric<'a>>
    where
        Self: 'a;

    fn scope(&self) -> Option<Self::Scope<'_>> {
        self.inner.scope.as_ref().map(Obj::new)
    }

    fn metrics(&self) -> Self::MetricIter<'_> {
        ObjIter::new(self.inner.metrics.iter())
    }

    fn schema_url(&self) -> Str<'_> {
        self.inner.schema_url.as_bytes()
    }
}

impl MetricView for Obj<'_, Metric> {
    type Data<'a>
        = Obj<'a, metric::Data>
    where
        Self: 'a;
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;

    fn name(&self) -> Str<'_> {
        self.inner.name.as_bytes()
    }

    fn description(&self) -> Str<'_> {
        self.inner.description.as_bytes()
    }

    fn unit(&self) -> Str<'_> {
        self.inner.unit.as_bytes()
    }

    fn data(&self) -> Option<Self::Data<'_>> {
        self.inner.data.as_ref().map(Obj::new)
    }

    fn metadata(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.metadata.iter())
    }
}

impl<'a> DataView<'a> for Obj<'a, metric::Data> {
    type Gauge<'b>
        = Obj<'b, Gauge>
    where
        Self: 'b;
    type Sum<'b>
        = Obj<'b, Sum>
    where
        Self: 'b;
    type Histogram<'b>
        = Obj<'b, Histogram>
    where
        Self: 'b;
    type ExponentialHistogram<'b>
        = Obj<'b, ExponentialHistogram>
    where
        Self: 'b;
    type Summary<'b>
        = Obj<'b, Summary>
    where
        Self: 'b;

    fn value_type(&self) -> DataType {
        match self.inner {
            metric::Data::Gauge(_) => DataType::Gauge,
            metric::Data::Sum(_) => DataType::Sum,
            metric::Data::Histogram(_) => DataType::Histogram,
            metric::Data::ExponentialHistogram(_) => DataType::ExponentialHistogram,
            metric::Data::Summary(_) => DataType::Summary,
        }
    }

    fn as_gauge(&self) -> Option<Self::Gauge<'_>> {
        match self.inner {
            metric::Data::Gauge(value) => Some(Obj::new(value)),
            _ => None,
        }
    }

    fn as_sum(&self) -> Option<Self::Sum<'_>> {
        match self.inner {
            metric::Data::Sum(value) => Some(Obj::new(value)),
            _ => None,
        }
    }

    fn as_histogram(&self) -> Option<Self::Histogram<'_>> {
        match self.inner {
            metric::Data::Histogram(value) => Some(Obj::new(value)),
            _ => None,
        }
    }

    fn as_exponential_histogram(&self) -> Option<Self::ExponentialHistogram<'_>> {
        match self.inner {
            metric::Data::ExponentialHistogram(value) => Some(Obj::new(value)),
            _ => None,
        }
    }

    fn as_summary(&self) -> Option<Self::Summary<'_>> {
        match self.inner {
            metric::Data::Summary(value) => Some(Obj::new(value)),
            _ => None,
        }
    }
}

impl GaugeView for Obj<'_, Gauge> {
    type NumberDataPoint<'a>
        = Obj<'a, NumberDataPoint>
    where
        Self: 'a;
    type NumberDataPointIter<'a>
        = ObjIter<'a, NumberDataPoint, Self::NumberDataPoint<'a>>
    where
        Self: 'a;

    fn data_points(&self) -> Self::NumberDataPointIter<'_> {
        ObjIter::new(self.inner.data_points.iter())
    }
}

impl SumView for Obj<'_, Sum> {
    type NumberDataPoint<'a>
        = Obj<'a, NumberDataPoint>
    where
        Self: 'a;
    type NumberDataPointIter<'a>
        = ObjIter<'a, NumberDataPoint, Self::NumberDataPoint<'a>>
    where
        Self: 'a;

    fn data_points(&self) -> Self::NumberDataPointIter<'_> {
        ObjIter::new(self.inner.data_points.iter())
    }

    fn aggregation_temporality(&self) -> AggregationTemporality {
        (self.inner.aggregation_temporality as u32).into()
    }

    fn is_monotonic(&self) -> bool {
        self.inner.is_monotonic
    }
}

impl NumberDataPointView for Obj<'_, NumberDataPoint> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;
    type Exemplar<'a>
        = Obj<'a, Exemplar>
    where
        Self: 'a;
    type ExemplarIter<'a>
        = ObjIter<'a, Exemplar, Self::Exemplar<'a>>
    where
        Self: 'a;

    fn start_time_unix_nano(&self) -> u64 {
        self.inner.start_time_unix_nano
    }

    fn time_unix_nano(&self) -> u64 {
        self.inner.time_unix_nano
    }

    fn value(&self) -> Option<Value> {
        match self.inner.value {
            Some(number_data_point::Value::AsInt(value)) => Some(Value::Integer(value)),
            Some(number_data_point::Value::AsDouble(value)) => Some(Value::Double(value)),
            None => None,
        }
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }

    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        ObjIter::new(self.inner.exemplars.iter())
    }

    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(self.inner.flags)
    }
}

impl ExemplarView for Obj<'_, Exemplar> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;

    fn filtered_attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.filtered_attributes.iter())
    }

    fn time_unix_nano(&self) -> u64 {
        self.inner.time_unix_nano
    }

    fn value(&self) -> Option<Value> {
        match self.inner.value {
            Some(exemplar::Value::AsInt(value)) => Some(Value::Integer(value)),
            Some(exemplar::Value::AsDouble(value)) => Some(Value::Double(value)),
            None => None,
        }
    }

    fn span_id(&self) -> Option<&SpanId> {
        parse_id(&self.inner.span_id)
    }

    fn trace_id(&self) -> Option<&TraceId> {
        parse_id(&self.inner.trace_id)
    }
}

impl HistogramView for Obj<'_, Histogram> {
    type HistogramDataPoint<'a>
        = Obj<'a, HistogramDataPoint>
    where
        Self: 'a;
    type HistogramDataPointIter<'a>
        = ObjIter<'a, HistogramDataPoint, Self::HistogramDataPoint<'a>>
    where
        Self: 'a;

    fn data_points(&self) -> Self::HistogramDataPointIter<'_> {
        ObjIter::new(self.inner.data_points.iter())
    }

    fn aggregation_temporality(&self) -> AggregationTemporality {
        (self.inner.aggregation_temporality as u32).into()
    }
}

impl HistogramDataPointView for Obj<'_, HistogramDataPoint> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;
    type BucketCountIter<'a>
        = std::iter::Copied<slice::Iter<'a, u64>>
    where
        Self: 'a;
    type ExplicitBoundsIter<'a>
        = std::iter::Copied<slice::Iter<'a, f64>>
    where
        Self: 'a;
    type Exemplar<'a>
        = Obj<'a, Exemplar>
    where
        Self: 'a;
    type ExemplarIter<'a>
        = ObjIter<'a, Exemplar, Self::Exemplar<'a>>
    where
        Self: 'a;

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }
    fn start_time_unix_nano(&self) -> u64 {
        self.inner.start_time_unix_nano
    }
    fn time_unix_nano(&self) -> u64 {
        self.inner.time_unix_nano
    }
    fn count(&self) -> u64 {
        self.inner.count
    }
    fn sum(&self) -> Option<f64> {
        self.inner.sum
    }
    fn bucket_counts(&self) -> Self::BucketCountIter<'_> {
        self.inner.bucket_counts.iter().copied()
    }
    fn explicit_bounds(&self) -> Self::ExplicitBoundsIter<'_> {
        self.inner.explicit_bounds.iter().copied()
    }
    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        ObjIter::new(self.inner.exemplars.iter())
    }
    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(self.inner.flags)
    }
    fn min(&self) -> Option<f64> {
        self.inner.min
    }
    fn max(&self) -> Option<f64> {
        self.inner.max
    }
}

impl ExponentialHistogramView for Obj<'_, ExponentialHistogram> {
    type ExponentialHistogramDataPoint<'a>
        = Obj<'a, ExponentialHistogramDataPoint>
    where
        Self: 'a;
    type ExponentialHistogramDataPointIter<'a>
        = ObjIter<'a, ExponentialHistogramDataPoint, Self::ExponentialHistogramDataPoint<'a>>
    where
        Self: 'a;

    fn data_points(&self) -> Self::ExponentialHistogramDataPointIter<'_> {
        ObjIter::new(self.inner.data_points.iter())
    }

    fn aggregation_temporality(&self) -> AggregationTemporality {
        (self.inner.aggregation_temporality as u32).into()
    }
}

impl ExponentialHistogramDataPointView for Obj<'_, ExponentialHistogramDataPoint> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;
    type Buckets<'a>
        = Obj<'a, exponential_histogram_data_point::Buckets>
    where
        Self: 'a;
    type Exemplar<'a>
        = Obj<'a, Exemplar>
    where
        Self: 'a;
    type ExemplarIter<'a>
        = ObjIter<'a, Exemplar, Self::Exemplar<'a>>
    where
        Self: 'a;

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }
    fn start_time_unix_nano(&self) -> u64 {
        self.inner.start_time_unix_nano
    }
    fn time_unix_nano(&self) -> u64 {
        self.inner.time_unix_nano
    }
    fn count(&self) -> u64 {
        self.inner.count
    }
    fn sum(&self) -> Option<f64> {
        self.inner.sum
    }
    fn scale(&self) -> i32 {
        self.inner.scale
    }
    fn zero_count(&self) -> u64 {
        self.inner.zero_count
    }
    fn positive(&self) -> Option<Self::Buckets<'_>> {
        self.inner.positive.as_ref().map(Obj::new)
    }
    fn negative(&self) -> Option<Self::Buckets<'_>> {
        self.inner.negative.as_ref().map(Obj::new)
    }
    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(self.inner.flags)
    }
    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        ObjIter::new(self.inner.exemplars.iter())
    }
    fn min(&self) -> Option<f64> {
        self.inner.min
    }
    fn max(&self) -> Option<f64> {
        self.inner.max
    }
    fn zero_threshold(&self) -> f64 {
        self.inner.zero_threshold
    }
}

impl BucketsView for Obj<'_, exponential_histogram_data_point::Buckets> {
    type BucketCountIter<'a>
        = std::iter::Copied<slice::Iter<'a, u64>>
    where
        Self: 'a;

    fn offset(&self) -> i32 {
        self.inner.offset
    }

    fn bucket_counts(&self) -> Self::BucketCountIter<'_> {
        self.inner.bucket_counts.iter().copied()
    }
}

impl SummaryView for Obj<'_, Summary> {
    type SummaryDataPoint<'a>
        = Obj<'a, SummaryDataPoint>
    where
        Self: 'a;
    type SummaryDataPointIter<'a>
        = ObjIter<'a, SummaryDataPoint, Self::SummaryDataPoint<'a>>
    where
        Self: 'a;

    fn data_points(&self) -> Self::SummaryDataPointIter<'_> {
        ObjIter::new(self.inner.data_points.iter())
    }
}

impl SummaryDataPointView for Obj<'_, SummaryDataPoint> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;
    type ValueAtQuantile<'a>
        = Obj<'a, ValueAtQuantile>
    where
        Self: 'a;
    type ValueAtQuantileIter<'a>
        = ObjIter<'a, ValueAtQuantile, Self::ValueAtQuantile<'a>>
    where
        Self: 'a;

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }
    fn start_time_unix_nano(&self) -> u64 {
        self.inner.start_time_unix_nano
    }
    fn time_unix_nano(&self) -> u64 {
        self.inner.time_unix_nano
    }
    fn count(&self) -> u64 {
        self.inner.count
    }
    fn sum(&self) -> f64 {
        self.inner.sum
    }
    fn quantile_values(&self) -> Self::ValueAtQuantileIter<'_> {
        ObjIter::new(self.inner.quantile_values.iter())
    }
    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(self.inner.flags)
    }
}

impl ValueAtQuantileView for Obj<'_, ValueAtQuantile> {
    fn quantile(&self) -> f64 {
        self.inner.quantile
    }

    fn value(&self) -> f64 {
        self.inner.value
    }
}

impl ResourceView for Obj<'_, Resource> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributesIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;

    fn attributes(&self) -> Self::AttributesIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }

    fn dropped_attributes_count(&self) -> u32 {
        self.inner.dropped_attributes_count
    }
}

impl InstrumentationScopeView for Obj<'_, InstrumentationScope> {
    type Attribute<'a>
        = ObjAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = AttributeIter<'a>
    where
        Self: 'a;

    fn name(&self) -> Option<Str<'_>> {
        non_empty(self.inner.name.as_bytes())
    }

    fn version(&self) -> Option<Str<'_>> {
        non_empty(self.inner.version.as_bytes())
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        AttributeIter::new(self.inner.attributes.iter())
    }

    fn dropped_attributes_count(&self) -> u32 {
        self.inner.dropped_attributes_count
    }
}

impl AttributeView for ObjAttribute<'_> {
    type Val<'a>
        = Obj<'a, AnyValue>
    where
        Self: 'a;

    fn key(&self) -> Str<'_> {
        self.key.as_bytes()
    }

    fn value(&self) -> Option<Self::Val<'_>> {
        self.value.clone()
    }
}

impl<'a> AnyValueView<'a> for Obj<'a, AnyValue> {
    type KeyValue = ObjAttribute<'a>;
    type ArrayIter<'b>
        = AnyValueIter<'a>
    where
        Self: 'b;
    type KeyValueIter<'b>
        = AttributeIter<'a>
    where
        Self: 'b;

    fn value_type(&self) -> ValueType {
        match self.inner.value.as_ref() {
            None => ValueType::Empty,
            Some(any_value::Value::StringValue(_)) => ValueType::String,
            Some(any_value::Value::BoolValue(_)) => ValueType::Bool,
            Some(any_value::Value::IntValue(_)) => ValueType::Int64,
            Some(any_value::Value::DoubleValue(_)) => ValueType::Double,
            Some(any_value::Value::ArrayValue(_)) => ValueType::Array,
            Some(any_value::Value::KvlistValue(_)) => ValueType::KeyValueList,
            Some(any_value::Value::BytesValue(_)) => ValueType::Bytes,
        }
    }

    fn as_string(&self) -> Option<Str<'_>> {
        match self.inner.value.as_ref()? {
            any_value::Value::StringValue(value) => Some(value.as_bytes()),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self.inner.value.as_ref()? {
            any_value::Value::BoolValue(value) => Some(*value),
            _ => None,
        }
    }

    fn as_int64(&self) -> Option<i64> {
        match self.inner.value.as_ref()? {
            any_value::Value::IntValue(value) => Some(*value),
            _ => None,
        }
    }

    fn as_double(&self) -> Option<f64> {
        match self.inner.value.as_ref()? {
            any_value::Value::DoubleValue(value) => Some(*value),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        match self.inner.value.as_ref()? {
            any_value::Value::BytesValue(value) => Some(value),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<Self::ArrayIter<'_>> {
        match self.inner.value.as_ref()? {
            any_value::Value::ArrayValue(value) => Some(AnyValueIter {
                iter: value.values.iter(),
            }),
            _ => None,
        }
    }

    fn as_kvlist(&self) -> Option<Self::KeyValueIter<'_>> {
        match self.inner.value.as_ref()? {
            any_value::Value::KvlistValue(value) => Some(AttributeIter::new(value.values.iter())),
            _ => None,
        }
    }
}

fn non_empty(value: &[u8]) -> Option<&[u8]> {
    (!value.is_empty()).then_some(value)
}

fn parse_id<const N: usize>(value: &[u8]) -> Option<&[u8; N]> {
    let id: &[u8; N] = value.try_into().ok()?;
    (id != &[0; N]).then_some(id)
}
