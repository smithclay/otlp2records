// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry view interfaces from `otap-df-pdata-views`.
//!
//! Synced from open-telemetry/otel-arrow commit
//! `f8cd17f084c1a766f887530531ad06f546080c90`.

#![allow(dead_code)]

pub type TraceId = [u8; 16];
pub type SpanId = [u8; 8];
pub type Str<'src> = &'src [u8];

pub trait AnyValueView<'val> {
    type KeyValue: AttributeView;
    type ArrayIter<'arr>: Iterator<Item = Self>
    where
        Self: 'arr;
    type KeyValueIter<'kv>: Iterator<Item = Self::KeyValue>
    where
        Self: 'kv;

    fn value_type(&self) -> ValueType;
    fn as_string(&self) -> Option<Str<'_>>;
    fn as_bool(&self) -> Option<bool>;
    fn as_int64(&self) -> Option<i64>;
    fn as_double(&self) -> Option<f64>;
    fn as_bytes(&self) -> Option<&[u8]>;
    fn as_array(&self) -> Option<Self::ArrayIter<'_>>;
    fn as_kvlist(&self) -> Option<Self::KeyValueIter<'_>>;

    fn is_scalar(&self) -> bool {
        !matches!(
            self.value_type(),
            ValueType::Array | ValueType::KeyValueList
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValueType {
    Empty,
    String,
    Bool,
    Int64,
    Double,
    Array,
    KeyValueList,
    Bytes,
}

pub trait AttributeView {
    type Val<'val>: AnyValueView<'val>
    where
        Self: 'val;

    fn key(&self) -> Str<'_>;
    fn value(&self) -> Option<Self::Val<'_>>;
}

pub trait InstrumentationScopeView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    fn name(&self) -> Option<Str<'_>>;
    fn version(&self) -> Option<Str<'_>>;
    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn dropped_attributes_count(&self) -> u32;
}

pub trait ResourceView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributesIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    fn attributes(&self) -> Self::AttributesIter<'_>;
    fn dropped_attributes_count(&self) -> u32;
}

pub trait LogsDataView {
    type ResourceLogs<'res>: ResourceLogsView
    where
        Self: 'res;
    type ResourcesIter<'res>: Iterator<Item = Self::ResourceLogs<'res>>
    where
        Self: 'res;

    fn resources(&self) -> Self::ResourcesIter<'_>;
}

pub trait ResourceLogsView {
    type Resource<'res>: ResourceView
    where
        Self: 'res;
    type ScopeLogs<'scp>: ScopeLogsView
    where
        Self: 'scp;
    type ScopesIter<'scp>: Iterator<Item = Self::ScopeLogs<'scp>>
    where
        Self: 'scp;

    fn resource(&self) -> Option<Self::Resource<'_>>;
    fn scopes(&self) -> Self::ScopesIter<'_>;
    fn schema_url(&self) -> Option<Str<'_>>;
}

pub trait ScopeLogsView {
    type Scope<'scp>: InstrumentationScopeView
    where
        Self: 'scp;
    type LogRecord<'rec>: LogRecordView
    where
        Self: 'rec;
    type LogRecordsIter<'rec>: Iterator<Item = Self::LogRecord<'rec>>
    where
        Self: 'rec;

    fn scope(&self) -> Option<Self::Scope<'_>>;
    fn log_records(&self) -> Self::LogRecordsIter<'_>;
    fn schema_url(&self) -> Option<Str<'_>>;
}

pub trait LogRecordView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;
    type Body<'bod>: AnyValueView<'bod>
    where
        Self: 'bod;

    fn time_unix_nano(&self) -> Option<u64>;
    fn observed_time_unix_nano(&self) -> Option<u64>;
    fn severity_number(&self) -> Option<i32>;
    fn severity_text(&self) -> Option<Str<'_>>;
    fn body(&self) -> Option<Self::Body<'_>>;
    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn dropped_attributes_count(&self) -> u32;
    fn flags(&self) -> Option<u32>;
    fn trace_id(&self) -> Option<&TraceId>;
    fn span_id(&self) -> Option<&SpanId>;
    fn event_name(&self) -> Option<Str<'_>>;
}

pub trait TracesView {
    type ResourceSpans<'res>: ResourceSpansView
    where
        Self: 'res;
    type ResourcesIter<'res>: Iterator<Item = Self::ResourceSpans<'res>>
    where
        Self: 'res;

    fn resources(&self) -> Self::ResourcesIter<'_>;
}

pub trait ResourceSpansView {
    type Resource<'res>: ResourceView
    where
        Self: 'res;
    type ScopeSpans<'scp>: ScopeSpansView
    where
        Self: 'scp;
    type ScopesIter<'scp>: Iterator<Item = Self::ScopeSpans<'scp>>
    where
        Self: 'scp;

    fn resource(&self) -> Option<Self::Resource<'_>>;
    fn scopes(&self) -> Self::ScopesIter<'_>;
    fn schema_url(&self) -> Option<Str<'_>>;
}

pub trait ScopeSpansView {
    type Scope<'scp>: InstrumentationScopeView
    where
        Self: 'scp;
    type Span<'sp>: SpanView
    where
        Self: 'sp;
    type SpanIter<'sp>: Iterator<Item = Self::Span<'sp>>
    where
        Self: 'sp;

    fn scope(&self) -> Option<Self::Scope<'_>>;
    fn spans(&self) -> Self::SpanIter<'_>;
    fn schema_url(&self) -> Option<Str<'_>>;
}

pub trait SpanView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;
    type Event<'ev>: EventView
    where
        Self: 'ev;
    type EventsIter<'ev>: Iterator<Item = Self::Event<'ev>>
    where
        Self: 'ev;
    type Link<'ln>: LinkView
    where
        Self: 'ln;
    type LinksIter<'ln>: Iterator<Item = Self::Link<'ln>>
    where
        Self: 'ln;
    type Status<'st>: StatusView
    where
        Self: 'st;

    fn trace_id(&self) -> Option<&TraceId>;
    fn span_id(&self) -> Option<&SpanId>;
    fn trace_state(&self) -> Option<Str<'_>>;
    fn parent_span_id(&self) -> Option<&SpanId>;
    fn flags(&self) -> Option<u32>;
    fn name(&self) -> Option<Str<'_>>;
    fn kind(&self) -> i32;
    fn start_time_unix_nano(&self) -> Option<u64>;
    fn end_time_unix_nano(&self) -> Option<u64>;
    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn dropped_attributes_count(&self) -> u32;
    fn events(&self) -> Self::EventsIter<'_>;
    fn dropped_events_count(&self) -> u32;
    fn links(&self) -> Self::LinksIter<'_>;
    fn dropped_links_count(&self) -> u32;
    fn status(&self) -> Option<Self::Status<'_>>;
}

pub trait EventView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    fn time_unix_nano(&self) -> Option<u64>;
    fn name(&self) -> Option<Str<'_>>;
    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn dropped_attributes_count(&self) -> u32;
}

pub trait LinkView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    fn trace_id(&self) -> Option<&TraceId>;
    fn span_id(&self) -> Option<&SpanId>;
    fn trace_state(&self) -> Option<Str<'_>>;
    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn dropped_attributes_count(&self) -> u32;
    fn flags(&self) -> Option<u32>;
}

pub trait StatusView {
    fn message(&self) -> Option<Str<'_>>;
    fn status_code(&self) -> i32;
}

pub trait MetricsView {
    type ResourceMetrics<'res>: ResourceMetricsView
    where
        Self: 'res;
    type ResourceMetricsIter<'res>: Iterator<Item = Self::ResourceMetrics<'res>>
    where
        Self: 'res;

    fn resources(&self) -> Self::ResourceMetricsIter<'_>;
}

pub trait ResourceMetricsView {
    type Resource<'res>: ResourceView
    where
        Self: 'res;
    type ScopeMetrics<'scp>: ScopeMetricsView
    where
        Self: 'scp;
    type ScopesIter<'scp>: Iterator<Item = Self::ScopeMetrics<'scp>>
    where
        Self: 'scp;

    fn resource(&self) -> Option<Self::Resource<'_>>;
    fn scopes(&self) -> Self::ScopesIter<'_>;
    fn schema_url(&self) -> Option<Str<'_>>;
}

pub trait ScopeMetricsView {
    type Scope<'scp>: InstrumentationScopeView
    where
        Self: 'scp;
    type Metric<'met>: MetricView
    where
        Self: 'met;
    type MetricIter<'met>: Iterator<Item = Self::Metric<'met>>
    where
        Self: 'met;

    fn scope(&self) -> Option<Self::Scope<'_>>;
    fn metrics(&self) -> Self::MetricIter<'_>;
    fn schema_url(&self) -> Str<'_>;
}

pub trait MetricView {
    type Data<'dat>: DataView<'dat>
    where
        Self: 'dat;
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    fn name(&self) -> Str<'_>;
    fn description(&self) -> Str<'_>;
    fn unit(&self) -> Str<'_>;
    fn data(&self) -> Option<Self::Data<'_>>;
    fn metadata(&self) -> Self::AttributeIter<'_>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DataType {
    Gauge = 1,
    Sum = 2,
    Histogram = 3,
    ExponentialHistogram = 4,
    Summary = 5,
}

impl DataType {
    #[must_use]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Gauge),
            2 => Some(Self::Sum),
            3 => Some(Self::Histogram),
            4 => Some(Self::ExponentialHistogram),
            5 => Some(Self::Summary),
            _ => None,
        }
    }
}

pub trait DataView<'val> {
    type Gauge<'gauge>: GaugeView
    where
        Self: 'gauge;
    type Sum<'sum>: SumView
    where
        Self: 'sum;
    type Histogram<'histogram>: HistogramView
    where
        Self: 'histogram;
    type ExponentialHistogram<'exp>: ExponentialHistogramView
    where
        Self: 'exp;
    type Summary<'summary>: SummaryView
    where
        Self: 'summary;

    fn value_type(&self) -> DataType;
    fn as_gauge(&self) -> Option<Self::Gauge<'_>>;
    fn as_sum(&self) -> Option<Self::Sum<'_>>;
    fn as_histogram(&self) -> Option<Self::Histogram<'_>>;
    fn as_exponential_histogram(&self) -> Option<Self::ExponentialHistogram<'_>>;
    fn as_summary(&self) -> Option<Self::Summary<'_>>;
}

pub trait GaugeView {
    type NumberDataPoint<'dp>: NumberDataPointView
    where
        Self: 'dp;
    type NumberDataPointIter<'dp>: Iterator<Item = Self::NumberDataPoint<'dp>>
    where
        Self: 'dp;
    fn data_points(&self) -> Self::NumberDataPointIter<'_>;
}

pub trait SumView {
    type NumberDataPoint<'dp>: NumberDataPointView
    where
        Self: 'dp;
    type NumberDataPointIter<'dp>: Iterator<Item = Self::NumberDataPoint<'dp>>
    where
        Self: 'dp;
    fn data_points(&self) -> Self::NumberDataPointIter<'_>;
    fn aggregation_temporality(&self) -> AggregationTemporality;
    fn is_monotonic(&self) -> bool;
}

pub trait NumberDataPointView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;
    type Exemplar<'ex>: ExemplarView
    where
        Self: 'ex;
    type ExemplarIter<'ex>: Iterator<Item = Self::Exemplar<'ex>>
    where
        Self: 'ex;

    fn start_time_unix_nano(&self) -> u64;
    fn time_unix_nano(&self) -> u64;
    fn value(&self) -> Option<Value>;
    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn exemplars(&self) -> Self::ExemplarIter<'_>;
    fn flags(&self) -> DataPointFlags;
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    Double(f64),
    Integer(i64),
}

impl Value {
    #[must_use]
    pub const fn as_double(&self) -> Option<f64> {
        match self {
            Self::Double(value) => Some(*value),
            Self::Integer(_) => None,
        }
    }

    #[must_use]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(*value),
            Self::Double(_) => None,
        }
    }
}

pub trait ExemplarView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;

    fn filtered_attributes(&self) -> Self::AttributeIter<'_>;
    fn time_unix_nano(&self) -> u64;
    fn value(&self) -> Option<Value>;
    fn span_id(&self) -> Option<&SpanId>;
    fn trace_id(&self) -> Option<&TraceId>;
}

pub trait SummaryView {
    type SummaryDataPoint<'dp>: SummaryDataPointView
    where
        Self: 'dp;
    type SummaryDataPointIter<'dp>: Iterator<Item = Self::SummaryDataPoint<'dp>>
    where
        Self: 'dp;
    fn data_points(&self) -> Self::SummaryDataPointIter<'_>;
}

pub trait SummaryDataPointView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;
    type ValueAtQuantile<'vaq>: ValueAtQuantileView
    where
        Self: 'vaq;
    type ValueAtQuantileIter<'vaq>: Iterator<Item = Self::ValueAtQuantile<'vaq>>
    where
        Self: 'vaq;

    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn start_time_unix_nano(&self) -> u64;
    fn time_unix_nano(&self) -> u64;
    fn count(&self) -> u64;
    fn sum(&self) -> f64;
    fn quantile_values(&self) -> Self::ValueAtQuantileIter<'_>;
    fn flags(&self) -> DataPointFlags;
}

pub trait ValueAtQuantileView {
    fn quantile(&self) -> f64;
    fn value(&self) -> f64;
}

#[derive(Clone, Copy)]
pub struct DataPointFlags(u32);

impl DataPointFlags {
    #[must_use]
    pub const fn new(flags: u32) -> Self {
        Self(flags)
    }

    #[must_use]
    pub const fn no_recorded_value(&self) -> bool {
        self.0 & 1 != 0
    }

    #[must_use]
    pub const fn into_inner(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum AggregationTemporality {
    Unspecified = 0,
    Delta = 1,
    Cumulative = 2,
}

impl From<u32> for AggregationTemporality {
    fn from(value: u32) -> Self {
        match value {
            1 => Self::Delta,
            2 => Self::Cumulative,
            _ => Self::Unspecified,
        }
    }
}

pub trait HistogramView {
    type HistogramDataPoint<'dp>: HistogramDataPointView
    where
        Self: 'dp;
    type HistogramDataPointIter<'dp>: Iterator<Item = Self::HistogramDataPoint<'dp>>
    where
        Self: 'dp;
    fn data_points(&self) -> Self::HistogramDataPointIter<'_>;
    fn aggregation_temporality(&self) -> AggregationTemporality;
}

pub trait HistogramDataPointView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;
    type BucketCountIter<'bc>: Iterator<Item = u64>
    where
        Self: 'bc;
    type ExplicitBoundsIter<'eb>: Iterator<Item = f64>
    where
        Self: 'eb;
    type Exemplar<'ex>: ExemplarView
    where
        Self: 'ex;
    type ExemplarIter<'ex>: Iterator<Item = Self::Exemplar<'ex>>
    where
        Self: 'ex;

    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn start_time_unix_nano(&self) -> u64;
    fn time_unix_nano(&self) -> u64;
    fn count(&self) -> u64;
    fn sum(&self) -> Option<f64>;
    fn bucket_counts(&self) -> Self::BucketCountIter<'_>;
    fn explicit_bounds(&self) -> Self::ExplicitBoundsIter<'_>;
    fn exemplars(&self) -> Self::ExemplarIter<'_>;
    fn flags(&self) -> DataPointFlags;
    fn min(&self) -> Option<f64>;
    fn max(&self) -> Option<f64>;
}

pub trait ExponentialHistogramView {
    type ExponentialHistogramDataPoint<'edp>: ExponentialHistogramDataPointView
    where
        Self: 'edp;
    type ExponentialHistogramDataPointIter<'edp>: Iterator<
        Item = Self::ExponentialHistogramDataPoint<'edp>,
    >
    where
        Self: 'edp;
    fn data_points(&self) -> Self::ExponentialHistogramDataPointIter<'_>;
    fn aggregation_temporality(&self) -> AggregationTemporality;
}

pub trait ExponentialHistogramDataPointView {
    type Attribute<'att>: AttributeView
    where
        Self: 'att;
    type AttributeIter<'att>: Iterator<Item = Self::Attribute<'att>>
    where
        Self: 'att;
    type Buckets<'b>: BucketsView
    where
        Self: 'b;
    type Exemplar<'ex>: ExemplarView
    where
        Self: 'ex;
    type ExemplarIter<'ex>: Iterator<Item = Self::Exemplar<'ex>>
    where
        Self: 'ex;

    fn attributes(&self) -> Self::AttributeIter<'_>;
    fn start_time_unix_nano(&self) -> u64;
    fn time_unix_nano(&self) -> u64;
    fn count(&self) -> u64;
    fn sum(&self) -> Option<f64>;
    fn scale(&self) -> i32;
    fn zero_count(&self) -> u64;
    fn positive(&self) -> Option<Self::Buckets<'_>>;
    fn negative(&self) -> Option<Self::Buckets<'_>>;
    fn flags(&self) -> DataPointFlags;
    fn exemplars(&self) -> Self::ExemplarIter<'_>;
    fn min(&self) -> Option<f64>;
    fn max(&self) -> Option<f64>;
    fn zero_threshold(&self) -> f64;
}

pub trait BucketsView {
    type BucketCountIter<'bc>: Iterator<Item = u64>
    where
        Self: 'bc;
    fn offset(&self) -> i32;
    fn bucket_counts(&self) -> Self::BucketCountIter<'_>;
}
