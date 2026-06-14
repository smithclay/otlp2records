use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    Array, Float64Array, ListArray, RecordBatch, StructArray, UInt16Array, UInt64Array,
};

use crate::{
    api::MetricBatches,
    batch::transform_metrics_view,
    views::pdata::{
        AggregationTemporality, BucketsView, DataPointFlags, DataType, DataView, ExemplarView,
        ExponentialHistogramDataPointView, ExponentialHistogramView, GaugeView,
        HistogramDataPointView, HistogramView, InstrumentationScopeView, MetricView, MetricsView,
        NumberDataPointView, ResourceMetricsView, ResourceView, ScopeMetricsView, SpanId, Str,
        SumView, SummaryDataPointView, SummaryView, TraceId, Value, ValueAtQuantileView,
    },
    Error, Result,
};

use super::validation::{validate, PayloadSchema};
use super::{
    logs::{
        bool_at, build_groups, column_string, column_u16, column_u32, decode_attr_parent_ids,
        decode_delta, decode_quasi_delta, decode_root_ids, f64_at, i32_at, i64_at, id, index_u16,
        index_u32, nested_string, nested_u32_value, replace, timestamp, validate_attrs, Attr16Iter,
        Attr32Iter, AttributeTable, AttributeTable32, OtapAttribute, ResourceGroup, ScopeGroup,
    },
    traces::{decode_attr_parent_ids_u32, decode_delta_u32},
};

#[allow(clippy::too_many_arguments)]
pub(super) fn normalize(
    metrics: RecordBatch,
    resource_attrs: Option<RecordBatch>,
    scope_attrs: Option<RecordBatch>,
    metric_attrs: Option<RecordBatch>,
    number_points: Option<RecordBatch>,
    summary_points: Option<RecordBatch>,
    histogram_points: Option<RecordBatch>,
    exp_histogram_points: Option<RecordBatch>,
    number_attrs: Option<RecordBatch>,
    summary_attrs: Option<RecordBatch>,
    histogram_attrs: Option<RecordBatch>,
    exp_histogram_attrs: Option<RecordBatch>,
    number_exemplars: Option<RecordBatch>,
    histogram_exemplars: Option<RecordBatch>,
    exp_histogram_exemplars: Option<RecordBatch>,
    number_exemplar_attrs: Option<RecordBatch>,
    histogram_exemplar_attrs: Option<RecordBatch>,
    exp_histogram_exemplar_attrs: Option<RecordBatch>,
) -> Result<MetricBatches> {
    validate(PayloadSchema::Metrics, "metrics", &metrics)?;
    for (payload, name, batch) in [
        (
            PayloadSchema::Attrs16,
            "resource attributes",
            resource_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs16,
            "scope attributes",
            scope_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs16,
            "metric attributes",
            metric_attrs.as_ref(),
        ),
        (
            PayloadSchema::NumberPoints,
            "number data points",
            number_points.as_ref(),
        ),
        (
            PayloadSchema::SummaryPoints,
            "summary data points",
            summary_points.as_ref(),
        ),
        (
            PayloadSchema::HistogramPoints,
            "histogram data points",
            histogram_points.as_ref(),
        ),
        (
            PayloadSchema::ExpHistogramPoints,
            "exponential histogram data points",
            exp_histogram_points.as_ref(),
        ),
        (
            PayloadSchema::Attrs32,
            "number attributes",
            number_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs32,
            "summary attributes",
            summary_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs32,
            "histogram attributes",
            histogram_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs32,
            "exponential histogram attributes",
            exp_histogram_attrs.as_ref(),
        ),
        (
            PayloadSchema::Exemplars,
            "number exemplars",
            number_exemplars.as_ref(),
        ),
        (
            PayloadSchema::Exemplars,
            "histogram exemplars",
            histogram_exemplars.as_ref(),
        ),
        (
            PayloadSchema::Exemplars,
            "exponential histogram exemplars",
            exp_histogram_exemplars.as_ref(),
        ),
        (
            PayloadSchema::Attrs32,
            "number exemplar attributes",
            number_exemplar_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs32,
            "histogram exemplar attributes",
            histogram_exemplar_attrs.as_ref(),
        ),
        (
            PayloadSchema::Attrs32,
            "exponential histogram exemplar attributes",
            exp_histogram_exemplar_attrs.as_ref(),
        ),
    ] {
        if let Some(batch) = batch {
            validate(payload, name, batch)?;
        }
    }
    for (name, batch) in [
        ("resource attributes", resource_attrs.as_ref()),
        ("scope attributes", scope_attrs.as_ref()),
        ("metric attributes", metric_attrs.as_ref()),
    ] {
        if let Some(batch) = batch {
            validate_attrs(name, batch)?;
        }
    }

    let metrics = decode_root_ids(metrics)?;
    let resource_attrs = resource_attrs.map(decode_attr_parent_ids).transpose()?;
    let scope_attrs = scope_attrs.map(decode_attr_parent_ids).transpose()?;
    let metric_attrs = metric_attrs.map(decode_attr_parent_ids).transpose()?;

    let number_points = number_points.map(decode_points).transpose()?;
    let summary_points = summary_points.map(decode_points).transpose()?;
    let histogram_points = histogram_points.map(decode_points).transpose()?;
    let exp_histogram_points = exp_histogram_points.map(decode_points).transpose()?;
    let number_attrs = number_attrs.map(decode_attr_parent_ids_u32).transpose()?;
    let summary_attrs = summary_attrs.map(decode_attr_parent_ids_u32).transpose()?;
    let histogram_attrs = histogram_attrs
        .map(decode_attr_parent_ids_u32)
        .transpose()?;
    let exp_histogram_attrs = exp_histogram_attrs
        .map(decode_attr_parent_ids_u32)
        .transpose()?;
    let number_exemplars = number_exemplars.map(decode_exemplars).transpose()?;
    let histogram_exemplars = histogram_exemplars.map(decode_exemplars).transpose()?;
    let exp_histogram_exemplars = exp_histogram_exemplars.map(decode_exemplars).transpose()?;
    let number_exemplar_attrs = number_exemplar_attrs
        .map(decode_attr_parent_ids_u32)
        .transpose()?;
    let histogram_exemplar_attrs = histogram_exemplar_attrs
        .map(decode_attr_parent_ids_u32)
        .transpose()?;
    let exp_histogram_exemplar_attrs = exp_histogram_exemplar_attrs
        .map(decode_attr_parent_ids_u32)
        .transpose()?;

    let view = OtapMetricsView::new(
        metrics,
        resource_attrs,
        scope_attrs,
        metric_attrs,
        number_points,
        summary_points,
        histogram_points,
        exp_histogram_points,
        number_attrs,
        summary_attrs,
        histogram_attrs,
        exp_histogram_attrs,
        number_exemplars,
        histogram_exemplars,
        exp_histogram_exemplars,
        number_exemplar_attrs,
        histogram_exemplar_attrs,
        exp_histogram_exemplar_attrs,
    )?;
    transform_metrics_view(&view)
}

struct ChildTable {
    batch: RecordBatch,
    by_parent: BTreeMap<u16, Vec<usize>>,
}

impl ChildTable {
    fn new(batch: RecordBatch) -> Result<Self> {
        let by_parent = index_u16(&batch, "parent_id")?;
        Ok(Self { batch, by_parent })
    }
    fn rows(&self, parent: u16) -> &[usize] {
        self.by_parent
            .get(&parent)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

struct ExemplarTable {
    batch: RecordBatch,
    by_parent: BTreeMap<u32, Vec<usize>>,
    attrs: Option<AttributeTable32>,
}

impl ExemplarTable {
    fn new(batch: RecordBatch, attrs: Option<RecordBatch>) -> Result<Self> {
        let by_parent = index_u32(&batch, "parent_id")?;
        Ok(Self {
            batch,
            by_parent,
            attrs: attrs.map(AttributeTable32::new).transpose()?,
        })
    }
    fn rows(&self, parent: Option<u32>) -> &[usize] {
        parent
            .and_then(|id| self.by_parent.get(&id))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

struct PointTable {
    children: ChildTable,
    attrs: Option<AttributeTable32>,
    exemplars: Option<ExemplarTable>,
}

impl PointTable {
    fn new(
        batch: Option<RecordBatch>,
        attrs: Option<RecordBatch>,
        exemplars: Option<RecordBatch>,
        exemplar_attrs: Option<RecordBatch>,
    ) -> Result<Option<Self>> {
        batch
            .map(|batch| {
                Ok(Self {
                    children: ChildTable::new(batch)?,
                    attrs: attrs.map(AttributeTable32::new).transpose()?,
                    exemplars: exemplars
                        .map(|batch| ExemplarTable::new(batch, exemplar_attrs))
                        .transpose()?,
                })
            })
            .transpose()
    }
}

struct OtapMetricsView {
    metrics: RecordBatch,
    resources: Vec<ResourceGroup>,
    resource_attrs: Option<AttributeTable>,
    scope_attrs: Option<AttributeTable>,
    metric_attrs: Option<AttributeTable>,
    number: Option<PointTable>,
    summary: Option<PointTable>,
    histogram: Option<PointTable>,
    exp_histogram: Option<PointTable>,
}

impl OtapMetricsView {
    #[allow(clippy::too_many_arguments)]
    fn new(
        metrics: RecordBatch,
        resource_attrs: Option<RecordBatch>,
        scope_attrs: Option<RecordBatch>,
        metric_attrs: Option<RecordBatch>,
        number_points: Option<RecordBatch>,
        summary_points: Option<RecordBatch>,
        histogram_points: Option<RecordBatch>,
        exp_histogram_points: Option<RecordBatch>,
        number_attrs: Option<RecordBatch>,
        summary_attrs: Option<RecordBatch>,
        histogram_attrs: Option<RecordBatch>,
        exp_histogram_attrs: Option<RecordBatch>,
        number_exemplars: Option<RecordBatch>,
        histogram_exemplars: Option<RecordBatch>,
        exp_histogram_exemplars: Option<RecordBatch>,
        number_exemplar_attrs: Option<RecordBatch>,
        histogram_exemplar_attrs: Option<RecordBatch>,
        exp_histogram_exemplar_attrs: Option<RecordBatch>,
    ) -> Result<Self> {
        let resources = build_groups(&metrics)?;
        Ok(Self {
            metrics,
            resources,
            resource_attrs: resource_attrs.map(AttributeTable::new).transpose()?,
            scope_attrs: scope_attrs.map(AttributeTable::new).transpose()?,
            metric_attrs: metric_attrs.map(AttributeTable::new).transpose()?,
            number: PointTable::new(
                number_points,
                number_attrs,
                number_exemplars,
                number_exemplar_attrs,
            )?,
            summary: PointTable::new(summary_points, summary_attrs, None, None)?,
            histogram: PointTable::new(
                histogram_points,
                histogram_attrs,
                histogram_exemplars,
                histogram_exemplar_attrs,
            )?,
            exp_histogram: PointTable::new(
                exp_histogram_points,
                exp_histogram_attrs,
                exp_histogram_exemplars,
                exp_histogram_exemplar_attrs,
            )?,
        })
    }
}

impl MetricsView for OtapMetricsView {
    type ResourceMetrics<'a>
        = OtapResourceMetrics<'a>
    where
        Self: 'a;
    type ResourceMetricsIter<'a>
        = ResourceIter<'a>
    where
        Self: 'a;
    fn resources(&self) -> Self::ResourceMetricsIter<'_> {
        ResourceIter {
            view: self,
            groups: self.resources.iter(),
        }
    }
}

struct ResourceIter<'a> {
    view: &'a OtapMetricsView,
    groups: std::slice::Iter<'a, ResourceGroup>,
}

impl<'a> Iterator for ResourceIter<'a> {
    type Item = OtapResourceMetrics<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapResourceMetrics {
            view: self.view,
            group: self.groups.next()?,
        })
    }
}

struct OtapResourceMetrics<'a> {
    view: &'a OtapMetricsView,
    group: &'a ResourceGroup,
}

impl ResourceMetricsView for OtapResourceMetrics<'_> {
    type Resource<'a>
        = OtapResource<'a>
    where
        Self: 'a;
    type ScopeMetrics<'a>
        = OtapScopeMetrics<'a>
    where
        Self: 'a;
    type ScopesIter<'a>
        = ScopeIter<'a>
    where
        Self: 'a;
    fn resource(&self) -> Option<Self::Resource<'_>> {
        Some(OtapResource {
            view: self.view,
            id: self.group.id,
            row: self.group.representative,
        })
    }
    fn scopes(&self) -> Self::ScopesIter<'_> {
        ScopeIter {
            view: self.view,
            groups: self.group.scopes.iter(),
        }
    }
    fn schema_url(&self) -> Option<Str<'_>> {
        nested_string(
            &self.view.metrics,
            "resource",
            "schema_url",
            self.group.representative,
        )
    }
}

struct ScopeIter<'a> {
    view: &'a OtapMetricsView,
    groups: std::slice::Iter<'a, ScopeGroup>,
}

impl<'a> Iterator for ScopeIter<'a> {
    type Item = OtapScopeMetrics<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapScopeMetrics {
            view: self.view,
            group: self.groups.next()?,
        })
    }
}

struct OtapScopeMetrics<'a> {
    view: &'a OtapMetricsView,
    group: &'a ScopeGroup,
}

impl ScopeMetricsView for OtapScopeMetrics<'_> {
    type Scope<'a>
        = OtapScope<'a>
    where
        Self: 'a;
    type Metric<'a>
        = OtapMetric<'a>
    where
        Self: 'a;
    type MetricIter<'a>
        = MetricIter<'a>
    where
        Self: 'a;
    fn scope(&self) -> Option<Self::Scope<'_>> {
        Some(OtapScope {
            view: self.view,
            id: self.group.id,
            row: self.group.representative,
        })
    }
    fn metrics(&self) -> Self::MetricIter<'_> {
        MetricIter {
            view: self.view,
            rows: self.group.rows.iter(),
        }
    }
    fn schema_url(&self) -> Str<'_> {
        column_string(&self.view.metrics, "schema_url", self.group.representative)
            .unwrap_or_default()
    }
}

struct OtapResource<'a> {
    view: &'a OtapMetricsView,
    id: Option<u16>,
    row: usize,
}

impl ResourceView for OtapResource<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributesIter<'a>
        = Attr16Iter<'a>
    where
        Self: 'a;
    fn attributes(&self) -> Self::AttributesIter<'_> {
        Attr16Iter::new(self.view.resource_attrs.as_ref(), self.id)
    }
    fn dropped_attributes_count(&self) -> u32 {
        nested_u32_value(
            &self.view.metrics,
            "resource",
            "dropped_attributes_count",
            self.row,
        )
        .unwrap_or_default()
    }
}

struct OtapScope<'a> {
    view: &'a OtapMetricsView,
    id: Option<u16>,
    row: usize,
}

impl InstrumentationScopeView for OtapScope<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr16Iter<'a>
    where
        Self: 'a;
    fn name(&self) -> Option<Str<'_>> {
        nested_string(&self.view.metrics, "scope", "name", self.row)
    }
    fn version(&self) -> Option<Str<'_>> {
        nested_string(&self.view.metrics, "scope", "version", self.row)
    }
    fn attributes(&self) -> Self::AttributeIter<'_> {
        Attr16Iter::new(self.view.scope_attrs.as_ref(), self.id)
    }
    fn dropped_attributes_count(&self) -> u32 {
        nested_u32_value(
            &self.view.metrics,
            "scope",
            "dropped_attributes_count",
            self.row,
        )
        .unwrap_or_default()
    }
}

struct MetricIter<'a> {
    view: &'a OtapMetricsView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> Iterator for MetricIter<'a> {
    type Item = OtapMetric<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapMetric {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct OtapMetric<'a> {
    view: &'a OtapMetricsView,
    row: usize,
}

impl MetricView for OtapMetric<'_> {
    type Data<'a>
        = OtapData<'a>
    where
        Self: 'a;
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr16Iter<'a>
    where
        Self: 'a;
    fn name(&self) -> Str<'_> {
        column_string(&self.view.metrics, "name", self.row).unwrap_or_default()
    }
    fn description(&self) -> Str<'_> {
        column_string(&self.view.metrics, "description", self.row).unwrap_or_default()
    }
    fn unit(&self) -> Str<'_> {
        column_string(&self.view.metrics, "unit", self.row).unwrap_or_default()
    }
    fn data(&self) -> Option<Self::Data<'_>> {
        let metric_type = self
            .view
            .metrics
            .column_by_name("metric_type")
            .and_then(|array| super::logs::u8_at(array, self.row))?;
        let metric_id = column_u16(&self.view.metrics, "id", self.row)?;
        let temporality = self
            .view
            .metrics
            .column_by_name("aggregation_temporality")
            .and_then(|array| i32_at(array, self.row))
            .map(|value| AggregationTemporality::from(value as u32))
            .unwrap_or(AggregationTemporality::Unspecified);
        let monotonic = self
            .view
            .metrics
            .column_by_name("is_monotonic")
            .and_then(|array| bool_at(array, self.row))
            .unwrap_or(false);
        match DataType::from_u8(metric_type)? {
            DataType::Gauge => Some(OtapData::GaugeData(NumberSet {
                view: self.view,
                metric_id,
            })),
            DataType::Sum => Some(OtapData::SumData(SumSet {
                view: self.view,
                metric_id,
                temporality,
                monotonic,
            })),
            DataType::Histogram => Some(OtapData::HistogramData(HistogramSet {
                view: self.view,
                metric_id,
                temporality,
            })),
            DataType::ExponentialHistogram => Some(OtapData::ExpHistogramData(ExpHistogramSet {
                view: self.view,
                metric_id,
                temporality,
            })),
            DataType::Summary => Some(OtapData::SummaryData(SummarySet {
                view: self.view,
                metric_id,
            })),
        }
    }
    fn metadata(&self) -> Self::AttributeIter<'_> {
        Attr16Iter::new(
            self.view.metric_attrs.as_ref(),
            column_u16(&self.view.metrics, "id", self.row),
        )
    }
}

#[allow(clippy::enum_variant_names)]
enum OtapData<'a> {
    GaugeData(NumberSet<'a>),
    SumData(SumSet<'a>),
    HistogramData(HistogramSet<'a>),
    ExpHistogramData(ExpHistogramSet<'a>),
    SummaryData(SummarySet<'a>),
}

impl<'a> DataView<'a> for OtapData<'a> {
    type Gauge<'b>
        = NumberSet<'b>
    where
        Self: 'b;
    type Sum<'b>
        = SumSet<'b>
    where
        Self: 'b;
    type Histogram<'b>
        = HistogramSet<'b>
    where
        Self: 'b;
    type ExponentialHistogram<'b>
        = ExpHistogramSet<'b>
    where
        Self: 'b;
    type Summary<'b>
        = SummarySet<'b>
    where
        Self: 'b;
    fn value_type(&self) -> DataType {
        match self {
            Self::GaugeData(_) => DataType::Gauge,
            Self::SumData(_) => DataType::Sum,
            Self::HistogramData(_) => DataType::Histogram,
            Self::ExpHistogramData(_) => DataType::ExponentialHistogram,
            Self::SummaryData(_) => DataType::Summary,
        }
    }
    fn as_gauge(&self) -> Option<Self::Gauge<'_>> {
        match self {
            Self::GaugeData(value) => Some(*value),
            _ => None,
        }
    }
    fn as_sum(&self) -> Option<Self::Sum<'_>> {
        match self {
            Self::SumData(value) => Some(*value),
            _ => None,
        }
    }
    fn as_histogram(&self) -> Option<Self::Histogram<'_>> {
        match self {
            Self::HistogramData(value) => Some(*value),
            _ => None,
        }
    }
    fn as_exponential_histogram(&self) -> Option<Self::ExponentialHistogram<'_>> {
        match self {
            Self::ExpHistogramData(value) => Some(*value),
            _ => None,
        }
    }
    fn as_summary(&self) -> Option<Self::Summary<'_>> {
        match self {
            Self::SummaryData(value) => Some(*value),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
struct NumberSet<'a> {
    view: &'a OtapMetricsView,
    metric_id: u16,
}

impl GaugeView for NumberSet<'_> {
    type NumberDataPoint<'a>
        = NumberPoint<'a>
    where
        Self: 'a;
    type NumberDataPointIter<'a>
        = NumberPointIter<'a>
    where
        Self: 'a;
    fn data_points(&self) -> Self::NumberDataPointIter<'_> {
        NumberPointIter::new(self.view, self.metric_id)
    }
}

#[derive(Clone, Copy)]
struct SumSet<'a> {
    view: &'a OtapMetricsView,
    metric_id: u16,
    temporality: AggregationTemporality,
    monotonic: bool,
}

impl SumView for SumSet<'_> {
    type NumberDataPoint<'a>
        = NumberPoint<'a>
    where
        Self: 'a;
    type NumberDataPointIter<'a>
        = NumberPointIter<'a>
    where
        Self: 'a;
    fn data_points(&self) -> Self::NumberDataPointIter<'_> {
        NumberPointIter::new(self.view, self.metric_id)
    }
    fn aggregation_temporality(&self) -> AggregationTemporality {
        self.temporality
    }
    fn is_monotonic(&self) -> bool {
        self.monotonic
    }
}

struct NumberPointIter<'a> {
    view: &'a OtapMetricsView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> NumberPointIter<'a> {
    fn new(view: &'a OtapMetricsView, metric_id: u16) -> Self {
        Self {
            view,
            rows: view
                .number
                .as_ref()
                .map(|table| table.children.rows(metric_id))
                .unwrap_or_default()
                .iter(),
        }
    }
}

impl<'a> Iterator for NumberPointIter<'a> {
    type Item = NumberPoint<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(NumberPoint {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct NumberPoint<'a> {
    view: &'a OtapMetricsView,
    row: usize,
}

impl NumberDataPointView for NumberPoint<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr32Iter<'a>
    where
        Self: 'a;
    type Exemplar<'a>
        = OtapExemplar<'a>
    where
        Self: 'a;
    type ExemplarIter<'a>
        = ExemplarIter<'a>
    where
        Self: 'a;
    fn start_time_unix_nano(&self) -> u64 {
        self.batch()
            .and_then(|batch| timestamp(batch, "start_time_unix_nano", self.row))
            .unwrap_or_default()
    }
    fn time_unix_nano(&self) -> u64 {
        self.batch()
            .and_then(|batch| timestamp(batch, "time_unix_nano", self.row))
            .unwrap_or_default()
    }
    fn value(&self) -> Option<Value> {
        let batch = self.batch()?;
        batch
            .column_by_name("int_value")
            .and_then(|array| i64_at(array, self.row))
            .map(Value::Integer)
            .or_else(|| {
                batch
                    .column_by_name("double_value")
                    .and_then(|array| f64_at(array, self.row))
                    .map(Value::Double)
            })
    }
    fn attributes(&self) -> Self::AttributeIter<'_> {
        let table = self.view.number.as_ref();
        Attr32Iter::new(
            table.and_then(|table| table.attrs.as_ref()),
            table.and_then(|table| column_u32(&table.children.batch, "id", self.row)),
        )
    }
    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        let table = self.view.number.as_ref();
        ExemplarIter::new(
            table.and_then(|table| table.exemplars.as_ref()),
            table.and_then(|table| column_u32(&table.children.batch, "id", self.row)),
        )
    }
    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(
            self.batch()
                .and_then(|batch| column_u32(batch, "flags", self.row))
                .unwrap_or_default(),
        )
    }
}

impl NumberPoint<'_> {
    fn batch(&self) -> Option<&RecordBatch> {
        Some(&self.view.number.as_ref()?.children.batch)
    }
}

#[derive(Clone, Copy)]
struct HistogramSet<'a> {
    view: &'a OtapMetricsView,
    metric_id: u16,
    temporality: AggregationTemporality,
}

impl HistogramView for HistogramSet<'_> {
    type HistogramDataPoint<'a>
        = HistogramPoint<'a>
    where
        Self: 'a;
    type HistogramDataPointIter<'a>
        = HistogramPointIter<'a>
    where
        Self: 'a;
    fn data_points(&self) -> Self::HistogramDataPointIter<'_> {
        HistogramPointIter::new(self.view, self.metric_id)
    }
    fn aggregation_temporality(&self) -> AggregationTemporality {
        self.temporality
    }
}

struct HistogramPointIter<'a> {
    view: &'a OtapMetricsView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> HistogramPointIter<'a> {
    fn new(view: &'a OtapMetricsView, metric_id: u16) -> Self {
        Self {
            view,
            rows: view
                .histogram
                .as_ref()
                .map(|table| table.children.rows(metric_id))
                .unwrap_or_default()
                .iter(),
        }
    }
}

impl<'a> Iterator for HistogramPointIter<'a> {
    type Item = HistogramPoint<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(HistogramPoint {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct HistogramPoint<'a> {
    view: &'a OtapMetricsView,
    row: usize,
}

impl HistogramDataPointView for HistogramPoint<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr32Iter<'a>
    where
        Self: 'a;
    type BucketCountIter<'a>
        = std::vec::IntoIter<u64>
    where
        Self: 'a;
    type ExplicitBoundsIter<'a>
        = std::vec::IntoIter<f64>
    where
        Self: 'a;
    type Exemplar<'a>
        = OtapExemplar<'a>
    where
        Self: 'a;
    type ExemplarIter<'a>
        = ExemplarIter<'a>
    where
        Self: 'a;
    fn attributes(&self) -> Self::AttributeIter<'_> {
        Attr32Iter::new(
            self.table().and_then(|table| table.attrs.as_ref()),
            self.id(),
        )
    }
    fn start_time_unix_nano(&self) -> u64 {
        timestamp(self.batch(), "start_time_unix_nano", self.row).unwrap_or_default()
    }
    fn time_unix_nano(&self) -> u64 {
        timestamp(self.batch(), "time_unix_nano", self.row).unwrap_or_default()
    }
    fn count(&self) -> u64 {
        column_u64(self.batch(), "count", self.row).unwrap_or_default()
    }
    fn sum(&self) -> Option<f64> {
        column_f64(self.batch(), "sum", self.row)
    }
    fn bucket_counts(&self) -> Self::BucketCountIter<'_> {
        list_u64(self.batch(), "bucket_counts", self.row).into_iter()
    }
    fn explicit_bounds(&self) -> Self::ExplicitBoundsIter<'_> {
        list_f64(self.batch(), "explicit_bounds", self.row).into_iter()
    }
    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        ExemplarIter::new(
            self.table().and_then(|table| table.exemplars.as_ref()),
            self.id(),
        )
    }
    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(column_u32(self.batch(), "flags", self.row).unwrap_or_default())
    }
    fn min(&self) -> Option<f64> {
        column_f64(self.batch(), "min", self.row)
    }
    fn max(&self) -> Option<f64> {
        column_f64(self.batch(), "max", self.row)
    }
}

impl HistogramPoint<'_> {
    fn table(&self) -> Option<&PointTable> {
        self.view.histogram.as_ref()
    }
    fn batch(&self) -> &RecordBatch {
        &self
            .table()
            .expect("iterator requires table")
            .children
            .batch
    }
    fn id(&self) -> Option<u32> {
        column_u32(self.batch(), "id", self.row)
    }
}

#[derive(Clone, Copy)]
struct ExpHistogramSet<'a> {
    view: &'a OtapMetricsView,
    metric_id: u16,
    temporality: AggregationTemporality,
}

impl ExponentialHistogramView for ExpHistogramSet<'_> {
    type ExponentialHistogramDataPoint<'a>
        = ExpHistogramPoint<'a>
    where
        Self: 'a;
    type ExponentialHistogramDataPointIter<'a>
        = ExpHistogramPointIter<'a>
    where
        Self: 'a;
    fn data_points(&self) -> Self::ExponentialHistogramDataPointIter<'_> {
        ExpHistogramPointIter::new(self.view, self.metric_id)
    }
    fn aggregation_temporality(&self) -> AggregationTemporality {
        self.temporality
    }
}

struct ExpHistogramPointIter<'a> {
    view: &'a OtapMetricsView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> ExpHistogramPointIter<'a> {
    fn new(view: &'a OtapMetricsView, metric_id: u16) -> Self {
        Self {
            view,
            rows: view
                .exp_histogram
                .as_ref()
                .map(|table| table.children.rows(metric_id))
                .unwrap_or_default()
                .iter(),
        }
    }
}

impl<'a> Iterator for ExpHistogramPointIter<'a> {
    type Item = ExpHistogramPoint<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(ExpHistogramPoint {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct ExpHistogramPoint<'a> {
    view: &'a OtapMetricsView,
    row: usize,
}

impl ExponentialHistogramDataPointView for ExpHistogramPoint<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr32Iter<'a>
    where
        Self: 'a;
    type Buckets<'a>
        = OtapBuckets<'a>
    where
        Self: 'a;
    type Exemplar<'a>
        = OtapExemplar<'a>
    where
        Self: 'a;
    type ExemplarIter<'a>
        = ExemplarIter<'a>
    where
        Self: 'a;
    fn attributes(&self) -> Self::AttributeIter<'_> {
        Attr32Iter::new(
            self.table().and_then(|table| table.attrs.as_ref()),
            self.id(),
        )
    }
    fn start_time_unix_nano(&self) -> u64 {
        timestamp(self.batch(), "start_time_unix_nano", self.row).unwrap_or_default()
    }
    fn time_unix_nano(&self) -> u64 {
        timestamp(self.batch(), "time_unix_nano", self.row).unwrap_or_default()
    }
    fn count(&self) -> u64 {
        column_u64(self.batch(), "count", self.row).unwrap_or_default()
    }
    fn sum(&self) -> Option<f64> {
        column_f64(self.batch(), "sum", self.row)
    }
    fn scale(&self) -> i32 {
        column_i32(self.batch(), "scale", self.row).unwrap_or_default()
    }
    fn zero_count(&self) -> u64 {
        column_u64(self.batch(), "zero_count", self.row).unwrap_or_default()
    }
    fn positive(&self) -> Option<Self::Buckets<'_>> {
        buckets(self.batch(), "positive", self.row)
    }
    fn negative(&self) -> Option<Self::Buckets<'_>> {
        buckets(self.batch(), "negative", self.row)
    }
    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(column_u32(self.batch(), "flags", self.row).unwrap_or_default())
    }
    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        ExemplarIter::new(
            self.table().and_then(|table| table.exemplars.as_ref()),
            self.id(),
        )
    }
    fn min(&self) -> Option<f64> {
        column_f64(self.batch(), "min", self.row)
    }
    fn max(&self) -> Option<f64> {
        column_f64(self.batch(), "max", self.row)
    }
    fn zero_threshold(&self) -> f64 {
        column_f64(self.batch(), "zero_threshold", self.row).unwrap_or_default()
    }
}

impl ExpHistogramPoint<'_> {
    fn table(&self) -> Option<&PointTable> {
        self.view.exp_histogram.as_ref()
    }
    fn batch(&self) -> &RecordBatch {
        &self
            .table()
            .expect("iterator requires table")
            .children
            .batch
    }
    fn id(&self) -> Option<u32> {
        column_u32(self.batch(), "id", self.row)
    }
}

struct OtapBuckets<'a> {
    values: &'a StructArray,
    row: usize,
}

impl BucketsView for OtapBuckets<'_> {
    type BucketCountIter<'a>
        = std::vec::IntoIter<u64>
    where
        Self: 'a;
    fn offset(&self) -> i32 {
        self.values
            .column_by_name("offset")
            .and_then(|array| i32_at(array, self.row))
            .unwrap_or_default()
    }
    fn bucket_counts(&self) -> Self::BucketCountIter<'_> {
        self.values
            .column_by_name("bucket_counts")
            .map(|array| list_u64_array(array, self.row))
            .unwrap_or_default()
            .into_iter()
    }
}

#[derive(Clone, Copy)]
struct SummarySet<'a> {
    view: &'a OtapMetricsView,
    metric_id: u16,
}

impl SummaryView for SummarySet<'_> {
    type SummaryDataPoint<'a>
        = SummaryPoint<'a>
    where
        Self: 'a;
    type SummaryDataPointIter<'a>
        = SummaryPointIter<'a>
    where
        Self: 'a;
    fn data_points(&self) -> Self::SummaryDataPointIter<'_> {
        SummaryPointIter {
            view: self.view,
            rows: self
                .view
                .summary
                .as_ref()
                .map(|table| table.children.rows(self.metric_id))
                .unwrap_or_default()
                .iter(),
        }
    }
}

struct SummaryPointIter<'a> {
    view: &'a OtapMetricsView,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> Iterator for SummaryPointIter<'a> {
    type Item = SummaryPoint<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(SummaryPoint {
            view: self.view,
            row: *self.rows.next()?,
        })
    }
}

struct SummaryPoint<'a> {
    view: &'a OtapMetricsView,
    row: usize,
}

struct EmptyQuantile;

impl ValueAtQuantileView for EmptyQuantile {
    fn quantile(&self) -> f64 {
        0.0
    }
    fn value(&self) -> f64 {
        0.0
    }
}

impl SummaryDataPointView for SummaryPoint<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr32Iter<'a>
    where
        Self: 'a;
    type ValueAtQuantile<'a>
        = EmptyQuantile
    where
        Self: 'a;
    type ValueAtQuantileIter<'a>
        = std::iter::Empty<EmptyQuantile>
    where
        Self: 'a;
    fn attributes(&self) -> Self::AttributeIter<'_> {
        let table = self.view.summary.as_ref();
        Attr32Iter::new(
            table.and_then(|table| table.attrs.as_ref()),
            table.and_then(|table| column_u32(&table.children.batch, "id", self.row)),
        )
    }
    fn start_time_unix_nano(&self) -> u64 {
        timestamp(self.batch(), "start_time_unix_nano", self.row).unwrap_or_default()
    }
    fn time_unix_nano(&self) -> u64 {
        timestamp(self.batch(), "time_unix_nano", self.row).unwrap_or_default()
    }
    fn count(&self) -> u64 {
        column_u64(self.batch(), "count", self.row).unwrap_or_default()
    }
    fn sum(&self) -> f64 {
        column_f64(self.batch(), "sum", self.row).unwrap_or_default()
    }
    fn quantile_values(&self) -> Self::ValueAtQuantileIter<'_> {
        std::iter::empty()
    }
    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(column_u32(self.batch(), "flags", self.row).unwrap_or_default())
    }
}

impl SummaryPoint<'_> {
    fn batch(&self) -> &RecordBatch {
        &self
            .view
            .summary
            .as_ref()
            .expect("iterator requires table")
            .children
            .batch
    }
}

struct ExemplarIter<'a> {
    table: Option<&'a ExemplarTable>,
    rows: std::slice::Iter<'a, usize>,
}

impl<'a> ExemplarIter<'a> {
    fn new(table: Option<&'a ExemplarTable>, parent: Option<u32>) -> Self {
        Self {
            table,
            rows: table
                .map(|table| table.rows(parent))
                .unwrap_or_default()
                .iter(),
        }
    }
}

impl<'a> Iterator for ExemplarIter<'a> {
    type Item = OtapExemplar<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        Some(OtapExemplar {
            table: self.table?,
            row: *self.rows.next()?,
        })
    }
}

struct OtapExemplar<'a> {
    table: &'a ExemplarTable,
    row: usize,
}

impl ExemplarView for OtapExemplar<'_> {
    type Attribute<'a>
        = OtapAttribute<'a>
    where
        Self: 'a;
    type AttributeIter<'a>
        = Attr32Iter<'a>
    where
        Self: 'a;
    fn filtered_attributes(&self) -> Self::AttributeIter<'_> {
        Attr32Iter::new(
            self.table.attrs.as_ref(),
            column_u32(&self.table.batch, "id", self.row),
        )
    }
    fn time_unix_nano(&self) -> u64 {
        timestamp(&self.table.batch, "time_unix_nano", self.row).unwrap_or_default()
    }
    fn value(&self) -> Option<Value> {
        self.table
            .batch
            .column_by_name("int_value")
            .and_then(|array| i64_at(array, self.row))
            .map(Value::Integer)
            .or_else(|| {
                self.table
                    .batch
                    .column_by_name("double_value")
                    .and_then(|array| f64_at(array, self.row))
                    .map(Value::Double)
            })
    }
    fn span_id(&self) -> Option<&SpanId> {
        id(&self.table.batch, "span_id", self.row)
    }
    fn trace_id(&self) -> Option<&TraceId> {
        id(&self.table.batch, "trace_id", self.row)
    }
}

fn decode_points(mut batch: RecordBatch) -> Result<RecordBatch> {
    batch = decode_delta_u32(batch, "id")?;
    decode_delta_u16(batch, "parent_id")
}

fn decode_exemplars(mut batch: RecordBatch) -> Result<RecordBatch> {
    batch = decode_delta_u32(batch, "id")?;
    decode_quasi_u32(batch, "parent_id")
}

fn decode_delta_u16(batch: RecordBatch, name: &str) -> Result<RecordBatch> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Otap(format!("{name} is missing")))?;
    if super::logs::is_plain(batch.schema().field(index)) {
        return Ok(batch);
    }
    let array = batch
        .column(index)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| Error::Otap(format!("{name} must be UInt16")))?;
    let decoded = decode_delta(array, name)?;
    replace(&batch, index, Arc::new(decoded))
}

fn decode_quasi_u32(batch: RecordBatch, name: &str) -> Result<RecordBatch> {
    decode_quasi_delta::<u32>(batch, name, |batch, left, right| {
        ["int_value", "double_value"]
            .iter()
            .all(|column| scalar_equal(batch, column, left, right))
    })
}

fn scalar_equal(batch: &RecordBatch, name: &str, left: usize, right: usize) -> bool {
    let Some(array) = batch.column_by_name(name) else {
        return true;
    };
    if array.is_null(left) || array.is_null(right) {
        return array.is_null(left) == array.is_null(right);
    }
    i64_at(array, left) == i64_at(array, right)
        && f64_at(array, left).map(f64::to_bits) == f64_at(array, right).map(f64::to_bits)
}

fn column_u64(batch: &RecordBatch, name: &str, row: usize) -> Option<u64> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<UInt64Array>()
        .and_then(|array| array.is_valid(row).then(|| array.value(row)))
}

fn column_i32(batch: &RecordBatch, name: &str, row: usize) -> Option<i32> {
    batch
        .column_by_name(name)
        .and_then(|array| i32_at(array, row))
}

fn column_f64(batch: &RecordBatch, name: &str, row: usize) -> Option<f64> {
    batch
        .column_by_name(name)
        .and_then(|array| f64_at(array, row))
}

fn list_u64(batch: &RecordBatch, name: &str, row: usize) -> Vec<u64> {
    batch
        .column_by_name(name)
        .map(|array| list_u64_array(array, row))
        .unwrap_or_default()
}

fn list_u64_array(array: &Arc<dyn Array>, row: usize) -> Vec<u64> {
    let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
        return Vec::new();
    };
    if list.is_null(row) {
        return Vec::new();
    }
    list.value(row)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .map(|values| values.iter().flatten().collect())
        .unwrap_or_default()
}

fn list_f64(batch: &RecordBatch, name: &str, row: usize) -> Vec<f64> {
    let Some(list) = batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<ListArray>())
    else {
        return Vec::new();
    };
    if list.is_null(row) {
        return Vec::new();
    }
    list.value(row)
        .as_any()
        .downcast_ref::<Float64Array>()
        .map(|values| values.iter().flatten().collect())
        .unwrap_or_default()
}

fn buckets<'a>(batch: &'a RecordBatch, name: &str, row: usize) -> Option<OtapBuckets<'a>> {
    let values = batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<StructArray>()?;
    (!values.is_null(row)).then_some(OtapBuckets { values, row })
}

#[cfg(test)]
mod transport_tests {
    use std::sync::Arc;

    use arrow_array::{Float64Array, Int64Array, UInt32Array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn exemplar_parent_ids_delta_decode_equal_null_values() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt32, false),
                Field::new("int_value", DataType::Int64, true),
                Field::new("double_value", DataType::Float64, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![3, 2])),
                Arc::new(Int64Array::from(vec![None, None])),
                Arc::new(Float64Array::from(vec![None, None])),
            ],
        )
        .unwrap();

        let decoded = decode_quasi_u32(batch, "parent_id").unwrap();
        let parent_ids = decoded
            .column_by_name("parent_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(parent_ids.values(), &[3, 5]);
    }
}
