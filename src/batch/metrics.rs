//! Metric request-to-RecordBatch conversion.

use arrow_array::{
    builder::{
        BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt32Builder, UInt64Builder,
    },
    RecordBatch,
};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use prost::Message;

use crate::{
    api::{MetricBatches, SkippedMetrics},
    options::{in_range, MetricShape, Plan},
    schema::{exp_histogram_schema_arc, gauge_schema_arc, histogram_schema_arc, sum_schema_arc},
    views::otlp::Obj,
    views::pdata::{
        BucketsView, DataType, DataView, ExponentialHistogramDataPointView,
        ExponentialHistogramView, GaugeView, HistogramDataPointView, HistogramView, MetricView,
        MetricsView, NumberDataPointView, ResourceMetricsView, ScopeMetricsView, SumView,
        SummaryView, Value,
    },
    Result,
};

use super::{
    context::{ContextDuplicateTracker, MetricMeta, ResourceContext, ScopeContext},
    profile::{
        measure_phase, measure_result, observe_counter, TransformCounter, TransformObserver,
        TransformPhase, TransformSignal,
    },
    util::{
        append_f64_list, append_finite, append_finite_opt, append_opt_n, append_opt_ts_ns,
        append_required_service_name_n, append_required_ts_ns, append_u64_list, array,
        finish_batch, opt_ts, string_builder,
    },
    view_json,
};

/// Which metric shapes a transform builds, and how.
#[derive(Clone, Copy)]
pub(crate) enum MetricsSelection<'a> {
    /// Every shape, every column, every row.
    All,
    /// Only this shape, narrowed by a plan made against its schema.
    Shape(MetricShape, &'a Plan),
}

impl MetricsSelection<'_> {
    /// The plan of each shape, in gauge/sum/histogram/exp-histogram order;
    /// `None` for a shape that is not built.
    fn plans(self) -> [Option<ShapePlan>; 4] {
        let shapes = [
            MetricShape::Gauge,
            MetricShape::Sum,
            MetricShape::Histogram,
            MetricShape::ExponentialHistogram,
        ];
        let mut plans: [Option<ShapePlan>; 4] = [None, None, None, None];
        for (slot, shape) in plans.iter_mut().zip(shapes) {
            *slot = match self {
                MetricsSelection::All => Some(ShapePlan::everything(shape)),
                MetricsSelection::Shape(wanted, plan) if wanted == shape => {
                    Some(ShapePlan::new(plan.clone(), &shape.schema_ref()))
                }
                MetricsSelection::Shape(..) => None,
            };
        }
        plans
    }
}

/// One metric shape's [`Plan`], with what its builders consult per point.
#[derive(Clone)]
struct ShapePlan {
    plan: Plan,
    time_range: Option<(i64, i64)>,
    start_time_range: Option<(i64, i64)>,
    metric_attributes: bool,
    exemplars: bool,
    resource_attributes: bool,
    scope_attributes: bool,
}

impl ShapePlan {
    fn new(plan: Plan, schema: &arrow_schema::Schema) -> Self {
        Self {
            time_range: plan.time_range(schema, "time_unix_nano"),
            start_time_range: plan.time_range(schema, "start_time_unix_nano"),
            metric_attributes: plan.builds_named(schema, "metric_attributes"),
            exemplars: plan.builds_named(schema, "exemplars_json"),
            resource_attributes: plan.builds_named(schema, "resource_attributes"),
            scope_attributes: plan.builds_named(schema, "scope_attributes"),
            plan,
        }
    }

    /// Every column of every point of `shape`.
    fn everything(shape: MetricShape) -> Self {
        let schema = shape.schema_ref();
        Self::new(Plan::all(&schema), &schema)
    }

    /// Whether a point passes the time ranges. A zero start time is emitted
    /// as NULL, so it fails a start time range.
    fn keeps(&self, time_unix_nano: u64, start_time_unix_nano: u64) -> bool {
        in_range(self.time_range, Some(time_unix_nano))
            && in_range(self.start_time_range, opt_ts(start_time_unix_nano))
    }
}

pub fn transform_metrics_protobuf_observed(
    bytes: &[u8],
    observer: &mut Option<&mut dyn TransformObserver>,
    selection: MetricsSelection<'_>,
) -> Result<MetricBatches> {
    let request = measure_result(
        observer,
        TransformSignal::Metrics,
        TransformPhase::ProtobufDecode,
        || ExportMetricsServiceRequest::decode(bytes),
    )?;
    transform_metrics_request_observed(request, observer, selection)
}

#[cfg(test)]
fn transform_metrics_request(request: ExportMetricsServiceRequest) -> Result<MetricBatches> {
    transform_metrics_request_observed(request, &mut None, MetricsSelection::All)
}

pub fn transform_metrics_request_observed(
    request: ExportMetricsServiceRequest,
    observer: &mut Option<&mut dyn TransformObserver>,
    selection: MetricsSelection<'_>,
) -> Result<MetricBatches> {
    transform_metrics_view_observed(&Obj::new(&request), observer, selection)
}

pub(crate) fn transform_metrics_view<V: MetricsView>(
    view: &V,
    selection: MetricsSelection<'_>,
) -> Result<MetricBatches> {
    transform_metrics_view_observed(view, &mut None, selection)
}

pub(crate) fn transform_metrics_view_observed<V: MetricsView>(
    view: &V,
    observer: &mut Option<&mut dyn TransformObserver>,
    selection: MetricsSelection<'_>,
) -> Result<MetricBatches> {
    let plans = selection.plans();
    let builds = plans.each_ref().map(Option::is_some);
    let resource_attributes = plans
        .iter()
        .flatten()
        .any(|shape| shape.resource_attributes);
    let scope_attributes = plans.iter().flatten().any(|shape| shape.scope_attributes);
    // A shape that is not built keeps empty builders.
    let [gauge_plan, sum_plan, histogram_plan, exp_histogram_plan] = plans;
    let mut capacities = MetricCapacities::default();
    measure_phase(
        observer,
        TransformSignal::Metrics,
        TransformPhase::MetricsCapacity,
        || {
            for resource_metrics in view.resources() {
                for scope_metrics in resource_metrics.scopes() {
                    for metric in scope_metrics.metrics() {
                        let Some(data) = metric.data() else {
                            continue;
                        };
                        match data.value_type() {
                            DataType::Gauge if builds[0] => {
                                capacities.gauge += data.as_gauge().unwrap().data_points().count();
                            }
                            DataType::Sum if builds[1] => {
                                capacities.sum += data.as_sum().unwrap().data_points().count();
                            }
                            DataType::Histogram if builds[2] => {
                                capacities.histogram +=
                                    data.as_histogram().unwrap().data_points().count();
                            }
                            DataType::ExponentialHistogram if builds[3] => {
                                capacities.exp_histogram += data
                                    .as_exponential_histogram()
                                    .unwrap()
                                    .data_points()
                                    .count();
                            }
                            _ => {}
                        }
                    }
                }
            }
        },
    );
    observe_counter(
        observer,
        TransformSignal::Metrics,
        TransformCounter::OutputRows,
        (capacities.gauge + capacities.sum + capacities.histogram + capacities.exp_histogram)
            as u64,
    );

    let (mut gauge, mut sum, mut histogram, mut exp_histogram) = measure_phase(
        observer,
        TransformSignal::Metrics,
        TransformPhase::BuilderInit,
        || {
            (
                GaugeBuilders::with_capacity(
                    capacities.gauge,
                    gauge_plan.unwrap_or_else(|| ShapePlan::everything(MetricShape::Gauge)),
                ),
                SumBuilders::with_capacity(
                    capacities.sum,
                    sum_plan.unwrap_or_else(|| ShapePlan::everything(MetricShape::Sum)),
                ),
                HistogramBuilders::with_capacity(
                    capacities.histogram,
                    histogram_plan.unwrap_or_else(|| ShapePlan::everything(MetricShape::Histogram)),
                ),
                ExpHistogramBuilders::with_capacity(
                    capacities.exp_histogram,
                    exp_histogram_plan.unwrap_or_else(|| {
                        ShapePlan::everything(MetricShape::ExponentialHistogram)
                    }),
                ),
            )
        },
    );
    let mut skipped = SkippedMetrics::default();
    let mut duplicates = observer.is_some().then(ContextDuplicateTracker::default);

    for resource_metrics in view.resources() {
        let resource_view = resource_metrics.resource();
        let Some(resource) = ResourceContext::from_view_observed(
            resource_view.as_ref(),
            TransformSignal::Metrics,
            observer,
            duplicates.as_mut(),
            // Only one shape is built when any has a service filter.
            |service| {
                [
                    &gauge.shape,
                    &sum.shape,
                    &histogram.shape,
                    &exp_histogram.shape,
                ]
                .into_iter()
                .zip(builds)
                .any(|(shape, built)| built && shape.plan.keeps_service(service))
            },
            resource_attributes,
        ) else {
            continue;
        };

        for scope_metrics in resource_metrics.scopes() {
            let scope_view = scope_metrics.scope();
            let scope = ScopeContext::from_view_observed(
                scope_view.as_ref(),
                TransformSignal::Metrics,
                observer,
                duplicates.as_mut(),
                scope_attributes,
            );

            for metric in scope_metrics.metrics() {
                let metric_name = String::from_utf8_lossy(metric.name());
                let metric_description = String::from_utf8_lossy(metric.description());
                let metric_unit = String::from_utf8_lossy(metric.unit());
                let Some(data) = metric.data() else {
                    continue;
                };
                let meta = MetricMeta {
                    name: metric_name.as_ref(),
                    description: metric_description.as_ref(),
                    unit: metric_unit.as_ref(),
                };

                match data.value_type() {
                    DataType::Gauge if builds[0] => {
                        let data = data.as_gauge().unwrap();
                        let rows_before = gauge.rows;
                        for point in data.data_points() {
                            if !gauge
                                .shape
                                .keeps(point.time_unix_nano(), point.start_time_unix_nano())
                            {
                                continue;
                            }
                            if let Some(value) = metric_point_value(point.value(), &mut skipped) {
                                measure_result(
                                    observer,
                                    TransformSignal::Metrics,
                                    TransformPhase::ArrowAppend,
                                    || gauge.append(&point, value, meta),
                                )?;
                            }
                        }
                        let appended = gauge.rows - rows_before;
                        if appended > 0 {
                            measure_phase(
                                observer,
                                TransformSignal::Metrics,
                                TransformPhase::ArrowAppend,
                                || {
                                    append_number_metric_context(
                                        &mut gauge, appended, &resource, &scope,
                                    );
                                },
                            );
                        }
                    }
                    DataType::Sum if builds[1] => {
                        let data = data.as_sum().unwrap();
                        let rows_before = sum.rows;
                        for point in data.data_points() {
                            if !sum
                                .shape
                                .keeps(point.time_unix_nano(), point.start_time_unix_nano())
                            {
                                continue;
                            }
                            if let Some(value) = metric_point_value(point.value(), &mut skipped) {
                                measure_result(
                                    observer,
                                    TransformSignal::Metrics,
                                    TransformPhase::ArrowAppend,
                                    || {
                                        sum.append(
                                            &point,
                                            value,
                                            data.aggregation_temporality() as i32,
                                            data.is_monotonic(),
                                            meta,
                                        )
                                    },
                                )?;
                            }
                        }
                        let appended = sum.rows - rows_before;
                        if appended > 0 {
                            measure_phase(
                                observer,
                                TransformSignal::Metrics,
                                TransformPhase::ArrowAppend,
                                || {
                                    append_number_metric_context(
                                        &mut sum, appended, &resource, &scope,
                                    );
                                },
                            );
                        }
                    }
                    DataType::Histogram if builds[2] => {
                        let data = data.as_histogram().unwrap();
                        let rows_before = histogram.rows;
                        for point in data.data_points() {
                            if !histogram
                                .shape
                                .keeps(point.time_unix_nano(), point.start_time_unix_nano())
                            {
                                continue;
                            }
                            measure_result(
                                observer,
                                TransformSignal::Metrics,
                                TransformPhase::ArrowAppend,
                                || {
                                    histogram.append(
                                        &point,
                                        data.aggregation_temporality() as i32,
                                        meta,
                                    )
                                },
                            )?;
                        }
                        let appended = histogram.rows - rows_before;
                        if appended > 0 {
                            measure_phase(
                                observer,
                                TransformSignal::Metrics,
                                TransformPhase::ArrowAppend,
                                || {
                                    histogram.append_context(appended, &resource, &scope);
                                },
                            );
                        }
                    }
                    DataType::ExponentialHistogram if builds[3] => {
                        let data = data.as_exponential_histogram().unwrap();
                        let rows_before = exp_histogram.rows;
                        for point in data.data_points() {
                            if !exp_histogram
                                .shape
                                .keeps(point.time_unix_nano(), point.start_time_unix_nano())
                            {
                                continue;
                            }
                            measure_result(
                                observer,
                                TransformSignal::Metrics,
                                TransformPhase::ArrowAppend,
                                || {
                                    exp_histogram.append(
                                        &point,
                                        data.aggregation_temporality() as i32,
                                        meta,
                                    )
                                },
                            )?;
                        }
                        let appended = exp_histogram.rows - rows_before;
                        if appended > 0 {
                            measure_phase(
                                observer,
                                TransformSignal::Metrics,
                                TransformPhase::ArrowAppend,
                                || {
                                    exp_histogram.append_context(appended, &resource, &scope);
                                },
                            );
                        }
                    }
                    DataType::Summary => {
                        skipped.summaries += data.as_summary().unwrap().data_points().count();
                    }
                    // A shape this transform does not build.
                    _ => {}
                }
            }
        }
    }

    measure_result(
        observer,
        TransformSignal::Metrics,
        TransformPhase::ArrowFinalize,
        || {
            Ok(MetricBatches {
                gauge: gauge.finish_if_non_empty()?,
                sum: sum.finish_if_non_empty()?,
                histogram: histogram.finish_if_non_empty()?,
                exp_histogram: exp_histogram.finish_if_non_empty()?,
                skipped,
            })
        },
    )
}

#[derive(Default)]
struct MetricCapacities {
    gauge: usize,
    sum: usize,
    histogram: usize,
    exp_histogram: usize,
}

enum NumberPointValue {
    Int(i64),
    Double(f64),
}

struct GaugeBuilders {
    shape: ShapePlan,
    rows: usize,
    time_unix_nano: TimestampNanosecondBuilder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    int_value: Int64Builder,
    double_value: Float64Builder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: UInt32Builder,
    exemplars_json: StringBuilder,
    json_scratch: String,
}

impl GaugeBuilders {
    fn with_capacity(rows: usize, shape: ShapePlan) -> Self {
        Self {
            shape,
            rows: 0,
            time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            start_time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            name: string_builder(rows),
            description: string_builder(rows),
            unit: string_builder(rows),
            int_value: Int64Builder::with_capacity(rows),
            double_value: Float64Builder::with_capacity(rows),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: UInt32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
            json_scratch: String::new(),
        }
    }

    fn append<P: NumberDataPointView>(
        &mut self,
        point: &P,
        value: NumberPointValue,
        meta: MetricMeta<'_>,
    ) -> Result<()> {
        append_metric_common(self, point, value, meta)?;
        self.rows += 1;
        Ok(())
    }

    fn finish_if_non_empty(mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        finish_batch(
            gauge_schema_arc(),
            &self.shape.plan,
            vec![
                array(self.time_unix_nano.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.name.finish()),
                array(self.description.finish()),
                array(self.unit.finish()),
                array(self.int_value.finish()),
                array(self.double_value.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.resource_attributes.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_attributes.finish()),
                array(self.metric_attributes.finish()),
                array(self.flags.finish()),
                array(self.exemplars_json.finish()),
            ],
            self.rows,
        )
        .map(Some)
    }
}

struct SumBuilders {
    shape: ShapePlan,
    rows: usize,
    time_unix_nano: TimestampNanosecondBuilder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    int_value: Int64Builder,
    double_value: Float64Builder,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: UInt32Builder,
    exemplars_json: StringBuilder,
    aggregation_temporality: Int32Builder,
    is_monotonic: BooleanBuilder,
    json_scratch: String,
}

impl SumBuilders {
    fn with_capacity(rows: usize, shape: ShapePlan) -> Self {
        Self {
            shape,
            rows: 0,
            time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            start_time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            name: string_builder(rows),
            description: string_builder(rows),
            unit: string_builder(rows),
            int_value: Int64Builder::with_capacity(rows),
            double_value: Float64Builder::with_capacity(rows),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: UInt32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
            aggregation_temporality: Int32Builder::with_capacity(rows),
            is_monotonic: BooleanBuilder::with_capacity(rows),
            json_scratch: String::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append<P: NumberDataPointView>(
        &mut self,
        point: &P,
        value: NumberPointValue,
        aggregation_temporality: i32,
        is_monotonic: bool,
        meta: MetricMeta<'_>,
    ) -> Result<()> {
        append_metric_common(self, point, value, meta)?;
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
        finish_batch(
            sum_schema_arc(),
            &self.shape.plan,
            vec![
                array(self.time_unix_nano.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.name.finish()),
                array(self.description.finish()),
                array(self.unit.finish()),
                array(self.int_value.finish()),
                array(self.double_value.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.resource_attributes.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_attributes.finish()),
                array(self.metric_attributes.finish()),
                array(self.flags.finish()),
                array(self.exemplars_json.finish()),
                array(self.aggregation_temporality.finish()),
                array(self.is_monotonic.finish()),
            ],
            self.rows,
        )
        .map(Some)
    }
}

trait NumberMetricBuilders {
    fn time_unix_nano(&mut self) -> &mut TimestampNanosecondBuilder;
    fn start_time_unix_nano(&mut self) -> &mut TimestampNanosecondBuilder;
    fn name(&mut self) -> &mut StringBuilder;
    fn description(&mut self) -> &mut StringBuilder;
    fn unit(&mut self) -> &mut StringBuilder;
    fn int_value(&mut self) -> &mut Int64Builder;
    fn double_value(&mut self) -> &mut Float64Builder;
    fn service_name(&mut self) -> &mut StringBuilder;
    fn service_namespace(&mut self) -> &mut StringBuilder;
    fn service_instance_id(&mut self) -> &mut StringBuilder;
    fn resource_attributes(&mut self) -> &mut StringBuilder;
    fn scope_name(&mut self) -> &mut StringBuilder;
    fn scope_version(&mut self) -> &mut StringBuilder;
    fn scope_attributes(&mut self) -> &mut StringBuilder;
    fn flags(&mut self) -> &mut UInt32Builder;
    fn json_builders(&mut self) -> (&mut StringBuilder, &mut StringBuilder, &mut String);
    fn shape(&self) -> &ShapePlan;
}

impl NumberMetricBuilders for GaugeBuilders {
    fn shape(&self) -> &ShapePlan {
        &self.shape
    }
    fn time_unix_nano(&mut self) -> &mut TimestampNanosecondBuilder {
        &mut self.time_unix_nano
    }
    fn start_time_unix_nano(&mut self) -> &mut TimestampNanosecondBuilder {
        &mut self.start_time_unix_nano
    }
    fn name(&mut self) -> &mut StringBuilder {
        &mut self.name
    }
    fn description(&mut self) -> &mut StringBuilder {
        &mut self.description
    }
    fn unit(&mut self) -> &mut StringBuilder {
        &mut self.unit
    }
    fn int_value(&mut self) -> &mut Int64Builder {
        &mut self.int_value
    }
    fn double_value(&mut self) -> &mut Float64Builder {
        &mut self.double_value
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
    fn flags(&mut self) -> &mut UInt32Builder {
        &mut self.flags
    }
    fn json_builders(&mut self) -> (&mut StringBuilder, &mut StringBuilder, &mut String) {
        (
            &mut self.metric_attributes,
            &mut self.exemplars_json,
            &mut self.json_scratch,
        )
    }
}

impl NumberMetricBuilders for SumBuilders {
    fn shape(&self) -> &ShapePlan {
        &self.shape
    }
    fn time_unix_nano(&mut self) -> &mut TimestampNanosecondBuilder {
        &mut self.time_unix_nano
    }
    fn start_time_unix_nano(&mut self) -> &mut TimestampNanosecondBuilder {
        &mut self.start_time_unix_nano
    }
    fn name(&mut self) -> &mut StringBuilder {
        &mut self.name
    }
    fn description(&mut self) -> &mut StringBuilder {
        &mut self.description
    }
    fn unit(&mut self) -> &mut StringBuilder {
        &mut self.unit
    }
    fn int_value(&mut self) -> &mut Int64Builder {
        &mut self.int_value
    }
    fn double_value(&mut self) -> &mut Float64Builder {
        &mut self.double_value
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
    fn flags(&mut self) -> &mut UInt32Builder {
        &mut self.flags
    }
    fn json_builders(&mut self) -> (&mut StringBuilder, &mut StringBuilder, &mut String) {
        (
            &mut self.metric_attributes,
            &mut self.exemplars_json,
            &mut self.json_scratch,
        )
    }
}

fn append_metric_common<B: NumberMetricBuilders, P: NumberDataPointView>(
    builders: &mut B,
    point: &P,
    value: NumberPointValue,
    meta: MetricMeta<'_>,
) -> Result<()> {
    append_required_ts_ns(
        builders.time_unix_nano(),
        point.time_unix_nano(),
        "metric.time_unix_nano",
    )?;
    append_opt_ts_ns(
        builders.start_time_unix_nano(),
        point.start_time_unix_nano(),
        "metric.start_time_unix_nano",
    )?;
    builders.name().append_value(meta.name);
    builders.description().append_value(meta.description);
    builders.unit().append_value(meta.unit);
    match value {
        NumberPointValue::Int(value) => {
            builders.int_value().append_value(value);
            builders.double_value().append_null();
        }
        NumberPointValue::Double(value) => {
            builders.int_value().append_null();
            builders.double_value().append_value(value);
        }
    }
    builders.flags().append_value(point.flags().into_inner());
    let (wants_attributes, wants_exemplars) = {
        let shape = builders.shape();
        (shape.metric_attributes, shape.exemplars)
    };
    let (metric_attributes, exemplars_json, json_scratch) = builders.json_builders();
    if wants_attributes {
        view_json::append_attributes(metric_attributes, point.attributes(), json_scratch)?;
    }
    if wants_exemplars {
        view_json::append_exemplars(exemplars_json, point.exemplars(), json_scratch)?;
    }
    Ok(())
}

fn append_number_metric_context<B: NumberMetricBuilders>(
    builders: &mut B,
    rows: usize,
    resource: &ResourceContext,
    scope: &ScopeContext,
) {
    append_required_service_name_n(
        builders.service_name(),
        resource.service_name.as_deref(),
        rows,
    );
    append_opt_n(
        builders.service_namespace(),
        resource.service_namespace.as_deref(),
        rows,
    );
    append_opt_n(
        builders.service_instance_id(),
        resource.service_instance_id.as_deref(),
        rows,
    );
    append_opt_n(
        builders.resource_attributes(),
        resource.attributes_json.as_deref(),
        rows,
    );
    append_opt_n(builders.scope_name(), scope.name.as_deref(), rows);
    append_opt_n(builders.scope_version(), scope.version.as_deref(), rows);
    append_opt_n(
        builders.scope_attributes(),
        scope.attributes_json.as_deref(),
        rows,
    );
}

struct HistogramBuilders {
    shape: ShapePlan,
    rows: usize,
    time_unix_nano: TimestampNanosecondBuilder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    count: UInt64Builder,
    sum: Float64Builder,
    min: Float64Builder,
    max: Float64Builder,
    bucket_counts: ListBuilder<UInt64Builder>,
    explicit_bounds: ListBuilder<Float64Builder>,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: UInt32Builder,
    exemplars_json: StringBuilder,
    aggregation_temporality: Int32Builder,
    json_scratch: String,
}

impl HistogramBuilders {
    fn with_capacity(rows: usize, shape: ShapePlan) -> Self {
        Self {
            shape,
            rows: 0,
            time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            start_time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            name: string_builder(rows),
            description: string_builder(rows),
            unit: string_builder(rows),
            count: UInt64Builder::with_capacity(rows),
            sum: Float64Builder::with_capacity(rows),
            min: Float64Builder::with_capacity(rows),
            max: Float64Builder::with_capacity(rows),
            bucket_counts: ListBuilder::new(UInt64Builder::new()),
            explicit_bounds: ListBuilder::new(Float64Builder::new()),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: UInt32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
            aggregation_temporality: Int32Builder::with_capacity(rows),
            json_scratch: String::new(),
        }
    }

    fn append<P: HistogramDataPointView>(
        &mut self,
        point: &P,
        aggregation_temporality: i32,
        meta: MetricMeta<'_>,
    ) -> Result<()> {
        append_required_ts_ns(
            &mut self.time_unix_nano,
            point.time_unix_nano(),
            "histogram.time_unix_nano",
        )?;
        append_opt_ts_ns(
            &mut self.start_time_unix_nano,
            point.start_time_unix_nano(),
            "histogram.start_time_unix_nano",
        )?;
        self.name.append_value(meta.name);
        self.description.append_value(meta.description);
        self.unit.append_value(meta.unit);
        self.count.append_value(point.count());
        append_finite_opt(&mut self.sum, point.sum());
        append_finite_opt(&mut self.min, point.min());
        append_finite_opt(&mut self.max, point.max());
        let bucket_counts: Vec<_> = point.bucket_counts().collect();
        let explicit_bounds: Vec<_> = point.explicit_bounds().collect();
        append_u64_list(&mut self.bucket_counts, &bucket_counts);
        append_f64_list(&mut self.explicit_bounds, &explicit_bounds);
        if self.shape.metric_attributes {
            view_json::append_attributes(
                &mut self.metric_attributes,
                point.attributes(),
                &mut self.json_scratch,
            )?;
        }
        self.flags.append_value(point.flags().into_inner());
        if self.shape.exemplars {
            view_json::append_exemplars(
                &mut self.exemplars_json,
                point.exemplars(),
                &mut self.json_scratch,
            )?;
        }
        self.aggregation_temporality
            .append_value(aggregation_temporality);
        self.rows += 1;
        Ok(())
    }

    fn append_context(&mut self, rows: usize, resource: &ResourceContext, scope: &ScopeContext) {
        append_metric_resource_scope_n(
            &mut self.service_name,
            &mut self.service_namespace,
            &mut self.service_instance_id,
            &mut self.resource_attributes,
            &mut self.scope_name,
            &mut self.scope_version,
            &mut self.scope_attributes,
            rows,
            resource,
            scope,
        );
    }

    fn finish_if_non_empty(mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        finish_batch(
            histogram_schema_arc(),
            &self.shape.plan,
            vec![
                array(self.time_unix_nano.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.name.finish()),
                array(self.description.finish()),
                array(self.unit.finish()),
                array(self.count.finish()),
                array(self.sum.finish()),
                array(self.min.finish()),
                array(self.max.finish()),
                array(self.bucket_counts.finish()),
                array(self.explicit_bounds.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.resource_attributes.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_attributes.finish()),
                array(self.metric_attributes.finish()),
                array(self.flags.finish()),
                array(self.exemplars_json.finish()),
                array(self.aggregation_temporality.finish()),
            ],
            self.rows,
        )
        .map(Some)
    }
}

struct ExpHistogramBuilders {
    shape: ShapePlan,
    rows: usize,
    time_unix_nano: TimestampNanosecondBuilder,
    start_time_unix_nano: TimestampNanosecondBuilder,
    name: StringBuilder,
    description: StringBuilder,
    unit: StringBuilder,
    count: UInt64Builder,
    sum: Float64Builder,
    min: Float64Builder,
    max: Float64Builder,
    scale: Int32Builder,
    zero_count: UInt64Builder,
    zero_threshold: Float64Builder,
    positive_offset: Int32Builder,
    positive_bucket_counts: ListBuilder<UInt64Builder>,
    negative_offset: Int32Builder,
    negative_bucket_counts: ListBuilder<UInt64Builder>,
    service_name: StringBuilder,
    service_namespace: StringBuilder,
    service_instance_id: StringBuilder,
    resource_attributes: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    metric_attributes: StringBuilder,
    flags: UInt32Builder,
    exemplars_json: StringBuilder,
    aggregation_temporality: Int32Builder,
    json_scratch: String,
}

impl ExpHistogramBuilders {
    fn with_capacity(rows: usize, shape: ShapePlan) -> Self {
        Self {
            shape,
            rows: 0,
            time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            start_time_unix_nano: TimestampNanosecondBuilder::with_capacity(rows),
            name: string_builder(rows),
            description: string_builder(rows),
            unit: string_builder(rows),
            count: UInt64Builder::with_capacity(rows),
            sum: Float64Builder::with_capacity(rows),
            min: Float64Builder::with_capacity(rows),
            max: Float64Builder::with_capacity(rows),
            scale: Int32Builder::with_capacity(rows),
            zero_count: UInt64Builder::with_capacity(rows),
            zero_threshold: Float64Builder::with_capacity(rows),
            positive_offset: Int32Builder::with_capacity(rows),
            positive_bucket_counts: ListBuilder::new(UInt64Builder::new()),
            negative_offset: Int32Builder::with_capacity(rows),
            negative_bucket_counts: ListBuilder::new(UInt64Builder::new()),
            service_name: string_builder(rows),
            service_namespace: string_builder(rows),
            service_instance_id: string_builder(rows),
            resource_attributes: string_builder(rows),
            scope_name: string_builder(rows),
            scope_version: string_builder(rows),
            scope_attributes: string_builder(rows),
            metric_attributes: string_builder(rows),
            flags: UInt32Builder::with_capacity(rows),
            exemplars_json: string_builder(rows),
            aggregation_temporality: Int32Builder::with_capacity(rows),
            json_scratch: String::new(),
        }
    }

    fn append<P: ExponentialHistogramDataPointView>(
        &mut self,
        point: &P,
        aggregation_temporality: i32,
        meta: MetricMeta<'_>,
    ) -> Result<()> {
        append_required_ts_ns(
            &mut self.time_unix_nano,
            point.time_unix_nano(),
            "exp_histogram.time_unix_nano",
        )?;
        append_opt_ts_ns(
            &mut self.start_time_unix_nano,
            point.start_time_unix_nano(),
            "exp_histogram.start_time_unix_nano",
        )?;
        self.name.append_value(meta.name);
        self.description.append_value(meta.description);
        self.unit.append_value(meta.unit);
        self.count.append_value(point.count());
        append_finite_opt(&mut self.sum, point.sum());
        append_finite_opt(&mut self.min, point.min());
        append_finite_opt(&mut self.max, point.max());
        self.scale.append_value(point.scale());
        self.zero_count.append_value(point.zero_count());
        append_finite(&mut self.zero_threshold, point.zero_threshold());
        if let Some(positive) = point.positive() {
            self.positive_offset.append_value(positive.offset());
            let counts: Vec<_> = positive.bucket_counts().collect();
            append_u64_list(&mut self.positive_bucket_counts, &counts);
        } else {
            self.positive_offset.append_null();
            self.positive_bucket_counts.append_null();
        }
        if let Some(negative) = point.negative() {
            self.negative_offset.append_value(negative.offset());
            let counts: Vec<_> = negative.bucket_counts().collect();
            append_u64_list(&mut self.negative_bucket_counts, &counts);
        } else {
            self.negative_offset.append_null();
            self.negative_bucket_counts.append_null();
        }
        if self.shape.metric_attributes {
            view_json::append_attributes(
                &mut self.metric_attributes,
                point.attributes(),
                &mut self.json_scratch,
            )?;
        }
        self.flags.append_value(point.flags().into_inner());
        if self.shape.exemplars {
            view_json::append_exemplars(
                &mut self.exemplars_json,
                point.exemplars(),
                &mut self.json_scratch,
            )?;
        }
        self.aggregation_temporality
            .append_value(aggregation_temporality);
        self.rows += 1;
        Ok(())
    }

    fn append_context(&mut self, rows: usize, resource: &ResourceContext, scope: &ScopeContext) {
        append_metric_resource_scope_n(
            &mut self.service_name,
            &mut self.service_namespace,
            &mut self.service_instance_id,
            &mut self.resource_attributes,
            &mut self.scope_name,
            &mut self.scope_version,
            &mut self.scope_attributes,
            rows,
            resource,
            scope,
        );
    }

    fn finish_if_non_empty(mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        finish_batch(
            exp_histogram_schema_arc(),
            &self.shape.plan,
            vec![
                array(self.time_unix_nano.finish()),
                array(self.start_time_unix_nano.finish()),
                array(self.name.finish()),
                array(self.description.finish()),
                array(self.unit.finish()),
                array(self.count.finish()),
                array(self.sum.finish()),
                array(self.min.finish()),
                array(self.max.finish()),
                array(self.scale.finish()),
                array(self.zero_count.finish()),
                array(self.zero_threshold.finish()),
                array(self.positive_offset.finish()),
                array(self.positive_bucket_counts.finish()),
                array(self.negative_offset.finish()),
                array(self.negative_bucket_counts.finish()),
                array(self.service_name.finish()),
                array(self.service_namespace.finish()),
                array(self.service_instance_id.finish()),
                array(self.resource_attributes.finish()),
                array(self.scope_name.finish()),
                array(self.scope_version.finish()),
                array(self.scope_attributes.finish()),
                array(self.metric_attributes.finish()),
                array(self.flags.finish()),
                array(self.exemplars_json.finish()),
                array(self.aggregation_temporality.finish()),
            ],
            self.rows,
        )
        .map(Some)
    }
}

#[allow(clippy::too_many_arguments)]
fn append_metric_resource_scope_n(
    service_name: &mut StringBuilder,
    service_namespace: &mut StringBuilder,
    service_instance_id: &mut StringBuilder,
    resource_attributes: &mut StringBuilder,
    scope_name: &mut StringBuilder,
    scope_version: &mut StringBuilder,
    scope_attributes: &mut StringBuilder,
    rows: usize,
    resource: &ResourceContext,
    scope: &ScopeContext,
) {
    append_required_service_name_n(service_name, resource.service_name.as_deref(), rows);
    append_opt_n(
        service_namespace,
        resource.service_namespace.as_deref(),
        rows,
    );
    append_opt_n(
        service_instance_id,
        resource.service_instance_id.as_deref(),
        rows,
    );
    append_opt_n(
        resource_attributes,
        resource.attributes_json.as_deref(),
        rows,
    );
    append_opt_n(scope_name, scope.name.as_deref(), rows);
    append_opt_n(scope_version, scope.version.as_deref(), rows);
    append_opt_n(scope_attributes, scope.attributes_json.as_deref(), rows);
}

#[inline]
fn metric_point_value(
    value: Option<Value>,
    skipped: &mut SkippedMetrics,
) -> Option<NumberPointValue> {
    match value {
        Some(Value::Integer(value)) => Some(NumberPointValue::Int(value)),
        Some(Value::Double(value)) if value.is_nan() => {
            skipped.nan_values += 1;
            None
        }
        Some(Value::Double(value)) if value.is_infinite() => {
            skipped.infinity_values += 1;
            None
        }
        Some(Value::Double(value)) => Some(NumberPointValue::Double(value)),
        None => {
            skipped.missing_values += 1;
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int32Array};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, number_data_point, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    };

    use super::*;

    #[test]
    fn out_of_spec_aggregation_temporality_is_unspecified() {
        // Proto3 open enums can carry values outside 0..=2. The view traits
        // (otel-arrow-dfe-pdata-views) expose a closed enum, so such a value
        // normalizes to 0 (Unspecified) rather than passing through.
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "m".into(),
                        data: Some(metric::Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1,
                                value: Some(number_data_point::Value::AsInt(5)),
                                ..Default::default()
                            }],
                            aggregation_temporality: 3,
                            is_monotonic: true,
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batches = transform_metrics_request(request).unwrap();
        let sum = batches.sum.expect("sum batch present");
        let temporality = sum
            .column_by_name("aggregation_temporality")
            .expect("aggregation_temporality column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32 temporality column");
        assert_eq!(temporality.value(0), 0);
    }
}
