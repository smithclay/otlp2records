//! The configurable transform: one entry point per signal, with options and
//! an observer that combine freely.

use arrow_array::RecordBatch;

use crate::{
    batch::{MetricsSelection, Observation, TransformObserver, TransformSignal},
    schema::{logs_schema_arc, traces_schema_arc},
    Error, InputFormat, MetricBatches, MetricShape, MetricShapeBatch, Result, TransformOptions,
};

/// Transforms OTLP bytes of one input format, building what its
/// [`TransformOptions`] ask for and reporting phase timings to an optional
/// [`TransformObserver`].
///
/// ```no_run
/// # fn run(bytes: &[u8]) -> otlp2records::Result<()> {
/// use otlp2records::{InputFormat, MetricShape, TransformOptions, Transformer};
///
/// let schema = otlp2records::logs_schema();
/// let options = TransformOptions::new()
///     .with_columns([schema.index_of("body").unwrap()])
///     .with_service_filter(|service| service == "checkout");
/// let mut transformer = Transformer::new(InputFormat::Auto).options(options);
/// let logs = transformer.logs(bytes)?;
/// let gauges = Transformer::new(InputFormat::Auto).metric_shape(MetricShape::Gauge, bytes)?;
/// # Ok(()) }
/// ```
///
/// The [`transform_logs`](crate::transform_logs) family of functions is the
/// same transform with default options and no observer.
pub struct Transformer<'o> {
    format: InputFormat,
    options: TransformOptions,
    observer: Option<&'o mut dyn TransformObserver>,
}

impl<'o> Transformer<'o> {
    /// A transform of `format` input that builds every column of every row.
    #[must_use]
    pub fn new(format: InputFormat) -> Self {
        Self {
            format,
            options: TransformOptions::new(),
            observer: None,
        }
    }

    /// Build only the columns and rows `options` asks for.
    #[must_use]
    pub fn options(mut self, options: TransformOptions) -> Self {
        self.options = options;
        self
    }

    /// Report phase timings and counters to `observer`.
    #[must_use]
    pub fn observer(mut self, observer: &'o mut dyn TransformObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// What one transform of `signal` reports to `observer`.
    fn observation<'a>(
        observer: &'a mut Option<&'o mut dyn TransformObserver>,
        signal: TransformSignal,
    ) -> Observation<'a> {
        let observer = observer
            .as_mut()
            .map(|o| &mut **o as &mut dyn TransformObserver);
        Observation::new(signal, observer)
    }

    /// Transforms OTLP logs.
    pub fn logs(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        // Planned first, so an options error is never retried as another format.
        let plan = self.options.plan(&logs_schema_arc())?;
        let mut obs = Self::observation(&mut self.observer, TransformSignal::Logs);
        super::logs::transform_logs_observed(bytes, self.format, &mut obs, &plan)
    }

    /// Transforms OTLP traces.
    pub fn traces(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        let plan = self.options.plan(&traces_schema_arc())?;
        let mut obs = Self::observation(&mut self.observer, TransformSignal::Traces);
        super::traces::transform_traces_observed(bytes, self.format, &mut obs, &plan)
    }

    /// Transforms OTLP metrics into every shape. A column index means
    /// something different in each shape, so options that narrow are an
    /// [`Error::InvalidOptions`]; narrow one shape with
    /// [`Self::metric_shape`].
    pub fn metrics(&mut self, bytes: &[u8]) -> Result<MetricBatches> {
        if self.options.narrows() {
            return Err(Error::InvalidOptions(
                "options narrow a single metric shape; use metric_shape".into(),
            ));
        }
        let mut obs = Self::observation(&mut self.observer, TransformSignal::Metrics);
        super::metrics::transform_metrics_observed(
            bytes,
            self.format,
            &mut obs,
            MetricsSelection::All,
        )
    }

    /// Transforms OTLP metrics into one shape (column indexes are the
    /// shape's). The other shapes are not built. A shape the input has none
    /// of is an empty batch.
    pub fn metric_shape(&mut self, shape: MetricShape, bytes: &[u8]) -> Result<MetricShapeBatch> {
        let plan = self.options.plan(&shape.schema_ref())?;
        let mut obs = Self::observation(&mut self.observer, TransformSignal::Metrics);
        super::metrics::transform_metrics_observed(
            bytes,
            self.format,
            &mut obs,
            MetricsSelection::Shape(shape, &plan),
        )
        .map(|batches| batches.into_shape(shape))
    }
}

impl MetricBatches {
    /// The one shape a narrowed transform built, which it always returns.
    pub(crate) fn into_shape(self, shape: MetricShape) -> MetricShapeBatch {
        let batch = match shape {
            MetricShape::Gauge => self.gauge,
            MetricShape::Sum => self.sum,
            MetricShape::Histogram => self.histogram,
            MetricShape::ExponentialHistogram => self.exp_histogram,
        };
        MetricShapeBatch {
            batch: batch.expect("a single-shape transform always finishes its shape"),
            skipped: self.skipped,
        }
    }
}
