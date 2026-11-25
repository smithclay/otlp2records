//! Narrowing a transform to the columns and rows a reader needs.
//!
//! A query engine that reads OTLP files usually wants a few columns of a few
//! rows. [`TransformOptions`] lets it say so, and the transform then skips the
//! work for everything else: a column that is not requested is not built
//! (attribute and body JSON is most of a transform's cost) and not returned,
//! and a row that fails a predicate is dropped before any of its columns are
//! built.
//!
//! The row predicates are those that can be decided while walking the request:
//! a filter on `service_name`, asked once per resource, and inclusive ranges on
//! timestamp columns, which follow SQL comparison semantics (a NULL timestamp
//! is outside every range). A caller with other filters applies them itself
//! after the transform.

use std::{fmt, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit};

use crate::{
    schema::{exp_histogram_schema_arc, gauge_schema_arc, histogram_schema_arc, sum_schema_arc},
    Error, Result,
};

/// Which metric shape a metrics transform builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricShape {
    Gauge,
    Sum,
    Histogram,
    ExponentialHistogram,
}

impl MetricShape {
    /// The schema of this shape's batches.
    #[must_use]
    pub fn schema(self) -> Schema {
        self.schema_ref().as_ref().clone()
    }

    pub(crate) fn schema_ref(self) -> SchemaRef {
        match self {
            MetricShape::Gauge => gauge_schema_arc(),
            MetricShape::Sum => sum_schema_arc(),
            MetricShape::Histogram => histogram_schema_arc(),
            MetricShape::ExponentialHistogram => exp_histogram_schema_arc(),
        }
    }
}

/// Decides, from a resource's emitted `service_name`, whether its rows are kept.
type ServiceFilter = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// An inclusive range of nanosecond timestamps on one timestamp column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeRange {
    /// Index of a `Timestamp(Nanosecond)` column in the signal's schema.
    column: usize,
    min: i64,
    max: i64,
}

/// What a transform builds. The default builds every column of every row.
///
/// Columns are addressed by their index in the signal's schema (for metrics,
/// the requested shape's).
#[derive(Clone, Default)]
pub struct TransformOptions {
    columns: Option<Vec<usize>>,
    service_filter: Option<ServiceFilter>,
    time_ranges: Vec<TimeRange>,
}

impl fmt::Debug for TransformOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransformOptions")
            .field("columns", &self.columns)
            .field("service_filter", &self.service_filter.is_some())
            .field("time_ranges", &self.time_ranges)
            .finish()
    }
}

impl TransformOptions {
    /// Every column of every row.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build and return only these columns, in this order. An index past the
    /// schema is rejected when the transform validates the options.
    #[must_use]
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = usize>) -> Self {
        self.columns = Some(columns.into_iter().collect());
        self
    }

    /// Keep only the rows of resources whose `service_name` passes `filter`.
    /// It is called once per resource with the value the transform emits, so a
    /// resource without `service.name` is asked about `"unknown"`.
    #[must_use]
    pub fn with_service_filter(
        mut self,
        filter: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.service_filter = Some(Arc::new(filter));
        self
    }

    /// Keep only rows whose `column` (a nanosecond timestamp column) is within
    /// `[min, max]`. Several ranges on the same column intersect.
    #[must_use]
    pub fn with_time_range(mut self, column: usize, min: i64, max: i64) -> Self {
        self.time_ranges.push(TimeRange { column, min, max });
        self
    }

    /// Checks the options against the schema they will be applied to. Every
    /// public entry point plans once, before format detection, so a bad option
    /// is reported as such rather than retried as a decode failure.
    pub(crate) fn plan(&self, schema: &Schema) -> Result<Plan> {
        let width = schema.fields().len();
        let mut built = vec![self.columns.is_none(); width];
        for &column in self.columns.iter().flatten() {
            let Some(slot) = built.get_mut(column) else {
                return Err(invalid(format!(
                    "column {column} is past the schema's {width}"
                )));
            };
            *slot = true;
        }
        let mut time_ranges: Vec<TimeRange> = Vec::new();
        for range in &self.time_ranges {
            let Some(field) = schema.fields().get(range.column) else {
                return Err(invalid(format!(
                    "time range on column {} past the schema's {width}",
                    range.column
                )));
            };
            if !matches!(
                field.data_type(),
                DataType::Timestamp(TimeUnit::Nanosecond, _)
            ) {
                return Err(invalid(format!(
                    "time range on column {} ({}), which is not a nanosecond timestamp",
                    range.column,
                    field.name()
                )));
            }
            match time_ranges.iter_mut().find(|r| r.column == range.column) {
                Some(existing) => {
                    existing.min = existing.min.max(range.min);
                    existing.max = existing.max.min(range.max);
                }
                None => time_ranges.push(*range),
            }
        }
        Ok(Plan {
            built,
            output: self.columns.clone(),
            service_filter: self.service_filter.clone(),
            time_ranges,
        })
    }
}

fn invalid(message: String) -> Error {
    Error::InvalidOptions(message)
}

/// [`TransformOptions`] resolved against one schema.
#[derive(Clone)]
pub(crate) struct Plan {
    /// Whether each column of the schema is built.
    built: Vec<bool>,
    /// The columns returned, in order; `None` for the whole schema.
    output: Option<Vec<usize>>,
    service_filter: Option<ServiceFilter>,
    time_ranges: Vec<TimeRange>,
}

impl Plan {
    /// Every column of every row of `schema`.
    pub(crate) fn all(schema: &Schema) -> Self {
        Self {
            built: vec![true; schema.fields().len()],
            output: None,
            service_filter: None,
            time_ranges: Vec::new(),
        }
    }

    /// Whether the named column is built. Panics on a name the schema lacks,
    /// which is a bug in the builder asking.
    pub(crate) fn builds_named(&self, schema: &Schema, name: &str) -> bool {
        let index = schema
            .index_of(name)
            .unwrap_or_else(|_| panic!("no column {name}"));
        self.built[index]
    }

    /// Whether a resource whose emitted `service_name` is `service_name`
    /// (`None` emits `"unknown"`) keeps its rows.
    pub(crate) fn keeps_service(&self, service_name: Option<&str>) -> bool {
        self.service_filter
            .as_ref()
            .is_none_or(|filter| filter(service_name.unwrap_or(crate::batch::UNKNOWN_SERVICE_NAME)))
    }

    /// The range rows must fall in on the named column, if any.
    pub(crate) fn time_range(&self, schema: &Schema, name: &str) -> Option<(i64, i64)> {
        let index = schema.index_of(name).ok()?;
        self.time_ranges
            .iter()
            .find(|range| range.column == index)
            .map(|range| (range.min, range.max))
    }

    /// The batch of `rows` rows this plan returns from one array per schema
    /// column. Arrays of columns that were not built are dropped unread.
    pub(crate) fn finish(
        &self,
        schema: SchemaRef,
        arrays: Vec<ArrayRef>,
        rows: usize,
    ) -> Result<RecordBatch> {
        let Some(output) = &self.output else {
            return Ok(RecordBatch::try_new(schema, arrays)?);
        };
        let schema = Arc::new(schema.project(output)?);
        let columns = output.iter().map(|&i| Arc::clone(&arrays[i])).collect();
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        Ok(RecordBatch::try_new_with_options(
            schema, columns, &options,
        )?)
    }
}

/// Whether `value` (a raw OTLP nanosecond timestamp) passes `range`. A value
/// that does not fit in `i64` is kept, so the builder reports it as it would
/// without options.
#[inline]
pub(crate) fn in_range(range: Option<(i64, i64)>, value: Option<u64>) -> bool {
    let Some((min, max)) = range else {
        return true;
    };
    match value {
        None => false,
        Some(value) => match i64::try_from(value) {
            Ok(value) => min <= value && value <= max,
            Err(_) => true,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_plan_builds_everything() {
        let schema = crate::logs_schema();
        let plan = TransformOptions::new().plan(&schema).unwrap();
        assert!(plan.built.iter().all(|built| *built));
        assert!(plan.output.is_none());
        assert!(plan.keeps_service(None));
    }

    #[test]
    fn rejects_columns_past_the_schema() {
        let schema = crate::logs_schema();
        let width = schema.fields().len();
        for column in [width, usize::MAX] {
            let error = TransformOptions::new()
                .with_columns([column])
                .plan(&schema)
                .err()
                .expect("rejected");
            assert!(matches!(error, Error::InvalidOptions(_)), "{error}");
        }
    }

    #[test]
    fn rejects_time_ranges_on_other_columns() {
        let schema = crate::logs_schema();
        let body = schema.index_of("body").unwrap();
        let error = TransformOptions::new()
            .with_time_range(body, 0, 1)
            .plan(&schema)
            .err()
            .expect("rejected");
        assert!(error.to_string().contains("not a nanosecond timestamp"));
    }

    #[test]
    fn ranges_on_one_column_intersect() {
        let schema = crate::logs_schema();
        let plan = TransformOptions::new()
            .with_time_range(0, 10, 100)
            .with_time_range(0, 50, 200)
            .plan(&schema)
            .unwrap();
        assert_eq!(plan.time_range(&schema, "time_unix_nano"), Some((50, 100)));
        assert_eq!(plan.time_range(&schema, "observed_time_unix_nano"), None);
    }

    #[test]
    fn missing_service_name_is_asked_about_unknown() {
        let plan = TransformOptions::new()
            .with_service_filter(|name| name == "unknown")
            .plan(&crate::logs_schema())
            .unwrap();
        assert!(plan.keeps_service(None));
        assert!(!plan.keeps_service(Some("checkout")));
    }

    #[test]
    fn range_semantics() {
        assert!(in_range(None, None));
        assert!(!in_range(Some((0, 10)), None));
        assert!(in_range(Some((0, 10)), Some(10)));
        assert!(!in_range(Some((0, 10)), Some(11)));
        assert!(in_range(Some((0, 10)), Some(u64::MAX)));
    }
}
