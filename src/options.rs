//! Narrowing a transform to the columns and rows a reader needs.
//!
//! A query engine that reads OTLP files usually wants a few columns of a few
//! rows. [`TransformOptions`] lets it say so, and the transform then skips the
//! work for everything else: unrequested columns are not built (attribute and
//! body JSON is most of a transform's cost), and rows that fail a predicate are
//! dropped before any of their columns are.
//!
//! The predicates are pruning, not query evaluation. They are deliberately
//! limited to what can be decided cheaply while walking the request
//! (`service_name` membership and inclusive timestamp ranges), and they follow
//! SQL comparison semantics: a NULL timestamp fails a range. A caller that has
//! richer filters applies them itself after the transform.

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
/// the requested shape's). A column that is not requested is still present in
/// the output, as an all-null array whose field is marked nullable, so the
/// output schema keeps its column order and names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransformOptions {
    columns: Option<Vec<usize>>,
    service_names: Option<Vec<String>>,
    time_ranges: Vec<TimeRange>,
}

impl TransformOptions {
    /// Every column of every row.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build only these columns. An index past the schema is rejected when the
    /// transform validates the options.
    #[must_use]
    pub fn with_columns(mut self, columns: impl IntoIterator<Item = usize>) -> Self {
        self.columns = Some(columns.into_iter().collect());
        self
    }

    /// Keep only rows whose `service_name` is one of `names`. The comparison is
    /// on the value the transform emits, so a resource without `service.name`
    /// matches `"unknown"`.
    #[must_use]
    pub fn with_service_names<S: Into<String>>(
        mut self,
        names: impl IntoIterator<Item = S>,
    ) -> Self {
        self.service_names = Some(names.into_iter().map(Into::into).collect());
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
        let columns = match &self.columns {
            None => vec![true; width],
            Some(requested) => {
                let mut columns = vec![false; width];
                for &column in requested {
                    let Some(slot) = columns.get_mut(column) else {
                        return Err(invalid(format!(
                            "column {column} is past the schema's {width}"
                        )));
                    };
                    *slot = true;
                }
                columns
            }
        };
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
            columns,
            service_names: self.service_names.clone(),
            time_ranges,
        })
    }
}

fn invalid(message: String) -> Error {
    Error::InvalidInput(format!("transform options: {message}"))
}

/// [`TransformOptions`] resolved against one schema.
#[derive(Debug, Clone)]
pub(crate) struct Plan {
    /// Whether each column of the schema is built.
    columns: Vec<bool>,
    service_names: Option<Vec<String>>,
    time_ranges: Vec<TimeRange>,
}

impl Plan {
    /// Every column of every row of `schema`.
    pub(crate) fn all(schema: &Schema) -> Self {
        Self {
            columns: vec![true; schema.fields().len()],
            service_names: None,
            time_ranges: Vec::new(),
        }
    }

    /// Whether the column at `index` is built.
    #[inline]
    pub(crate) fn builds(&self, index: usize) -> bool {
        self.columns[index]
    }

    /// Whether the named column is built. Panics on a name the schema lacks,
    /// which is a bug in the builder asking.
    pub(crate) fn builds_named(&self, schema: &Schema, name: &str) -> bool {
        self.builds(
            schema
                .index_of(name)
                .unwrap_or_else(|_| panic!("no column {name}")),
        )
    }

    /// Whether every column is built.
    pub(crate) fn builds_everything(&self) -> bool {
        self.columns.iter().all(|built| *built)
    }

    /// Whether a resource whose emitted `service_name` is `service_name`
    /// (`None` emits `"unknown"`) keeps its rows.
    pub(crate) fn keeps_service(&self, service_name: Option<&str>) -> bool {
        let Some(names) = &self.service_names else {
            return true;
        };
        let value = service_name.unwrap_or(crate::batch::UNKNOWN_SERVICE_NAME);
        names.iter().any(|name| name == value)
    }

    /// The range rows must fall in on the named column, if any.
    pub(crate) fn time_range(&self, schema: &Schema, name: &str) -> Option<(i64, i64)> {
        let index = schema.index_of(name).ok()?;
        self.time_ranges
            .iter()
            .find(|range| range.column == index)
            .map(|range| (range.min, range.max))
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
        let plan = TransformOptions::new().plan(&crate::logs_schema()).unwrap();
        assert!(plan.builds_everything());
        assert!(plan.keeps_service(None));
    }

    #[test]
    fn rejects_columns_past_the_schema() {
        let schema = crate::logs_schema();
        let width = schema.fields().len();
        assert!(TransformOptions::new()
            .with_columns([width])
            .plan(&schema)
            .is_err());
        assert!(TransformOptions::new()
            .with_columns([usize::MAX])
            .plan(&schema)
            .is_err());
    }

    #[test]
    fn rejects_time_ranges_on_other_columns() {
        let schema = crate::logs_schema();
        let body = schema.index_of("body").unwrap();
        let error = TransformOptions::new()
            .with_time_range(body, 0, 1)
            .plan(&schema)
            .unwrap_err();
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
    fn missing_service_name_matches_unknown() {
        let plan = TransformOptions::new()
            .with_service_names(["unknown"])
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
