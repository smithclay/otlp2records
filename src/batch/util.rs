//! Generic Arrow builder and scalar conversion helpers.

use std::sync::Arc;

use arrow_array::{
    builder::{
        FixedSizeBinaryBuilder, Float64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt64Builder,
    },
    Array, ArrayRef,
};

use arrow_schema::Schema;

use crate::{options::Plan, DecodeError, Error, Result};

#[inline]
pub(super) fn append_opt_n(builder: &mut StringBuilder, value: Option<&str>, n: usize) {
    match value {
        Some(value) => {
            for _ in 0..n {
                builder.append_value(value);
            }
        }
        None => builder.append_nulls(n),
    }
}

#[inline]
pub(super) fn append_required_service_name_n(
    builder: &mut StringBuilder,
    value: Option<&str>,
    n: usize,
) {
    let value = value.unwrap_or(super::UNKNOWN_SERVICE_NAME);
    for _ in 0..n {
        builder.append_value(value);
    }
}

#[inline]
pub(super) fn append_empty_as_null(builder: &mut StringBuilder, value: &str) {
    if value.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(value);
    }
}

#[inline]
pub(super) fn append_finite(builder: &mut Float64Builder, value: f64) {
    if value.is_finite() {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

#[inline]
pub(super) fn append_finite_opt(builder: &mut Float64Builder, value: Option<f64>) {
    match value {
        Some(value) => append_finite(builder, value),
        None => builder.append_null(),
    }
}

#[inline]
pub(super) fn append_opt_ts_ns(
    builder: &mut TimestampNanosecondBuilder,
    value: u64,
    field: &str,
) -> Result<()> {
    match opt_ts(value) {
        Some(value) => builder.append_value(u64_to_i64(value, field)?),
        None => builder.append_null(),
    }
    Ok(())
}

#[inline]
pub(super) fn append_required_ts_ns(
    builder: &mut TimestampNanosecondBuilder,
    value: u64,
    field: &str,
) -> Result<()> {
    builder.append_value(u64_to_i64(value, field)?);
    Ok(())
}

#[inline]
pub(super) fn append_fixed_or_null(
    builder: &mut FixedSizeBinaryBuilder,
    value: &[u8],
    len: usize,
) -> Result<()> {
    if value.is_empty() {
        builder.append_null();
        return Ok(());
    }
    if value.len() != len {
        return Err(Error::Decode(DecodeError::Unsupported(format!(
            "fixed binary length mismatch: expected {len}, got {}",
            value.len()
        ))));
    }
    builder.append_value(value)?;
    Ok(())
}

#[inline]
pub(super) fn append_fixed_required(
    builder: &mut FixedSizeBinaryBuilder,
    value: &[u8],
    len: usize,
    field: &str,
) -> Result<()> {
    if value.len() != len {
        return Err(Error::Decode(DecodeError::Unsupported(format!(
            "{field} fixed binary length mismatch: expected {len}, got {}",
            value.len()
        ))));
    }
    builder.append_value(value)?;
    Ok(())
}

#[inline]
pub(super) fn append_u64_list(builder: &mut ListBuilder<UInt64Builder>, values: &[u64]) {
    for value in values {
        builder.values().append_value(*value);
    }
    builder.append(true);
}

#[inline]
pub(super) fn append_f64_list(builder: &mut ListBuilder<Float64Builder>, values: &[f64]) {
    for value in values {
        if value.is_finite() {
            builder.values().append_value(*value);
        } else {
            builder.values().append_null();
        }
    }
    builder.append(true);
}

#[inline]
pub(super) fn string_builder(rows: usize) -> StringBuilder {
    string_builder_bytes(rows, rows.saturating_mul(32))
}

#[inline]
pub(super) fn string_builder_bytes(rows: usize, data_capacity: usize) -> StringBuilder {
    StringBuilder::with_capacity(rows, data_capacity)
}

/// A JSON string column that a plan may leave out. One that is not built
/// reserves nothing and ignores appends, and the plan drops its empty array,
/// so a builder appends to it unconditionally.
pub(super) struct JsonColumn {
    builder: StringBuilder,
    built: bool,
}

impl JsonColumn {
    /// The column `name` of `schema`, with room for `bytes` of JSON if `plan`
    /// builds it.
    pub(super) fn new(plan: &Plan, schema: &Schema, name: &str, rows: usize, bytes: usize) -> Self {
        let built = plan.builds_named(schema, name);
        let builder = if built {
            string_builder_bytes(rows, bytes)
        } else {
            StringBuilder::new()
        };
        Self { builder, built }
    }

    /// Whether the plan builds this column.
    pub(super) fn built(&self) -> bool {
        self.built
    }

    /// Runs `append` against the column's builder, if it is built.
    pub(super) fn append(
        &mut self,
        append: impl FnOnce(&mut StringBuilder) -> Result<()>,
    ) -> Result<()> {
        if self.built {
            append(&mut self.builder)
        } else {
            Ok(())
        }
    }

    /// Appends `value` (NULL when `None`) `n` times, if the column is built.
    pub(super) fn append_opt_n(&mut self, value: Option<&str>, n: usize) {
        if self.built {
            append_opt_n(&mut self.builder, value, n);
        }
    }

    pub(super) fn finish(&mut self) -> ArrayRef {
        array(self.builder.finish())
    }
}

#[inline]
pub(super) fn array<A: Array + 'static>(array: A) -> ArrayRef {
    Arc::new(array)
}

/// The value a nullable timestamp column emits for a raw OTLP field, where
/// zero means unset.
#[inline]
pub(super) fn opt_ts(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

#[inline]
pub(super) fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        Error::Decode(DecodeError::Unsupported(format!(
            "timestamp overflow: {field} value {value} exceeds i64::MAX (year 2262)"
        )))
    })
}
