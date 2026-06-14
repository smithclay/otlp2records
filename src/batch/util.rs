//! Generic Arrow builder and scalar conversion helpers.

use std::sync::Arc;

use arrow_array::{
    builder::{
        FixedSizeBinaryBuilder, Float64Builder, ListBuilder, StringBuilder,
        TimestampNanosecondBuilder, UInt64Builder,
    },
    Array, ArrayRef, RecordBatch,
};

use crate::{DecodeError, Error, Result};

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
    let value = value.unwrap_or("unknown");
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
    if value == 0 {
        builder.append_null();
    } else {
        builder.append_value(u64_to_i64(value, field)?);
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

#[inline]
pub(super) fn array<A: Array + 'static>(array: A) -> ArrayRef {
    Arc::new(array)
}

#[inline]
pub(super) fn record_batch(
    schema: Arc<arrow_schema::Schema>,
    arrays: Vec<ArrayRef>,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(schema, arrays)?)
}

#[inline]
pub(super) fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        Error::Decode(DecodeError::Unsupported(format!(
            "timestamp overflow: {field} value {value} exceeds i64::MAX (year 2262)"
        )))
    })
}
