//! Generic Arrow builder and scalar conversion helpers.

use std::sync::Arc;

use arrow_array::{
    builder::{Float64Builder, StringBuilder},
    Array, ArrayRef, RecordBatch,
};
use const_hex::{encode as hex_encode, encode_to_str};

use crate::{DecodeError, Error, Result};

#[inline]
pub(super) fn non_empty_str(value: Option<&str>) -> Option<String> {
    value
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[inline]
pub(super) fn append_opt(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

#[inline]
pub(super) fn append_required_service_name(builder: &mut StringBuilder, value: Option<&str>) {
    builder.append_value(value.unwrap_or("unknown"));
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
pub(super) fn append_hex_or_null(builder: &mut StringBuilder, bytes: &[u8]) {
    if bytes.is_empty() {
        builder.append_null();
    } else if bytes.len() <= 32 {
        let mut buf = [0_u8; 64];
        let hex = encode_to_str(bytes, &mut buf[..bytes.len() * 2])
            .expect("hex buffer length is exactly input length * 2");
        builder.append_value(hex);
    } else {
        builder.append_value(hex_encode(bytes));
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
    schema: arrow_schema::Schema,
    arrays: Vec<ArrayRef>,
) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
}

#[inline]
pub(super) fn u64_to_i64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        Error::Decode(DecodeError::Unsupported(format!(
            "timestamp overflow: {field} value {value} exceeds i64::MAX (year 2262)"
        )))
    })
}
