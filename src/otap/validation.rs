use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, TimeUnit};

use super::wire::{
    ATTR_BOOL, ATTR_BYTES, ATTR_DOUBLE, ATTR_INT, ATTR_KEY, ATTR_SER, ATTR_STR, ATTR_TYPE,
};
use crate::{Error, Result};

#[derive(Clone, Copy)]
enum Simple {
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int32,
    Int64,
    Float64,
    Utf8,
    Binary,
    FixedSizeBinary(i32),
    TimestampNanosecond,
    DurationNanosecond,
}

#[derive(Clone, Copy, PartialEq)]
enum KeySize {
    U8,
    U16,
}

#[derive(Clone, Copy)]
enum Expected {
    Simple(Simple),
    Dictionary {
        min_key_size: KeySize,
        value_type: Simple,
    },
    Struct(&'static [Column]),
    List(Simple),
    ListStruct(&'static [Column]),
}

#[derive(Clone, Copy)]
struct Column {
    name: &'static str,
    expected: Expected,
    required: bool,
}

macro_rules! column {
    ($name:expr, $expected:expr, required) => {
        Column {
            name: $name,
            expected: $expected,
            required: true,
        }
    };
    ($name:expr, $expected:expr) => {
        Column {
            name: $name,
            expected: $expected,
            required: false,
        }
    };
}

const fn simple(value: Simple) -> Expected {
    Expected::Simple(value)
}

const fn dict(min_key_size: KeySize, value_type: Simple) -> Expected {
    Expected::Dictionary {
        min_key_size,
        value_type,
    }
}

const RESOURCE: &[Column] = &[
    column!("id", simple(Simple::UInt16)),
    column!("dropped_attributes_count", simple(Simple::UInt32)),
    column!("schema_url", dict(KeySize::U8, Simple::Utf8)),
];

const SCOPE: &[Column] = &[
    column!("id", simple(Simple::UInt16)),
    column!("dropped_attributes_count", simple(Simple::UInt32)),
    column!("name", dict(KeySize::U8, Simple::Utf8)),
    column!("version", dict(KeySize::U8, Simple::Utf8)),
];

const STATUS: &[Column] = &[
    column!("code", dict(KeySize::U8, Simple::Int32)),
    column!("status_message", dict(KeySize::U8, Simple::Utf8)),
];

const ATTRS_16: &[Column] = &[
    column!("parent_id", simple(Simple::UInt16), required),
    column!(ATTR_KEY, dict(KeySize::U8, Simple::Utf8), required),
    column!(ATTR_TYPE, simple(Simple::UInt8), required),
    column!(ATTR_STR, dict(KeySize::U16, Simple::Utf8)),
    column!(ATTR_INT, dict(KeySize::U16, Simple::Int64)),
    column!(ATTR_DOUBLE, simple(Simple::Float64)),
    column!(ATTR_BOOL, simple(Simple::Boolean)),
    column!(ATTR_BYTES, dict(KeySize::U16, Simple::Binary)),
    column!(ATTR_SER, dict(KeySize::U16, Simple::Binary)),
];

const ATTRS_32: &[Column] = &[
    column!("parent_id", dict(KeySize::U8, Simple::UInt32), required),
    column!(ATTR_KEY, dict(KeySize::U8, Simple::Utf8), required),
    column!(ATTR_TYPE, simple(Simple::UInt8), required),
    column!(ATTR_STR, dict(KeySize::U16, Simple::Utf8)),
    column!(ATTR_INT, dict(KeySize::U16, Simple::Int64)),
    column!(ATTR_DOUBLE, simple(Simple::Float64)),
    column!(ATTR_BOOL, simple(Simple::Boolean)),
    column!(ATTR_BYTES, dict(KeySize::U16, Simple::Binary)),
    column!(ATTR_SER, dict(KeySize::U16, Simple::Binary)),
];

const SPANS: &[Column] = &[
    column!(
        "start_time_unix_nano",
        simple(Simple::TimestampNanosecond),
        required
    ),
    column!(
        "duration_time_unix_nano",
        dict(KeySize::U8, Simple::DurationNanosecond),
        required
    ),
    column!(
        "trace_id",
        dict(KeySize::U8, Simple::FixedSizeBinary(16)),
        required
    ),
    column!(
        "span_id",
        dict(KeySize::U8, Simple::FixedSizeBinary(8)),
        required
    ),
    column!("name", dict(KeySize::U8, Simple::Utf8), required),
    column!("id", simple(Simple::UInt16)),
    column!("kind", dict(KeySize::U8, Simple::Int32)),
    column!("parent_span_id", simple(Simple::FixedSizeBinary(8))),
    column!("dropped_attributes_count", simple(Simple::UInt32)),
    column!("dropped_events_count", simple(Simple::UInt32)),
    column!("dropped_links_count", simple(Simple::UInt32)),
    column!("schema_url", dict(KeySize::U8, Simple::Utf8)),
    column!("trace_state", dict(KeySize::U8, Simple::Utf8)),
    column!("flags", simple(Simple::UInt32)),
    column!("resource", Expected::Struct(RESOURCE)),
    column!("scope", Expected::Struct(SCOPE)),
    column!("status", Expected::Struct(STATUS)),
];

const SPAN_EVENTS: &[Column] = &[
    column!("parent_id", simple(Simple::UInt16), required),
    column!("name", dict(KeySize::U8, Simple::Utf8), required),
    column!("id", simple(Simple::UInt32)),
    column!("time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("dropped_attributes_count", simple(Simple::UInt32)),
];

const SPAN_LINKS: &[Column] = &[
    column!("parent_id", simple(Simple::UInt16), required),
    column!("id", simple(Simple::UInt32)),
    column!(
        "span_id",
        dict(KeySize::U8, Simple::FixedSizeBinary(8)),
        required
    ),
    column!(
        "trace_id",
        dict(KeySize::U8, Simple::FixedSizeBinary(16)),
        required
    ),
    column!("trace_state", dict(KeySize::U8, Simple::Utf8)),
    column!("flags", simple(Simple::UInt32)),
    column!("dropped_attributes_count", simple(Simple::UInt32)),
];

const METRICS: &[Column] = &[
    column!("id", simple(Simple::UInt16), required),
    column!("metric_type", simple(Simple::UInt8), required),
    column!("name", dict(KeySize::U8, Simple::Utf8), required),
    column!("aggregation_temporality", dict(KeySize::U8, Simple::Int32)),
    column!("description", dict(KeySize::U8, Simple::Utf8)),
    column!("is_monotonic", simple(Simple::Boolean)),
    column!("unit", dict(KeySize::U8, Simple::Utf8)),
    column!("schema_url", dict(KeySize::U8, Simple::Utf8)),
    column!("resource", Expected::Struct(RESOURCE)),
    column!("scope", Expected::Struct(SCOPE)),
];

const NUMBER_POINTS: &[Column] = &[
    column!("parent_id", simple(Simple::UInt16), required),
    column!("start_time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("int_value", simple(Simple::Int64)),
    column!("double_value", simple(Simple::Float64)),
    column!("id", simple(Simple::UInt32)),
    column!("flags", simple(Simple::UInt32)),
];

const QUANTILE: &[Column] = &[
    column!("quantile", simple(Simple::Float64)),
    column!("value", simple(Simple::Float64)),
];

const SUMMARY_POINTS: &[Column] = &[
    column!("parent_id", simple(Simple::UInt16), required),
    column!("id", simple(Simple::UInt32)),
    column!("count", simple(Simple::UInt64)),
    column!("sum", simple(Simple::Float64)),
    column!("start_time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("flags", simple(Simple::UInt32)),
    column!("quantile", Expected::ListStruct(QUANTILE)),
    column!("value", Expected::List(Simple::Float64)),
];

const HISTOGRAM_POINTS: &[Column] = &[
    column!("parent_id", simple(Simple::UInt16), required),
    column!("id", simple(Simple::UInt32)),
    column!("count", simple(Simple::UInt64)),
    column!("sum", simple(Simple::Float64)),
    column!("min", simple(Simple::Float64)),
    column!("max", simple(Simple::Float64)),
    column!("bucket_counts", Expected::List(Simple::UInt64)),
    column!("explicit_bounds", Expected::List(Simple::Float64)),
    column!("start_time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("flags", simple(Simple::UInt32)),
];

const EXP_BUCKETS: &[Column] = &[
    column!("bucket_counts", Expected::List(Simple::UInt64)),
    column!("offset", simple(Simple::Int32)),
];

const EXP_HISTOGRAM_POINTS: &[Column] = &[
    column!("parent_id", simple(Simple::UInt16), required),
    column!("id", simple(Simple::UInt32)),
    column!("count", simple(Simple::UInt64)),
    column!("sum", simple(Simple::Float64)),
    column!("min", simple(Simple::Float64)),
    column!("max", simple(Simple::Float64)),
    column!("scale", simple(Simple::Int32)),
    column!("zero_count", simple(Simple::UInt64)),
    column!("zero_threshold", simple(Simple::Float64)),
    column!("positive", Expected::Struct(EXP_BUCKETS)),
    column!("negative", Expected::Struct(EXP_BUCKETS)),
    column!("start_time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("time_unix_nano", simple(Simple::TimestampNanosecond)),
    column!("flags", simple(Simple::UInt32)),
];

const EXEMPLARS: &[Column] = &[
    column!("parent_id", dict(KeySize::U8, Simple::UInt32), required),
    column!("id", simple(Simple::UInt32)),
    column!("double_value", simple(Simple::Float64)),
    column!("int_value", dict(KeySize::U8, Simple::Int64)),
    column!("span_id", dict(KeySize::U8, Simple::FixedSizeBinary(8))),
    column!(
        "time_unix_nano",
        simple(Simple::TimestampNanosecond),
        required
    ),
    column!("trace_id", dict(KeySize::U8, Simple::FixedSizeBinary(16))),
];

#[derive(Clone, Copy)]
pub(super) enum PayloadSchema {
    Attrs16,
    Attrs32,
    Spans,
    SpanEvents,
    SpanLinks,
    Metrics,
    NumberPoints,
    SummaryPoints,
    HistogramPoints,
    ExpHistogramPoints,
    Exemplars,
}

pub(super) fn validate(payload: PayloadSchema, context: &str, batch: &RecordBatch) -> Result<()> {
    let columns = match payload {
        PayloadSchema::Attrs16 => ATTRS_16,
        PayloadSchema::Attrs32 => ATTRS_32,
        PayloadSchema::Spans => SPANS,
        PayloadSchema::SpanEvents => SPAN_EVENTS,
        PayloadSchema::SpanLinks => SPAN_LINKS,
        PayloadSchema::Metrics => METRICS,
        PayloadSchema::NumberPoints => NUMBER_POINTS,
        PayloadSchema::SummaryPoints => SUMMARY_POINTS,
        PayloadSchema::HistogramPoints => HISTOGRAM_POINTS,
        PayloadSchema::ExpHistogramPoints => EXP_HISTOGRAM_POINTS,
        PayloadSchema::Exemplars => EXEMPLARS,
    };
    validate_columns(context, batch.schema().fields(), batch.columns(), columns)
}

fn validate_columns(
    context: &str,
    fields: &arrow_schema::Fields,
    arrays: &[ArrayRef],
    expected: &'static [Column],
) -> Result<()> {
    for (field, array) in fields.iter().zip(arrays) {
        let Some(column) = expected.iter().find(|column| column.name == field.name()) else {
            return Err(Error::Otap(format!(
                "{context} has unknown column {:?}",
                field.name()
            )));
        };
        if !matches_expected(array, column.expected)? {
            return Err(Error::Otap(format!(
                "{context}.{} has type {:?}, which is not canonical OTAP",
                field.name(),
                field.data_type()
            )));
        }
        if column.required && array.null_count() > 0 {
            return Err(Error::Otap(format!(
                "{context}.{} is required but contains nulls",
                field.name()
            )));
        }
    }
    for column in expected.iter().filter(|column| column.required) {
        if fields.iter().all(|field| field.name() != column.name) {
            return Err(Error::Otap(format!(
                "{context} is missing required column {:?}",
                column.name
            )));
        }
    }
    Ok(())
}

fn matches_expected(array: &ArrayRef, expected: Expected) -> Result<bool> {
    Ok(match expected {
        Expected::Simple(simple) => simple.matches(array.data_type()),
        Expected::Dictionary {
            min_key_size,
            value_type,
        } => match array.data_type() {
            data_type if value_type.matches(data_type) => true,
            DataType::Dictionary(key, value) => {
                // Accept signed and unsigned keys at each width to match the
                // decoder's `expect_dict_or`/`dictionary_value!` handling, so a
                // legitimate `Dictionary(Int8, ...)` producer is not rejected
                // here only to be decoded successfully elsewhere.
                value_type.matches(value)
                    && matches!(
                        (min_key_size, key.as_ref()),
                        (KeySize::U8, DataType::UInt8)
                            | (KeySize::U8, DataType::Int8)
                            | (KeySize::U8, DataType::UInt16)
                            | (KeySize::U8, DataType::Int16)
                            | (KeySize::U16, DataType::UInt16)
                            | (KeySize::U16, DataType::Int16)
                    )
            }
            _ => false,
        },
        Expected::Struct(columns) => {
            let Some(values) = array.as_any().downcast_ref::<StructArray>() else {
                return Ok(false);
            };
            validate_columns(
                "nested OTAP struct",
                values.fields(),
                values.columns(),
                columns,
            )?;
            true
        }
        Expected::List(simple) => {
            let DataType::List(field) = array.data_type() else {
                return Ok(false);
            };
            if !simple.matches(field.data_type()) {
                return Ok(false);
            }
            // Canonical OTAP repeated scalar columns (histogram bucket counts and
            // explicit bounds, exponential bucket counts, summary quantile values)
            // never carry null elements. A null element would shorten the decoded
            // Vec and misalign bucket counts against their bounds, so reject it
            // here rather than letting the view layer silently drop it.
            if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
                for row in 0..list.len() {
                    if list.is_valid(row) && list.value(row).null_count() > 0 {
                        return Err(Error::Otap(
                            "an OTAP list column contains a null element, which is not canonical OTAP"
                                .into(),
                        ));
                    }
                }
            }
            true
        }
        Expected::ListStruct(columns) => {
            let DataType::List(field) = array.data_type() else {
                return Ok(false);
            };
            let DataType::Struct(fields) = field.data_type() else {
                return Ok(false);
            };
            fields.iter().all(|field| {
                columns
                    .iter()
                    .find(|column| column.name == field.name())
                    .is_some_and(|column| match column.expected {
                        Expected::Simple(simple) => simple.matches(field.data_type()),
                        _ => false,
                    })
            }) && columns
                .iter()
                .filter(|column| column.required)
                .all(|column| fields.iter().any(|field| field.name() == column.name))
        }
    })
}

impl Simple {
    fn matches(self, actual: &DataType) -> bool {
        match self {
            Self::Boolean => actual == &DataType::Boolean,
            Self::UInt8 => actual == &DataType::UInt8,
            Self::UInt16 => actual == &DataType::UInt16,
            Self::UInt32 => actual == &DataType::UInt32,
            Self::UInt64 => actual == &DataType::UInt64,
            Self::Int32 => actual == &DataType::Int32,
            Self::Int64 => actual == &DataType::Int64,
            Self::Float64 => actual == &DataType::Float64,
            Self::Utf8 => actual == &DataType::Utf8,
            Self::Binary => actual == &DataType::Binary,
            Self::FixedSizeBinary(width) => actual == &DataType::FixedSizeBinary(width),
            Self::TimestampNanosecond => {
                matches!(actual, DataType::Timestamp(TimeUnit::Nanosecond, _))
            }
            Self::DurationNanosecond => actual == &DataType::Duration(TimeUnit::Nanosecond),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        types::UInt64Type, Int64Array, ListArray, RecordBatch, UInt16Array, UInt64Array,
    };
    use arrow_schema::{Field, Schema};

    use super::*;

    #[test]
    fn rejects_wrong_span_duration_type() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(
                    "start_time_unix_nano",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("duration_time_unix_nano", DataType::Int64, false),
                Field::new("trace_id", DataType::FixedSizeBinary(16), false),
                Field::new("span_id", DataType::FixedSizeBinary(8), false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(arrow_array::TimestampNanosecondArray::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(
                    arrow_array::FixedSizeBinaryArray::try_from_iter([vec![0; 16]].into_iter())
                        .unwrap(),
                ),
                Arc::new(
                    arrow_array::FixedSizeBinaryArray::try_from_iter([vec![0; 8]].into_iter())
                        .unwrap(),
                ),
                Arc::new(arrow_array::StringArray::from(vec!["span"])),
            ],
        )
        .unwrap();

        assert!(validate(PayloadSchema::Spans, "spans", &batch).is_err());
    }

    #[test]
    fn rejects_scalar_histogram_bucket_counts() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt16, false),
                Field::new("bucket_counts", DataType::UInt64, true),
            ])),
            vec![
                Arc::new(UInt16Array::from(vec![1])),
                Arc::new(UInt64Array::from(vec![1])),
            ],
        )
        .unwrap();

        assert!(validate(PayloadSchema::HistogramPoints, "histogram points", &batch).is_err());
    }

    #[test]
    fn rejects_null_element_in_histogram_bucket_counts() {
        let bucket_counts = ListArray::from_iter_primitive::<UInt64Type, _, _>(vec![Some(vec![
            Some(1u64),
            None,
            Some(3u64),
        ])]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("parent_id", DataType::UInt16, false),
                Field::new("bucket_counts", bucket_counts.data_type().clone(), true),
            ])),
            vec![
                Arc::new(UInt16Array::from(vec![1])),
                Arc::new(bucket_counts),
            ],
        )
        .unwrap();

        assert!(validate(PayloadSchema::HistogramPoints, "histogram points", &batch).is_err());
    }
}
