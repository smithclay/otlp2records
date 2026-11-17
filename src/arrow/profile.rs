//! Output Arrow type profiles.
//!
//! Every transform and decoder emits the *canonical* schema: OTLP ids as
//! `FixedSizeBinary`, span duration as `Duration(Nanosecond)`, counters as
//! unsigned integers. That is the faithful Arrow rendering and stays the
//! default.
//!
//! Some consumers want a different physical shape. [`ArrowProfile::DuckDb`]
//! re-types a finished batch so DuckDB's stock Arrow importer
//! (`duckdb_data_chunk_from_arrow`) produces the column types the
//! `duckdb-otlp` extension exposes, without a hand-written converter:
//!
//! | Canonical                       | `DuckDb` profile | DuckDB type |
//! |---------------------------------|------------------|-------------|
//! | `FixedSizeBinary(N)`            | `Utf8`, lowercase hex (`2N` chars) | `VARCHAR` |
//! | `Duration(_)`                   | `Int64`, raw value | `BIGINT` |
//! | `UInt8` / `UInt16` / `UInt32`   | `Int32` | `INTEGER` |
//! | `UInt64`                        | `Int64` (error above `i64::MAX`) | `BIGINT` |
//! | `List(T)` / `LargeList(T)`      | same list, element mapped as above | `LIST` |
//!
//! Every other type passes through unchanged. Nulls stay null: an absent or
//! all-zero id is already null in the canonical batch and is not rendered.
//!
//! The profile is a single post-processing pass over a finished
//! [`RecordBatch`]; columns that need no change are shared, not copied.

use std::sync::Arc;

use arrow_array::types::{Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use arrow_array::ArrowPrimitiveType;
use arrow_array::{
    make_array, Array, ArrayRef, FixedSizeBinaryArray, LargeListArray, ListArray, PrimitiveArray,
    RecordBatch, RecordBatchOptions, StringArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema};

use crate::error::{Error, Result};
use crate::MetricBatches;

/// Arrow type profile applied to transform/decoder output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ArrowProfile {
    /// The canonical schema (the default). Output is returned untouched.
    #[default]
    Canonical,
    /// Types DuckDB's stock Arrow importer maps to the `duckdb-otlp` column
    /// types. See the module docs for the mapping.
    DuckDb,
}

impl ArrowProfile {
    /// The schema a batch has after this profile is applied.
    pub fn schema(self, schema: &Schema) -> Schema {
        match self {
            ArrowProfile::Canonical => schema.clone(),
            ArrowProfile::DuckDb => Schema::new_with_metadata(
                schema.fields().iter().map(duckdb_field).collect::<Vec<_>>(),
                schema.metadata().clone(),
            ),
        }
    }

    /// The data type a column of `data_type` has after this profile is applied.
    pub fn data_type(self, data_type: &DataType) -> DataType {
        match self {
            ArrowProfile::Canonical => data_type.clone(),
            ArrowProfile::DuckDb => duckdb_type(data_type),
        }
    }

    /// Re-type `batch` for this profile.
    ///
    /// `Canonical` returns the batch unchanged. `DuckDb` fails only when a
    /// `UInt32`/`UInt64` value does not fit the signed target type.
    pub fn apply(self, batch: RecordBatch) -> Result<RecordBatch> {
        match self {
            ArrowProfile::Canonical => Ok(batch),
            ArrowProfile::DuckDb => apply_duckdb(batch),
        }
    }

    /// [`apply`](Self::apply) to every present metric shape. Skipped counters
    /// are carried over unchanged.
    pub fn apply_metrics(self, batches: MetricBatches) -> Result<MetricBatches> {
        if self == ArrowProfile::Canonical {
            return Ok(batches);
        }
        let apply = |batch: Option<RecordBatch>| batch.map(|b| self.apply(b)).transpose();
        Ok(MetricBatches {
            gauge: apply(batches.gauge)?,
            sum: apply(batches.sum)?,
            histogram: apply(batches.histogram)?,
            exp_histogram: apply(batches.exp_histogram)?,
            skipped: batches.skipped,
        })
    }
}

fn duckdb_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::FixedSizeBinary(_) => DataType::Utf8,
        DataType::Duration(_) => DataType::Int64,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => DataType::Int32,
        DataType::UInt64 => DataType::Int64,
        DataType::List(item) => DataType::List(duckdb_field(item)),
        DataType::LargeList(item) => DataType::LargeList(duckdb_field(item)),
        other => other.clone(),
    }
}

fn duckdb_field(field: &FieldRef) -> FieldRef {
    let mapped = duckdb_type(field.data_type());
    if &mapped == field.data_type() {
        return Arc::clone(field);
    }
    Arc::new(Field::clone(field).with_data_type(mapped))
}

fn apply_duckdb(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    if !schema
        .fields()
        .iter()
        .any(|f| &duckdb_type(f.data_type()) != f.data_type())
    {
        return Ok(batch);
    }
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| duckdb_array(field.name(), column))
        .collect::<Result<Vec<_>>>()?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    Ok(RecordBatch::try_new_with_options(
        Arc::new(ArrowProfile::DuckDb.schema(&schema)),
        columns,
        &options,
    )?)
}

/// Re-type one column. `column` names the source column in error messages.
fn duckdb_array(column: &str, array: &ArrayRef) -> Result<ArrayRef> {
    Ok(match array.data_type() {
        DataType::FixedSizeBinary(_) => Arc::new(hex_encode(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("FixedSizeBinary array"),
        )),
        // A Duration of any unit is a plain i64 buffer: relabel it, zero-copy.
        DataType::Duration(_) => make_array(
            array
                .to_data()
                .into_builder()
                .data_type(DataType::Int64)
                .build()?,
        ),
        DataType::UInt8 => Arc::new(widen::<UInt8Type>(array)),
        DataType::UInt16 => Arc::new(widen::<UInt16Type>(array)),
        DataType::UInt32 => Arc::new(checked_signed::<UInt32Type, Int32Type>(column, array)?),
        DataType::UInt64 => Arc::new(checked_signed::<UInt64Type, Int64Type>(column, array)?),
        DataType::List(item) if &duckdb_type(item.data_type()) != item.data_type() => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("List array");
            Arc::new(ListArray::try_new(
                duckdb_field(item),
                list.offsets().clone(),
                duckdb_array(column, list.values())?,
                list.nulls().cloned(),
            )?)
        }
        DataType::LargeList(item) if &duckdb_type(item.data_type()) != item.data_type() => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .expect("LargeList array");
            Arc::new(LargeListArray::try_new(
                duckdb_field(item),
                list.offsets().clone(),
                duckdb_array(column, list.values())?,
                list.nulls().cloned(),
            )?)
        }
        _ => Arc::clone(array),
    })
}

/// Lowercase hex, two characters per byte. Null slots stay null.
fn hex_encode(array: &FixedSizeBinaryArray) -> StringArray {
    let width = array.value_length() as usize;
    let mut buf = vec![0u8; width * 2];
    let mut builder = arrow_array::builder::StringBuilder::with_capacity(
        array.len(),
        (array.len() - array.null_count()) * width * 2,
    );
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
            continue;
        }
        const_hex::encode_to_slice(array.value(i), &mut buf)
            .expect("hex buffer is exactly twice the value width");
        // const_hex only writes ASCII hex digits.
        builder.append_value(std::str::from_utf8(&buf).expect("ASCII hex"));
    }
    builder.finish()
}

/// UInt8/UInt16 -> Int32: always representable.
fn widen<S>(array: &ArrayRef) -> PrimitiveArray<Int32Type>
where
    S: ArrowPrimitiveType,
    S::Native: Into<i32>,
{
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<S>>()
        .expect("unsigned array")
        .unary(Into::into)
}

/// UInt32 -> Int32, UInt64 -> Int64, rejecting values above the signed
/// maximum instead of wrapping. Only valid slots are checked.
fn checked_signed<S, T>(column: &str, array: &ArrayRef) -> Result<PrimitiveArray<T>>
where
    S: ArrowPrimitiveType,
    T: ArrowPrimitiveType,
    S::Native: TryInto<T::Native> + std::fmt::Display,
{
    let source = array
        .as_any()
        .downcast_ref::<PrimitiveArray<S>>()
        .expect("unsigned array");
    source.try_unary(|value| {
        value.try_into().map_err(|_| {
            Error::InvalidInput(format!(
                "column '{column}': {} value {value} exceeds the {} range of the DuckDb Arrow profile",
                S::DATA_TYPE,
                T::DATA_TYPE,
            ))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{ListBuilder, UInt64Builder};
    use arrow_array::{
        DurationNanosecondArray, Int32Array, Int64Array, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };

    fn duckdb(array: ArrayRef) -> Result<ArrayRef> {
        duckdb_array("col", &array)
    }

    #[test]
    fn fixed_size_binary_becomes_lowercase_hex_and_keeps_nulls() {
        let ids = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![
                Some(vec![0x00, 0x01, 0xab, 0xff]),
                None,
                Some(vec![0xde, 0xad, 0xbe, 0xef]),
            ]
            .into_iter(),
            4,
        )
        .unwrap();
        let out = duckdb(Arc::new(ids.clone())).unwrap();
        assert_eq!(out.data_type(), &DataType::Utf8);
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(out.value(0), "0001abff");
        assert!(out.is_null(1));
        assert_eq!(out.value(2), "deadbeef");

        // A sliced input encodes only its own window.
        let sliced = duckdb(Arc::new(ids.slice(1, 2))).unwrap();
        let sliced = sliced.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(sliced.len(), 2);
        assert!(sliced.is_null(0));
        assert_eq!(sliced.value(1), "deadbeef");
    }

    #[test]
    fn hex_matches_byte_by_byte_rendering_for_every_byte_value() {
        let all: Vec<u8> = (0..=255u8).collect();
        let ids = FixedSizeBinaryArray::try_from_iter(all.chunks(16)).unwrap();
        let out = duckdb(Arc::new(ids)).unwrap();
        let out = out.as_any().downcast_ref::<StringArray>().unwrap();
        for (row, chunk) in all.chunks(16).enumerate() {
            let expected: String = chunk.iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(out.value(row), expected);
        }
    }

    #[test]
    fn duration_becomes_raw_int64() {
        let durations = DurationNanosecondArray::from(vec![Some(1), None, Some(i64::MAX)]);
        let out = duckdb(Arc::new(durations)).unwrap();
        let out = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out, &Int64Array::from(vec![Some(1), None, Some(i64::MAX)]));
    }

    #[test]
    fn narrow_unsigned_widens_to_int32() {
        let out = duckdb(Arc::new(UInt8Array::from(vec![Some(u8::MAX), None]))).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(255), None])
        );
        let out = duckdb(Arc::new(UInt16Array::from(vec![Some(u16::MAX), None]))).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(65_535), None])
        );
        let out = duckdb(Arc::new(UInt32Array::from(vec![
            Some(i32::MAX as u32),
            None,
        ])))
        .unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int32Array>().unwrap(),
            &Int32Array::from(vec![Some(i32::MAX), None])
        );
    }

    #[test]
    fn uint64_converts_up_to_i64_max() {
        let out = duckdb(Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(i64::MAX as u64),
            None,
        ])))
        .unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(0), Some(i64::MAX), None])
        );
    }

    #[test]
    fn uint64_above_i64_max_is_an_error_not_a_wrap() {
        let err = duckdb_array(
            "count",
            &(Arc::new(UInt64Array::from(vec![1, i64::MAX as u64 + 1])) as ArrayRef),
        )
        .unwrap_err();
        let message = err.to_string();
        assert!(matches!(err, Error::InvalidInput(_)), "{message}");
        assert!(message.contains("count"), "{message}");
        assert!(message.contains("9223372036854775808"), "{message}");
    }

    #[test]
    fn uint32_above_i32_max_is_an_error() {
        let err = duckdb(Arc::new(UInt32Array::from(vec![u32::MAX]))).unwrap_err();
        assert!(matches!(err, Error::InvalidInput(_)), "{err}");
    }

    #[test]
    fn out_of_range_value_under_a_null_slot_is_ignored() {
        let values = UInt64Array::new(vec![u64::MAX, 7].into(), Some(vec![false, true].into()));
        let out = duckdb(Arc::new(values)).unwrap();
        assert_eq!(
            out.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![None, Some(7)])
        );
    }

    #[test]
    fn list_elements_are_mapped_and_list_nulls_kept() {
        let mut builder = ListBuilder::new(UInt64Builder::new());
        builder.append_value([Some(1), Some(2)]);
        builder.append_null();
        builder.append_value([]);
        builder.append_value([Some(i64::MAX as u64), None]);
        let list = builder.finish();

        let out = duckdb(Arc::new(list)).unwrap();
        assert_eq!(
            out.data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
        );
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.len(), 4);
        assert!(out.is_null(1));
        assert_eq!(out.value(2).len(), 0);
        let last = out.value(3);
        let last = last.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(last, &Int64Array::from(vec![Some(i64::MAX), None]));

        let mut builder = ListBuilder::new(UInt64Builder::new());
        builder.append_value([Some(u64::MAX)]);
        assert!(duckdb(Arc::new(builder.finish())).is_err());
    }

    #[test]
    fn canonical_profile_returns_the_batch_untouched() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::FixedSizeBinary(2),
            true,
        )]));
        let ids = FixedSizeBinaryArray::try_from_iter([[1u8, 2]].iter()).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids)]).unwrap();
        let out = ArrowProfile::Canonical.apply(batch.clone()).unwrap();
        assert!(Arc::ptr_eq(&out.schema(), &batch.schema()));
        assert!(Arc::ptr_eq(out.column(0), batch.column(0)));
        assert_eq!(ArrowProfile::default(), ArrowProfile::Canonical);
        assert_eq!(ArrowProfile::Canonical.schema(&schema), *schema);
    }

    #[test]
    fn duckdb_profile_shares_columns_it_does_not_change() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("flags", DataType::UInt32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let out = ArrowProfile::DuckDb.apply(batch.clone()).unwrap();
        assert!(Arc::ptr_eq(out.column(0), batch.column(0)));
        assert_eq!(out.schema().field(1).data_type(), &DataType::Int32);

        // A batch with nothing to re-type is returned as-is.
        let plain = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["a"]))],
        )
        .unwrap();
        let out = ArrowProfile::DuckDb.apply(plain.clone()).unwrap();
        assert!(Arc::ptr_eq(&out.schema(), &plain.schema()));
    }

    #[test]
    fn duckdb_profile_keeps_field_names_nullability_and_metadata() {
        let field = Field::new("trace_id", DataType::FixedSizeBinary(16), false).with_metadata(
            [("k".to_string(), "v".to_string())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        );
        let schema = Schema::new_with_metadata(
            vec![field],
            [("s".to_string(), "m".to_string())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        );
        let mapped = ArrowProfile::DuckDb.schema(&schema);
        let out = mapped.field(0);
        assert_eq!(out.name(), "trace_id");
        assert!(!out.is_nullable());
        assert_eq!(out.data_type(), &DataType::Utf8);
        assert_eq!(out.metadata().get("k").map(String::as_str), Some("v"));
        assert_eq!(mapped.metadata(), schema.metadata());
    }

    #[test]
    fn empty_batch_is_retyped() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "duration",
            DataType::Duration(arrow_schema::TimeUnit::Nanosecond),
            false,
        )]));
        let out = ArrowProfile::DuckDb
            .apply(RecordBatch::new_empty(schema))
            .unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(out.schema().field(0).data_type(), &DataType::Int64);
    }
}
