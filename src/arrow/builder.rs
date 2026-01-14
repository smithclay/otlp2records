//! Arrow RecordBatch builder
//!
//! Converts VRL Values to Arrow RecordBatches using schema-driven building.

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use vrl::value::{KeyString, Value};

/// Converts a slice of VRL Values to an Arrow RecordBatch.
///
/// # Arguments
///
/// * `values` - Slice of VRL Object values to convert
/// * `schema` - Arrow schema defining the expected fields and types
///
/// # Returns
///
/// A RecordBatch containing the converted data, or an ArrowError if conversion fails.
///
/// # Type Mapping
///
/// VRL types are converted to Arrow types as follows:
/// - `Value::Integer` -> Int64/Int32/TimestampMillisecond (depending on schema)
/// - `Value::Float` -> Float64
/// - `Value::Boolean` -> Boolean
/// - `Value::Bytes` -> Utf8 (String)
/// - `Value::Null` -> null in the appropriate column
///
/// # Example
///
/// ```ignore
/// use vrl::value::{Value, ObjectMap};
/// use arrow::datatypes::{Schema, Field, DataType};
///
/// let schema = Schema::new(vec![
///     Field::new("name", DataType::Utf8, false),
///     Field::new("count", DataType::Int64, true),
/// ]);
///
/// let mut map = ObjectMap::new();
/// map.insert("name".into(), Value::Bytes("test".into()));
/// map.insert("count".into(), Value::Integer(42));
/// let values = vec![Value::Object(map)];
///
/// let batch = values_to_arrow(&values, &schema)?;
/// ```
pub fn values_to_arrow(values: &[Value], schema: &Schema) -> Result<RecordBatch, ArrowError> {
    let num_rows = values.len();
    let num_fields = schema.fields().len();

    // Pre-allocate column builders based on schema types
    let mut builders: Vec<ColumnBuilder> = schema
        .fields()
        .iter()
        .map(|field| ColumnBuilder::new(field.data_type(), num_rows))
        .collect();

    // Build field name lookup for extraction
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    // Iterate over values and populate builders
    for value in values {
        match value {
            Value::Object(map) => {
                for (idx, field_name) in field_names.iter().enumerate() {
                    let key: KeyString = (*field_name).into();
                    let field_value = map.get(&key);
                    builders[idx].append(field_value)?;
                }
            }
            _ => {
                // Non-object values: append nulls for all fields
                for builder in &mut builders {
                    builder.append(None)?;
                }
            }
        }
    }

    // Build arrays from builders
    let arrays: Vec<ArrayRef> = builders
        .into_iter()
        .zip(schema.fields().iter())
        .map(|(builder, field)| builder.finish(field.data_type()))
        .collect();

    // Validate we have the right number of columns
    if arrays.len() != num_fields {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Expected {} columns but got {}",
            num_fields,
            arrays.len()
        )));
    }

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
}

/// Internal builder enum for different Arrow column types.
///
/// This allows dynamic column building based on schema without complex generics.
enum ColumnBuilder {
    Timestamp(TimestampMillisecondBuilder),
    Int64(Int64Builder),
    Int32(Int32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    String(StringBuilder),
}

impl ColumnBuilder {
    /// Create a new column builder for the given Arrow data type.
    ///
    /// # Panics
    ///
    /// Panics if the data type is not supported. Supported types:
    /// - Timestamp(Millisecond, _)
    /// - Int64
    /// - Int32
    /// - Float64
    /// - Boolean
    /// - Utf8
    fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                ColumnBuilder::Timestamp(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Utf8 => {
                ColumnBuilder::String(StringBuilder::with_capacity(capacity, capacity * 32))
            }
            unsupported => {
                panic!(
                    "Unsupported Arrow data type: {unsupported:?}. Supported types: Timestamp(Millisecond), Int64, Int32, Float64, Boolean, Utf8"
                );
            }
        }
    }

    /// Append a VRL value to the builder.
    fn append(&mut self, value: Option<&Value>) -> Result<(), ArrowError> {
        match self {
            ColumnBuilder::Timestamp(builder) => append_timestamp(builder, value),
            ColumnBuilder::Int64(builder) => append_int64(builder, value),
            ColumnBuilder::Int32(builder) => append_int32(builder, value),
            ColumnBuilder::Float64(builder) => append_float64(builder, value),
            ColumnBuilder::Boolean(builder) => append_boolean(builder, value),
            ColumnBuilder::String(builder) => append_string(builder, value),
        }
    }

    /// Finish building and return the ArrayRef.
    fn finish(self, data_type: &DataType) -> ArrayRef {
        match self {
            ColumnBuilder::Timestamp(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int64(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int32(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float64(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::Boolean(mut builder) => Arc::new(builder.finish()),
            ColumnBuilder::String(mut builder) => {
                // Handle different string-like types
                match data_type {
                    DataType::Utf8 => Arc::new(builder.finish()),
                    _ => Arc::new(builder.finish()),
                }
            }
        }
    }
}

/// Append a VRL value to a TimestampMillisecondBuilder.
fn append_timestamp(
    builder: &mut TimestampMillisecondBuilder,
    value: Option<&Value>,
) -> Result<(), ArrowError> {
    match value {
        Some(Value::Integer(i)) => {
            builder.append_value(*i);
            Ok(())
        }
        Some(Value::Float(f)) => {
            // Convert float to integer milliseconds
            builder.append_value(f.into_inner() as i64);
            Ok(())
        }
        Some(Value::Null) | None => {
            builder.append_null();
            Ok(())
        }
        Some(other) => Err(ArrowError::InvalidArgumentError(format!(
            "Cannot convert {:?} to timestamp",
            value_type_name(other)
        ))),
    }
}

/// Append a VRL value to an Int64Builder.
fn append_int64(builder: &mut Int64Builder, value: Option<&Value>) -> Result<(), ArrowError> {
    match value {
        Some(Value::Integer(i)) => {
            builder.append_value(*i);
            Ok(())
        }
        Some(Value::Float(f)) => {
            // Convert float to integer
            builder.append_value(f.into_inner() as i64);
            Ok(())
        }
        Some(Value::Null) | None => {
            builder.append_null();
            Ok(())
        }
        Some(other) => Err(ArrowError::InvalidArgumentError(format!(
            "Cannot convert {:?} to int64",
            value_type_name(other)
        ))),
    }
}

/// Append a VRL value to an Int32Builder.
fn append_int32(builder: &mut Int32Builder, value: Option<&Value>) -> Result<(), ArrowError> {
    match value {
        Some(Value::Integer(i)) => {
            let val = i32::try_from(*i).map_err(|_| {
                ArrowError::InvalidArgumentError(format!(
                    "Integer {} is out of range for i32 (valid range: {} to {})",
                    i,
                    i32::MIN,
                    i32::MAX
                ))
            })?;
            builder.append_value(val);
            Ok(())
        }
        Some(Value::Float(f)) => {
            let float_val = f.into_inner();
            if float_val.is_nan() || float_val.is_infinite() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Cannot convert non-finite float {float_val} to i32"
                )));
            }
            if float_val < (i32::MIN as f64) || float_val > (i32::MAX as f64) {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Float {} is out of range for i32 (valid range: {} to {})",
                    float_val,
                    i32::MIN,
                    i32::MAX
                )));
            }
            builder.append_value(float_val as i32);
            Ok(())
        }
        Some(Value::Null) | None => {
            builder.append_null();
            Ok(())
        }
        Some(other) => Err(ArrowError::InvalidArgumentError(format!(
            "Cannot convert {:?} to int32",
            value_type_name(other)
        ))),
    }
}

/// Append a VRL value to a Float64Builder.
fn append_float64(builder: &mut Float64Builder, value: Option<&Value>) -> Result<(), ArrowError> {
    match value {
        Some(Value::Float(f)) => {
            builder.append_value(f.into_inner());
            Ok(())
        }
        Some(Value::Integer(i)) => {
            // Convert integer to float
            builder.append_value(*i as f64);
            Ok(())
        }
        Some(Value::Null) | None => {
            builder.append_null();
            Ok(())
        }
        Some(other) => Err(ArrowError::InvalidArgumentError(format!(
            "Cannot convert {:?} to float64",
            value_type_name(other)
        ))),
    }
}

/// Append a VRL value to a BooleanBuilder.
fn append_boolean(builder: &mut BooleanBuilder, value: Option<&Value>) -> Result<(), ArrowError> {
    match value {
        Some(Value::Boolean(b)) => {
            builder.append_value(*b);
            Ok(())
        }
        Some(Value::Null) | None => {
            builder.append_null();
            Ok(())
        }
        Some(other) => Err(ArrowError::InvalidArgumentError(format!(
            "Cannot convert {:?} to boolean",
            value_type_name(other)
        ))),
    }
}

/// Append a VRL value to a StringBuilder.
///
/// # Note on UTF-8 handling
///
/// When converting `Value::Bytes` to strings, invalid UTF-8 sequences are replaced
/// with the Unicode replacement character (U+FFFD) using lossy conversion. This
/// ensures the function never fails for byte data, but may result in data modification
/// if the input contains invalid UTF-8.
fn append_string(builder: &mut StringBuilder, value: Option<&Value>) -> Result<(), ArrowError> {
    match value {
        Some(Value::Bytes(b)) => {
            // Note: Uses lossy conversion - invalid UTF-8 becomes U+FFFD
            let s = String::from_utf8_lossy(b);
            builder.append_value(&s);
            Ok(())
        }
        Some(Value::Integer(i)) => {
            // Convert integer to string
            builder.append_value(i.to_string());
            Ok(())
        }
        Some(Value::Float(f)) => {
            // Convert float to string
            builder.append_value(f.to_string());
            Ok(())
        }
        Some(Value::Boolean(b)) => {
            // Convert boolean to string
            builder.append_value(b.to_string());
            Ok(())
        }
        Some(Value::Null) | None => {
            builder.append_null();
            Ok(())
        }
        Some(Value::Object(_)) | Some(Value::Array(_)) => {
            // Serialize complex types as JSON
            let json_str = serde_json::to_string(value.unwrap())
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
            builder.append_value(&json_str);
            Ok(())
        }
        Some(Value::Timestamp(ts)) => {
            // Convert timestamp to ISO string
            builder.append_value(ts.to_string());
            Ok(())
        }
        Some(Value::Regex(r)) => {
            builder.append_value(r.to_string());
            Ok(())
        }
    }
}

/// Get a human-readable name for a VRL Value type.
fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Bytes(_) => "bytes",
        Value::Integer(_) => "integer",
        Value::Float(_) => "float",
        Value::Boolean(_) => "boolean",
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::Timestamp(_) => "timestamp",
        Value::Regex(_) => "regex",
        Value::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMillisecondArray,
    };
    use arrow::datatypes::{Field, TimeUnit};
    use bytes::Bytes;
    use ordered_float::NotNan;
    use vrl::value::ObjectMap;

    fn make_object(pairs: Vec<(&str, Value)>) -> Value {
        let mut map = ObjectMap::new();
        for (k, v) in pairs {
            map.insert(k.into(), v);
        }
        Value::Object(map)
    }

    #[test]
    fn test_empty_values() {
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, true)]);

        let batch = values_to_arrow(&[], &schema).unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn test_string_column() {
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);

        let values = vec![
            make_object(vec![("name", Value::Bytes(Bytes::from("alice")))]),
            make_object(vec![("name", Value::Bytes(Bytes::from("bob")))]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "alice");
        assert_eq!(col.value(1), "bob");
    }

    #[test]
    fn test_int64_column() {
        let schema = Schema::new(vec![Field::new("count", DataType::Int64, false)]);

        let values = vec![
            make_object(vec![("count", Value::Integer(100))]),
            make_object(vec![("count", Value::Integer(200))]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 100);
        assert_eq!(col.value(1), 200);
    }

    #[test]
    fn test_int32_column() {
        let schema = Schema::new(vec![Field::new("severity", DataType::Int32, false)]);

        let values = vec![
            make_object(vec![("severity", Value::Integer(5))]),
            make_object(vec![("severity", Value::Integer(10))]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 5);
        assert_eq!(col.value(1), 10);
    }

    #[test]
    fn test_float64_column() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, false)]);

        let values = vec![
            make_object(vec![("value", Value::Float(NotNan::new(3.14).unwrap()))]),
            make_object(vec![("value", Value::Float(NotNan::new(2.71).unwrap()))]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 3.14).abs() < 0.001);
        assert!((col.value(1) - 2.71).abs() < 0.001);
    }

    #[test]
    fn test_boolean_column() {
        let schema = Schema::new(vec![Field::new("active", DataType::Boolean, false)]);

        let values = vec![
            make_object(vec![("active", Value::Boolean(true))]),
            make_object(vec![("active", Value::Boolean(false))]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.value(0));
        assert!(!col.value(1));
    }

    #[test]
    fn test_timestamp_column() {
        let schema = Schema::new(vec![Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]);

        let values = vec![
            make_object(vec![("timestamp", Value::Integer(1700000000000))]),
            make_object(vec![("timestamp", Value::Integer(1700000001000))]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(col.value(0), 1700000000000);
        assert_eq!(col.value(1), 1700000001000);
    }

    #[test]
    fn test_null_values() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]);

        let values = vec![
            make_object(vec![
                ("name", Value::Bytes(Bytes::from("test"))),
                ("count", Value::Integer(42)),
            ]),
            make_object(vec![("name", Value::Null), ("count", Value::Null)]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "test");
        assert!(name_col.is_null(1));

        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 42);
        assert!(count_col.is_null(1));
    }

    #[test]
    fn test_missing_field_becomes_null() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("optional", DataType::Utf8, true),
        ]);

        let values = vec![
            make_object(vec![("name", Value::Bytes(Bytes::from("test")))]),
            // Missing 'optional' field
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let optional_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(optional_col.is_null(0));
    }

    #[test]
    fn test_integer_to_float_coercion() {
        let schema = Schema::new(vec![Field::new("value", DataType::Float64, false)]);

        let values = vec![make_object(vec![("value", Value::Integer(42))])];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((col.value(0) - 42.0).abs() < 0.001);
    }

    #[test]
    fn test_json_object_to_string() {
        let schema = Schema::new(vec![Field::new("attrs", DataType::Utf8, true)]);

        let values = vec![make_object(vec![(
            "attrs",
            make_object(vec![
                ("key1", Value::Bytes(Bytes::from("value1"))),
                ("key2", Value::Integer(42)),
            ]),
        )])];

        let batch = values_to_arrow(&values, &schema).unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let json_str = col.value(0);
        // Verify it's valid JSON
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert!(parsed.is_object());
    }

    #[test]
    fn test_multi_column_batch() {
        let schema = Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("severity", DataType::Int32, false),
            Field::new("value", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]);

        let values = vec![
            make_object(vec![
                ("timestamp", Value::Integer(1700000000000)),
                ("service_name", Value::Bytes(Bytes::from("my-service"))),
                ("severity", Value::Integer(5)),
                ("value", Value::Float(NotNan::new(99.9).unwrap())),
                ("active", Value::Boolean(true)),
            ]),
            make_object(vec![
                ("timestamp", Value::Integer(1700000001000)),
                ("service_name", Value::Bytes(Bytes::from("other-service"))),
                ("severity", Value::Integer(3)),
                ("value", Value::Null),
                ("active", Value::Boolean(false)),
            ]),
        ];

        let batch = values_to_arrow(&values, &schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);

        // Verify timestamp column
        let ts_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), 1700000000000);

        // Verify string column
        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "my-service");

        // Verify nullable float column
        let value_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(!value_col.is_null(0));
        assert!(value_col.is_null(1));
    }

    #[test]
    fn test_non_object_value_produces_nulls() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
        ]);

        // Pass a non-object value (just an integer)
        let values = vec![Value::Integer(42)];

        let batch = values_to_arrow(&values, &schema).unwrap();

        // All columns should be null
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }

    #[test]
    fn test_schema_field_order_is_preserved() {
        // Schema with fields in specific order
        let schema = Schema::new(vec![
            Field::new("c", DataType::Utf8, false),
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let values = vec![make_object(vec![
            ("a", Value::Bytes(Bytes::from("alpha"))),
            ("b", Value::Bytes(Bytes::from("beta"))),
            ("c", Value::Bytes(Bytes::from("gamma"))),
        ])];

        let batch = values_to_arrow(&values, &schema).unwrap();

        // Columns should match schema order, not object key order
        let col0 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col1 = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col2 = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(col0.value(0), "gamma"); // c
        assert_eq!(col1.value(0), "alpha"); // a
        assert_eq!(col2.value(0), "beta"); // b
    }
}
