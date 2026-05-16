//! JSON output serialization (NDJSON format)
//!
//! Serializes Arrow RecordBatches to newline-delimited JSON (NDJSON).
//! Each row becomes a JSON object with field names from the schema.

use arrow_array::RecordBatch;
use arrow_json::LineDelimitedWriter;

use crate::error::Error;

/// Serialize a RecordBatch to NDJSON format (newline-delimited JSON)
///
/// Each row is serialized as a JSON object with field names from the schema.
/// Each row ends with a newline character ('\n').
///
/// # Arguments
///
/// * `batch` - The RecordBatch to serialize
///
/// # Returns
///
/// * `Ok(Vec<u8>)` - The NDJSON data as bytes
/// * `Err(Error)` - If serialization fails
///
/// # Example
///
/// ```ignore
/// use arrow_array::RecordBatch;
/// use otlp2records::output::to_json;
///
/// let batch: RecordBatch = /* create batch */;
/// let json_bytes = to_json(&batch)?;
/// // Each line is a JSON object:
/// // {"field1": "value1", "field2": 42}
/// // {"field1": "value2", "field2": 43}
/// ```
pub fn to_json(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut buffer);
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let name_array = Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"]));
        let value_array = Arc::new(Int64Array::from(vec![1, 2, 3]));

        RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap()
    }

    #[test]
    fn test_to_json_basic() {
        let batch = create_test_batch();
        let result = to_json(&batch).unwrap();
        let json_str = String::from_utf8(result).unwrap();

        // Split into lines and verify each is valid JSON
        let lines: Vec<&str> = json_str.lines().collect();
        assert_eq!(lines.len(), 3);

        // Parse and verify each line
        let obj1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(obj1["name"], "alpha");
        assert_eq!(obj1["value"], 1);

        let obj2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(obj2["name"], "beta");
        assert_eq!(obj2["value"], 2);

        let obj3: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(obj3["name"], "gamma");
        assert_eq!(obj3["value"], 3);
    }

    #[test]
    fn test_to_json_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let name_array = Arc::new(StringArray::from(Vec::<&str>::new()));
        let value_array = Arc::new(Int64Array::from(Vec::<i64>::new()));

        let batch = RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap();
        let result = to_json(&batch).unwrap();
        let json_str = String::from_utf8(result).unwrap();

        // Empty batch should produce empty output (or just whitespace)
        assert!(json_str.trim().is_empty());
    }

    #[test]
    fn test_to_json_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let name_array = Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")]));
        let value_array = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch = RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap();
        let result = to_json(&batch).unwrap();
        let json_str = String::from_utf8(result).unwrap();

        let lines: Vec<&str> = json_str.lines().collect();
        assert_eq!(lines.len(), 3);

        // First row: both present
        let obj1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(obj1["name"], "alpha");
        assert_eq!(obj1["value"], 1);

        // Second row: name is null
        let obj2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert!(obj2["name"].is_null());
        assert_eq!(obj2["value"], 2);

        // Third row: value is null
        let obj3: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(obj3["name"], "gamma");
        assert!(obj3["value"].is_null());
    }
}
