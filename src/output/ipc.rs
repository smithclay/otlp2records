//! Arrow IPC output serialization
//!
//! Serializes Arrow RecordBatches to IPC streaming format.
//! This format is useful for cross-language interoperability (Python, JavaScript, etc.)

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;

use crate::error::Error;

/// Serialize a RecordBatch to Arrow IPC streaming format
///
/// Uses the Arrow IPC streaming format which is suitable for:
/// - Cross-language interoperability (Python via pyarrow, JavaScript via arrow.js)
/// - Streaming data between processes
/// - Memory-efficient data transfer
///
/// # Arguments
///
/// * `batch` - The RecordBatch to serialize
///
/// # Returns
///
/// * `Ok(Vec<u8>)` - The IPC data as bytes
/// * `Err(Error)` - If serialization fails
///
/// # Example
///
/// ```ignore
/// use arrow_array::RecordBatch;
/// use otlp2records::output::to_ipc;
///
/// let batch: RecordBatch = /* create batch */;
/// let ipc_bytes = to_ipc(&batch)?;
/// // Can be read by pyarrow.ipc.open_stream() or similar
/// ```
pub fn to_ipc(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, StringArray};
    use arrow_ipc::reader::StreamReader;
    use arrow_schema::{DataType, Field, Schema};
    use std::io::Cursor;
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
    fn test_to_ipc_basic() {
        let batch = create_test_batch();
        let result = to_ipc(&batch).unwrap();

        // Verify it's not empty
        assert!(!result.is_empty());

        // Verify we can read it back
        let cursor = Cursor::new(result);
        let reader = StreamReader::try_new(cursor, None).unwrap();

        // Read back the batches
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();
        assert_eq!(batches.len(), 1);

        let read_batch = &batches[0];
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);

        // Verify schema
        assert_eq!(read_batch.schema().field(0).name(), "name");
        assert_eq!(read_batch.schema().field(1).name(), "value");
    }

    #[test]
    fn test_to_ipc_roundtrip() {
        let original_batch = create_test_batch();
        let ipc_bytes = to_ipc(&original_batch).unwrap();

        // Read back
        let cursor = Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();

        let read_batch = &batches[0];

        // Verify data integrity
        let name_col = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(name_col.value(0), "alpha");
        assert_eq!(name_col.value(1), "beta");
        assert_eq!(name_col.value(2), "gamma");

        assert_eq!(value_col.value(0), 1);
        assert_eq!(value_col.value(1), 2);
        assert_eq!(value_col.value(2), 3);
    }

    #[test]
    fn test_to_ipc_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let name_array = Arc::new(StringArray::from(Vec::<&str>::new()));
        let value_array = Arc::new(Int64Array::from(Vec::<i64>::new()));

        let batch = RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap();
        let result = to_ipc(&batch).unwrap();

        // Should still produce valid IPC (with schema at minimum)
        assert!(!result.is_empty());

        // Verify we can read it back
        let cursor = Cursor::new(result);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_to_ipc_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, true),
        ]));

        let name_array = Arc::new(StringArray::from(vec![Some("alpha"), None, Some("gamma")]));
        let value_array = Arc::new(Int64Array::from(vec![Some(1), Some(2), None]));

        let batch = RecordBatch::try_new(schema, vec![name_array, value_array]).unwrap();
        let result = to_ipc(&batch).unwrap();

        // Verify roundtrip preserves nulls
        let cursor = Cursor::new(result);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.map(|r| r.unwrap()).collect();

        let read_batch = &batches[0];
        let name_col = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert!(!name_col.is_null(0));
        assert!(name_col.is_null(1));
        assert!(!name_col.is_null(2));

        assert!(!value_col.is_null(0));
        assert!(!value_col.is_null(1));
        assert!(value_col.is_null(2));
    }
}
