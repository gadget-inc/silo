//! Arrow IPC serialization utilities for streaming query results over gRPC.
//!
//! This module provides functions to serialize Arrow RecordBatches to IPC format
//! for efficient transport and deserialize them on the receiving end.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::error::Result as DfResult;
use std::io::Cursor;

/// Serialize an Arrow schema to IPC stream format.
/// This produces just the schema message without any record batches.
pub fn schema_to_ipc(schema: &SchemaRef) -> DfResult<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Serialize a RecordBatch to IPC stream format.
/// Each batch is serialized independently for streaming.
pub fn batch_to_ipc(batch: &RecordBatch) -> DfResult<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserialize IPC stream data to get the schema and record batches.
/// Returns the schema and a vector of record batches.
pub fn ipc_to_batches(data: &[u8]) -> DfResult<(SchemaRef, Vec<RecordBatch>)> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    Ok((schema, batches))
}

/// Deserialize IPC stream data and return just the batches.
/// Useful when schema is already known.
pub fn ipc_to_batches_only(data: &[u8]) -> DfResult<Vec<RecordBatch>> {
    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None)?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_roundtrip_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let ipc_data = batch_to_ipc(&batch).unwrap();
        let (decoded_schema, decoded_batches) = ipc_to_batches(&ipc_data).unwrap();

        assert_eq!(decoded_schema, schema);
        assert_eq!(decoded_batches.len(), 1);
        assert_eq!(decoded_batches[0].num_rows(), 3);
    }

    #[test]
    fn test_schema_only() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("count", DataType::Int64, false),
        ]));

        let ipc_data = schema_to_ipc(&schema).unwrap();
        let (decoded_schema, decoded_batches) = ipc_to_batches(&ipc_data).unwrap();

        assert_eq!(decoded_schema, schema);
        assert!(decoded_batches.is_empty());
    }
}
