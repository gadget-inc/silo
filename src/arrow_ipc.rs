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
