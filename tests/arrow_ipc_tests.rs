//! Tests for Arrow IPC serialization utilities.

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use silo::arrow_ipc::{batch_to_ipc, ipc_to_batches, schema_to_ipc};
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
