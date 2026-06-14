use std::sync::Arc;

use arrow_array::{
    DurationNanosecondArray, FixedSizeBinaryArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt32Array,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use otlp2records::{
    otap::{ArrowPayload, ArrowPayloadType, BatchArrowRecords, OtapDecoder},
    transform_traces, InputFormat,
};
use prost::Message;

const INITIAL_BAR: &[u8] = include_bytes!("fixtures/otap/traces-initial.bar");
const REUSE_BAR: &[u8] = include_bytes!("fixtures/otap/traces-reuse.bar");
const INITIAL_OTLP: &[u8] = include_bytes!("fixtures/otap/traces-initial.otlp");
const REUSE_OTLP: &[u8] = include_bytes!("fixtures/otap/traces-reuse.otlp");

#[test]
fn upstream_otap_traces_match_equivalent_otlp() {
    let actual = OtapDecoder::new().decode_traces(INITIAL_BAR).unwrap();
    let expected = transform_traces(INITIAL_OTLP, InputFormat::Protobuf).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn upstream_otap_traces_reuse_stream_state() {
    let mut decoder = OtapDecoder::new();
    decoder.decode_traces(INITIAL_BAR).unwrap();
    let actual = decoder.decode_traces(REUSE_BAR).unwrap();
    let expected = transform_traces(REUSE_OTLP, InputFormat::Protobuf).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn trace_schema_reuse_requires_the_same_session() {
    let error = OtapDecoder::new().decode_traces(REUSE_BAR).unwrap_err();
    assert!(error.to_string().contains("initial Arrow IPC stream"));
}

#[test]
fn rejects_missing_root_spans_payload() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_traces(&bytes).unwrap_err();
    assert!(error.to_string().contains("missing required Spans payload"));
}

#[test]
fn rejects_noncanonical_optional_span_column_type() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(
                "start_time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "duration_time_unix_nano",
                DataType::Duration(TimeUnit::Nanosecond),
                false,
            ),
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("span_id", DataType::FixedSizeBinary(8), false),
            Field::new("name", DataType::Utf8, false),
            Field::new("kind", DataType::UInt32, true),
        ])),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![1])),
            Arc::new(DurationNanosecondArray::from(vec![1])),
            Arc::new(FixedSizeBinaryArray::try_from_iter([vec![1; 16]].into_iter()).unwrap()),
            Arc::new(FixedSizeBinaryArray::try_from_iter([vec![2; 8]].into_iter()).unwrap()),
            Arc::new(StringArray::from(vec!["span"])),
            Arc::new(UInt32Array::from(vec![1])),
        ],
    )
    .unwrap();

    let error = OtapDecoder::new()
        .decode_traces(&envelope(ArrowPayloadType::Spans, batch))
        .unwrap_err();
    assert!(error.to_string().contains("spans.kind has type UInt32"));
}

fn envelope(payload_type: ArrowPayloadType, batch: RecordBatch) -> Vec<u8> {
    let mut record = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut record, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![ArrowPayload {
            schema_id: "test".into(),
            r#type: payload_type as i32,
            record,
        }],
        headers: vec![],
    }
    .encode_to_vec()
}
