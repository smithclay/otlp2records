use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use otlp2records::{
    otap::{ArrowPayload, ArrowPayloadType, BatchArrowRecords, OtapDecoder},
    transform_logs, InputFormat,
};
use prost::Message;

const INITIAL_BAR: &[u8] = include_bytes!("fixtures/otap/logs-initial.bar");
const REUSE_BAR: &[u8] = include_bytes!("fixtures/otap/logs-reuse.bar");
const INITIAL_OTLP: &[u8] = include_bytes!("fixtures/otap/logs-initial.otlp");
const REUSE_OTLP: &[u8] = include_bytes!("fixtures/otap/logs-reuse.otlp");
const ZSTD_BAR: &[u8] = include_bytes!("fixtures/otap/logs-zstd.bar");

#[test]
fn upstream_otap_initial_stream_matches_equivalent_otlp() {
    let actual = OtapDecoder::new().decode_logs(INITIAL_BAR).unwrap();
    let expected = transform_logs(INITIAL_OTLP, InputFormat::Protobuf).unwrap();
    assert_eq!(actual, expected);
}

#[cfg(feature = "otap-zstd")]
#[test]
fn upstream_default_zstd_stream_matches_equivalent_otlp() {
    let actual = OtapDecoder::new().decode_logs(ZSTD_BAR).unwrap();
    let expected = transform_logs(INITIAL_OTLP, InputFormat::Protobuf).unwrap();
    assert_eq!(actual, expected);
}

#[cfg(not(feature = "otap-zstd"))]
#[test]
fn rejects_zstd_stream_without_feature() {
    // The default build does not link Arrow's Zstandard backend, so decoding
    // upstream's zstd-compressed output must surface a clear error rather than
    // panic or silently mis-decode.
    let error = OtapDecoder::new().decode_logs(ZSTD_BAR).unwrap_err();
    assert!(
        error.to_string().contains("Arrow IPC"),
        "unexpected: {error}"
    );
}

#[test]
fn upstream_otap_reuses_schema_and_dictionary_state() {
    let first = BatchArrowRecords::decode(INITIAL_BAR).unwrap();
    let second = BatchArrowRecords::decode(REUSE_BAR).unwrap();
    assert_eq!(schema_ids(&first), schema_ids(&second));
    assert!(second
        .arrow_payloads
        .iter()
        .all(|payload| !payload.record.windows(6).any(|bytes| bytes == b"schema")));

    let mut decoder = OtapDecoder::new();
    decoder.decode_logs(INITIAL_BAR).unwrap();
    let actual = decoder.decode_logs(REUSE_BAR).unwrap();
    let expected = transform_logs(REUSE_OTLP, InputFormat::Protobuf).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn schema_reuse_requires_the_same_decoder_session() {
    let error = OtapDecoder::new().decode_logs(REUSE_BAR).unwrap_err();
    assert!(error.to_string().contains("initial Arrow IPC stream"));
}

#[test]
fn rejects_malformed_protobuf() {
    let error = OtapDecoder::new().decode_logs(&[0xff]).unwrap_err();
    assert!(error.to_string().contains("protobuf"));
}

#[test]
fn rejects_unknown_payload_type() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![ArrowPayload {
            schema_id: "x".into(),
            r#type: 999,
            record: vec![1],
        }],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(error.to_string().contains("unknown Arrow payload type 999"));
}

#[test]
fn rejects_missing_root_logs_payload() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(error.to_string().contains("missing required Logs payload"));
}

#[test]
fn rejects_invalid_otap_logs_schema() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(UInt32Array::from(vec![1]))]).unwrap();
    let mut record = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut record, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![ArrowPayload {
            schema_id: "invalid".into(),
            r#type: ArrowPayloadType::Logs as i32,
            record,
        }],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(error.to_string().contains("expected UInt16"));
}

#[test]
fn rejects_duplicate_payload() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![
            payload("a", ArrowPayloadType::ResourceAttrs, ipc_record()),
            payload("b", ArrowPayloadType::ResourceAttrs, ipc_record()),
        ],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(
        error.to_string().contains("duplicate"),
        "unexpected: {error}"
    );
}

#[test]
fn rejects_schema_id_reused_with_different_payload_type() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![
            payload("shared", ArrowPayloadType::ResourceAttrs, ipc_record()),
            payload("shared", ArrowPayloadType::ScopeAttrs, ipc_record()),
        ],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(
        error.to_string().contains("changed payload type"),
        "unexpected: {error}"
    );
}

#[test]
fn rejects_empty_schema_id() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![payload("", ArrowPayloadType::Logs, ipc_record())],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(
        error.to_string().contains("empty schema_id"),
        "unexpected: {error}"
    );
}

#[test]
fn rejects_empty_record() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![payload("a", ArrowPayloadType::Logs, vec![])],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(
        error.to_string().contains("empty Arrow IPC record"),
        "unexpected: {error}"
    );
}

#[test]
fn rejects_payload_not_valid_for_logs() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![payload("a", ArrowPayloadType::Spans, ipc_record())],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_logs(&bytes).unwrap_err();
    assert!(
        error.to_string().contains("not valid for logs"),
        "unexpected: {error}"
    );
}

fn payload(schema_id: &str, r#type: ArrowPayloadType, record: Vec<u8>) -> ArrowPayload {
    ArrowPayload {
        schema_id: schema_id.into(),
        r#type: r#type as i32,
        record,
    }
}

/// A minimal but valid Arrow IPC stream fragment. Its columns are irrelevant to
/// the envelope-level guards under test; it only needs to decode so the first
/// payload registers a stream before the duplicate / type-change checks fire.
fn ipc_record() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(UInt32Array::from(vec![1]))]).unwrap();
    let mut record = Vec::new();
    let mut writer = StreamWriter::try_new(&mut record, &batch.schema()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    record
}

fn schema_ids(batch: &BatchArrowRecords) -> Vec<(i32, &str)> {
    let mut ids = batch
        .arrow_payloads
        .iter()
        .map(|payload| (payload.r#type, payload.schema_id.as_str()))
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}
