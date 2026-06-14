use std::sync::Arc;

use arrow_array::{RecordBatch, StringArray, UInt16Array, UInt32Array, UInt8Array};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use otlp2records::{
    otap::{ArrowPayload, ArrowPayloadType, BatchArrowRecords, OtapDecoder},
    transform_metrics, InputFormat, MetricBatches,
};
use prost::Message;

const INITIAL_BAR: &[u8] = include_bytes!("fixtures/otap/metrics-initial.bar");
const REUSE_BAR: &[u8] = include_bytes!("fixtures/otap/metrics-reuse.bar");
const INITIAL_OTLP: &[u8] = include_bytes!("fixtures/otap/metrics-initial.otlp");
const REUSE_OTLP: &[u8] = include_bytes!("fixtures/otap/metrics-reuse.otlp");

#[test]
fn upstream_otap_metrics_match_equivalent_otlp() {
    let actual = OtapDecoder::new().decode_metrics(INITIAL_BAR).unwrap();
    let expected = transform_metrics(INITIAL_OTLP, InputFormat::Protobuf).unwrap();
    assert_batches_eq(&actual, &expected);
}

#[test]
fn upstream_otap_metrics_reuse_stream_state() {
    let mut decoder = OtapDecoder::new();
    decoder.decode_metrics(INITIAL_BAR).unwrap();
    let actual = decoder.decode_metrics(REUSE_BAR).unwrap();
    let expected = transform_metrics(REUSE_OTLP, InputFormat::Protobuf).unwrap();
    assert_batches_eq(&actual, &expected);
}

#[test]
fn metric_schema_reuse_requires_the_same_session() {
    let error = OtapDecoder::new().decode_metrics(REUSE_BAR).unwrap_err();
    assert!(error.to_string().contains("initial Arrow IPC stream"));
}

#[test]
fn rejects_missing_root_metrics_payload() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_metrics(&bytes).unwrap_err();
    assert!(error
        .to_string()
        .contains("missing required UnivariateMetrics"));
}

#[test]
fn rejects_multivariate_metrics_until_upstream_defines_a_schema() {
    let bytes = BatchArrowRecords {
        batch_id: 1,
        arrow_payloads: vec![ArrowPayload {
            schema_id: "unsupported".into(),
            r#type: ArrowPayloadType::MultivariateMetrics as i32,
            record: vec![],
        }],
        headers: vec![],
    }
    .encode_to_vec();
    let error = OtapDecoder::new().decode_metrics(&bytes).unwrap_err();
    assert!(error
        .to_string()
        .contains("upstream has no canonical schema"));
}

#[test]
fn rejects_noncanonical_optional_metric_column_type() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt16, false),
            Field::new("metric_type", DataType::UInt8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::UInt32, true),
        ])),
        vec![
            Arc::new(UInt16Array::from(vec![1])),
            Arc::new(UInt8Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["metric"])),
            Arc::new(UInt32Array::from(vec![1])),
        ],
    )
    .unwrap();

    let error = OtapDecoder::new()
        .decode_metrics(&envelope(ArrowPayloadType::UnivariateMetrics, batch))
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("metrics.description has type UInt32"));
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

fn assert_batches_eq(actual: &MetricBatches, expected: &MetricBatches) {
    assert_eq!(actual.gauge, expected.gauge);
    assert_eq!(actual.sum, expected.sum);
    assert_eq!(actual.histogram, expected.histogram);
    assert_eq!(actual.exp_histogram, expected.exp_histogram);
    assert_eq!(actual.skipped.summaries, expected.skipped.summaries);
    assert_eq!(actual.skipped.nan_values, expected.skipped.nan_values);
    assert_eq!(
        actual.skipped.infinity_values,
        expected.skipped.infinity_values
    );
    assert_eq!(
        actual.skipped.missing_values,
        expected.skipped.missing_values
    );
}
