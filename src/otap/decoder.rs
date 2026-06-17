use std::{collections::HashMap, io::Cursor};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use prost::Message;

use crate::{api::MetricBatches, Error, Result};

use super::wire::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};

struct StreamState {
    payload_type: ArrowPayloadType,
    reader: StreamReader<Cursor<Vec<u8>>>,
}

impl StreamState {
    fn new(payload_type: ArrowPayloadType, bytes: Vec<u8>) -> Result<Self> {
        let reader = StreamReader::try_new(Cursor::new(bytes), None)
            .map_err(|error| Error::Otap(format!("invalid initial Arrow IPC stream: {error}")))?;
        Ok(Self {
            payload_type,
            reader,
        })
    }

    fn decode(&mut self, bytes: Vec<u8>) -> Result<RecordBatch> {
        *self.reader.get_mut() = Cursor::new(bytes);
        self.next_batch()
    }

    fn next_batch(&mut self) -> Result<RecordBatch> {
        self.reader
            .next()
            .ok_or_else(|| Error::Otap("Arrow IPC payload contained no record batch".into()))?
            .map_err(|error| Error::Otap(format!("invalid Arrow IPC record batch: {error}")))
    }
}

/// Stateful decoder for a canonical OTAP stream.
///
/// A decoder instance must be retained for the lifetime of an OTAP stream
/// because later `BatchArrowRecords` messages can omit schemas and reuse Arrow
/// dictionaries established by earlier messages.
#[derive(Default)]
pub struct OtapDecoder {
    streams: HashMap<String, StreamState>,
}

impl OtapDecoder {
    /// Creates an empty OTAP stream session.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes one canonical `BatchArrowRecords` message into normalized logs.
    pub fn decode_logs(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        let envelope = BatchArrowRecords::decode(bytes)?;
        self.decode_logs_envelope(envelope, bytes.len())
    }

    /// Decodes one already-deserialized `BatchArrowRecords` into normalized logs,
    /// reusing dictionary/schema state from earlier messages on this decoder.
    ///
    /// Used by the gRPC streaming path, where tonic has already decoded the
    /// protobuf envelope; `input_bytes` is the wire size (for builder sizing and
    /// admission accounting). `decode_logs` is the equivalent for raw bytes.
    pub fn decode_logs_message(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
    ) -> Result<RecordBatch> {
        self.decode_logs_envelope(envelope, input_bytes)
    }

    /// Decodes one canonical `BatchArrowRecords` message into normalized traces.
    pub fn decode_traces(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        let envelope = BatchArrowRecords::decode(bytes)?;
        self.decode_traces_envelope(envelope, bytes.len())
    }

    /// Decodes one already-deserialized `BatchArrowRecords` into normalized
    /// traces (the streaming counterpart of [`Self::decode_traces`]).
    pub fn decode_traces_message(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
    ) -> Result<RecordBatch> {
        self.decode_traces_envelope(envelope, input_bytes)
    }

    /// Decodes one canonical `BatchArrowRecords` message into normalized metrics.
    pub fn decode_metrics(&mut self, bytes: &[u8]) -> Result<MetricBatches> {
        let envelope = BatchArrowRecords::decode(bytes)?;
        self.decode_metrics_envelope(envelope)
    }

    /// Decodes one already-deserialized `BatchArrowRecords` into normalized
    /// metric shapes (the streaming counterpart of [`Self::decode_metrics`]).
    pub fn decode_metrics_message(&mut self, envelope: BatchArrowRecords) -> Result<MetricBatches> {
        self.decode_metrics_envelope(envelope)
    }

    fn decode_logs_envelope(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
    ) -> Result<RecordBatch> {
        let mut decoded = HashMap::new();

        for payload in envelope.arrow_payloads {
            let payload_type = ArrowPayloadType::try_from(payload.r#type).map_err(|_| {
                Error::Otap(format!("unknown Arrow payload type {}", payload.r#type))
            })?;
            if !matches!(
                payload_type,
                ArrowPayloadType::ResourceAttrs
                    | ArrowPayloadType::ScopeAttrs
                    | ArrowPayloadType::Logs
                    | ArrowPayloadType::LogAttrs
            ) {
                return Err(Error::Otap(format!(
                    "payload type {payload_type:?} is not valid for logs"
                )));
            }
            if decoded.contains_key(&payload_type) {
                return Err(Error::Otap(format!(
                    "duplicate {payload_type:?} payload in batch {}",
                    envelope.batch_id
                )));
            }

            let batch = self.decode_payload(payload_type, payload)?;
            let _ = decoded.insert(payload_type, batch);
        }

        let logs = decoded
            .remove(&ArrowPayloadType::Logs)
            .ok_or_else(|| Error::Otap("missing required Logs payload".into()))?;
        super::logs::normalize(
            logs,
            decoded.remove(&ArrowPayloadType::ResourceAttrs),
            decoded.remove(&ArrowPayloadType::ScopeAttrs),
            decoded.remove(&ArrowPayloadType::LogAttrs),
            input_bytes,
        )
    }

    fn decode_traces_envelope(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
    ) -> Result<RecordBatch> {
        let mut decoded = HashMap::new();
        for payload in envelope.arrow_payloads {
            let payload_type = ArrowPayloadType::try_from(payload.r#type).map_err(|_| {
                Error::Otap(format!("unknown Arrow payload type {}", payload.r#type))
            })?;
            if !matches!(
                payload_type,
                ArrowPayloadType::ResourceAttrs
                    | ArrowPayloadType::ScopeAttrs
                    | ArrowPayloadType::Spans
                    | ArrowPayloadType::SpanAttrs
                    | ArrowPayloadType::SpanEvents
                    | ArrowPayloadType::SpanLinks
                    | ArrowPayloadType::SpanEventAttrs
                    | ArrowPayloadType::SpanLinkAttrs
            ) {
                return Err(Error::Otap(format!(
                    "payload type {payload_type:?} is not valid for traces"
                )));
            }
            if decoded.contains_key(&payload_type) {
                return Err(Error::Otap(format!(
                    "duplicate {payload_type:?} payload in batch {}",
                    envelope.batch_id
                )));
            }
            let batch = self.decode_payload(payload_type, payload)?;
            let _ = decoded.insert(payload_type, batch);
        }
        let spans = decoded
            .remove(&ArrowPayloadType::Spans)
            .ok_or_else(|| Error::Otap("missing required Spans payload".into()))?;
        super::traces::normalize(
            spans,
            decoded.remove(&ArrowPayloadType::ResourceAttrs),
            decoded.remove(&ArrowPayloadType::ScopeAttrs),
            decoded.remove(&ArrowPayloadType::SpanAttrs),
            decoded.remove(&ArrowPayloadType::SpanEvents),
            decoded.remove(&ArrowPayloadType::SpanEventAttrs),
            decoded.remove(&ArrowPayloadType::SpanLinks),
            decoded.remove(&ArrowPayloadType::SpanLinkAttrs),
            input_bytes,
        )
    }

    fn decode_metrics_envelope(&mut self, envelope: BatchArrowRecords) -> Result<MetricBatches> {
        let mut decoded = HashMap::new();
        for payload in envelope.arrow_payloads {
            let payload_type = ArrowPayloadType::try_from(payload.r#type).map_err(|_| {
                Error::Otap(format!("unknown Arrow payload type {}", payload.r#type))
            })?;
            if !matches!(
                payload_type,
                ArrowPayloadType::ResourceAttrs
                    | ArrowPayloadType::ScopeAttrs
                    | ArrowPayloadType::UnivariateMetrics
                    | ArrowPayloadType::NumberDataPoints
                    | ArrowPayloadType::SummaryDataPoints
                    | ArrowPayloadType::HistogramDataPoints
                    | ArrowPayloadType::ExpHistogramDataPoints
                    | ArrowPayloadType::NumberDpAttrs
                    | ArrowPayloadType::SummaryDpAttrs
                    | ArrowPayloadType::HistogramDpAttrs
                    | ArrowPayloadType::ExpHistogramDpAttrs
                    | ArrowPayloadType::NumberDpExemplars
                    | ArrowPayloadType::HistogramDpExemplars
                    | ArrowPayloadType::ExpHistogramDpExemplars
                    | ArrowPayloadType::NumberDpExemplarAttrs
                    | ArrowPayloadType::HistogramDpExemplarAttrs
                    | ArrowPayloadType::ExpHistogramDpExemplarAttrs
                    | ArrowPayloadType::MetricAttrs
            ) {
                if payload_type == ArrowPayloadType::MultivariateMetrics {
                    return Err(Error::Otap(
                        "MultivariateMetrics is not supported because upstream has no canonical schema"
                            .into(),
                    ));
                }
                return Err(Error::Otap(format!(
                    "payload type {payload_type:?} is not valid for metrics"
                )));
            }
            if decoded.contains_key(&payload_type) {
                return Err(Error::Otap(format!(
                    "duplicate {payload_type:?} payload in batch {}",
                    envelope.batch_id
                )));
            }
            let batch = self.decode_payload(payload_type, payload)?;
            let _ = decoded.insert(payload_type, batch);
        }
        let metrics = decoded
            .remove(&ArrowPayloadType::UnivariateMetrics)
            .ok_or_else(|| Error::Otap("missing required UnivariateMetrics payload".into()))?;
        super::metrics::normalize(
            metrics,
            decoded.remove(&ArrowPayloadType::ResourceAttrs),
            decoded.remove(&ArrowPayloadType::ScopeAttrs),
            decoded.remove(&ArrowPayloadType::MetricAttrs),
            decoded.remove(&ArrowPayloadType::NumberDataPoints),
            decoded.remove(&ArrowPayloadType::SummaryDataPoints),
            decoded.remove(&ArrowPayloadType::HistogramDataPoints),
            decoded.remove(&ArrowPayloadType::ExpHistogramDataPoints),
            decoded.remove(&ArrowPayloadType::NumberDpAttrs),
            decoded.remove(&ArrowPayloadType::SummaryDpAttrs),
            decoded.remove(&ArrowPayloadType::HistogramDpAttrs),
            decoded.remove(&ArrowPayloadType::ExpHistogramDpAttrs),
            decoded.remove(&ArrowPayloadType::NumberDpExemplars),
            decoded.remove(&ArrowPayloadType::HistogramDpExemplars),
            decoded.remove(&ArrowPayloadType::ExpHistogramDpExemplars),
            decoded.remove(&ArrowPayloadType::NumberDpExemplarAttrs),
            decoded.remove(&ArrowPayloadType::HistogramDpExemplarAttrs),
            decoded.remove(&ArrowPayloadType::ExpHistogramDpExemplarAttrs),
        )
    }

    fn decode_payload(
        &mut self,
        payload_type: ArrowPayloadType,
        payload: ArrowPayload,
    ) -> Result<RecordBatch> {
        if payload.schema_id.is_empty() {
            return Err(Error::Otap("Arrow payload has an empty schema_id".into()));
        }
        if payload.record.is_empty() {
            return Err(Error::Otap(format!(
                "{payload_type:?} payload has an empty Arrow IPC record"
            )));
        }

        if let Some(stream) = self.streams.get_mut(&payload.schema_id) {
            if stream.payload_type != payload_type {
                return Err(Error::Otap(format!(
                    "schema_id {:?} changed payload type from {:?} to {:?}",
                    payload.schema_id, stream.payload_type, payload_type
                )));
            }
            return stream.decode(payload.record);
        }

        self.streams
            .retain(|_, stream| stream.payload_type != payload_type);
        let mut stream = StreamState::new(payload_type, payload.record)?;
        let batch = stream.next_batch()?;
        let _ = self.streams.insert(payload.schema_id, stream);
        Ok(batch)
    }
}
