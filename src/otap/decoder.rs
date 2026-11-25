use std::{collections::HashMap, io::Cursor};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use prost::Message;

use crate::{
    api::{MetricBatches, MetricShapeBatch},
    batch::MetricsSelection,
    options::{MetricShape, Plan, TransformOptions},
    schema::{logs_schema_arc, traces_schema_arc},
    Error, Result,
};

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
///
/// Any error poisons the decoder: a failed message may have advanced some of
/// its payload streams and not others, so every later call fails too. Start a
/// new decoder for a new stream.
#[derive(Default)]
pub struct OtapDecoder {
    streams: HashMap<String, StreamState>,
    poisoned: Option<String>,
}

struct Signal {
    name: &'static str,
    root: ArrowPayloadType,
    accepts: &'static [ArrowPayloadType],
}

const LOGS: Signal = Signal {
    name: "logs",
    root: ArrowPayloadType::Logs,
    accepts: &[
        ArrowPayloadType::ResourceAttrs,
        ArrowPayloadType::ScopeAttrs,
        ArrowPayloadType::LogAttrs,
    ],
};

const TRACES: Signal = Signal {
    name: "traces",
    root: ArrowPayloadType::Spans,
    accepts: &[
        ArrowPayloadType::ResourceAttrs,
        ArrowPayloadType::ScopeAttrs,
        ArrowPayloadType::SpanAttrs,
        ArrowPayloadType::SpanEvents,
        ArrowPayloadType::SpanLinks,
        ArrowPayloadType::SpanEventAttrs,
        ArrowPayloadType::SpanLinkAttrs,
    ],
};

const METRICS: Signal = Signal {
    name: "metrics",
    root: ArrowPayloadType::UnivariateMetrics,
    accepts: &[
        ArrowPayloadType::ResourceAttrs,
        ArrowPayloadType::ScopeAttrs,
        ArrowPayloadType::NumberDataPoints,
        ArrowPayloadType::SummaryDataPoints,
        ArrowPayloadType::HistogramDataPoints,
        ArrowPayloadType::ExpHistogramDataPoints,
        ArrowPayloadType::NumberDpAttrs,
        ArrowPayloadType::SummaryDpAttrs,
        ArrowPayloadType::HistogramDpAttrs,
        ArrowPayloadType::ExpHistogramDpAttrs,
        ArrowPayloadType::NumberDpExemplars,
        ArrowPayloadType::HistogramDpExemplars,
        ArrowPayloadType::ExpHistogramDpExemplars,
        ArrowPayloadType::NumberDpExemplarAttrs,
        ArrowPayloadType::HistogramDpExemplarAttrs,
        ArrowPayloadType::ExpHistogramDpExemplarAttrs,
        ArrowPayloadType::MetricAttrs,
    ],
};

struct Payloads(HashMap<ArrowPayloadType, RecordBatch>);

impl Payloads {
    fn take(&mut self, payload_type: ArrowPayloadType) -> Option<RecordBatch> {
        self.0.remove(&payload_type)
    }
}

impl OtapDecoder {
    /// Creates an empty OTAP stream session.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes one canonical `BatchArrowRecords` message into normalized logs.
    pub fn decode_logs(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        self.decode_logs_with_options(bytes, &TransformOptions::new())
    }

    /// Like [`Self::decode_logs`], building only the columns and rows
    /// `options` asks for. The whole message is still decoded, so the stream
    /// state stays complete for later messages.
    pub fn decode_logs_with_options(
        &mut self,
        bytes: &[u8],
        options: &TransformOptions,
    ) -> Result<RecordBatch> {
        // Planned before decoding, so a caller's bad option never poisons the
        // decoder.
        let plan = options.plan(&logs_schema_arc())?;
        self.guarded(|decoder| {
            let envelope = BatchArrowRecords::decode(bytes)?;
            decoder.logs(envelope, bytes.len(), &plan)
        })
    }

    /// Decodes one already-deserialized `BatchArrowRecords` into normalized logs.
    /// `input_bytes` is its wire size, used to size the output builders.
    pub fn decode_logs_message(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
    ) -> Result<RecordBatch> {
        let plan = Plan::all(&logs_schema_arc());
        self.guarded(|decoder| decoder.logs(envelope, input_bytes, &plan))
    }

    /// Decodes one canonical `BatchArrowRecords` message into normalized traces.
    pub fn decode_traces(&mut self, bytes: &[u8]) -> Result<RecordBatch> {
        self.decode_traces_with_options(bytes, &TransformOptions::new())
    }

    /// Like [`Self::decode_traces`], building only the columns and rows
    /// `options` asks for.
    pub fn decode_traces_with_options(
        &mut self,
        bytes: &[u8],
        options: &TransformOptions,
    ) -> Result<RecordBatch> {
        let plan = options.plan(&traces_schema_arc())?;
        self.guarded(|decoder| {
            let envelope = BatchArrowRecords::decode(bytes)?;
            decoder.traces(envelope, bytes.len(), &plan)
        })
    }

    /// Decodes one already-deserialized `BatchArrowRecords` into normalized
    /// traces (the streaming counterpart of [`Self::decode_traces`]).
    pub fn decode_traces_message(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
    ) -> Result<RecordBatch> {
        let plan = Plan::all(&traces_schema_arc());
        self.guarded(|decoder| decoder.traces(envelope, input_bytes, &plan))
    }

    /// Decodes one canonical `BatchArrowRecords` message into normalized metrics.
    pub fn decode_metrics(&mut self, bytes: &[u8]) -> Result<MetricBatches> {
        self.guarded(|decoder| {
            decoder.metrics(BatchArrowRecords::decode(bytes)?, MetricsSelection::All)
        })
    }

    /// Decodes one message into one metric shape, narrowed by `options`
    /// (column indexes are the shape's). The other shapes are not built, and a
    /// shape the message has none of is an empty batch.
    pub fn decode_metric_shape(
        &mut self,
        bytes: &[u8],
        shape: MetricShape,
        options: &TransformOptions,
    ) -> Result<MetricShapeBatch> {
        let plan = options.plan(&shape.schema_ref())?;
        self.guarded(|decoder| {
            let envelope = BatchArrowRecords::decode(bytes)?;
            decoder.metrics(envelope, MetricsSelection::Shape(shape, &plan))
        })
        .map(|batches| batches.into_shape(shape))
    }

    /// Decodes one already-deserialized `BatchArrowRecords` into normalized
    /// metric shapes (the streaming counterpart of [`Self::decode_metrics`]).
    pub fn decode_metrics_message(&mut self, envelope: BatchArrowRecords) -> Result<MetricBatches> {
        self.guarded(|decoder| decoder.metrics(envelope, MetricsSelection::All))
    }

    fn guarded<T>(&mut self, decode: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if let Some(cause) = &self.poisoned {
            return Err(Error::Otap(format!(
                "OTAP decoder is poisoned by an earlier error ({cause}); start a new decoder"
            )));
        }
        decode(self).inspect_err(|error| self.poisoned = Some(error.to_string()))
    }

    fn logs(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
        plan: &Plan,
    ) -> Result<RecordBatch> {
        let (root, mut p) = self.payloads(&LOGS, envelope)?;
        super::logs::normalize(
            root,
            p.take(ArrowPayloadType::ResourceAttrs),
            p.take(ArrowPayloadType::ScopeAttrs),
            p.take(ArrowPayloadType::LogAttrs),
            input_bytes,
            plan,
        )
    }

    fn traces(
        &mut self,
        envelope: BatchArrowRecords,
        input_bytes: usize,
        plan: &Plan,
    ) -> Result<RecordBatch> {
        let (root, mut p) = self.payloads(&TRACES, envelope)?;
        super::traces::normalize(
            root,
            p.take(ArrowPayloadType::ResourceAttrs),
            p.take(ArrowPayloadType::ScopeAttrs),
            p.take(ArrowPayloadType::SpanAttrs),
            p.take(ArrowPayloadType::SpanEvents),
            p.take(ArrowPayloadType::SpanEventAttrs),
            p.take(ArrowPayloadType::SpanLinks),
            p.take(ArrowPayloadType::SpanLinkAttrs),
            input_bytes,
            plan,
        )
    }

    fn metrics(
        &mut self,
        envelope: BatchArrowRecords,
        selection: MetricsSelection<'_>,
    ) -> Result<MetricBatches> {
        let (root, mut p) = self.payloads(&METRICS, envelope)?;
        use ArrowPayloadType as T;
        super::metrics::normalize(
            root,
            p.take(T::ResourceAttrs),
            p.take(T::ScopeAttrs),
            p.take(T::MetricAttrs),
            p.take(T::NumberDataPoints),
            p.take(T::SummaryDataPoints),
            p.take(T::HistogramDataPoints),
            p.take(T::ExpHistogramDataPoints),
            p.take(T::NumberDpAttrs),
            p.take(T::SummaryDpAttrs),
            p.take(T::HistogramDpAttrs),
            p.take(T::ExpHistogramDpAttrs),
            p.take(T::NumberDpExemplars),
            p.take(T::HistogramDpExemplars),
            p.take(T::ExpHistogramDpExemplars),
            p.take(T::NumberDpExemplarAttrs),
            p.take(T::HistogramDpExemplarAttrs),
            p.take(T::ExpHistogramDpExemplarAttrs),
            selection,
        )
    }

    fn payloads(
        &mut self,
        signal: &Signal,
        envelope: BatchArrowRecords,
    ) -> Result<(RecordBatch, Payloads)> {
        let mut decoded = HashMap::new();
        for payload in envelope.arrow_payloads {
            let payload_type = ArrowPayloadType::try_from(payload.r#type).map_err(|_| {
                Error::Otap(format!("unknown Arrow payload type {}", payload.r#type))
            })?;
            if payload_type == ArrowPayloadType::MultivariateMetrics {
                return Err(Error::Otap(
                    "MultivariateMetrics is not supported because upstream has no canonical schema"
                        .into(),
                ));
            }
            if payload_type != signal.root && !signal.accepts.contains(&payload_type) {
                return Err(Error::Otap(format!(
                    "payload type {payload_type:?} is not valid for {}",
                    signal.name
                )));
            }
            if decoded.contains_key(&payload_type) {
                return Err(Error::Otap(format!(
                    "duplicate {payload_type:?} payload in batch {}",
                    envelope.batch_id
                )));
            }
            let batch = self.decode_payload(payload_type, payload)?;
            decoded.insert(payload_type, batch);
        }
        let mut payloads = Payloads(decoded);
        let root = payloads
            .take(signal.root)
            .ok_or_else(|| Error::Otap(format!("missing required {:?} payload", signal.root)))?;
        Ok((root, payloads))
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
