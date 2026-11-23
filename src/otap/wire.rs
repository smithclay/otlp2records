// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Canonical OpenTelemetry Arrow Protocol envelope definitions.
//!
//! Synced from `opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`
//! in open-telemetry/otel-arrow commit
//! `305e4dbe2381996a12e884c8960096bab7946c7a` (the proto is unchanged since
//! `f8cd17f084c1a766f887530531ad06f546080c90`, the original sync).

/// A collection of Arrow payloads sharing an OTAP batch ID.
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchArrowRecords {
    #[prost(int64, tag = "1")]
    pub batch_id: i64,
    #[prost(message, repeated, tag = "2")]
    pub arrow_payloads: Vec<ArrowPayload>,
    #[prost(bytes = "vec", tag = "3")]
    pub headers: Vec<u8>,
}

/// Per-batch acknowledgement the server returns on an OTAP stream: one
/// `BatchStatus` per received [`BatchArrowRecords`], echoing its `batch_id`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchStatus {
    #[prost(int64, tag = "1")]
    pub batch_id: i64,
    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status_code: i32,
    #[prost(string, tag = "3")]
    pub status_message: String,
}

/// OTAP stream status codes (otel-arrow `StatusCode`), mirroring the canonical
/// gRPC status codes. The numbering matches the proto and is intentionally
/// non-contiguous.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum StatusCode {
    Ok = 0,
    Canceled = 1,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    Aborted = 10,
    Internal = 13,
    Unavailable = 14,
    Unauthenticated = 16,
}

/// One Arrow IPC stream fragment in a [`BatchArrowRecords`] envelope.
#[derive(Clone, PartialEq, Eq, Hash, prost::Message)]
pub struct ArrowPayload {
    #[prost(string, tag = "1")]
    pub schema_id: String,
    #[prost(enumeration = "ArrowPayloadType", tag = "2")]
    pub r#type: i32,
    #[prost(bytes = "vec", tag = "3")]
    pub record: Vec<u8>,
}

/// OTLP `AnyValue` discriminant in an OTAP `type` column (otel-arrow
/// `AttributeValueType`). `Map` and `Slice` are CBOR-serialized into `ser`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub(crate) enum AnyValueType {
    Empty = 0,
    Str = 1,
    Int = 2,
    Double = 3,
    Bool = 4,
    Map = 5,
    Slice = 6,
    Bytes = 7,
}

impl AnyValueType {
    pub(crate) fn from_wire(value: u8) -> Option<Self> {
        Self::try_from(i32::from(value)).ok()
    }

    pub(crate) fn payload_column(self) -> Option<&'static str> {
        match self {
            Self::Empty => None,
            Self::Str => Some(ATTR_STR),
            Self::Int => Some(ATTR_INT),
            Self::Double => Some(ATTR_DOUBLE),
            Self::Bool => Some(ATTR_BOOL),
            Self::Map | Self::Slice => Some(ATTR_SER),
            Self::Bytes => Some(ATTR_BYTES),
        }
    }
}

/// Column names of an OTAP `AnyValue` record (attribute tables and the log
/// `body` struct).
pub(crate) const ATTR_KEY: &str = "key";
pub(crate) const ATTR_TYPE: &str = "type";
pub(crate) const ATTR_STR: &str = "str";
pub(crate) const ATTR_INT: &str = "int";
pub(crate) const ATTR_DOUBLE: &str = "double";
pub(crate) const ATTR_BOOL: &str = "bool";
pub(crate) const ATTR_BYTES: &str = "bytes";
pub(crate) const ATTR_SER: &str = "ser";

/// Canonical OTAP payload type numbers.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum ArrowPayloadType {
    Unknown = 0,
    ResourceAttrs = 1,
    ScopeAttrs = 2,
    UnivariateMetrics = 10,
    NumberDataPoints = 11,
    SummaryDataPoints = 12,
    HistogramDataPoints = 13,
    ExpHistogramDataPoints = 14,
    NumberDpAttrs = 15,
    SummaryDpAttrs = 16,
    HistogramDpAttrs = 17,
    ExpHistogramDpAttrs = 18,
    NumberDpExemplars = 19,
    HistogramDpExemplars = 20,
    ExpHistogramDpExemplars = 21,
    NumberDpExemplarAttrs = 22,
    HistogramDpExemplarAttrs = 23,
    ExpHistogramDpExemplarAttrs = 24,
    MultivariateMetrics = 25,
    MetricAttrs = 26,
    Logs = 30,
    LogAttrs = 31,
    Spans = 40,
    SpanAttrs = 41,
    SpanEvents = 42,
    SpanLinks = 43,
    SpanEventAttrs = 44,
    SpanLinkAttrs = 45,
}
