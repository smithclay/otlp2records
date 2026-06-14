// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Canonical OpenTelemetry Arrow Protocol envelope definitions.
//!
//! Synced from `opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`
//! in open-telemetry/otel-arrow commit
//! `f8cd17f084c1a766f887530531ad06f546080c90`.

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
