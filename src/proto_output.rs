//! Protobuf output helpers for compatibility endpoints.
//!
//! These APIs accept neutral Rust DTOs and keep protobuf wire construction
//! inside `otlp2records`.

use opentelemetry_proto::tonic::{
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    resource::v1::Resource as OtlpResource,
    trace::v1::{
        ResourceSpans as OtlpResourceSpans, ScopeSpans as OtlpScopeSpans, Span as OtlpSpan, Status,
        TracesData as OtlpTracesData,
    },
};
use prost::Message;

use crate::{Error, Result};

/// Tempo trace lookup response content type.
pub const TEMPO_TRACE_BY_ID_CONTENT_TYPE: &str = "application/protobuf";

/// Neutral trace model for protobuf compatibility output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TraceData {
    /// Resource span groups in the trace.
    pub resource_spans: Vec<ResourceSpans>,
}

/// Spans grouped under one resource.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResourceSpans {
    /// Resource attributes shared by the group.
    pub resource: Resource,
    /// Scope span groups under the resource.
    pub scope_spans: Vec<ScopeSpans>,
    /// Optional schema URL.
    pub schema_url: String,
}

/// Resource metadata.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Resource {
    /// Resource attributes.
    pub attributes: Vec<Attribute>,
}

/// Spans grouped under one instrumentation scope.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScopeSpans {
    /// Instrumentation scope metadata.
    pub scope: Scope,
    /// Spans in this scope.
    pub spans: Vec<Span>,
    /// Optional schema URL.
    pub schema_url: String,
}

/// Instrumentation scope metadata.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Scope {
    /// Scope name.
    pub name: String,
    /// Scope version.
    pub version: String,
    /// Scope attributes.
    pub attributes: Vec<Attribute>,
}

/// Neutral span DTO for protobuf compatibility output.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Span {
    /// 32-character hexadecimal trace ID.
    pub trace_id_hex: String,
    /// 16-character hexadecimal span ID.
    pub span_id_hex: String,
    /// Optional 16-character hexadecimal parent span ID.
    pub parent_span_id_hex: Option<String>,
    /// W3C trace state.
    pub trace_state: String,
    /// Span name.
    pub name: String,
    /// OTLP span kind integer.
    pub kind: i32,
    /// Start timestamp in unix nanoseconds.
    pub start_time_unix_nano: u64,
    /// End timestamp in unix nanoseconds.
    pub end_time_unix_nano: u64,
    /// Span attributes.
    pub attributes: Vec<Attribute>,
    /// Dropped attribute count.
    pub dropped_attributes_count: u32,
    /// Dropped event count.
    pub dropped_events_count: u32,
    /// Dropped link count.
    pub dropped_links_count: u32,
    /// Trace flags.
    pub flags: u32,
    /// Span status.
    pub status: SpanStatus,
}

/// Neutral OTLP attribute.
#[derive(Debug, Clone, PartialEq)]
pub struct Attribute {
    /// Attribute key.
    pub key: String,
    /// Attribute value.
    pub value: AttributeValue,
}

/// Neutral OTLP attribute scalar value.
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    /// String attribute.
    String(String),
    /// Integer attribute.
    Int(i64),
    /// Boolean attribute.
    Bool(bool),
    /// Floating point attribute.
    Double(f64),
    /// Bytes attribute.
    Bytes(Vec<u8>),
}

/// Neutral span status.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SpanStatus {
    /// OTLP status code integer.
    pub code: i32,
    /// Status message.
    pub message: String,
}

/// Summary of a decoded Tempo trace lookup protobuf response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TempoTraceSummary {
    /// Number of resource span groups.
    pub resource_span_count: usize,
    /// Number of scope span groups.
    pub scope_span_count: usize,
    /// Number of spans.
    pub span_count: usize,
    /// First span name, if present.
    pub first_span_name: Option<String>,
}

/// Encode Tempo's `TraceByIDResponse` protobuf envelope.
///
/// Tempo returns a `tempopb.TraceByIDResponse` whose first field is the OTLP
/// `TracesData` message. The wrapper type is intentionally private so callers
/// only depend on neutral DTOs.
pub fn encode_tempo_trace_by_id_response(trace: TraceData) -> Result<Vec<u8>> {
    let trace = trace_data_to_otlp(trace)?;
    Ok(TempoTraceByIdResponse { trace: Some(trace) }.encode_to_vec())
}

/// Decode a Tempo trace lookup protobuf response into a neutral summary.
///
/// This is intended for black-box compatibility tests in consumers that should
/// not import `prost` or `opentelemetry-proto` directly.
pub fn inspect_tempo_trace_by_id_response(bytes: &[u8]) -> Result<TempoTraceSummary> {
    let response = TempoTraceByIdResponse::decode(bytes)?;
    let mut summary = TempoTraceSummary::default();
    let Some(trace) = response.trace else {
        return Ok(summary);
    };

    summary.resource_span_count = trace.resource_spans.len();
    for resource_spans in trace.resource_spans {
        summary.scope_span_count += resource_spans.scope_spans.len();
        for scope_spans in resource_spans.scope_spans {
            summary.span_count += scope_spans.spans.len();
            if summary.first_span_name.is_none() {
                summary.first_span_name = scope_spans.spans.first().map(|span| span.name.clone());
            }
        }
    }
    Ok(summary)
}

fn trace_data_to_otlp(trace: TraceData) -> Result<OtlpTracesData> {
    Ok(OtlpTracesData {
        resource_spans: trace
            .resource_spans
            .into_iter()
            .map(resource_spans_to_otlp)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn resource_spans_to_otlp(resource_spans: ResourceSpans) -> Result<OtlpResourceSpans> {
    Ok(OtlpResourceSpans {
        resource: Some(OtlpResource {
            attributes: attributes_to_otlp(resource_spans.resource.attributes),
            dropped_attributes_count: 0,
            entity_refs: Vec::new(),
        }),
        scope_spans: resource_spans
            .scope_spans
            .into_iter()
            .map(scope_spans_to_otlp)
            .collect::<Result<Vec<_>>>()?,
        schema_url: resource_spans.schema_url,
    })
}

fn scope_spans_to_otlp(scope_spans: ScopeSpans) -> Result<OtlpScopeSpans> {
    Ok(OtlpScopeSpans {
        scope: Some(InstrumentationScope {
            name: scope_spans.scope.name,
            version: scope_spans.scope.version,
            attributes: attributes_to_otlp(scope_spans.scope.attributes),
            dropped_attributes_count: 0,
        }),
        spans: scope_spans
            .spans
            .into_iter()
            .map(span_to_otlp)
            .collect::<Result<Vec<_>>>()?,
        schema_url: scope_spans.schema_url,
    })
}

fn span_to_otlp(span: Span) -> Result<OtlpSpan> {
    Ok(OtlpSpan {
        trace_id: decode_hex_id("span.trace_id_hex", &span.trace_id_hex, 16)?,
        span_id: decode_hex_id("span.span_id_hex", &span.span_id_hex, 8)?,
        trace_state: span.trace_state,
        parent_span_id: match span.parent_span_id_hex {
            Some(parent) if !parent.is_empty() => {
                decode_hex_id("span.parent_span_id_hex", &parent, 8)?
            }
            _ => Vec::new(),
        },
        flags: span.flags,
        name: span.name,
        kind: span.kind,
        start_time_unix_nano: span.start_time_unix_nano,
        end_time_unix_nano: span.end_time_unix_nano,
        attributes: attributes_to_otlp(span.attributes),
        dropped_attributes_count: span.dropped_attributes_count,
        events: Vec::new(),
        dropped_events_count: span.dropped_events_count,
        links: Vec::new(),
        dropped_links_count: span.dropped_links_count,
        status: Some(Status {
            message: span.status.message,
            code: span.status.code,
        }),
    })
}

fn attributes_to_otlp(attributes: Vec<Attribute>) -> Vec<KeyValue> {
    attributes
        .into_iter()
        .map(|attribute| KeyValue {
            key: attribute.key,
            value: Some(any_value_to_otlp(attribute.value)),
        })
        .collect()
}

fn any_value_to_otlp(value: AttributeValue) -> AnyValue {
    let value = match value {
        AttributeValue::String(value) => any_value::Value::StringValue(value),
        AttributeValue::Int(value) => any_value::Value::IntValue(value),
        AttributeValue::Bool(value) => any_value::Value::BoolValue(value),
        AttributeValue::Double(value) => any_value::Value::DoubleValue(value),
        AttributeValue::Bytes(value) => any_value::Value::BytesValue(value),
    };
    AnyValue { value: Some(value) }
}

fn decode_hex_id(field: &str, value: &str, expected_bytes: usize) -> Result<Vec<u8>> {
    if value.len() != expected_bytes * 2 {
        return Err(Error::InvalidInput(format!(
            "{field} must be {} hex characters",
            expected_bytes * 2
        )));
    }
    const_hex::decode(value)
        .map_err(|err| Error::InvalidInput(format!("{field} must be valid hexadecimal: {err}")))
}

/// Tempo's `/api/v2/traces/{traceID}` returns a `tempopb.TraceByIDResponse`
/// proto whose first field is the OTLP `TracesData`.
#[derive(Clone, PartialEq, Message)]
struct TempoTraceByIdResponse {
    #[prost(message, optional, tag = "1")]
    trace: Option<OtlpTracesData>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::any_value,
        trace::v1::{span, status},
    };

    #[test]
    fn encodes_tempo_trace_by_id_response() {
        let bytes = encode_tempo_trace_by_id_response(sample_trace()).unwrap();
        let decoded = TempoTraceByIdResponse::decode(bytes.as_slice()).unwrap();
        let trace = decoded.trace.expect("trace should be present");

        let resource_spans = &trace.resource_spans[0];
        assert_eq!(
            resource_spans.resource.as_ref().unwrap().attributes[0].key,
            "service.name"
        );
        assert_eq!(
            resource_spans.scope_spans[0].scope.as_ref().unwrap().name,
            "test-scope"
        );

        let span = &resource_spans.scope_spans[0].spans[0];
        assert_eq!(span.trace_id.len(), 16);
        assert_eq!(span.span_id.len(), 8);
        assert_eq!(span.parent_span_id.len(), 8);
        assert_eq!(span.name, "GET /smoke");
        assert_eq!(span.kind, span::SpanKind::Server as i32);
        assert_eq!(span.start_time_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(span.end_time_unix_nano, 1_700_000_000_012_000_000);
        assert_eq!(
            span.status.as_ref().unwrap().code,
            status::StatusCode::Ok as i32
        );
        assert!(matches!(
            span.attributes[1]
                .value
                .as_ref()
                .unwrap()
                .value
                .as_ref()
                .unwrap(),
            any_value::Value::IntValue(200)
        ));
    }

    #[test]
    fn inspect_tempo_trace_by_id_response_returns_summary() {
        let bytes = encode_tempo_trace_by_id_response(sample_trace()).unwrap();
        let summary = inspect_tempo_trace_by_id_response(&bytes).unwrap();

        assert_eq!(summary.resource_span_count, 1);
        assert_eq!(summary.scope_span_count, 1);
        assert_eq!(summary.span_count, 1);
        assert_eq!(summary.first_span_name.as_deref(), Some("GET /smoke"));
    }

    #[test]
    fn rejects_invalid_trace_id_hex() {
        let mut trace = sample_trace();
        trace.resource_spans[0].scope_spans[0].spans[0].trace_id_hex = "bad".to_string();

        let err = encode_tempo_trace_by_id_response(trace).unwrap_err();
        assert!(err.to_string().contains("span.trace_id_hex"));
    }

    fn sample_trace() -> TraceData {
        TraceData {
            resource_spans: vec![ResourceSpans {
                resource: Resource {
                    attributes: vec![Attribute {
                        key: "service.name".to_string(),
                        value: AttributeValue::String("checkout".to_string()),
                    }],
                },
                scope_spans: vec![ScopeSpans {
                    scope: Scope {
                        name: "test-scope".to_string(),
                        version: "1.0.0".to_string(),
                        attributes: Vec::new(),
                    },
                    spans: vec![Span {
                        trace_id_hex: "11111111111111111111111111111111".to_string(),
                        span_id_hex: "2222222222222222".to_string(),
                        parent_span_id_hex: Some("3333333333333333".to_string()),
                        trace_state: "state=ok".to_string(),
                        name: "GET /smoke".to_string(),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_012_000_000,
                        attributes: vec![
                            Attribute {
                                key: "http.request.method".to_string(),
                                value: AttributeValue::String("GET".to_string()),
                            },
                            Attribute {
                                key: "http.response.status_code".to_string(),
                                value: AttributeValue::Int(200),
                            },
                        ],
                        status: SpanStatus {
                            code: status::StatusCode::Ok as i32,
                            message: String::new(),
                        },
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }
}
