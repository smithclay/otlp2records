//! OTLP trace decoding - protobuf and JSON

use bytes::Bytes;
use const_hex::encode as hex_encode;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use serde::Deserialize;
use std::sync::Arc;
use vrl::value::{ObjectMap, Value as VrlValue};

use super::common::{
    for_each_resource_scope, json_attrs_to_value, json_resource_to_value, json_scope_to_value,
    json_timestamp_to_i64, otlp_attributes_to_value, otlp_resource_to_value, otlp_scope_to_value,
    safe_timestamp_conversion, DecodeError, JsonInstrumentationScope, JsonKeyValue,
    JsonNumberOrString, JsonResource,
};

// ============================================================================
// Protobuf decoding
// ============================================================================

pub fn decode_protobuf(body: &[u8]) -> Result<Vec<VrlValue>, DecodeError> {
    let request = ExportTraceServiceRequest::decode(body)?;
    export_traces_to_vrl_proto(request)
}

fn export_traces_to_vrl_proto(
    request: ExportTraceServiceRequest,
) -> Result<Vec<VrlValue>, DecodeError> {
    let mut values = preallocate_span_values(&request.resource_spans, |rs| {
        rs.scope_spans.iter().map(|ss| ss.spans.len()).sum()
    });

    for_each_resource_scope(
        request.resource_spans,
        |resource_spans| {
            (
                otlp_resource_to_value(resource_spans.resource.as_ref()),
                resource_spans.scope_spans,
            )
        },
        |scope_spans| {
            (
                otlp_scope_to_value(scope_spans.scope.as_ref()),
                scope_spans.spans,
            )
        },
        |spans, resource, scope| {
            for span in spans {
                let events: Result<Vec<SpanEventParts>, DecodeError> = span
                    .events
                    .iter()
                    .map(|e| {
                        Ok(SpanEventParts {
                            time_unix_nano: safe_timestamp_conversion(
                                e.time_unix_nano,
                                "event.time_unix_nano",
                            )?,
                            name: Bytes::from(e.name.clone()),
                            attributes: otlp_attributes_to_value(&e.attributes),
                        })
                    })
                    .collect();
                let events = events?;

                let links: Vec<SpanLinkParts> = span
                    .links
                    .iter()
                    .map(|l| SpanLinkParts {
                        trace_id: Bytes::from(hex_encode(&l.trace_id)),
                        span_id: Bytes::from(hex_encode(&l.span_id)),
                        trace_state: Bytes::from(l.trace_state.clone()),
                        attributes: otlp_attributes_to_value(&l.attributes),
                    })
                    .collect();

                let (status_code, status_message) = span
                    .status
                    .as_ref()
                    .map(|s| (s.code as i64, Bytes::from(s.message.clone())))
                    .unwrap_or((0, Bytes::new()));

                let parts = SpanRecordParts {
                    trace_id: Bytes::from(hex_encode(&span.trace_id)),
                    span_id: Bytes::from(hex_encode(&span.span_id)),
                    parent_span_id: Bytes::from(hex_encode(&span.parent_span_id)),
                    trace_state: Bytes::from(span.trace_state),
                    name: Bytes::from(span.name),
                    kind: span.kind as i64,
                    start_time_unix_nano: safe_timestamp_conversion(
                        span.start_time_unix_nano,
                        "span.start_time_unix_nano",
                    )?,
                    end_time_unix_nano: safe_timestamp_conversion(
                        span.end_time_unix_nano,
                        "span.end_time_unix_nano",
                    )?,
                    attributes: otlp_attributes_to_value(&span.attributes),
                    status_code,
                    status_message,
                    events,
                    links,
                    resource: Arc::clone(&resource),
                    scope: Arc::clone(&scope),
                    dropped_attributes_count: span.dropped_attributes_count as i64,
                    dropped_events_count: span.dropped_events_count as i64,
                    dropped_links_count: span.dropped_links_count as i64,
                    flags: span.flags as i64,
                };

                values.push(build_span_record(parts));
            }

            Ok::<(), DecodeError>(())
        },
    )?;

    Ok(values)
}

// ============================================================================
// JSON decoding
// ============================================================================

pub fn decode_json(body: &[u8]) -> Result<Vec<VrlValue>, DecodeError> {
    // Normalize JSON to convert enum strings to numbers before parsing
    let normalized = super::normalize::normalize_json_bytes(body)?;
    let request: JsonExportTraceServiceRequest = serde_json::from_slice(&normalized)?;
    export_traces_json_to_vrl(request)
}

fn export_traces_json_to_vrl(
    request: JsonExportTraceServiceRequest,
) -> Result<Vec<VrlValue>, DecodeError> {
    let mut values = preallocate_span_values(&request.resource_spans, |rs| {
        rs.scope_spans.iter().map(|ss| ss.spans.len()).sum()
    });

    for_each_resource_scope(
        request.resource_spans,
        |resource_spans| {
            (
                json_resource_to_value(resource_spans.resource),
                resource_spans.scope_spans,
            )
        },
        |scope_spans| (json_scope_to_value(scope_spans.scope), scope_spans.spans),
        |spans, resource, scope| {
            for span in spans {
                let events: Vec<SpanEventParts> = span
                    .events
                    .into_iter()
                    .map(|e| {
                        Ok(SpanEventParts {
                            time_unix_nano: json_timestamp_to_i64(
                                &e.time_unix_nano,
                                "event.time_unix_nano",
                            )?,
                            name: Bytes::from(e.name),
                            attributes: json_attrs_to_value(e.attributes),
                        })
                    })
                    .collect::<Result<_, DecodeError>>()?;

                let links: Vec<SpanLinkParts> = span
                    .links
                    .into_iter()
                    .map(|l| SpanLinkParts {
                        trace_id: Bytes::from(l.trace_id),
                        span_id: Bytes::from(l.span_id),
                        trace_state: Bytes::from(l.trace_state),
                        attributes: json_attrs_to_value(l.attributes),
                    })
                    .collect();

                let parts = SpanRecordParts {
                    trace_id: Bytes::from(span.trace_id),
                    span_id: Bytes::from(span.span_id),
                    parent_span_id: Bytes::from(span.parent_span_id),
                    trace_state: Bytes::from(span.trace_state),
                    name: Bytes::from(span.name),
                    kind: span.kind as i64,
                    start_time_unix_nano: json_timestamp_to_i64(
                        &span.start_time_unix_nano,
                        "span.start_time_unix_nano",
                    )?,
                    end_time_unix_nano: json_timestamp_to_i64(
                        &span.end_time_unix_nano,
                        "span.end_time_unix_nano",
                    )?,
                    attributes: json_attrs_to_value(span.attributes),
                    status_code: span.status.code as i64,
                    status_message: Bytes::from(span.status.message),
                    events,
                    links,
                    resource: Arc::clone(&resource),
                    scope: Arc::clone(&scope),
                    dropped_attributes_count: span.dropped_attributes_count as i64,
                    dropped_events_count: span.dropped_events_count as i64,
                    dropped_links_count: span.dropped_links_count as i64,
                    flags: span.flags as i64,
                };

                values.push(build_span_record(parts));
            }

            Ok::<(), DecodeError>(())
        },
    )?;

    Ok(values)
}

// ============================================================================
// JSON struct definitions
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExportTraceServiceRequest {
    #[serde(default)]
    resource_spans: Vec<JsonResourceSpans>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonResourceSpans {
    #[serde(default)]
    resource: JsonResource,
    #[serde(default)]
    scope_spans: Vec<JsonScopeSpans>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonScopeSpans {
    #[serde(default)]
    scope: JsonInstrumentationScope,
    #[serde(default)]
    spans: Vec<JsonSpan>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSpan {
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
    #[serde(default)]
    parent_span_id: String,
    #[serde(default)]
    trace_state: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    kind: i32,
    #[serde(default)]
    start_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    end_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    events: Vec<JsonSpanEvent>,
    #[serde(default)]
    links: Vec<JsonSpanLink>,
    #[serde(default)]
    status: JsonStatus,
    #[serde(default)]
    dropped_attributes_count: u32,
    #[serde(default)]
    dropped_events_count: u32,
    #[serde(default)]
    dropped_links_count: u32,
    #[serde(default)]
    flags: u32,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSpanEvent {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    name: String,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSpanLink {
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
    #[serde(default)]
    trace_state: String,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonStatus {
    #[serde(default)]
    code: i32,
    #[serde(default)]
    message: String,
}

// ============================================================================
// Record builder
// ============================================================================

/// Precomputed fields for building a span record into VRL values
struct SpanRecordParts {
    trace_id: Bytes,
    span_id: Bytes,
    parent_span_id: Bytes,
    trace_state: Bytes,
    name: Bytes,
    kind: i64,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    attributes: VrlValue,
    status_code: i64,
    status_message: Bytes,
    events: Vec<SpanEventParts>,
    links: Vec<SpanLinkParts>,
    resource: Arc<VrlValue>,
    scope: Arc<VrlValue>,
    dropped_attributes_count: i64,
    dropped_events_count: i64,
    dropped_links_count: i64,
    flags: i64,
}

struct SpanEventParts {
    time_unix_nano: i64,
    name: Bytes,
    attributes: VrlValue,
}

struct SpanLinkParts {
    trace_id: Bytes,
    span_id: Bytes,
    trace_state: Bytes,
    attributes: VrlValue,
}

/// Pre-allocate a values Vec sized to the number of spans a request contains
fn preallocate_span_values<R, F>(resource_spans: &[R], count_spans: F) -> Vec<VrlValue>
where
    F: Fn(&R) -> usize,
{
    let capacity: usize = resource_spans.iter().map(&count_spans).sum();
    Vec::with_capacity(capacity)
}

/// Build a VRL-ready span record from parts
fn build_span_record(parts: SpanRecordParts) -> VrlValue {
    let mut map = ObjectMap::new();

    // Basic span identifiers
    map.insert("trace_id".into(), VrlValue::Bytes(parts.trace_id));
    map.insert("span_id".into(), VrlValue::Bytes(parts.span_id));
    map.insert(
        "parent_span_id".into(),
        VrlValue::Bytes(parts.parent_span_id),
    );
    map.insert("trace_state".into(), VrlValue::Bytes(parts.trace_state));
    map.insert("name".into(), VrlValue::Bytes(parts.name));
    map.insert("kind".into(), VrlValue::Integer(parts.kind));

    // Timestamps and duration
    map.insert(
        "start_time_unix_nano".into(),
        VrlValue::Integer(parts.start_time_unix_nano),
    );
    map.insert(
        "end_time_unix_nano".into(),
        VrlValue::Integer(parts.end_time_unix_nano),
    );
    map.insert(
        "duration_ns".into(),
        VrlValue::Integer(
            parts
                .end_time_unix_nano
                .saturating_sub(parts.start_time_unix_nano),
        ),
    );

    // Attributes
    map.insert("attributes".into(), parts.attributes);

    // Status
    map.insert("status_code".into(), VrlValue::Integer(parts.status_code));
    map.insert(
        "status_message".into(),
        VrlValue::Bytes(parts.status_message),
    );

    // Events
    let events_array: Vec<VrlValue> = parts
        .events
        .into_iter()
        .map(|e| {
            let mut event_map = ObjectMap::new();
            event_map.insert("time_unix_nano".into(), VrlValue::Integer(e.time_unix_nano));
            event_map.insert("name".into(), VrlValue::Bytes(e.name));
            event_map.insert("attributes".into(), e.attributes);
            VrlValue::Object(event_map)
        })
        .collect();
    map.insert("events".into(), VrlValue::Array(events_array));

    // Links
    let links_array: Vec<VrlValue> = parts
        .links
        .into_iter()
        .map(|l| {
            let mut link_map = ObjectMap::new();
            link_map.insert("trace_id".into(), VrlValue::Bytes(l.trace_id));
            link_map.insert("span_id".into(), VrlValue::Bytes(l.span_id));
            link_map.insert("trace_state".into(), VrlValue::Bytes(l.trace_state));
            link_map.insert("attributes".into(), l.attributes);
            VrlValue::Object(link_map)
        })
        .collect();
    map.insert("links".into(), VrlValue::Array(links_array));

    // Resource and scope
    map.insert("resource".into(), (*parts.resource).clone());
    map.insert("scope".into(), (*parts.scope).clone());

    // Dropped counts and flags
    map.insert(
        "dropped_attributes_count".into(),
        VrlValue::Integer(parts.dropped_attributes_count),
    );
    map.insert(
        "dropped_events_count".into(),
        VrlValue::Integer(parts.dropped_events_count),
    );
    map.insert(
        "dropped_links_count".into(),
        VrlValue::Integer(parts.dropped_links_count),
    );
    map.insert("flags".into(), VrlValue::Integer(parts.flags));

    VrlValue::Object(map)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
        resource::v1::Resource,
        trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status},
    };

    #[test]
    fn decodes_json_payload() {
        let body = r#"{
            "resourceSpans": [{
                "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "svc" } }]},
                "scopeSpans": [{
                    "scope": { "name": "lib", "version": "1" },
                    "spans": [{
                        "traceId": "0af7651916cd43dd8448eb211c80319c",
                        "spanId": "b7ad6b7169203331",
                        "parentSpanId": "",
                        "name": "test-span",
                        "kind": 2,
                        "startTimeUnixNano": "1000000000",
                        "endTimeUnixNano": "2000000000",
                        "attributes": [{ "key": "k", "value": { "stringValue": "v" } }],
                        "status": { "code": 1, "message": "OK" }
                    }]
                }]
            }]
        }"#;

        let records = decode_json(body.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);

        let record = &records[0];
        if let VrlValue::Object(map) = record {
            assert_eq!(
                map.get("name"),
                Some(&VrlValue::Bytes(Bytes::from("test-span")))
            );
            assert_eq!(map.get("kind"), Some(&VrlValue::Integer(2)));
            assert_eq!(
                map.get("duration_ns"),
                Some(&VrlValue::Integer(1_000_000_000))
            );
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn decodes_protobuf_payload() {
        let span = Span {
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
            parent_span_id: vec![],
            name: "test-span".to_string(),
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            attributes: vec![KeyValue {
                key: "k".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("v".to_string())),
                }),
            }],
            status: Some(Status {
                code: 1,
                message: "OK".to_string(),
            }),
            ..Default::default()
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "lib".to_string(),
                        version: "1".to_string(),
                        ..Default::default()
                    }),
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let records = decode_protobuf(&body).unwrap();
        assert_eq!(records.len(), 1);

        let record = &records[0];
        if let VrlValue::Object(map) = record {
            assert_eq!(
                map.get("name"),
                Some(&VrlValue::Bytes(Bytes::from("test-span")))
            );
            // trace_id should be hex encoded
            assert_eq!(
                map.get("trace_id"),
                Some(&VrlValue::Bytes(Bytes::from(
                    "000102030405060708090a0b0c0d0e0f"
                )))
            );
            assert_eq!(
                map.get("duration_ns"),
                Some(&VrlValue::Integer(1_000_000_000))
            );
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn rejects_overflow_span_start_time() {
        let span = Span {
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
            name: "test-span".to_string(),
            start_time_unix_nano: u64::MAX,
            end_time_unix_nano: 1_000_000_000,
            ..Default::default()
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DecodeError::Unsupported(msg) => {
                assert!(msg.contains("timestamp overflow"));
                assert!(msg.contains("start_time_unix_nano"));
            }
            _ => panic!("Expected DecodeError::Unsupported, got: {err:?}"),
        }
    }

    #[test]
    fn rejects_overflow_span_end_time() {
        let span = Span {
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
            name: "test-span".to_string(),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: u64::MAX,
            ..Default::default()
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DecodeError::Unsupported(msg) => {
                assert!(msg.contains("timestamp overflow"));
                assert!(msg.contains("end_time_unix_nano"));
            }
            _ => panic!("Expected DecodeError::Unsupported, got: {err:?}"),
        }
    }

    #[test]
    fn rejects_overflow_event_time() {
        let span = Span {
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
            name: "test-span".to_string(),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            events: vec![span::Event {
                time_unix_nano: u64::MAX,
                name: "test-event".to_string(),
                attributes: vec![],
                ..Default::default()
            }],
            ..Default::default()
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DecodeError::Unsupported(msg) => {
                assert!(msg.contains("timestamp overflow"));
                assert!(msg.contains("event.time_unix_nano"));
            }
            _ => panic!("Expected DecodeError::Unsupported, got: {err:?}"),
        }
    }

    #[test]
    fn accepts_valid_timestamps() {
        let span = Span {
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
            name: "test-span".to_string(),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            events: vec![span::Event {
                time_unix_nano: 1_500_000_000,
                name: "test-event".to_string(),
                attributes: vec![],
                ..Default::default()
            }],
            ..Default::default()
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body);

        assert!(result.is_ok());
        let spans = result.unwrap();
        assert_eq!(spans.len(), 1);
    }

    #[test]
    fn handles_empty_trace_request() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![],
        };

        let body = request.encode_to_vec();
        let records = decode_protobuf(&body).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn handles_empty_json_request() {
        let body = r#"{"resourceSpans": []}"#;
        let records = decode_json(body.as_bytes()).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn includes_span_links() {
        let span = Span {
            trace_id: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            span_id: vec![0, 1, 2, 3, 4, 5, 6, 7],
            name: "test-span".to_string(),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 2_000_000_000,
            links: vec![span::Link {
                trace_id: vec![15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
                span_id: vec![7, 6, 5, 4, 3, 2, 1, 0],
                trace_state: "vendor=test".to_string(),
                attributes: vec![],
                ..Default::default()
            }],
            ..Default::default()
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let records = decode_protobuf(&body).unwrap();
        assert_eq!(records.len(), 1);

        if let VrlValue::Object(map) = &records[0] {
            let links = map.get("links").unwrap();
            if let VrlValue::Array(arr) = links {
                assert_eq!(arr.len(), 1);
            } else {
                panic!("expected array");
            }
        } else {
            panic!("expected object");
        }
    }
}
