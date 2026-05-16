//! OTLP trace JSON decoding into protobuf request structs.

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status},
};
use serde::Deserialize;

use super::common::{
    decode_bytes_field, json_attrs_to_otlp, json_resource_to_otlp, json_scope_to_otlp,
    json_timestamp_to_u64, DecodeError, JsonInstrumentationScope, JsonKeyValue, JsonNumberOrString,
    JsonResource,
};

pub fn decode_json_request(body: &[u8]) -> Result<ExportTraceServiceRequest, DecodeError> {
    let normalized = super::normalize::normalize_json_bytes(body)?;
    let request: JsonExportTraceServiceRequest = serde_json::from_slice(&normalized)?;
    export_traces_json_to_request(request)
}

fn export_traces_json_to_request(
    request: JsonExportTraceServiceRequest,
) -> Result<ExportTraceServiceRequest, DecodeError> {
    let mut resource_spans = Vec::with_capacity(request.resource_spans.len());

    for resource in request.resource_spans {
        let mut scope_spans = Vec::with_capacity(resource.scope_spans.len());
        for scope in resource.scope_spans {
            let mut spans = Vec::with_capacity(scope.spans.len());
            for span in scope.spans {
                let events = span
                    .events
                    .into_iter()
                    .map(|event| {
                        Ok(span::Event {
                            time_unix_nano: json_timestamp_to_u64(
                                &event.time_unix_nano,
                                "event.time_unix_nano",
                            )?,
                            name: event.name,
                            attributes: json_attrs_to_otlp(event.attributes),
                            dropped_attributes_count: 0,
                        })
                    })
                    .collect::<Result<Vec<_>, DecodeError>>()?;
                let links = span
                    .links
                    .into_iter()
                    .map(|link| span::Link {
                        trace_id: decode_bytes_field(&link.trace_id),
                        span_id: decode_bytes_field(&link.span_id),
                        trace_state: link.trace_state,
                        attributes: json_attrs_to_otlp(link.attributes),
                        dropped_attributes_count: 0,
                        flags: 0,
                    })
                    .collect();

                spans.push(Span {
                    trace_id: decode_bytes_field(&span.trace_id),
                    span_id: decode_bytes_field(&span.span_id),
                    trace_state: span.trace_state,
                    parent_span_id: decode_bytes_field(&span.parent_span_id),
                    flags: span.flags,
                    name: span.name,
                    kind: span.kind,
                    start_time_unix_nano: json_timestamp_to_u64(
                        &span.start_time_unix_nano,
                        "span.start_time_unix_nano",
                    )?,
                    end_time_unix_nano: json_timestamp_to_u64(
                        &span.end_time_unix_nano,
                        "span.end_time_unix_nano",
                    )?,
                    attributes: json_attrs_to_otlp(span.attributes),
                    dropped_attributes_count: span.dropped_attributes_count,
                    events,
                    dropped_events_count: span.dropped_events_count,
                    links,
                    dropped_links_count: span.dropped_links_count,
                    status: Some(Status {
                        message: span.status.message,
                        code: span.status.code,
                    }),
                });
            }
            scope_spans.push(ScopeSpans {
                scope: Some(json_scope_to_otlp(scope.scope)),
                spans,
                schema_url: String::new(),
            });
        }
        resource_spans.push(ResourceSpans {
            resource: Some(json_resource_to_otlp(resource.resource)),
            scope_spans,
            schema_url: String::new(),
        });
    }

    Ok(ExportTraceServiceRequest { resource_spans })
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_json_request() {
        let body = r#"{
            "resourceSpans": [{
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "0af7651916cd43dd8448eb211c80319c",
                        "spanId": "b7ad6b7169203331",
                        "name": "test-span",
                        "kind": 2,
                        "startTimeUnixNano": "1000000000",
                        "endTimeUnixNano": "2000000000"
                    }]
                }]
            }]
        }"#;

        let request = decode_json_request(body.as_bytes()).unwrap();
        let span = &request.resource_spans[0].scope_spans[0].spans[0];
        assert_eq!(span.name, "test-span");
        assert_eq!(span.trace_id.len(), 16);
    }
}
