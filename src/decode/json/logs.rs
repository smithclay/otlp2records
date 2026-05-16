//! OTLP log JSON decoding into protobuf request structs.

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
};
use serde::Deserialize;

use super::common::{
    decode_bytes_field, json_any_value_to_otlp, json_attrs_to_otlp, json_resource_to_otlp,
    json_scope_to_otlp, json_timestamp_to_u64, DecodeError, JsonAnyValue, JsonInstrumentationScope,
    JsonKeyValue, JsonNumberOrString, JsonResource,
};

pub fn decode_json_request(body: &[u8]) -> Result<ExportLogsServiceRequest, DecodeError> {
    let normalized = super::normalize::normalize_json_bytes(body)?;
    let request: JsonExportLogsServiceRequest = serde_json::from_slice(&normalized)?;
    export_logs_json_to_request(request)
}

fn export_logs_json_to_request(
    request: JsonExportLogsServiceRequest,
) -> Result<ExportLogsServiceRequest, DecodeError> {
    let mut resource_logs = Vec::with_capacity(request.resource_logs.len());

    for resource in request.resource_logs {
        let mut scope_logs = Vec::with_capacity(resource.scope_logs.len());
        for scope in resource.scope_logs {
            let mut log_records = Vec::with_capacity(scope.log_records.len());
            for record in scope.log_records {
                log_records.push(LogRecord {
                    time_unix_nano: json_timestamp_to_u64(
                        &record.time_unix_nano,
                        "log.time_unix_nano",
                    )?,
                    observed_time_unix_nano: json_timestamp_to_u64(
                        &record.observed_time_unix_nano,
                        "log.observed_time_unix_nano",
                    )?,
                    severity_number: record.severity_number,
                    severity_text: record.severity_text,
                    body: record.body.map(json_any_value_to_otlp),
                    attributes: json_attrs_to_otlp(record.attributes),
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: decode_bytes_field(&record.trace_id),
                    span_id: decode_bytes_field(&record.span_id),
                    event_name: String::new(),
                });
            }
            scope_logs.push(ScopeLogs {
                scope: Some(json_scope_to_otlp(scope.scope)),
                log_records,
                schema_url: String::new(),
            });
        }
        resource_logs.push(ResourceLogs {
            resource: Some(json_resource_to_otlp(resource.resource)),
            scope_logs,
            schema_url: String::new(),
        });
    }

    Ok(ExportLogsServiceRequest { resource_logs })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonExportLogsServiceRequest {
    #[serde(default)]
    resource_logs: Vec<JsonResourceLogs>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonResourceLogs {
    #[serde(default)]
    resource: JsonResource,
    #[serde(default)]
    scope_logs: Vec<JsonScopeLogs>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonScopeLogs {
    #[serde(default)]
    scope: JsonInstrumentationScope,
    #[serde(default)]
    log_records: Vec<JsonLogRecord>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonLogRecord {
    #[serde(default)]
    time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    observed_time_unix_nano: JsonNumberOrString,
    #[serde(default)]
    severity_number: i32,
    #[serde(default)]
    severity_text: String,
    #[serde(default)]
    body: Option<JsonAnyValue>,
    #[serde(default)]
    attributes: Vec<JsonKeyValue>,
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_json_request() {
        let body = r#"{
            "resourceLogs": [{
                "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "svc" } }]},
                "scopeLogs": [{
                    "scope": { "name": "lib", "version": "1" },
                    "logRecords": [{
                        "timeUnixNano": "123",
                        "observedTimeUnixNano": "124",
                        "severityNumber": 9,
                        "severityText": "INFO",
                        "body": { "stringValue": "hello" },
                        "attributes": [{ "key": "k", "value": { "stringValue": "v" } }],
                        "traceId": "0af7651916cd43dd8448eb211c80319c",
                        "spanId": "b7ad6b7169203331"
                    }]
                }]
            }]
        }"#;

        let request = decode_json_request(body.as_bytes()).unwrap();
        let record = &request.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(record.severity_number, 9);
        assert_eq!(record.trace_id.len(), 16);
    }
}
