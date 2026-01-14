//! OTLP log decoding - protobuf and JSON

use bytes::Bytes;
use const_hex::encode as hex_encode;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use serde::Deserialize;
use std::sync::Arc;
use vrl::value::{ObjectMap, Value as VrlValue};

use super::common::{
    for_each_resource_scope, json_any_value_to_vrl, json_attrs_to_value, json_resource_to_value,
    json_scope_to_value, json_timestamp_to_i64, otlp_any_value_to_vrl, otlp_attributes_to_value,
    otlp_resource_to_value, otlp_scope_to_value, safe_timestamp_conversion, DecodeError,
    JsonAnyValue, JsonInstrumentationScope, JsonKeyValue, JsonNumberOrString, JsonResource,
};

// ============================================================================
// Protobuf decoding
// ============================================================================

pub fn decode_protobuf(body: &[u8]) -> Result<Vec<VrlValue>, DecodeError> {
    let request = ExportLogsServiceRequest::decode(body)?;
    export_logs_to_vrl_proto(request)
}

fn export_logs_to_vrl_proto(
    request: ExportLogsServiceRequest,
) -> Result<Vec<VrlValue>, DecodeError> {
    let mut values = preallocate_log_values(&request.resource_logs, |rl| {
        rl.scope_logs.iter().map(|sl| sl.log_records.len()).sum()
    });

    for_each_resource_scope(
        request.resource_logs,
        |resource_logs| {
            (
                otlp_resource_to_value(resource_logs.resource.as_ref()),
                resource_logs.scope_logs,
            )
        },
        |scope_logs| {
            (
                otlp_scope_to_value(scope_logs.scope.as_ref()),
                scope_logs.log_records,
            )
        },
        |log_records, resource, scope| {
            for log_record in log_records {
                let body = log_record
                    .body
                    .as_ref()
                    .map(otlp_any_value_to_vrl)
                    .unwrap_or(VrlValue::Null);

                let parts = LogRecordParts {
                    time_unix_nano: safe_timestamp_conversion(
                        log_record.time_unix_nano,
                        "log.time_unix_nano",
                    )?,
                    observed_time_unix_nano: safe_timestamp_conversion(
                        log_record.observed_time_unix_nano,
                        "log.observed_time_unix_nano",
                    )?,
                    severity_number: log_record.severity_number as i64,
                    severity_text: Bytes::from(log_record.severity_text),
                    body,
                    trace_id: Bytes::from(hex_encode(&log_record.trace_id)),
                    span_id: Bytes::from(hex_encode(&log_record.span_id)),
                    attributes: otlp_attributes_to_value(&log_record.attributes),
                    resource: Arc::clone(&resource),
                    scope: Arc::clone(&scope),
                };

                values.push(build_log_record(parts));
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
    let request: JsonExportLogsServiceRequest = serde_json::from_slice(&normalized)?;
    export_logs_json_to_vrl(request)
}

fn export_logs_json_to_vrl(
    request: JsonExportLogsServiceRequest,
) -> Result<Vec<VrlValue>, DecodeError> {
    let mut values = preallocate_log_values(&request.resource_logs, |rl| {
        rl.scope_logs.iter().map(|sl| sl.log_records.len()).sum()
    });

    for_each_resource_scope(
        request.resource_logs,
        |resource_logs| {
            (
                json_resource_to_value(resource_logs.resource),
                resource_logs.scope_logs,
            )
        },
        |scope_logs| {
            (
                json_scope_to_value(scope_logs.scope),
                scope_logs.log_records,
            )
        },
        |log_records, resource, scope| {
            for log_record in log_records {
                let body = log_record
                    .body
                    .map(json_any_value_to_vrl)
                    .unwrap_or(VrlValue::Null);

                let parts = LogRecordParts {
                    time_unix_nano: json_timestamp_to_i64(
                        &log_record.time_unix_nano,
                        "log.time_unix_nano",
                    )?,
                    observed_time_unix_nano: json_timestamp_to_i64(
                        &log_record.observed_time_unix_nano,
                        "log.observed_time_unix_nano",
                    )?,
                    severity_number: log_record.severity_number as i64,
                    severity_text: Bytes::from(log_record.severity_text),
                    body,
                    trace_id: Bytes::from(log_record.trace_id),
                    span_id: Bytes::from(log_record.span_id),
                    attributes: json_attrs_to_value(log_record.attributes),
                    resource: Arc::clone(&resource),
                    scope: Arc::clone(&scope),
                };

                values.push(build_log_record(parts));
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

// ============================================================================
// Record builder
// ============================================================================

/// Precomputed fields for building a log record into VRL values
struct LogRecordParts {
    time_unix_nano: i64,
    observed_time_unix_nano: i64,
    severity_number: i64,
    severity_text: Bytes,
    body: VrlValue,
    trace_id: Bytes,
    span_id: Bytes,
    attributes: VrlValue,
    resource: Arc<VrlValue>,
    scope: Arc<VrlValue>,
}

/// Pre-allocate a values Vec sized to the number of log records a request contains
fn preallocate_log_values<R, F>(resource_logs: &[R], count_logs: F) -> Vec<VrlValue>
where
    F: Fn(&R) -> usize,
{
    let capacity: usize = resource_logs.iter().map(&count_logs).sum();
    Vec::with_capacity(capacity)
}

/// Build a VRL-ready log record from parts
fn build_log_record(parts: LogRecordParts) -> VrlValue {
    let mut map = ObjectMap::new();
    map.insert(
        "time_unix_nano".into(),
        VrlValue::Integer(parts.time_unix_nano),
    );
    map.insert(
        "observed_time_unix_nano".into(),
        VrlValue::Integer(parts.observed_time_unix_nano),
    );
    map.insert(
        "severity_number".into(),
        VrlValue::Integer(parts.severity_number),
    );
    map.insert("severity_text".into(), VrlValue::Bytes(parts.severity_text));
    map.insert("body".into(), parts.body);
    map.insert("trace_id".into(), VrlValue::Bytes(parts.trace_id));
    map.insert("span_id".into(), VrlValue::Bytes(parts.span_id));
    map.insert("attributes".into(), parts.attributes);
    map.insert("resource".into(), (*parts.resource).clone());
    map.insert("scope".into(), (*parts.scope).clone());
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
        logs::v1::LogRecord,
        resource::v1::Resource,
    };

    #[test]
    fn decodes_json_payload() {
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

        let records = decode_json(body.as_bytes()).unwrap();
        assert_eq!(records.len(), 1);

        let record = &records[0];
        if let VrlValue::Object(map) = record {
            assert_eq!(map.get("severity_number"), Some(&VrlValue::Integer(9)));
            assert_eq!(
                map.get("severity_text"),
                Some(&VrlValue::Bytes(Bytes::from("INFO")))
            );
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn decodes_protobuf_payload() {
        let log = LogRecord {
            time_unix_nano: 123,
            observed_time_unix_nano: 124,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("hello".to_string())),
            }),
            attributes: vec![KeyValue {
                key: "k".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("v".to_string())),
                }),
            }],
            trace_id: vec![0, 1, 2, 3],
            span_id: vec![4, 5, 6, 7],
            ..Default::default()
        };

        let request = ExportLogsServiceRequest {
            resource_logs: vec![opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![],
                    ..Default::default()
                }),
                scope_logs: vec![opentelemetry_proto::tonic::logs::v1::ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "lib".to_string(),
                        version: "1".to_string(),
                        ..Default::default()
                    }),
                    log_records: vec![log],
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
            assert_eq!(map.get("time_unix_nano"), Some(&VrlValue::Integer(123)));
            // trace_id should be hex encoded
            assert_eq!(
                map.get("trace_id"),
                Some(&VrlValue::Bytes(Bytes::from("00010203")))
            );
        } else {
            panic!("expected object");
        }
    }

    #[test]
    fn rejects_overflow_log_time() {
        let log = LogRecord {
            time_unix_nano: u64::MAX,
            observed_time_unix_nano: 124,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("hello".to_string())),
            }),
            ..Default::default()
        };

        let request = ExportLogsServiceRequest {
            resource_logs: vec![opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                resource: Some(Resource::default()),
                scope_logs: vec![opentelemetry_proto::tonic::logs::v1::ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: vec![log],
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
                assert!(msg.contains("time_unix_nano"));
            }
            _ => panic!("Expected DecodeError::Unsupported, got: {err:?}"),
        }
    }

    #[test]
    fn rejects_overflow_log_observed_time() {
        let log = LogRecord {
            time_unix_nano: 123,
            observed_time_unix_nano: u64::MAX,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("hello".to_string())),
            }),
            ..Default::default()
        };

        let request = ExportLogsServiceRequest {
            resource_logs: vec![opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                resource: Some(Resource::default()),
                scope_logs: vec![opentelemetry_proto::tonic::logs::v1::ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: vec![log],
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
                assert!(msg.contains("observed_time_unix_nano"));
            }
            _ => panic!("Expected DecodeError::Unsupported, got: {err:?}"),
        }
    }

    #[test]
    fn accepts_valid_log_timestamps() {
        let log = LogRecord {
            time_unix_nano: 123,
            observed_time_unix_nano: 124,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(any_value::Value::StringValue("hello".to_string())),
            }),
            ..Default::default()
        };

        let request = ExportLogsServiceRequest {
            resource_logs: vec![opentelemetry_proto::tonic::logs::v1::ResourceLogs {
                resource: Some(Resource::default()),
                scope_logs: vec![opentelemetry_proto::tonic::logs::v1::ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: vec![log],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let body = request.encode_to_vec();
        let result = decode_protobuf(&body);

        assert!(result.is_ok());
        let logs = result.unwrap();
        assert_eq!(logs.len(), 1);
    }

    #[test]
    fn handles_empty_log_request() {
        let request = ExportLogsServiceRequest {
            resource_logs: vec![],
        };

        let body = request.encode_to_vec();
        let records = decode_protobuf(&body).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn handles_empty_json_request() {
        let body = r#"{"resourceLogs": []}"#;
        let records = decode_json(body.as_bytes()).unwrap();
        assert!(records.is_empty());
    }
}
