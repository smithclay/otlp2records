use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use otlp2records::{logs_schema, transform_logs, InputFormat};
use prost::Message;

fn value(value: any_value::Value) -> AnyValue {
    AnyValue { value: Some(value) }
}

fn attribute(key: &str, value: any_value::Value) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(self::value(value)),
    }
}

fn request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![
                    attribute(
                        "service.name",
                        any_value::Value::StringValue("checkout".to_string()),
                    ),
                    attribute(
                        "service.namespace",
                        any_value::Value::StringValue("store".to_string()),
                    ),
                    attribute("resource.int", any_value::Value::IntValue(7)),
                ],
                dropped_attributes_count: 1,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "sdk".to_string(),
                    version: "1.2.3".to_string(),
                    attributes: vec![attribute("scope.bool", any_value::Value::BoolValue(true))],
                    dropped_attributes_count: 2,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    observed_time_unix_nano: 1_700_000_000_000_000_100,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(value(any_value::Value::KvlistValue(KeyValueList {
                        values: vec![
                            attribute("message", any_value::Value::StringValue("paid".to_string())),
                            attribute(
                                "items",
                                any_value::Value::ArrayValue(ArrayValue {
                                    values: vec![
                                        value(any_value::Value::IntValue(2)),
                                        value(any_value::Value::DoubleValue(3.5)),
                                    ],
                                }),
                            ),
                        ],
                    }))),
                    attributes: vec![
                        attribute("string", any_value::Value::StringValue("v".to_string())),
                        attribute("integer", any_value::Value::IntValue(-4)),
                        attribute("double", any_value::Value::DoubleValue(1.25)),
                        attribute("boolean", any_value::Value::BoolValue(false)),
                        attribute("bytes", any_value::Value::BytesValue(b"raw".to_vec())),
                    ],
                    dropped_attributes_count: 0,
                    flags: 0,
                    trace_id: (0_u8..16).collect(),
                    span_id: (16_u8..24).collect(),
                    event_name: "order.paid".to_string(),
                }],
                schema_url: "scope-schema".to_string(),
            }],
            schema_url: "resource-schema".to_string(),
        }],
    }
}

#[test]
fn protobuf_and_json_logs_share_the_same_view_normalization() {
    let request = request();
    let protobuf = transform_logs(&request.encode_to_vec(), InputFormat::Protobuf).unwrap();

    let json = br#"{
      "resourceLogs": [{
        "resource": {
          "attributes": [
            {"key":"service.name","value":{"stringValue":"checkout"}},
            {"key":"service.namespace","value":{"stringValue":"store"}},
            {"key":"resource.int","value":{"intValue":"7"}}
          ],
          "droppedAttributesCount": 1
        },
        "schemaUrl": "resource-schema",
        "scopeLogs": [{
          "scope": {
            "name": "sdk",
            "version": "1.2.3",
            "attributes": [{"key":"scope.bool","value":{"boolValue":true}}],
            "droppedAttributesCount": 2
          },
          "schemaUrl": "scope-schema",
          "logRecords": [{
            "timeUnixNano": "1700000000000000000",
            "observedTimeUnixNano": "1700000000000000100",
            "severityNumber": 9,
            "severityText": "INFO",
            "body": {"kvlistValue":{"values":[
              {"key":"message","value":{"stringValue":"paid"}},
              {"key":"items","value":{"arrayValue":{"values":[
                {"intValue":"2"},{"doubleValue":3.5}
              ]}}}
            ]}},
            "attributes": [
              {"key":"string","value":{"stringValue":"v"}},
              {"key":"integer","value":{"intValue":"-4"}},
              {"key":"double","value":{"doubleValue":1.25}},
              {"key":"boolean","value":{"boolValue":false}},
              {"key":"bytes","value":{"bytesValue":"cmF3"}}
            ],
            "traceId": "AAECAwQFBgcICQoLDA0ODw==",
            "spanId": "EBESExQVFhc=",
            "eventName": "order.paid"
          }]
        }]
      }]
    }"#;
    let json = transform_logs(json, InputFormat::Json).unwrap();

    assert_eq!(protobuf.schema().as_ref(), &logs_schema());
    assert_eq!(json.schema().as_ref(), &logs_schema());
    assert_eq!(protobuf, json);
}

#[test]
fn view_contract_rejects_all_zero_otlp_identifiers() {
    let mut request = request();
    let record = &mut request.resource_logs[0].scope_logs[0].log_records[0];
    record.trace_id = vec![0; 16];
    record.span_id = vec![0; 8];

    let batch = transform_logs(&request.encode_to_vec(), InputFormat::Protobuf).unwrap();
    assert_eq!(batch.column_by_name("trace_id").unwrap().null_count(), 1);
    assert_eq!(batch.column_by_name("span_id").unwrap().null_count(), 1);
}
