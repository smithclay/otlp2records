//! Rust transformation functions for decoded OTLP records.

use bytes::Bytes;

use crate::convert::value_to_json_lossy;
use crate::value::{ObjectMap, Value};

pub fn transform_log(value: Value) -> Value {
    let input = object_or_empty(value);
    let mut out = ObjectMap::with_capacity(16);

    out.insert(
        "timestamp".into(),
        Value::Integer(nanos_to_micros(field(&input, "time_unix_nano"))),
    );
    out.insert(
        "observed_timestamp".into(),
        Value::Integer(nanos_to_micros(field(&input, "observed_time_unix_nano"))),
    );
    out.insert("trace_id".into(), string_or_null(field(&input, "trace_id")));
    out.insert("span_id".into(), string_or_null(field(&input, "span_id")));
    insert_service_fields(&mut out, &input);
    out.insert(
        "severity_number".into(),
        Value::Integer(int_or_default(field(&input, "severity_number"), 0)),
    );
    out.insert(
        "severity_text".into(),
        default_string(string_or_null(field(&input, "severity_text"))),
    );
    out.insert("body".into(), transform_body(field(&input, "body")));
    insert_resource_scope_json(&mut out, &input);
    out.insert(
        "log_attributes".into(),
        json_or_null(field(&input, "attributes")),
    );
    out.insert("_table".into(), Value::Bytes(Bytes::from_static(b"logs")));

    Value::Object(out)
}

pub fn transform_trace(value: Value) -> Value {
    let input = object_or_empty(value);
    let mut out = ObjectMap::with_capacity(26);

    out.insert(
        "timestamp".into(),
        Value::Integer(nanos_to_micros(field(&input, "start_time_unix_nano"))),
    );
    out.insert(
        "end_timestamp".into(),
        Value::Integer(nanos_to_millis(field(&input, "end_time_unix_nano"))),
    );
    out.insert(
        "duration".into(),
        Value::Integer(nanos_to_millis(field(&input, "duration_ns"))),
    );
    out.insert("trace_id".into(), string_or_null(field(&input, "trace_id")));
    out.insert("span_id".into(), string_or_null(field(&input, "span_id")));
    out.insert(
        "parent_span_id".into(),
        string_or_null(field(&input, "parent_span_id")),
    );
    out.insert(
        "trace_state".into(),
        string_or_null(field(&input, "trace_state")),
    );
    insert_service_fields(&mut out, &input);
    out.insert(
        "span_name".into(),
        default_string(string_or_null(field(&input, "name"))),
    );
    out.insert(
        "span_kind".into(),
        Value::Integer(int_or_default(field(&input, "kind"), 0)),
    );
    out.insert(
        "status_code".into(),
        Value::Integer(int_or_default(field(&input, "status_code"), 0)),
    );
    out.insert(
        "status_message".into(),
        string_or_null(field(&input, "status_message")),
    );
    insert_resource_scope_json(&mut out, &input);
    out.insert(
        "span_attributes".into(),
        json_or_null(field(&input, "attributes")),
    );
    out.insert("events_json".into(), json_or_null(field(&input, "events")));
    out.insert("links_json".into(), json_or_null(field(&input, "links")));
    out.insert(
        "dropped_attributes_count".into(),
        Value::Integer(int_or_default(field(&input, "dropped_attributes_count"), 0)),
    );
    out.insert(
        "dropped_events_count".into(),
        Value::Integer(int_or_default(field(&input, "dropped_events_count"), 0)),
    );
    out.insert(
        "dropped_links_count".into(),
        Value::Integer(int_or_default(field(&input, "dropped_links_count"), 0)),
    );
    out.insert(
        "flags".into(),
        Value::Integer(int_or_default(field(&input, "flags"), 0)),
    );
    out.insert("_table".into(), Value::Bytes(Bytes::from_static(b"traces")));

    Value::Object(out)
}

pub fn transform_gauge(value: Value) -> Value {
    let input = object_or_empty(value);
    let mut out = metric_common_fields(&input);
    out.insert("value".into(), clone_or_null(field(&input, "value")));
    insert_metric_service_and_json(&mut out, &input);
    out.insert("_table".into(), Value::Bytes(Bytes::from_static(b"gauge")));
    Value::Object(out)
}

pub fn transform_sum(value: Value) -> Value {
    let input = object_or_empty(value);
    let mut out = metric_common_fields(&input);
    out.insert("value".into(), clone_or_null(field(&input, "value")));
    insert_metric_service_and_json(&mut out, &input);
    out.insert(
        "aggregation_temporality".into(),
        Value::Integer(int_or_default(field(&input, "aggregation_temporality"), 0)),
    );
    out.insert(
        "is_monotonic".into(),
        match field(&input, "is_monotonic") {
            Some(Value::Boolean(value)) => Value::Boolean(*value),
            _ => Value::Boolean(false),
        },
    );
    out.insert("_table".into(), Value::Bytes(Bytes::from_static(b"sum")));
    Value::Object(out)
}

pub fn transform_histogram(value: Value) -> Value {
    let input = object_or_empty(value);
    let mut out = metric_common_fields(&input);
    copy_fields(
        &mut out,
        &input,
        &[
            "count",
            "sum",
            "min",
            "max",
            "bucket_counts",
            "explicit_bounds",
        ],
    );
    insert_metric_service_and_json(&mut out, &input);
    out.insert(
        "aggregation_temporality".into(),
        Value::Integer(int_or_default(field(&input, "aggregation_temporality"), 0)),
    );
    out.insert(
        "_table".into(),
        Value::Bytes(Bytes::from_static(b"histogram")),
    );
    Value::Object(out)
}

pub fn transform_exp_histogram(value: Value) -> Value {
    let input = object_or_empty(value);
    let mut out = metric_common_fields(&input);
    copy_fields(
        &mut out,
        &input,
        &[
            "count",
            "sum",
            "min",
            "max",
            "scale",
            "zero_count",
            "zero_threshold",
            "positive_offset",
            "positive_bucket_counts",
            "negative_offset",
            "negative_bucket_counts",
        ],
    );
    insert_metric_service_and_json(&mut out, &input);
    out.insert(
        "aggregation_temporality".into(),
        Value::Integer(int_or_default(field(&input, "aggregation_temporality"), 0)),
    );
    out.insert(
        "_table".into(),
        Value::Bytes(Bytes::from_static(b"exp_histogram")),
    );
    Value::Object(out)
}

fn metric_common_fields(input: &ObjectMap) -> ObjectMap {
    let mut out = ObjectMap::with_capacity(24);
    out.insert(
        "timestamp".into(),
        Value::Integer(nanos_to_micros(field(input, "time_unix_nano"))),
    );
    out.insert(
        "start_timestamp".into(),
        Value::Integer(nanos_to_millis(field(input, "start_time_unix_nano"))),
    );
    out.insert(
        "metric_name".into(),
        default_string(string_or_null(field(input, "metric_name"))),
    );
    out.insert(
        "metric_description".into(),
        default_string(string_or_null(field(input, "metric_description"))),
    );
    out.insert(
        "metric_unit".into(),
        default_string(string_or_null(field(input, "metric_unit"))),
    );
    out
}

fn insert_metric_service_and_json(out: &mut ObjectMap, input: &ObjectMap) {
    insert_service_fields(out, input);
    insert_resource_scope_json(out, input);
    out.insert(
        "metric_attributes".into(),
        json_or_null(field(input, "attributes")),
    );
    out.insert(
        "flags".into(),
        Value::Integer(int_or_default(field(input, "flags"), 0)),
    );
    out.insert(
        "exemplars_json".into(),
        json_or_null(field(input, "exemplars")),
    );
}

fn insert_service_fields(out: &mut ObjectMap, input: &ObjectMap) {
    let resource_attrs = nested_field(input, &["resource", "attributes"]);
    out.insert(
        "service_name".into(),
        get_attr(
            resource_attrs,
            "service.name",
            Value::Bytes(Bytes::from_static(b"unknown")),
        ),
    );
    out.insert(
        "service_namespace".into(),
        get_attr(resource_attrs, "service.namespace", Value::Null),
    );
    out.insert(
        "service_instance_id".into(),
        get_attr(resource_attrs, "service.instance.id", Value::Null),
    );
}

fn insert_resource_scope_json(out: &mut ObjectMap, input: &ObjectMap) {
    out.insert(
        "resource_attributes".into(),
        json_or_null(nested_field(input, &["resource", "attributes"])),
    );
    out.insert(
        "scope_name".into(),
        string_or_null(nested_field(input, &["scope", "name"])),
    );
    out.insert(
        "scope_version".into(),
        string_or_null(nested_field(input, &["scope", "version"])),
    );
    out.insert(
        "scope_attributes".into(),
        json_or_null(nested_field(input, &["scope", "attributes"])),
    );
}

fn copy_fields(out: &mut ObjectMap, input: &ObjectMap, fields: &[&str]) {
    for name in fields {
        out.insert((*name).into(), clone_or_null(field(input, name)));
    }
}

fn object_or_empty(value: Value) -> ObjectMap {
    match value {
        Value::Object(map) => map,
        _ => ObjectMap::new(),
    }
}

fn field<'a>(map: &'a ObjectMap, key: &str) -> Option<&'a Value> {
    map.get(key)
}

fn nested_field<'a>(map: &'a ObjectMap, path: &[&str]) -> Option<&'a Value> {
    let mut current = field(map, path[0])?;
    for key in &path[1..] {
        current = match deref_value(current) {
            Value::Object(map) => map.get(*key)?,
            _ => return None,
        };
    }
    Some(current)
}

fn clone_or_null(value: Option<&Value>) -> Value {
    value.cloned().unwrap_or(Value::Null)
}

fn nanos_to_micros(value: Option<&Value>) -> i64 {
    integer_like(value).unwrap_or(0) / 1_000
}

fn nanos_to_millis(value: Option<&Value>) -> i64 {
    integer_like(value).unwrap_or(0) / 1_000_000
}

fn integer_like(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Integer(value)) => Some(*value),
        Some(Value::Float(value)) => Some(value.into_inner() as i64),
        _ => None,
    }
}

fn int_or_default(value: Option<&Value>, default: i64) -> i64 {
    match value {
        Some(Value::Integer(value)) => *value,
        _ => default,
    }
}

fn string_or_null(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Bytes(bytes)) if !bytes.is_empty() => {
            Value::Bytes(Bytes::from(String::from_utf8_lossy(bytes).into_owned()))
        }
        Some(Value::Integer(value)) => Value::Bytes(Bytes::from(value.to_string())),
        Some(Value::Float(value)) => Value::Bytes(Bytes::from(value.to_string())),
        Some(Value::Boolean(value)) => Value::Bytes(Bytes::from(value.to_string())),
        _ => Value::Null,
    }
}

fn default_string(value: Value) -> Value {
    match value {
        Value::Null => Value::Bytes(Bytes::new()),
        value => value,
    }
}

fn transform_body(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Object(_)) | Some(Value::Array(_)) => {
            Value::Bytes(Bytes::from(value_to_json_lossy(value.unwrap()).to_string()))
        }
        _ => string_or_null(value),
    }
}

fn json_or_null(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Null) | None => Value::Null,
        Some(Value::Bytes(bytes)) if bytes.is_empty() => Value::Null,
        Some(Value::Array(items)) if items.is_empty() => Value::Null,
        Some(Value::Object(map)) if map.is_empty() => Value::Null,
        Some(Value::Shared(value)) => json_or_null(Some(value)),
        Some(value) => Value::Bytes(Bytes::from(value_to_json_lossy(value).to_string())),
    }
}

fn get_attr(object: Option<&Value>, key: &str, default: Value) -> Value {
    let Some(Value::Object(map)) = object.map(deref_value) else {
        return default;
    };

    match map.get(key) {
        Some(Value::Null) | None => default,
        Some(Value::Bytes(bytes)) if bytes.is_empty() => default,
        Some(value) => value.clone(),
    }
}

fn deref_value(value: &Value) -> &Value {
    match value {
        Value::Shared(value) => deref_value(value),
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ordered_float::NotNan;

    fn obj(pairs: Vec<(&str, Value)>) -> Value {
        let mut map = ObjectMap::new();
        for (key, value) in pairs {
            map.insert(key.into(), value);
        }
        Value::Object(map)
    }

    fn bytes(text: &str) -> Value {
        Value::Bytes(Bytes::from(text.to_string()))
    }

    fn float(value: f64) -> Value {
        Value::Float(NotNan::new(value).unwrap())
    }

    fn get<'a>(value: &'a Value, key: &str) -> &'a Value {
        match value {
            Value::Object(map) => map
                .get(key)
                .unwrap_or_else(|| panic!("missing field {key}")),
            _ => panic!("expected object"),
        }
    }

    // The timestamp split is deliberate and inherited from the original VRL
    // scripts: `timestamp` is nanos->micros, while `end_timestamp`/`duration`/
    // `start_timestamp` are nanos->millis. These tests lock that contract in.
    #[test]
    fn trace_timestamps_use_micros_and_millis() {
        let out = transform_trace(obj(vec![
            (
                "start_time_unix_nano",
                Value::Integer(1_703_265_600_000_000_000),
            ),
            (
                "end_time_unix_nano",
                Value::Integer(1_703_265_600_100_000_000),
            ),
            ("duration_ns", Value::Integer(100_000_000)),
        ]));
        assert_eq!(
            get(&out, "timestamp"),
            &Value::Integer(1_703_265_600_000_000)
        );
        assert_eq!(
            get(&out, "end_timestamp"),
            &Value::Integer(1_703_265_600_100)
        );
        assert_eq!(get(&out, "duration"), &Value::Integer(100));
    }

    #[test]
    fn metric_timestamps_use_micros_and_millis() {
        let out = transform_gauge(obj(vec![
            ("time_unix_nano", Value::Integer(2_000_000_000)),
            ("start_time_unix_nano", Value::Integer(1_000_000_000)),
        ]));
        assert_eq!(get(&out, "timestamp"), &Value::Integer(2_000_000));
        assert_eq!(get(&out, "start_timestamp"), &Value::Integer(1_000));
    }

    #[test]
    fn missing_timestamp_defaults_to_zero() {
        let out = transform_log(obj(vec![]));
        assert_eq!(get(&out, "timestamp"), &Value::Integer(0));
    }

    #[test]
    fn float_timestamp_is_truncated() {
        let out = transform_gauge(obj(vec![("time_unix_nano", float(2_000_000_000.0))]));
        assert_eq!(get(&out, "timestamp"), &Value::Integer(2_000_000));
    }

    #[test]
    fn service_name_defaults_to_unknown() {
        let out = transform_log(obj(vec![]));
        assert_eq!(get(&out, "service_name"), &bytes("unknown"));
        assert_eq!(get(&out, "service_namespace"), &Value::Null);
        assert_eq!(get(&out, "service_instance_id"), &Value::Null);
    }

    #[test]
    fn service_fields_extracted_from_resource() {
        let resource = obj(vec![(
            "attributes",
            obj(vec![
                ("service.name", bytes("checkout")),
                ("service.namespace", bytes("shop")),
            ]),
        )]);
        let out = transform_log(obj(vec![("resource", resource)]));
        assert_eq!(get(&out, "service_name"), &bytes("checkout"));
        assert_eq!(get(&out, "service_namespace"), &bytes("shop"));
        assert_eq!(get(&out, "service_instance_id"), &Value::Null);
    }

    #[test]
    fn scope_fields_extracted() {
        let scope = obj(vec![("name", bytes("my-scope")), ("version", bytes("1.0"))]);
        let out = transform_log(obj(vec![("scope", scope)]));
        assert_eq!(get(&out, "scope_name"), &bytes("my-scope"));
        assert_eq!(get(&out, "scope_version"), &bytes("1.0"));
    }

    #[test]
    fn empty_attributes_collapse_to_null() {
        let out = transform_log(obj(vec![("attributes", Value::Object(ObjectMap::new()))]));
        assert_eq!(get(&out, "log_attributes"), &Value::Null);
    }

    #[test]
    fn non_empty_attributes_serialize_to_json() {
        let out = transform_log(obj(vec![("attributes", obj(vec![("k", bytes("v"))]))]));
        assert_eq!(get(&out, "log_attributes"), &bytes(r#"{"k":"v"}"#));
    }

    #[test]
    fn structured_body_encoded_as_json() {
        let out = transform_log(obj(vec![("body", obj(vec![("msg", bytes("hi"))]))]));
        assert_eq!(get(&out, "body"), &bytes(r#"{"msg":"hi"}"#));
    }

    #[test]
    fn string_body_passed_through() {
        let out = transform_log(obj(vec![("body", bytes("plain message"))]));
        assert_eq!(get(&out, "body"), &bytes("plain message"));
    }

    #[test]
    fn string_field_coerces_scalars_and_nulls_empties() {
        let out = transform_log(obj(vec![
            ("severity_text", Value::Integer(5)),
            ("trace_id", Value::Bytes(Bytes::new())),
        ]));
        assert_eq!(get(&out, "severity_text"), &bytes("5"));
        assert_eq!(get(&out, "trace_id"), &Value::Null);
    }

    #[test]
    fn histogram_copies_bucket_fields() {
        let out = transform_histogram(obj(vec![
            ("count", Value::Integer(5)),
            ("bucket_counts", bytes("[1,2,2]")),
            ("explicit_bounds", bytes("[0.5,1.0]")),
            ("aggregation_temporality", Value::Integer(2)),
        ]));
        assert_eq!(get(&out, "count"), &Value::Integer(5));
        assert_eq!(get(&out, "bucket_counts"), &bytes("[1,2,2]"));
        assert_eq!(get(&out, "explicit_bounds"), &bytes("[0.5,1.0]"));
        assert_eq!(get(&out, "aggregation_temporality"), &Value::Integer(2));
        assert_eq!(get(&out, "min"), &Value::Null);
    }

    #[test]
    fn exp_histogram_copies_fields() {
        let out = transform_exp_histogram(obj(vec![
            ("scale", Value::Integer(3)),
            ("zero_count", Value::Integer(0)),
            ("positive_bucket_counts", bytes("[1,1]")),
        ]));
        assert_eq!(get(&out, "scale"), &Value::Integer(3));
        assert_eq!(get(&out, "zero_count"), &Value::Integer(0));
        assert_eq!(get(&out, "positive_bucket_counts"), &bytes("[1,1]"));
        assert_eq!(get(&out, "negative_bucket_counts"), &Value::Null);
    }

    #[test]
    fn sum_is_monotonic_defaults_to_false() {
        let out = transform_sum(obj(vec![]));
        assert_eq!(get(&out, "is_monotonic"), &Value::Boolean(false));
        assert_eq!(get(&out, "aggregation_temporality"), &Value::Integer(0));
    }

    #[test]
    fn sum_preserves_is_monotonic_and_value() {
        let out = transform_sum(obj(vec![
            ("is_monotonic", Value::Boolean(true)),
            ("value", float(1.5)),
        ]));
        assert_eq!(get(&out, "is_monotonic"), &Value::Boolean(true));
        assert_eq!(get(&out, "value"), &float(1.5));
    }

    #[test]
    fn non_object_input_yields_all_defaults() {
        let out = transform_log(Value::Null);
        assert_eq!(get(&out, "service_name"), &bytes("unknown"));
        assert_eq!(get(&out, "timestamp"), &Value::Integer(0));
    }

    #[test]
    fn transforms_tag_their_table() {
        assert_eq!(get(&transform_log(obj(vec![])), "_table"), &bytes("logs"));
        assert_eq!(
            get(&transform_trace(obj(vec![])), "_table"),
            &bytes("traces")
        );
        assert_eq!(
            get(&transform_gauge(obj(vec![])), "_table"),
            &bytes("gauge")
        );
        assert_eq!(get(&transform_sum(obj(vec![])), "_table"), &bytes("sum"));
        assert_eq!(
            get(&transform_histogram(obj(vec![])), "_table"),
            &bytes("histogram")
        );
        assert_eq!(
            get(&transform_exp_histogram(obj(vec![])), "_table"),
            &bytes("exp_histogram")
        );
    }
}
