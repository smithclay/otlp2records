//! Common utilities shared across OTLP decoders

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bytes::Bytes;
use opentelemetry_proto::tonic::common::v1::{
    any_value, AnyValue, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ordered_float::NotNan;
use serde::Deserialize;
use std::sync::Arc;
use vrl::value::{KeyString, ObjectMap, Value as VrlValue};

// ============================================================================
// Error types
// ============================================================================

/// Errors that can occur during OTLP decoding
#[derive(Debug)]
pub enum DecodeError {
    /// JSON deserialization failed
    Json(serde_json::Error),
    /// Protobuf decoding failed
    Protobuf(prost::DecodeError),
    /// General parse error (e.g., UTF-8, JSONL line errors)
    Parse(String),
    /// Unsupported or invalid payload
    Unsupported(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Json(e) => write!(f, "JSON decode error: {}", e),
            DecodeError::Protobuf(e) => write!(f, "protobuf decode error: {}", e),
            DecodeError::Parse(msg) => write!(f, "parse error: {}", msg),
            DecodeError::Unsupported(msg) => write!(f, "unsupported payload: {}", msg),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DecodeError::Json(e) => Some(e),
            DecodeError::Protobuf(e) => Some(e),
            DecodeError::Parse(_) => None,
            DecodeError::Unsupported(_) => None,
        }
    }
}

impl From<serde_json::Error> for DecodeError {
    fn from(e: serde_json::Error) -> Self {
        DecodeError::Json(e)
    }
}

impl From<prost::DecodeError> for DecodeError {
    fn from(e: prost::DecodeError) -> Self {
        DecodeError::Protobuf(e)
    }
}

// ============================================================================
// Float handling
// ============================================================================

/// Convert f64 into VRL Value, dropping non-finite numbers consistently
pub fn finite_float_to_vrl(value: f64) -> VrlValue {
    if value.is_nan() || value.is_infinite() {
        VrlValue::Null
    } else {
        // Use unwrap_or_else for defensive handling of edge cases
        match NotNan::new(value) {
            Ok(n) => VrlValue::Float(n),
            Err(_) => VrlValue::Null,
        }
    }
}

// ============================================================================
// Protobuf utilities
// ============================================================================

/// Convert protobuf Resource to VRL Value
pub fn otlp_resource_to_value(resource: Option<&Resource>) -> VrlValue {
    let mut map = ObjectMap::new();
    let attributes = resource
        .map(|res| otlp_attributes_to_value(&res.attributes))
        .unwrap_or_else(|| VrlValue::Object(ObjectMap::new()));
    map.insert("attributes".into(), attributes);
    VrlValue::Object(map)
}

/// Convert protobuf InstrumentationScope to VRL Value
pub fn otlp_scope_to_value(scope: Option<&InstrumentationScope>) -> VrlValue {
    let mut map = ObjectMap::new();
    if let Some(scope) = scope {
        map.insert(
            "name".into(),
            VrlValue::Bytes(Bytes::from(scope.name.clone())),
        );
        map.insert(
            "version".into(),
            VrlValue::Bytes(Bytes::from(scope.version.clone())),
        );
        map.insert(
            "attributes".into(),
            otlp_attributes_to_value(&scope.attributes),
        );
    } else {
        map.insert("name".into(), VrlValue::Bytes(Bytes::new()));
        map.insert("version".into(), VrlValue::Bytes(Bytes::new()));
        map.insert("attributes".into(), VrlValue::Object(ObjectMap::new()));
    }
    VrlValue::Object(map)
}

/// Convert protobuf KeyValue array to VRL Object
pub fn otlp_attributes_to_value(attrs: &[KeyValue]) -> VrlValue {
    let map: ObjectMap = attrs
        .iter()
        .filter_map(|kv| {
            kv.value
                .as_ref()
                .map(|v| (KeyString::from(kv.key.clone()), otlp_any_value_to_vrl(v)))
        })
        .collect();
    VrlValue::Object(map)
}

/// Convert protobuf AnyValue to VRL Value
pub fn otlp_any_value_to_vrl(av: &AnyValue) -> VrlValue {
    match av.value.as_ref() {
        Some(any_value::Value::StringValue(s)) => VrlValue::Bytes(Bytes::from(s.clone())),
        Some(any_value::Value::BoolValue(b)) => VrlValue::Boolean(*b),
        Some(any_value::Value::IntValue(i)) => VrlValue::Integer(*i),
        Some(any_value::Value::DoubleValue(d)) => finite_float_to_vrl(*d),
        Some(any_value::Value::ArrayValue(arr)) => {
            VrlValue::Array(arr.values.iter().map(otlp_any_value_to_vrl).collect())
        }
        Some(any_value::Value::KvlistValue(kvlist)) => kvlist_to_object(kvlist),
        Some(any_value::Value::BytesValue(bytes)) => VrlValue::Bytes(Bytes::from(bytes.clone())),
        None => VrlValue::Null,
    }
}

fn kvlist_to_object(kvlist: &KeyValueList) -> VrlValue {
    let map: ObjectMap = kvlist
        .values
        .iter()
        .filter_map(|kv| {
            kv.value
                .as_ref()
                .map(|v| (KeyString::from(kv.key.clone()), otlp_any_value_to_vrl(v)))
        })
        .collect();
    VrlValue::Object(map)
}

/// Safely convert u64 timestamp to i64, returning error on overflow
pub fn safe_timestamp_conversion(timestamp: u64, field_name: &str) -> Result<i64, DecodeError> {
    i64::try_from(timestamp).map_err(|_| {
        DecodeError::Unsupported(format!(
            "timestamp overflow: {} value {} exceeds i64::MAX (year 2262)",
            field_name, timestamp
        ))
    })
}

/// Traverse OTLP resources and scopes, reusing resource/scope VRL values via Arc.
pub fn for_each_resource_scope<R, S, T, I, J, RF, SF, CF, E>(
    resources: I,
    mut split: RF,
    mut split_scope: SF,
    mut callback: CF,
) -> Result<(), E>
where
    I: IntoIterator<Item = R>,
    J: IntoIterator<Item = S>,
    RF: FnMut(R) -> (VrlValue, J),
    SF: FnMut(S) -> (VrlValue, T),
    CF: FnMut(T, Arc<VrlValue>, Arc<VrlValue>) -> Result<(), E>,
{
    for resource in resources {
        let (resource_value, scopes) = split(resource);
        let resource_value = Arc::new(resource_value);

        for scope in scopes {
            let (scope_value, payload) = split_scope(scope);
            let scope_value = Arc::new(scope_value);
            callback(
                payload,
                Arc::clone(&resource_value),
                Arc::clone(&scope_value),
            )?;
        }
    }

    Ok(())
}

// ============================================================================
// JSON utilities
// ============================================================================

/// Quick heuristic to detect whether a payload looks like JSON.
pub fn looks_like_json(body: &[u8]) -> bool {
    body.iter()
        .find(|b| !b.is_ascii_whitespace())
        .map(|b| *b == b'{' || *b == b'[')
        .unwrap_or(false)
}

/// Decode a bytes field that may be hex, base64, or raw string
pub fn decode_bytes_field(encoded: &str) -> Vec<u8> {
    hex_to_bytes(encoded)
        .or_else(|| BASE64.decode(encoded.as_bytes()).ok())
        .unwrap_or_else(|| encoded.as_bytes().to_vec())
}

/// Convert hex string to bytes
pub fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) || hex.is_empty() {
        return None;
    }

    let mut out = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        let hi = from_hex(pair[0])?;
        let lo = from_hex(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

/// Convert a single hex character to its numeric value
fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// JSON number or string (for timestamps that may overflow JSON number precision)
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(untagged)]
pub enum JsonNumberOrString {
    String(String),
    Number(serde_json::Number),
    #[default]
    Missing,
}

impl JsonNumberOrString {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            JsonNumberOrString::String(s) => s.parse().ok(),
            JsonNumberOrString::Number(n) => n.as_i64(),
            JsonNumberOrString::Missing => None,
        }
    }
}

/// Convert JSON timestamp (string or number) to i64, rejecting invalid values.
/// Missing values default to 0 to align with protobuf default semantics.
pub fn json_timestamp_to_i64(value: &JsonNumberOrString, field: &str) -> Result<i64, DecodeError> {
    match value {
        JsonNumberOrString::Missing => Ok(0),
        JsonNumberOrString::String(s) => {
            let parsed = s.parse::<i128>().map_err(|_| {
                DecodeError::Unsupported(format!(
                    "invalid timestamp: {} value {} is not an integer",
                    field, s
                ))
            })?;
            if parsed < 0 {
                return Err(DecodeError::Unsupported(format!(
                    "invalid timestamp: {} value {} is negative",
                    field, s
                )));
            }
            if parsed > i64::MAX as i128 {
                return Err(DecodeError::Unsupported(format!(
                    "timestamp overflow: {} value {} exceeds i64::MAX (year 2262)",
                    field, s
                )));
            }
            Ok(parsed as i64)
        }
        JsonNumberOrString::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i < 0 {
                    return Err(DecodeError::Unsupported(format!(
                        "invalid timestamp: {} value {} is negative",
                        field, n
                    )));
                }
                Ok(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).map_err(|_| {
                    DecodeError::Unsupported(format!(
                        "timestamp overflow: {} value {} exceeds i64::MAX (year 2262)",
                        field, u
                    ))
                })
            } else {
                Err(DecodeError::Unsupported(format!(
                    "invalid timestamp: {} value {} is not an integer",
                    field, n
                )))
            }
        }
    }
}

/// JSON key-value pair
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonKeyValue {
    pub key: String,
    #[serde(default)]
    pub value: Option<JsonAnyValue>,
}

/// JSON any value (union of all possible OTLP value types)
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonAnyValue {
    #[serde(default)]
    pub string_value: Option<String>,
    #[serde(default)]
    pub int_value: Option<JsonNumberOrString>,
    #[serde(default)]
    pub double_value: Option<f64>,
    #[serde(default)]
    pub bool_value: Option<bool>,
    #[serde(default)]
    pub array_value: Option<JsonArrayValue>,
    #[serde(default)]
    pub kvlist_value: Option<JsonKvlistValue>,
    #[serde(default)]
    pub bytes_value: Option<String>,
}

/// JSON array value
#[derive(Debug, Default, Deserialize)]
pub struct JsonArrayValue {
    #[serde(default)]
    pub values: Vec<JsonAnyValue>,
}

/// JSON key-value list
#[derive(Debug, Default, Deserialize)]
pub struct JsonKvlistValue {
    #[serde(default)]
    pub values: Vec<JsonKeyValue>,
}

/// JSON resource
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonResource {
    #[serde(default)]
    pub attributes: Vec<JsonKeyValue>,
}

/// JSON instrumentation scope
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonInstrumentationScope {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub attributes: Vec<JsonKeyValue>,
}

/// Convert JSON AnyValue to VRL Value
pub fn json_any_value_to_vrl(av: JsonAnyValue) -> VrlValue {
    if let Some(s) = av.string_value {
        VrlValue::Bytes(Bytes::from(s))
    } else if let Some(i) = av.int_value {
        i.as_i64().map(VrlValue::Integer).unwrap_or(VrlValue::Null)
    } else if let Some(d) = av.double_value {
        finite_float_to_vrl(d)
    } else if let Some(b) = av.bool_value {
        VrlValue::Boolean(b)
    } else if let Some(arr) = av.array_value {
        VrlValue::Array(arr.values.into_iter().map(json_any_value_to_vrl).collect())
    } else if let Some(kv) = av.kvlist_value {
        json_attrs_to_value(kv.values)
    } else if let Some(bytes) = av.bytes_value {
        VrlValue::Bytes(Bytes::from(decode_bytes_field(&bytes)))
    } else {
        VrlValue::Null
    }
}

/// Convert JSON attributes to VRL Value
pub fn json_attrs_to_value(attrs: Vec<JsonKeyValue>) -> VrlValue {
    let map: ObjectMap = attrs
        .into_iter()
        .filter_map(|kv| kv.value.map(|v| (kv.key.into(), json_any_value_to_vrl(v))))
        .collect();
    VrlValue::Object(map)
}

/// Convert JSON resource to VRL Value
pub fn json_resource_to_value(resource: JsonResource) -> VrlValue {
    let mut map = ObjectMap::new();
    map.insert(
        "attributes".into(),
        json_attrs_to_value(resource.attributes),
    );
    VrlValue::Object(map)
}

/// Convert JSON instrumentation scope to VRL Value
pub fn json_scope_to_value(scope: JsonInstrumentationScope) -> VrlValue {
    let mut map = ObjectMap::new();
    map.insert("name".into(), VrlValue::Bytes(Bytes::from(scope.name)));
    map.insert(
        "version".into(),
        VrlValue::Bytes(Bytes::from(scope.version)),
    );
    map.insert("attributes".into(), json_attrs_to_value(scope.attributes));
    VrlValue::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finite_float_handles_normal_values() {
        let result = finite_float_to_vrl(42.5);
        assert!(matches!(result, VrlValue::Float(_)));
    }

    #[test]
    fn finite_float_handles_nan() {
        let result = finite_float_to_vrl(f64::NAN);
        assert!(matches!(result, VrlValue::Null));
    }

    #[test]
    fn finite_float_handles_infinity() {
        let result = finite_float_to_vrl(f64::INFINITY);
        assert!(matches!(result, VrlValue::Null));

        let result = finite_float_to_vrl(f64::NEG_INFINITY);
        assert!(matches!(result, VrlValue::Null));
    }

    #[test]
    fn json_timestamp_missing_defaults_to_zero() {
        let value = JsonNumberOrString::Missing;
        assert_eq!(json_timestamp_to_i64(&value, "ts").unwrap(), 0);
    }

    #[test]
    fn json_timestamp_rejects_negative() {
        let value = JsonNumberOrString::String("-1".to_string());
        assert!(json_timestamp_to_i64(&value, "ts").is_err());
    }

    #[test]
    fn json_timestamp_rejects_float() {
        let num = serde_json::Number::from_f64(1.5).unwrap();
        let value = JsonNumberOrString::Number(num);
        assert!(json_timestamp_to_i64(&value, "ts").is_err());
    }

    #[test]
    fn json_timestamp_rejects_overflow() {
        let value = JsonNumberOrString::String((i64::MAX as i128 + 1).to_string());
        assert!(json_timestamp_to_i64(&value, "ts").is_err());
    }

    #[test]
    fn hex_to_bytes_works() {
        assert_eq!(hex_to_bytes("0102"), Some(vec![1, 2]));
        assert_eq!(hex_to_bytes("abcd"), Some(vec![0xab, 0xcd]));
        assert_eq!(hex_to_bytes(""), None);
        assert_eq!(hex_to_bytes("123"), None); // odd length
        assert_eq!(hex_to_bytes("gg"), None); // invalid chars
    }

    #[test]
    fn decode_bytes_field_handles_hex() {
        assert_eq!(decode_bytes_field("0102030405"), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn decode_bytes_field_handles_base64() {
        assert_eq!(decode_bytes_field("SGVsbG8="), b"Hello".to_vec());
    }

    #[test]
    fn decode_bytes_field_handles_raw_string() {
        assert_eq!(decode_bytes_field("hello"), b"hello".to_vec());
    }

    #[test]
    fn decode_error_display() {
        let err = DecodeError::Unsupported("test".into());
        assert_eq!(format!("{}", err), "unsupported payload: test");
    }

    #[test]
    fn safe_timestamp_accepts_valid() {
        assert_eq!(safe_timestamp_conversion(123, "test").unwrap(), 123);
        assert_eq!(
            safe_timestamp_conversion(i64::MAX as u64, "test").unwrap(),
            i64::MAX
        );
    }

    #[test]
    fn safe_timestamp_rejects_overflow() {
        let result = safe_timestamp_conversion(u64::MAX, "test");
        assert!(result.is_err());
    }
}
