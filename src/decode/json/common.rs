//! Common utilities shared across OTLP JSON decoders.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use opentelemetry_proto::tonic::{
    common::v1::{any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList},
    resource::v1::Resource,
};
use serde::Deserialize;

/// Errors that can occur during OTLP decoding.
#[derive(Debug)]
pub enum DecodeError {
    /// JSON deserialization failed.
    Json(serde_json::Error),
    /// Protobuf decoding failed.
    Protobuf(prost::DecodeError),
    /// General parse error such as UTF-8 or JSONL line errors.
    Parse(String),
    /// Unsupported or invalid payload.
    Unsupported(String),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Json(e) => write!(f, "JSON decode error: {e}"),
            DecodeError::Protobuf(e) => write!(f, "protobuf decode error: {e}"),
            DecodeError::Parse(msg) => write!(f, "parse error: {msg}"),
            DecodeError::Unsupported(msg) => write!(f, "unsupported payload: {msg}"),
        }
    }
}

impl std::error::Error for DecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DecodeError::Json(e) => Some(e),
            DecodeError::Protobuf(e) => Some(e),
            DecodeError::Parse(_) | DecodeError::Unsupported(_) => None,
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

/// Quick heuristic to detect whether a payload looks like JSON.
pub fn looks_like_json(body: &[u8]) -> bool {
    body.iter()
        .find(|b| !b.is_ascii_whitespace())
        .map(|b| *b == b'{' || *b == b'[')
        .unwrap_or(false)
}

/// Decode a bytes field that may be hex, base64, or raw string.
pub fn decode_bytes_field(encoded: &str) -> Vec<u8> {
    hex_to_bytes(encoded)
        .or_else(|| BASE64.decode(encoded.as_bytes()).ok())
        .unwrap_or_else(|| encoded.as_bytes().to_vec())
}

/// Convert hex string to bytes.
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

fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// JSON number or string for OTLP integer fields.
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

/// Convert JSON timestamp to u64 while preserving the existing i64 overflow guard.
pub fn json_timestamp_to_u64(value: &JsonNumberOrString, field: &str) -> Result<u64, DecodeError> {
    match value {
        JsonNumberOrString::Missing => Ok(0),
        JsonNumberOrString::String(s) => {
            let parsed = s.parse::<i128>().map_err(|_| {
                DecodeError::Unsupported(format!(
                    "invalid timestamp: {field} value {s} is not an integer"
                ))
            })?;
            if parsed < 0 {
                return Err(DecodeError::Unsupported(format!(
                    "invalid timestamp: {field} value {s} is negative"
                )));
            }
            if parsed > i64::MAX as i128 {
                return Err(DecodeError::Unsupported(format!(
                    "timestamp overflow: {field} value {s} exceeds i64::MAX (year 2262)"
                )));
            }
            Ok(parsed as u64)
        }
        JsonNumberOrString::Number(n) => {
            if let Some(i) = n.as_i64() {
                if i < 0 {
                    return Err(DecodeError::Unsupported(format!(
                        "invalid timestamp: {field} value {n} is negative"
                    )));
                }
                Ok(i as u64)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).map(|_| u).map_err(|_| {
                    DecodeError::Unsupported(format!(
                        "timestamp overflow: {field} value {u} exceeds i64::MAX (year 2262)"
                    ))
                })
            } else {
                Err(DecodeError::Unsupported(format!(
                    "invalid timestamp: {field} value {n} is not an integer"
                )))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonKeyValue {
    pub key: String,
    #[serde(default)]
    pub value: Option<JsonAnyValue>,
}

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

#[derive(Debug, Default, Deserialize)]
pub struct JsonArrayValue {
    #[serde(default)]
    pub values: Vec<JsonAnyValue>,
}

#[derive(Debug, Default, Deserialize)]
pub struct JsonKvlistValue {
    #[serde(default)]
    pub values: Vec<JsonKeyValue>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonResource {
    #[serde(default)]
    pub attributes: Vec<JsonKeyValue>,
}

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

pub fn json_any_value_to_otlp(av: JsonAnyValue) -> AnyValue {
    let value = if let Some(s) = av.string_value {
        Some(any_value::Value::StringValue(s))
    } else if let Some(i) = av.int_value {
        i.as_i64().map(any_value::Value::IntValue)
    } else if let Some(d) = av.double_value {
        Some(any_value::Value::DoubleValue(d))
    } else if let Some(b) = av.bool_value {
        Some(any_value::Value::BoolValue(b))
    } else if let Some(arr) = av.array_value {
        Some(any_value::Value::ArrayValue(ArrayValue {
            values: arr.values.into_iter().map(json_any_value_to_otlp).collect(),
        }))
    } else if let Some(kv) = av.kvlist_value {
        Some(any_value::Value::KvlistValue(KeyValueList {
            values: json_attrs_to_otlp(kv.values),
        }))
    } else {
        av.bytes_value
            .map(|bytes| any_value::Value::BytesValue(decode_bytes_field(&bytes)))
    };

    AnyValue { value }
}

pub fn json_attrs_to_otlp(attrs: Vec<JsonKeyValue>) -> Vec<KeyValue> {
    attrs
        .into_iter()
        .map(|kv| KeyValue {
            key: kv.key,
            value: kv.value.map(json_any_value_to_otlp),
        })
        .collect()
}

pub fn json_resource_to_otlp(resource: JsonResource) -> Resource {
    Resource {
        attributes: json_attrs_to_otlp(resource.attributes),
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    }
}

pub fn json_scope_to_otlp(scope: JsonInstrumentationScope) -> InstrumentationScope {
    InstrumentationScope {
        name: scope.name,
        version: scope.version,
        attributes: json_attrs_to_otlp(scope.attributes),
        dropped_attributes_count: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_timestamp_missing_defaults_to_zero() {
        let value = JsonNumberOrString::Missing;
        assert_eq!(json_timestamp_to_u64(&value, "ts").unwrap(), 0);
    }

    #[test]
    fn json_timestamp_rejects_negative() {
        let value = JsonNumberOrString::String("-1".to_string());
        assert!(json_timestamp_to_u64(&value, "ts").is_err());
    }

    #[test]
    fn json_timestamp_rejects_float() {
        let num = serde_json::Number::from_f64(1.5).unwrap();
        let value = JsonNumberOrString::Number(num);
        assert!(json_timestamp_to_u64(&value, "ts").is_err());
    }

    #[test]
    fn decode_bytes_field_handles_hex() {
        assert_eq!(decode_bytes_field("0102030405"), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn decode_bytes_field_handles_base64() {
        assert_eq!(decode_bytes_field("SGVsbG8="), b"Hello".to_vec());
    }
}
