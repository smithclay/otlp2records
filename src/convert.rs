//! Shared conversion utilities for record values to JSON.

use crate::value::Value;

/// Convert a record value to serde_json::Value.
/// Returns None for values that cannot be represented in JSON (NaN, Infinity).
/// Null values in objects are omitted (not serialized).
pub fn value_to_json(v: &Value) -> Option<serde_json::Value> {
    match v {
        Value::Bytes(b) => Some(serde_json::Value::String(
            String::from_utf8_lossy(b).to_string(),
        )),
        Value::Integer(i) => Some(serde_json::Value::Number((*i).into())),
        Value::Float(f) => {
            let inner = f.into_inner();
            serde_json::Number::from_f64(inner).map(serde_json::Value::Number)
        }
        Value::Boolean(b) => Some(serde_json::Value::Bool(*b)),
        Value::Null => Some(serde_json::Value::Null),
        Value::Array(arr) => {
            let items: Vec<_> = arr.iter().filter_map(value_to_json).collect();
            Some(serde_json::Value::Array(items))
        }
        Value::Shared(value) => value_to_json(value),
        Value::Object(map) => {
            // Skip null values in objects - they represent deleted/absent fields
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter(|(_, v)| !matches!(v, Value::Null))
                .filter_map(|(k, v)| value_to_json(v).map(|jv| (k.to_string(), jv)))
                .collect();
            Some(serde_json::Value::Object(obj))
        }
    }
}

/// Convert a record value to serde_json::Value, using null for unconvertible values.
pub fn value_to_json_lossy(v: &Value) -> serde_json::Value {
    value_to_json(v).unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::ObjectMap;
    use bytes::Bytes;
    use ordered_float::NotNan;

    #[test]
    fn test_value_to_json_string() {
        let v = Value::Bytes(Bytes::from("hello"));
        let result = value_to_json(&v);
        assert_eq!(result, Some(serde_json::Value::String("hello".to_string())));
    }

    #[test]
    fn test_value_to_json_integer() {
        let v = Value::Integer(42);
        let result = value_to_json(&v);
        assert_eq!(result, Some(serde_json::json!(42)));
    }

    #[test]
    fn test_value_to_json_float() {
        let v = Value::Float(NotNan::new(3.14).unwrap());
        let result = value_to_json(&v);
        assert_eq!(result, Some(serde_json::json!(3.14)));
    }

    #[test]
    fn test_value_to_json_boolean() {
        let v = Value::Boolean(true);
        let result = value_to_json(&v);
        assert_eq!(result, Some(serde_json::Value::Bool(true)));
    }

    #[test]
    fn test_value_to_json_null() {
        let v = Value::Null;
        let result = value_to_json(&v);
        assert_eq!(result, Some(serde_json::Value::Null));
    }

    #[test]
    fn test_value_to_json_array() {
        let v = Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        let result = value_to_json(&v);
        assert_eq!(result, Some(serde_json::json!([1, 2, 3])));
    }

    #[test]
    fn test_value_to_json_object() {
        let mut map = ObjectMap::new();
        map.insert("key".into(), Value::Bytes(Bytes::from("value")));
        map.insert("number".into(), Value::Integer(42));
        let v = Value::Object(map);
        let result = value_to_json(&v);
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj["key"], serde_json::json!("value"));
        assert_eq!(obj["number"], serde_json::json!(42));
    }

    #[test]
    fn test_value_to_json_object_skips_nulls() {
        let mut map = ObjectMap::new();
        map.insert("present".into(), Value::Bytes(Bytes::from("value")));
        map.insert("absent".into(), Value::Null);
        let v = Value::Object(map);
        let result = value_to_json(&v);
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj["present"], serde_json::json!("value"));
        assert!(obj.get("absent").is_none());
    }

    #[test]
    fn test_value_to_json_lossy() {
        // Test that lossy conversion returns null for unconvertible values
        let v = Value::Bytes(Bytes::from("test"));
        assert_eq!(
            value_to_json_lossy(&v),
            serde_json::Value::String("test".to_string())
        );
    }
}
