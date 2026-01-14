//! Helper VRL functions for common transformation patterns
//! These simplify repetitive VRL code for OTLP processing

use vrl::compiler::prelude::*;
use vrl::value::Value;

// --- string_or_null ---
/// Converts value to string if non-empty, null otherwise
#[derive(Clone, Copy, Debug)]
pub struct StringOrNull;

impl Function for StringOrNull {
    fn identifier(&self) -> &'static str {
        "string_or_null"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "value",
            kind: kind::ANY,
            required: true,
        }]
    }

    fn compile(
        &self,
        _state: &TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        Ok(StringOrNullFn { value }.as_expr())
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }
}

#[derive(Debug, Clone)]
struct StringOrNullFn {
    value: Box<dyn Expression>,
}

impl FunctionExpression for StringOrNullFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let s = match value {
            Value::Null => return Ok(Value::Null),
            Value::Bytes(b) => {
                if b.is_empty() {
                    return Ok(Value::Null);
                }
                String::from_utf8_lossy(&b).to_string()
            }
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            // Objects and arrays: return null (not string-convertible)
            _ => return Ok(Value::Null),
        };
        if s.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(Value::Bytes(s.into()))
        }
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::bytes().add_null().infallible()
    }
}

// --- nanos_to_millis ---
/// Safely converts nanoseconds to milliseconds (integer division)
#[derive(Clone, Copy, Debug)]
pub struct NanosToMillis;

impl Function for NanosToMillis {
    fn identifier(&self) -> &'static str {
        "nanos_to_millis"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "value",
            kind: kind::ANY,
            required: true,
        }]
    }

    fn compile(
        &self,
        _state: &TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        Ok(NanosToMillisFn { value }.as_expr())
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }
}

#[derive(Debug, Clone)]
struct NanosToMillisFn {
    value: Box<dyn Expression>,
}

impl FunctionExpression for NanosToMillisFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let nanos = match value {
            Value::Integer(i) => i,
            Value::Float(f) => {
                let float_val = f.into_inner();
                if float_val.is_nan() || float_val.is_infinite() {
                    return Ok(Value::Integer(0));
                }
                // Use safe bounds for conversion
                const MAX_SAFE_FLOAT: f64 = 9_223_372_036_854_774_784.0;
                const MIN_SAFE_FLOAT: f64 = i64::MIN as f64;
                if !(MIN_SAFE_FLOAT..=MAX_SAFE_FLOAT).contains(&float_val) {
                    return Ok(Value::Integer(0));
                }
                float_val as i64
            }
            Value::Null => return Ok(Value::Integer(0)),
            _ => return Ok(Value::Integer(0)),
        };
        // Integer division - safe, no overflow possible when dividing
        Ok(Value::Integer(nanos / 1_000_000))
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::integer().infallible()
    }
}

// --- json_or_null ---
/// Encodes value as JSON string if non-empty, null otherwise
#[derive(Clone, Copy, Debug)]
pub struct JsonOrNull;

impl Function for JsonOrNull {
    fn identifier(&self) -> &'static str {
        "json_or_null"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[Parameter {
            keyword: "value",
            kind: kind::ANY,
            required: true,
        }]
    }

    fn compile(
        &self,
        _state: &TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        Ok(JsonOrNullFn { value }.as_expr())
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }
}

#[derive(Debug, Clone)]
struct JsonOrNullFn {
    value: Box<dyn Expression>,
}

impl FunctionExpression for JsonOrNullFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let is_empty = match &value {
            Value::Null => true,
            Value::Bytes(b) => b.is_empty(),
            Value::Array(arr) => arr.is_empty(),
            Value::Object(map) => map.is_empty(),
            _ => false,
        };
        if is_empty {
            Ok(Value::Null)
        } else {
            let json = crate::convert::vrl_value_to_json_lossy(&value);
            Ok(Value::Bytes(json.to_string().into()))
        }
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::bytes().add_null().infallible()
    }
}

// --- int_or_default ---
/// Returns the integer value if present, or the default if null
#[derive(Clone, Copy, Debug)]
pub struct IntOrDefault;

impl Function for IntOrDefault {
    fn identifier(&self) -> &'static str {
        "int_or_default"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "value",
                kind: kind::ANY,
                required: true,
            },
            Parameter {
                keyword: "default",
                kind: kind::INTEGER,
                required: true,
            },
        ]
    }

    fn compile(
        &self,
        _state: &TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        let default = arguments.required("default");
        Ok(IntOrDefaultFn { value, default }.as_expr())
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }
}

#[derive(Debug, Clone)]
struct IntOrDefaultFn {
    value: Box<dyn Expression>,
    default: Box<dyn Expression>,
}

impl FunctionExpression for IntOrDefaultFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        let default = self.default.resolve(ctx)?;
        match value {
            Value::Integer(i) => Ok(Value::Integer(i)),
            Value::Null => Ok(default),
            _ => Ok(default),
        }
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::integer().infallible()
    }
}

// --- get_attr ---
/// Gets an attribute from an object with a default value
#[derive(Clone, Copy, Debug)]
pub struct GetAttr;

impl Function for GetAttr {
    fn identifier(&self) -> &'static str {
        "get_attr"
    }

    fn parameters(&self) -> &'static [Parameter] {
        &[
            Parameter {
                keyword: "object",
                kind: kind::ANY, // Accept any type - we handle non-objects gracefully
                required: true,
            },
            Parameter {
                keyword: "key",
                kind: kind::BYTES,
                required: true,
            },
            Parameter {
                keyword: "default",
                kind: kind::ANY, // Accept any type for default
                required: false,
            },
        ]
    }

    fn compile(
        &self,
        _state: &TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let object = arguments.required("object");
        let key = arguments.required("key");
        let default = arguments.optional("default");
        Ok(GetAttrFn {
            object,
            key,
            default,
        }
        .as_expr())
    }

    fn examples(&self) -> &'static [Example] {
        &[]
    }
}

#[derive(Debug, Clone)]
struct GetAttrFn {
    object: Box<dyn Expression>,
    key: Box<dyn Expression>,
    default: Option<Box<dyn Expression>>,
}

impl FunctionExpression for GetAttrFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let object = self.object.resolve(ctx)?;
        let key = self.key.resolve(ctx)?;

        let default_value = match &self.default {
            Some(expr) => expr.resolve(ctx)?,
            None => Value::Null,
        };

        let key_str = match key {
            Value::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
            _ => return Ok(default_value),
        };

        match object {
            Value::Object(map) => {
                let key_string: KeyString = key_str.as_str().into();
                match map.get(&key_string) {
                    Some(v) if !matches!(v, Value::Null) => {
                        // Convert to string if it's bytes, otherwise use as-is
                        match v {
                            Value::Bytes(b) => {
                                if b.is_empty() {
                                    Ok(default_value)
                                } else {
                                    Ok(v.clone())
                                }
                            }
                            _ => Ok(v.clone()),
                        }
                    }
                    _ => Ok(default_value),
                }
            }
            Value::Null => Ok(default_value),
            _ => Ok(default_value),
        }
    }

    fn type_def(&self, _state: &TypeState) -> TypeDef {
        TypeDef::bytes().add_null().infallible()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use vrl::compiler::runtime::Runtime;
    use vrl::compiler::{compile, TargetValue, TimeZone};
    use vrl::value::ObjectMap;

    fn run_vrl(source: &str, input: Value) -> Result<Value, String> {
        let fns = crate::transform::functions::all();
        let result = compile(source, &fns).map_err(|e| format!("{e:?}"))?;

        let mut runtime = Runtime::default();
        let mut target = TargetValue {
            value: input,
            metadata: Value::Object(Default::default()),
            secrets: Default::default(),
        };

        let tz = TimeZone::Named(chrono_tz::UTC);
        runtime
            .resolve(&mut target, &result.program, &tz)
            .map_err(|e| format!("{e:?}"))?;

        Ok(target.value)
    }

    #[test]
    fn test_string_or_null_string() {
        let result = run_vrl(". = string_or_null(\"hello\")", Value::Null);
        assert_eq!(result.unwrap(), Value::Bytes(Bytes::from("hello")));
    }

    #[test]
    fn test_string_or_null_empty_string() {
        let result = run_vrl(". = string_or_null(\"\")", Value::Null);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_string_or_null_integer() {
        let result = run_vrl(". = string_or_null(42)", Value::Null);
        assert_eq!(result.unwrap(), Value::Bytes(Bytes::from("42")));
    }

    #[test]
    fn test_string_or_null_null() {
        let result = run_vrl(". = string_or_null(null)", Value::Null);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_string_or_null_object() {
        let result = run_vrl(". = string_or_null({})", Value::Null);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_nanos_to_millis_integer() {
        let result = run_vrl(". = nanos_to_millis(1_000_000_000)", Value::Null);
        assert_eq!(result.unwrap(), Value::Integer(1000));
    }

    #[test]
    fn test_nanos_to_millis_small() {
        let result = run_vrl(". = nanos_to_millis(500_000)", Value::Null);
        assert_eq!(result.unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_nanos_to_millis_null() {
        let result = run_vrl(". = nanos_to_millis(null)", Value::Null);
        assert_eq!(result.unwrap(), Value::Integer(0));
    }

    #[test]
    fn test_json_or_null_object() {
        let mut map = ObjectMap::new();
        map.insert("key".into(), Value::Bytes(Bytes::from("value")));
        let input = Value::Object(map);
        let result = run_vrl(". = json_or_null(.)", input);
        assert_eq!(
            result.unwrap(),
            Value::Bytes(Bytes::from("{\"key\":\"value\"}"))
        );
    }

    #[test]
    fn test_json_or_null_empty_object() {
        let result = run_vrl(". = json_or_null({})", Value::Null);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_json_or_null_empty_array() {
        let result = run_vrl(". = json_or_null([])", Value::Null);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_json_or_null_null() {
        let result = run_vrl(". = json_or_null(null)", Value::Null);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_int_or_default_integer() {
        let result = run_vrl(". = int_or_default(42, 0)", Value::Null);
        assert_eq!(result.unwrap(), Value::Integer(42));
    }

    #[test]
    fn test_int_or_default_null() {
        let result = run_vrl(". = int_or_default(null, 100)", Value::Null);
        assert_eq!(result.unwrap(), Value::Integer(100));
    }

    #[test]
    fn test_int_or_default_string() {
        let result = run_vrl(". = int_or_default(\"not an int\", 50)", Value::Null);
        assert_eq!(result.unwrap(), Value::Integer(50));
    }

    #[test]
    fn test_get_attr_present() {
        let mut map = ObjectMap::new();
        map.insert("name".into(), Value::Bytes(Bytes::from("test")));
        let input = Value::Object(map);
        let result = run_vrl(". = get_attr(., \"name\", \"default\")", input);
        assert_eq!(result.unwrap(), Value::Bytes(Bytes::from("test")));
    }

    #[test]
    fn test_get_attr_missing() {
        let map = ObjectMap::new();
        let input = Value::Object(map);
        let result = run_vrl(". = get_attr(., \"missing\", \"default\")", input);
        assert_eq!(result.unwrap(), Value::Bytes(Bytes::from("default")));
    }

    #[test]
    fn test_get_attr_null_object() {
        let result = run_vrl(". = get_attr(null, \"key\", \"default\")", Value::Null);
        assert_eq!(result.unwrap(), Value::Bytes(Bytes::from("default")));
    }

    #[test]
    fn test_get_attr_empty_string_value() {
        let mut map = ObjectMap::new();
        map.insert("name".into(), Value::Bytes(Bytes::from("")));
        let input = Value::Object(map);
        let result = run_vrl(". = get_attr(., \"name\", \"default\")", input);
        assert_eq!(result.unwrap(), Value::Bytes(Bytes::from("default")));
    }

    #[test]
    fn test_get_attr_no_default() {
        let map = ObjectMap::new();
        let input = Value::Object(map);
        let result = run_vrl(". = get_attr(., \"missing\")", input);
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_get_attr_integer_value() {
        let mut map = ObjectMap::new();
        map.insert("count".into(), Value::Integer(42));
        let input = Value::Object(map);
        let result = run_vrl(". = get_attr(., \"count\", 0)", input);
        assert_eq!(result.unwrap(), Value::Integer(42));
    }
}
