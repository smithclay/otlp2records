// src/transform/runtime.rs
//! VRL runtime for transforming OTLP data

use once_cell::sync::Lazy;
use std::collections::HashMap;
use vrl::compiler::runtime::Runtime;
use vrl::compiler::{compile, Program, TargetValue, TimeZone};
use vrl::value::{KeyString, Value};

static UTC_TIMEZONE: Lazy<TimeZone> = Lazy::new(|| TimeZone::Named(chrono_tz::UTC));

use super::functions;

// Include compiled VRL sources from build.rs
include!(concat!(env!("OUT_DIR"), "/compiled_vrl.rs"));

// Lazy-compiled programs with our custom functions
pub static OTLP_LOGS_PROGRAM: Lazy<Program> = Lazy::new(|| {
    let fns = functions::all();
    compile(OTLP_LOGS_SOURCE, &fns)
        .expect("OTLP_LOGS VRL should compile")
        .program
});

pub static OTLP_TRACES_PROGRAM: Lazy<Program> = Lazy::new(|| {
    let fns = functions::all();
    compile(OTLP_TRACES_SOURCE, &fns)
        .expect("OTLP_TRACES VRL should compile")
        .program
});

pub static OTLP_GAUGE_PROGRAM: Lazy<Program> = Lazy::new(|| {
    let fns = functions::all();
    compile(OTLP_GAUGE_SOURCE, &fns)
        .expect("OTLP_GAUGE VRL should compile")
        .program
});

pub static OTLP_SUM_PROGRAM: Lazy<Program> = Lazy::new(|| {
    let fns = functions::all();
    compile(OTLP_SUM_SOURCE, &fns)
        .expect("OTLP_SUM VRL should compile")
        .program
});

pub static OTLP_HISTOGRAM_PROGRAM: Lazy<Program> = Lazy::new(|| {
    let fns = functions::all();
    compile(OTLP_HISTOGRAM_SOURCE, &fns)
        .expect("OTLP_HISTOGRAM VRL should compile")
        .program
});

pub static OTLP_EXP_HISTOGRAM_PROGRAM: Lazy<Program> = Lazy::new(|| {
    let fns = functions::all();
    compile(OTLP_EXP_HISTOGRAM_SOURCE, &fns)
        .expect("OTLP_EXP_HISTOGRAM VRL should compile")
        .program
});

/// VRL transformation error
#[derive(Debug)]
pub struct VrlError(pub String);

impl std::fmt::Display for VrlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "VRL error: {}", self.0)
    }
}

impl std::error::Error for VrlError {}

/// VRL transformer wrapping the VRL Runtime
pub struct VrlTransformer {
    runtime: Runtime,
}

impl VrlTransformer {
    /// Create a new VRL transformer
    pub fn new() -> Self {
        Self {
            runtime: Runtime::default(),
        }
    }

    /// Transform a single record using the given program.
    ///
    /// Returns `(table_name, transformed_value)` where `table_name` is extracted from
    /// the `_table` field set by the VRL program.
    ///
    /// # Table Name Extraction
    ///
    /// The `_table` field is removed from the output and returned separately as the table name.
    /// If the `_table` field is missing or is not a string, the table name defaults to `"unknown"`.
    /// This fallback behavior is intentional to ensure the transform always succeeds, but callers
    /// should be aware that an `"unknown"` table name may indicate a misconfigured VRL program.
    pub fn transform(
        &mut self,
        program: &Program,
        input: Value,
    ) -> Result<(String, Value), VrlError> {
        let mut target = TargetValue {
            value: input,
            metadata: Value::Object(Default::default()),
            secrets: Default::default(),
        };

        self.runtime
            .resolve(&mut target, program, &UTC_TIMEZONE)
            .map_err(|e| VrlError(format!("{e:?}")))?;

        let table_key: KeyString = "_table".into();
        let table = if let Value::Object(ref map) = target.value {
            map.get(&table_key)
                .and_then(|v| match v {
                    Value::Bytes(b) => Some(String::from_utf8_lossy(b).to_string()),
                    _ => None,
                })
                .unwrap_or_else(|| "unknown".to_string())
        } else {
            "unknown".to_string()
        };

        if let Value::Object(ref mut map) = target.value {
            map.remove(&table_key);
        }

        Ok((table, target.value))
    }

    /// Transform a batch of records using the given program.
    /// Returns a HashMap grouping transformed values by their _table routing key.
    pub fn transform_batch(
        &mut self,
        program: &Program,
        inputs: Vec<Value>,
    ) -> Result<HashMap<String, Vec<Value>>, VrlError> {
        let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();

        for (idx, input) in inputs.into_iter().enumerate() {
            let (table, output) = self
                .transform(program, input)
                .map_err(|e| VrlError(format!("record {}: {}", idx, e.0)))?;
            grouped.entry(table).or_default().push(output);
        }

        Ok(grouped)
    }
}

/// Force initialization of all VRL programs.
/// Call during worker startup to avoid cold-start latency.
#[cfg(target_arch = "wasm32")]
pub fn init_programs() {
    // Access each Lazy to force initialization
    let _ = &*OTLP_LOGS_PROGRAM;
    let _ = &*OTLP_TRACES_PROGRAM;
    let _ = &*OTLP_GAUGE_PROGRAM;
    let _ = &*OTLP_SUM_PROGRAM;
    let _ = &*OTLP_HISTOGRAM_PROGRAM;
    let _ = &*OTLP_EXP_HISTOGRAM_PROGRAM;
}

impl Default for VrlTransformer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use vrl::value::ObjectMap;

    #[test]
    fn test_vrl_transformer_new() {
        let transformer = VrlTransformer::new();
        assert!(std::mem::size_of_val(&transformer) > 0);
    }

    #[test]
    fn test_vrl_transformer_default() {
        let transformer = VrlTransformer::default();
        assert!(std::mem::size_of_val(&transformer) > 0);
    }

    #[test]
    fn test_transform_extracts_table() {
        use vrl::compiler::compile;

        // Create a simple VRL program that sets _table
        let source = r#"._table = "test_table""#;
        let fns = functions::all();
        let result = compile(source, &fns).expect("should compile");

        let mut transformer = VrlTransformer::new();
        let mut input_map = ObjectMap::new();
        input_map.insert("data".into(), Value::Bytes(Bytes::from("hello")));
        let input = Value::Object(input_map);

        let (table, output) = transformer.transform(&result.program, input).unwrap();

        assert_eq!(table, "test_table");
        // _table should be removed from output
        if let Value::Object(map) = output {
            let table_key: KeyString = "_table".into();
            assert!(map.get(&table_key).is_none());
        } else {
            panic!("Expected Object output");
        }
    }

    #[test]
    fn test_transform_default_table_when_missing() {
        use vrl::compiler::compile;

        // Program that doesn't set _table
        let source = r#".processed = true"#;
        let fns = functions::all();
        let result = compile(source, &fns).expect("should compile");

        let mut transformer = VrlTransformer::new();
        let mut input_map = ObjectMap::new();
        input_map.insert("data".into(), Value::Bytes(Bytes::from("hello")));
        let input = Value::Object(input_map);

        let (table, _) = transformer.transform(&result.program, input).unwrap();

        assert_eq!(table, "unknown");
    }

    #[test]
    fn test_transform_batch() {
        use vrl::compiler::compile;

        // Create a program that routes based on input type
        let source = r#"
            if .type == "logs" {
                ._table = "logs"
            } else {
                ._table = "other"
            }
        "#;
        let fns = functions::all();
        let result = compile(source, &fns).expect("should compile");

        let mut transformer = VrlTransformer::new();

        let inputs: Vec<Value> = vec![
            {
                let mut map = ObjectMap::new();
                map.insert("type".into(), Value::Bytes(Bytes::from("logs")));
                Value::Object(map)
            },
            {
                let mut map = ObjectMap::new();
                map.insert("type".into(), Value::Bytes(Bytes::from("traces")));
                Value::Object(map)
            },
            {
                let mut map = ObjectMap::new();
                map.insert("type".into(), Value::Bytes(Bytes::from("logs")));
                Value::Object(map)
            },
        ];

        let grouped = transformer
            .transform_batch(&result.program, inputs)
            .unwrap();

        assert_eq!(grouped.get("logs").map(|v| v.len()), Some(2));
        assert_eq!(grouped.get("other").map(|v| v.len()), Some(1));
    }

    #[test]
    fn test_vrl_error_display() {
        let err = VrlError("test error".to_string());
        assert_eq!(format!("{err}"), "VRL error: test error");
    }
}
