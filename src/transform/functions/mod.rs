//! Custom VRL stdlib functions for WASM compatibility
//! These replace VRL's stdlib which depends on zstd (C code)

mod core;
mod helpers;

use vrl::compiler::Function;

pub use self::core::{EncodeJson, Floor, Get, IsArray, IsEmpty, IsObject, ToInt, ToString_};
pub use helpers::{GetAttr, IntOrDefault, JsonOrNull, NanosToMicros, NanosToMillis, StringOrNull};

/// Get all custom functions for VRL compilation
pub fn all() -> Vec<Box<dyn Function>> {
    vec![
        // Core stdlib replacements
        Box::new(ToInt),
        Box::new(ToString_),
        Box::new(EncodeJson),
        Box::new(Get),
        Box::new(IsEmpty),
        Box::new(IsObject),
        Box::new(IsArray),
        Box::new(Floor),
        // Helper functions for common patterns
        Box::new(StringOrNull),
        Box::new(NanosToMillis),
        Box::new(NanosToMicros),
        Box::new(JsonOrNull),
        Box::new(IntOrDefault),
        Box::new(GetAttr),
    ]
}
