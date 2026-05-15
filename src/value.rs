//! Lightweight record value representation used between decoding and Arrow building.

use bytes::Bytes;
use indexmap::IndexMap;
use ordered_float::NotNan;
use std::sync::Arc;

pub type KeyString = String;
pub type ObjectMap = IndexMap<KeyString, Value>;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Bytes(Bytes),
    Integer(i64),
    Float(NotNan<f64>),
    Boolean(bool),
    Object(ObjectMap),
    Array(Vec<Value>),
    Shared(Arc<Value>),
    Null,
}
