//! Stateful native OTAP input.
//!
//! This module accepts canonical OTAP envelopes but emits only this crate's
//! existing normalized, flattened logs, traces, and metrics schemas.

mod decoder;
mod logs;
mod metrics;
mod traces;
mod validation;
mod wire;

pub use decoder::OtapDecoder;
pub use wire::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};
pub(crate) use wire::{
    ATTR_BOOL, ATTR_BYTES, ATTR_DOUBLE, ATTR_INT, ATTR_KEY, ATTR_SER, ATTR_STR, ATTR_TYPE,
    VALUE_BOOL, VALUE_BYTES, VALUE_EMPTY, VALUE_F64, VALUE_I64, VALUE_MAP, VALUE_SLICE, VALUE_STR,
};
