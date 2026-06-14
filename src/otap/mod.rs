//! Stateful native OTAP input.
//!
//! This module accepts canonical OTAP envelopes but emits only this crate's
//! existing normalized, flattened logs, traces, and metrics schemas.

mod decoder;
mod logs;
mod metrics;
mod schema;
mod traces;
mod wire;

pub use decoder::OtapDecoder;
pub use wire::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};
