//! Stateful native OTAP input.
//!
//! This module accepts canonical OTAP envelopes but emits only this crate's
//! existing normalized, flattened logs, traces, and metrics schemas.

// `otap-zstd` enables Arrow's Zstandard backend (`arrow-ipc/zstd`), which
// compiles the bundled C `zstd-sys`. That backend cannot target
// `wasm32-unknown-unknown`, so the combination is unsupported and will not
// build. Reject it explicitly: this documents the constraint at its source and
// stays authoritative even in an environment where the C dependency happens to
// build (Cargo compiles `zstd-sys` first, so its own opaque C error usually
// surfaces before this one). WASM OTAP supports uncompressed and LZ4 Arrow IPC
// only.
#[cfg(all(feature = "otap-zstd", target_arch = "wasm32"))]
compile_error!(
    "the `otap-zstd` feature enables Arrow's C Zstandard backend, which cannot \
     target wasm32; build WASM without `otap-zstd` (WASM OTAP supports \
     uncompressed and LZ4 Arrow IPC only)"
);

mod decoder;
mod logs;
mod metrics;
mod traces;
mod validation;
mod wire;

pub use decoder::OtapDecoder;
pub use wire::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};
