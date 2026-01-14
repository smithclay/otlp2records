//! Output serialization for Arrow RecordBatches
//!
//! Provides serialization to various formats:
//! - JSON (NDJSON - newline-delimited JSON)
//! - Arrow IPC (streaming format for cross-language interop)
//! - Parquet (optional, behind feature flag)

mod ipc;
mod json;

#[cfg(feature = "parquet")]
mod parquet;

pub use ipc::to_ipc;
pub use json::to_json;

#[cfg(feature = "parquet")]
pub use parquet::{to_parquet, to_parquet_bytes};
