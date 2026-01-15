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
pub use parquet::{to_parquet, to_parquet_bytes, write_parquet};

// Re-export WriterProperties for callers who want to customize parquet output
#[cfg(feature = "parquet")]
pub use ::parquet::file::properties::WriterProperties as ParquetWriterProperties;

// Re-export compression types so callers don't need to depend on parquet directly
#[cfg(feature = "parquet")]
pub use ::parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
