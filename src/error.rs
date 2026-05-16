//! Error types for otlp2records

use crate::decode::DecodeError;
use thiserror::Error;

/// Result type alias using the crate's Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during OTLP to RecordBatch transformation
#[derive(Debug, Error)]
pub enum Error {
    /// Error decoding OTLP data
    #[error("decode error: {0}")]
    Decode(#[from] DecodeError),

    /// Error decoding protobuf data
    #[error("protobuf decode error: {0}")]
    ProtobufDecode(#[from] prost::DecodeError),

    /// Error during Arrow operations
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Error during JSON serialization/deserialization
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Invalid input data
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// Schema mismatch error
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
}
