//! otlp2records - Transform OTLP telemetry to Arrow RecordBatches
//!
//! This crate provides synchronous, WASM-compatible transformation of OpenTelemetry
//! Protocol (OTLP) data (logs, traces, metrics) into Arrow RecordBatches.
//!
//! # Design Principles
//!
//! - **No I/O**: Core never touches network or filesystem
//! - **No async**: Pure synchronous transforms
//! - **WASM-first**: All dependencies compile to wasm32
//! - **Arrow-native**: RecordBatch is the canonical output format
//!
//! # High-level API
//!
//! The simplest way to use this crate is with the high-level transform functions:
//!
//! ```ignore
//! use otlp2records::{transform_logs, transform_traces, transform_metrics, InputFormat};
//!
//! let batch = transform_logs(bytes, InputFormat::Protobuf)?;
//! let batch = transform_traces(bytes, InputFormat::Json)?;
//! let batches = transform_metrics(bytes, InputFormat::Protobuf)?;
//! ```

mod api;
mod arrow;
mod batch;
mod decode;
mod error;
pub mod fixtures;
mod output;
pub mod proto_output;
mod schema;

#[cfg(feature = "ffi")]
#[path = "bindings/ffi.rs"]
pub mod ffi;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
#[path = "bindings/wasm.rs"]
pub mod wasm;

pub use api::{
    transform_logs, transform_logs_decoded_for_bench, transform_logs_json,
    transform_logs_partitioned, transform_logs_with_observer, transform_metrics,
    transform_metrics_decoded_for_bench, transform_metrics_json, transform_metrics_partitioned,
    transform_metrics_with_observer, transform_traces, transform_traces_decoded_for_bench,
    transform_traces_json, transform_traces_partitioned, transform_traces_with_observer,
    JsonMetricBatches, MetricBatches, SkippedMetrics,
};
pub use arrow::{
    extract_min_timestamp_micros, extract_service_name, group_batch_by_service, PartitionedBatch,
    PartitionedMetrics, ServiceGroupedBatches,
};
pub use batch::{
    TransformCounter, TransformCounterValue, TransformObserver, TransformPhase,
    TransformPhaseTiming, TransformSignal,
};
pub use decode::{DecodeError, InputFormat};
pub use error::{Error, Result};
#[cfg(feature = "parquet")]
pub use output::to_parquet;
pub use output::{to_ipc, to_json};
pub use schema::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, schema_def, schema_defs,
    sum_schema, traces_schema, FieldType, SchemaDef, SchemaField,
};
