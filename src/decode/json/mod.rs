//! OTLP JSON DTO normalization and conversion into protobuf request structs.

mod common;
mod logs;
mod metrics;
mod normalize;
mod traces;

pub use common::{looks_like_json, DecodeError};

pub(crate) use logs::decode_json_request as decode_logs_json_request;
pub(crate) use metrics::decode_json_request as decode_metrics_json_request;
pub(crate) use traces::decode_json_request as decode_traces_json_request;
