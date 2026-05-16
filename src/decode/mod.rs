//! OTLP decode support for public transforms.

mod format;
mod json;
mod jsonl;

pub use format::InputFormat;
pub use json::{looks_like_json, DecodeError};

pub(crate) use json::{
    decode_logs_json_request, decode_metrics_json_request, decode_traces_json_request,
};
pub(crate) use jsonl::{
    decode_logs_jsonl_request, decode_metrics_jsonl_request, decode_traces_jsonl_request,
};
