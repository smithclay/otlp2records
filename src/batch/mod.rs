//! OTLP request-to-Arrow batch engine.

mod context;
mod json;
mod logs;
mod metrics;
mod profile;
mod traces;
mod util;

#[cfg(feature = "bench-internals")]
pub(crate) use logs::transform_logs_request;
pub(crate) use logs::{
    transform_logs_protobuf, transform_logs_protobuf_observed, transform_logs_request_observed,
};
#[cfg(feature = "bench-internals")]
pub(crate) use metrics::transform_metrics_request;
pub(crate) use metrics::{
    transform_metrics_protobuf, transform_metrics_protobuf_observed,
    transform_metrics_request_observed,
};
pub(crate) use profile::observe_phase;
pub use profile::{
    TransformCounter, TransformCounterValue, TransformObserver, TransformPhase,
    TransformPhaseTiming, TransformSignal,
};
#[cfg(feature = "bench-internals")]
pub(crate) use traces::transform_traces_request;
pub(crate) use traces::{
    transform_traces_protobuf, transform_traces_protobuf_observed,
    transform_traces_request_observed,
};
