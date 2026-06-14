//! OTLP request-to-Arrow batch engine.

mod context;
mod logs;
mod metrics;
mod otap;
mod profile;
mod traces;
mod util;
mod view_json;

pub(crate) use logs::{
    transform_logs_protobuf, transform_logs_protobuf_observed, transform_logs_request_observed,
};
pub(crate) use metrics::{
    transform_metrics_protobuf, transform_metrics_protobuf_observed,
    transform_metrics_request_observed,
};
pub(crate) use otap::{
    transform_logs_protobuf_otap, transform_logs_protobuf_otap_observed,
    transform_logs_request_otap, transform_logs_request_otap_observed,
    transform_metrics_protobuf_otap, transform_metrics_protobuf_otap_observed,
    transform_metrics_request_otap, transform_metrics_request_otap_observed,
    transform_traces_protobuf_otap, transform_traces_protobuf_otap_observed,
    transform_traces_request_otap, transform_traces_request_otap_observed,
};
pub(crate) use profile::observe_phase;
pub use profile::{
    TransformCounter, TransformCounterValue, TransformObserver, TransformPhase,
    TransformPhaseTiming, TransformSignal,
};

pub(crate) use logs::transform_logs_view;
pub(crate) use metrics::transform_metrics_view;
pub(crate) use traces::{
    transform_traces_protobuf, transform_traces_protobuf_observed,
    transform_traces_request_observed, transform_traces_view,
};
