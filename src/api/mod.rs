//! Public API orchestration.

mod auto;
mod json;
mod logs;
mod metrics;
mod partitioned;
mod traces;
mod types;

pub use logs::{
    transform_logs, transform_logs_json, transform_logs_with_observer, transform_logs_with_schema,
    transform_logs_with_schema_and_observer,
};
pub use metrics::{
    transform_metrics, transform_metrics_json, transform_metrics_with_observer,
    transform_metrics_with_schema, transform_metrics_with_schema_and_observer,
};
pub use partitioned::{
    transform_logs_partitioned, transform_metrics_partitioned, transform_traces_partitioned,
};
pub use traces::{
    transform_traces, transform_traces_json, transform_traces_with_observer,
    transform_traces_with_schema, transform_traces_with_schema_and_observer,
};
pub use types::{
    JsonMetricBatches, LogsOutput, MetricBatches, MetricsOutput, OtapLogsBatches,
    OtapMetricsBatches, OtapTracesBatches, SchemaOutput, SkippedMetrics, TracesOutput,
};

pub(crate) use json::{batch_to_json_values, optional_batch_to_json_values};
