//! Schema ownership for Arrow batches.

mod arrow;
mod field;
mod otap;

pub use arrow::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, traces_schema,
};
pub(crate) use arrow::{
    exp_histogram_schema_arc, gauge_schema_arc, histogram_schema_arc, logs_schema_arc,
    sum_schema_arc, traces_schema_arc,
};
// The OTAP `pub fn otap_*_schema()` accessors are intentionally not re-exported
// here or at the crate root (see lib.rs and S8). Internal builders use the
// `_arc` variants so a single `Lazy<Arc<Schema>>` static is shared by reference
// instead of cloned on every accessor call.
pub(crate) use otap::{
    otap_attrs_u16_schema_arc, otap_attrs_u32_schema_arc, otap_exemplars_schema_arc,
    otap_exp_histogram_data_points_schema_arc, otap_histogram_data_points_schema_arc,
    otap_logs_schema_arc, otap_metrics_schema_arc, otap_number_data_points_schema_arc,
    otap_quantile_schema_arc, otap_span_events_schema_arc, otap_span_links_schema_arc,
    otap_spans_schema_arc, otap_summary_data_points_schema_arc,
};
