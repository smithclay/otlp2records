//! Schema ownership for Arrow batches and public schema inspection.

mod arrow;
mod defs;
mod otap;

pub use arrow::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, traces_schema,
};
pub use defs::{schema_def, schema_defs, FieldType, SchemaDef, SchemaField};
pub use otap::{
    otap_attrs_u16_schema, otap_attrs_u32_schema, otap_exemplars_schema,
    otap_exp_histogram_data_points_schema, otap_histogram_data_points_schema, otap_logs_schema,
    otap_metrics_schema, otap_number_data_points_schema, otap_quantile_schema,
    otap_span_events_schema, otap_span_links_schema, otap_spans_schema,
    otap_summary_data_points_schema,
};
