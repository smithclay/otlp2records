//! Schema ownership for Arrow batches.

mod arrow;
mod field;

pub use arrow::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, traces_schema,
};
pub(crate) use arrow::{
    exp_histogram_schema_arc, gauge_schema_arc, histogram_schema_arc, logs_schema_arc,
    sum_schema_arc, traces_schema_arc,
};
