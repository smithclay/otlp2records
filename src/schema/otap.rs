//! OTAP star Arrow schema accessors.
//!
//! The `otap_*_schema()` functions are preserved for parity with the normalized
//! schema accessors (and so external consumers can reintroduce them at the
//! crate root if needed). Internal call sites use the `_arc` variants instead
//! so the cached `Lazy<Arc<Schema>>` is shared by reference, eliminating the
//! per-call `Schema::clone` + `Arc::new` pair from each `record_batch` build.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use once_cell::sync::Lazy;

use super::field::{
    binary, bool_field, duration_ns, f64_field, f64_list, fixed, i32_field, i64_field, ts_ns,
    u16_field, u32_field, u64_field, u64_list, u8_field, utf8,
};
use crate::otap::{
    ATTR_BOOL, ATTR_BYTES, ATTR_DOUBLE, ATTR_INT, ATTR_KEY, ATTR_SER, ATTR_STR, ATTR_TYPE,
};

static OTAP_LOGS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        [
            resource_scope_header(),
            vec![
                ts_ns("time_unix_nano", true),
                ts_ns("observed_time_unix_nano", true),
                fixed("trace_id", 16, true),
                fixed("span_id", 8, true),
                i32_field("severity_number", true),
                utf8("severity_text", true),
                utf8("event_name", true),
                u8_field("body_type", false),
                utf8("body_str", true),
                i64_field("body_int", true),
                f64_field("body_double", true),
                bool_field("body_bool", true),
                binary("body_bytes", true),
                binary("body_ser", true),
                u32_field("dropped_attributes_count", true),
                u32_field("flags", true),
            ],
        ]
        .concat(),
    ))
});

static OTAP_SPANS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        [
            resource_scope_header(),
            vec![
                ts_ns("start_time_unix_nano", false),
                duration_ns("duration_time_unix_nano", false),
                fixed("trace_id", 16, false),
                fixed("span_id", 8, false),
                utf8("trace_state", true),
                fixed("parent_span_id", 8, true),
                utf8("name", false),
                i32_field("kind", true),
                u32_field("dropped_attributes_count", true),
                u32_field("dropped_events_count", true),
                u32_field("dropped_links_count", true),
                i32_field("status_code", true),
                utf8("status_status_message", true),
                u32_field("flags", true),
            ],
        ]
        .concat(),
    ))
});

static OTAP_METRICS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        [
            resource_scope_header(),
            vec![
                u8_field("metric_type", false),
                utf8("name", false),
                utf8("description", true),
                utf8("unit", true),
                i32_field("aggregation_temporality", true),
                bool_field("is_monotonic", true),
            ],
        ]
        .concat(),
    ))
});

static OTAP_ATTRS_U16_SCHEMA: Lazy<Arc<Schema>> =
    Lazy::new(|| Arc::new(attrs_schema(DataType::UInt16)));
static OTAP_ATTRS_U32_SCHEMA: Lazy<Arc<Schema>> =
    Lazy::new(|| Arc::new(attrs_schema(DataType::UInt32)));

static OTAP_NUMBER_DPS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        [
            data_point_prefix(),
            vec![
                i64_field("int_value", true),
                f64_field("double_value", true),
                u32_field("flags", true),
            ],
        ]
        .concat(),
    ))
});

static OTAP_EXEMPLARS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        u32_field("id", false),
        u32_field("parent_id", false),
        ts_ns("time_unix_nano", false),
        i64_field("int_value", true),
        f64_field("double_value", true),
        fixed("span_id", 8, true),
        fixed("trace_id", 16, true),
    ]))
});

// Quantile values live in the separate `quantile` child table (see B3 in the
// review notes). Keeping a single representation avoids forcing consumers to
// dedupe between an embedded list and the relational table.
static OTAP_SUMMARY_DPS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        [
            data_point_prefix(),
            vec![
                u64_field("count", true),
                f64_field("sum", true),
                u32_field("flags", true),
            ],
        ]
        .concat(),
    ))
});

static OTAP_QUANTILE_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        u32_field("parent_id", false),
        f64_field("quantile", true),
        f64_field("value", true),
    ]))
});

static OTAP_HISTOGRAM_DPS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        [
            data_point_prefix(),
            vec![
                u64_field("count", true),
                f64_field("sum", true),
                u64_list("bucket_counts", true),
                f64_list("explicit_bounds", true),
                u32_field("flags", true),
                f64_field("min", true),
                f64_field("max", true),
            ],
        ]
        .concat(),
    ))
});

static OTAP_EXP_HISTOGRAM_DPS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(
        [
            data_point_prefix(),
            vec![
                u64_field("count", true),
                f64_field("sum", true),
                i32_field("scale", true),
                u64_field("zero_count", true),
                i32_field("positive_offset", true),
                u64_list("positive_bucket_counts", true),
                i32_field("negative_offset", true),
                u64_list("negative_bucket_counts", true),
                u32_field("flags", true),
                f64_field("min", true),
                f64_field("max", true),
                f64_field("zero_threshold", true),
            ],
        ]
        .concat(),
    ))
});

static OTAP_SPAN_EVENTS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        ts_ns("time_unix_nano", true),
        utf8("name", false),
        u32_field("dropped_attributes_count", true),
    ]))
});

static OTAP_SPAN_LINKS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        fixed("trace_id", 16, false),
        fixed("span_id", 8, false),
        utf8("trace_state", true),
        u32_field("dropped_attributes_count", true),
        u32_field("flags", true),
    ]))
});

#[allow(dead_code)]
pub fn otap_logs_schema() -> Schema {
    (**OTAP_LOGS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_spans_schema() -> Schema {
    (**OTAP_SPANS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_metrics_schema() -> Schema {
    (**OTAP_METRICS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_attrs_u16_schema() -> Schema {
    (**OTAP_ATTRS_U16_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_attrs_u32_schema() -> Schema {
    (**OTAP_ATTRS_U32_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_number_data_points_schema() -> Schema {
    (**OTAP_NUMBER_DPS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_exemplars_schema() -> Schema {
    (**OTAP_EXEMPLARS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_summary_data_points_schema() -> Schema {
    (**OTAP_SUMMARY_DPS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_quantile_schema() -> Schema {
    (**OTAP_QUANTILE_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_histogram_data_points_schema() -> Schema {
    (**OTAP_HISTOGRAM_DPS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_exp_histogram_data_points_schema() -> Schema {
    (**OTAP_EXP_HISTOGRAM_DPS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_span_events_schema() -> Schema {
    (**OTAP_SPAN_EVENTS_SCHEMA).clone()
}

#[allow(dead_code)]
pub fn otap_span_links_schema() -> Schema {
    (**OTAP_SPAN_LINKS_SCHEMA).clone()
}

pub(crate) fn otap_logs_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_LOGS_SCHEMA)
}

pub(crate) fn otap_spans_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_SPANS_SCHEMA)
}

pub(crate) fn otap_metrics_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_METRICS_SCHEMA)
}

pub(crate) fn otap_attrs_u16_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_ATTRS_U16_SCHEMA)
}

pub(crate) fn otap_attrs_u32_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_ATTRS_U32_SCHEMA)
}

pub(crate) fn otap_number_data_points_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_NUMBER_DPS_SCHEMA)
}

pub(crate) fn otap_exemplars_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_EXEMPLARS_SCHEMA)
}

pub(crate) fn otap_summary_data_points_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_SUMMARY_DPS_SCHEMA)
}

pub(crate) fn otap_quantile_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_QUANTILE_SCHEMA)
}

pub(crate) fn otap_histogram_data_points_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_HISTOGRAM_DPS_SCHEMA)
}

pub(crate) fn otap_exp_histogram_data_points_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_EXP_HISTOGRAM_DPS_SCHEMA)
}

pub(crate) fn otap_span_events_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_SPAN_EVENTS_SCHEMA)
}

pub(crate) fn otap_span_links_schema_arc() -> Arc<Schema> {
    Arc::clone(&OTAP_SPAN_LINKS_SCHEMA)
}

fn attrs_schema(parent_type: DataType) -> Schema {
    Schema::new(vec![
        Field::new("parent_id", parent_type, false),
        utf8(ATTR_KEY, false),
        u8_field(ATTR_TYPE, false),
        utf8(ATTR_STR, true),
        i64_field(ATTR_INT, true),
        f64_field(ATTR_DOUBLE, true),
        bool_field(ATTR_BOOL, true),
        binary(ATTR_BYTES, true),
        binary(ATTR_SER, true),
    ])
}

/// Leading resource/scope identity columns shared by every top-level OTAP star
/// table (logs, spans, metrics). Builders in `batch::otap` populate these
/// positionally, so this order is load-bearing and must stay first.
fn resource_scope_header() -> Vec<Field> {
    vec![
        u16_field("id", false),
        u16_field("resource_id", true),
        utf8("resource_schema_url", true),
        u32_field("resource_dropped_attributes_count", true),
        u16_field("scope_id", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        u32_field("scope_dropped_attributes_count", true),
        utf8("schema_url", true),
    ]
}

/// Leading id/parent/time columns shared by every metric data-point table
/// (number, summary, histogram, exponential histogram). Like
/// [`resource_scope_header`], the order is positionally load-bearing.
fn data_point_prefix() -> Vec<Field> {
    vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        ts_ns("start_time_unix_nano", true),
        ts_ns("time_unix_nano", false),
    ]
}
