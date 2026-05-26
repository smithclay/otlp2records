//! OTAP star Arrow schema accessors.

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use once_cell::sync::Lazy;

static OTAP_LOGS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u16_field("id", false),
        u16_field("resource_id", true),
        utf8("resource_schema_url", true),
        u32_field("resource_dropped_attributes_count", true),
        u16_field("scope_id", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        u32_field("scope_dropped_attributes_count", true),
        utf8("schema_url", true),
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
    ])
});

static OTAP_SPANS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u16_field("id", false),
        u16_field("resource_id", true),
        utf8("resource_schema_url", true),
        u32_field("resource_dropped_attributes_count", true),
        u16_field("scope_id", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        u32_field("scope_dropped_attributes_count", true),
        utf8("schema_url", true),
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
    ])
});

static OTAP_METRICS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u16_field("id", false),
        u16_field("resource_id", true),
        utf8("resource_schema_url", true),
        u32_field("resource_dropped_attributes_count", true),
        u16_field("scope_id", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        u32_field("scope_dropped_attributes_count", true),
        utf8("schema_url", true),
        u8_field("metric_type", false),
        utf8("name", false),
        utf8("description", true),
        utf8("unit", true),
        i32_field("aggregation_temporality", true),
        bool_field("is_monotonic", true),
    ])
});

static OTAP_ATTRS_U16_SCHEMA: Lazy<Schema> = Lazy::new(|| attrs_schema(DataType::UInt16));
static OTAP_ATTRS_U32_SCHEMA: Lazy<Schema> = Lazy::new(|| attrs_schema(DataType::UInt32));

static OTAP_NUMBER_DPS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        ts_ns("start_time_unix_nano", true),
        ts_ns("time_unix_nano", false),
        i64_field("int_value", true),
        f64_field("double_value", true),
        u32_field("flags", true),
    ])
});

static OTAP_EXEMPLARS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("id", false),
        u32_field("parent_id", false),
        ts_ns("time_unix_nano", false),
        i64_field("int_value", true),
        f64_field("double_value", true),
        fixed("span_id", 8, true),
        fixed("trace_id", 16, true),
    ])
});

static OTAP_SUMMARY_DPS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        ts_ns("start_time_unix_nano", true),
        ts_ns("time_unix_nano", false),
        u64_field("count", true),
        f64_field("sum", true),
        quantile_list("quantile", true),
        u32_field("flags", true),
    ])
});

static OTAP_QUANTILE_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("parent_id", false),
        f64_field("quantile", true),
        f64_field("value", true),
    ])
});

static OTAP_HISTOGRAM_DPS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        ts_ns("start_time_unix_nano", true),
        ts_ns("time_unix_nano", false),
        u64_field("count", true),
        f64_field("sum", true),
        u64_list("bucket_counts", true),
        f64_list("explicit_bounds", true),
        u32_field("flags", true),
        f64_field("min", true),
        f64_field("max", true),
    ])
});

static OTAP_EXP_HISTOGRAM_DPS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        ts_ns("start_time_unix_nano", true),
        ts_ns("time_unix_nano", false),
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
    ])
});

static OTAP_SPAN_EVENTS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        ts_ns("time_unix_nano", true),
        utf8("name", false),
        u32_field("dropped_attributes_count", true),
    ])
});

static OTAP_SPAN_LINKS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        u32_field("id", false),
        u16_field("parent_id", false),
        fixed("trace_id", 16, false),
        fixed("span_id", 8, false),
        utf8("trace_state", true),
        u32_field("dropped_attributes_count", true),
        u32_field("flags", true),
    ])
});

pub fn otap_logs_schema() -> Schema {
    OTAP_LOGS_SCHEMA.clone()
}

pub fn otap_spans_schema() -> Schema {
    OTAP_SPANS_SCHEMA.clone()
}

pub fn otap_metrics_schema() -> Schema {
    OTAP_METRICS_SCHEMA.clone()
}

pub fn otap_attrs_u16_schema() -> Schema {
    OTAP_ATTRS_U16_SCHEMA.clone()
}

pub fn otap_attrs_u32_schema() -> Schema {
    OTAP_ATTRS_U32_SCHEMA.clone()
}

pub fn otap_number_data_points_schema() -> Schema {
    OTAP_NUMBER_DPS_SCHEMA.clone()
}

pub fn otap_exemplars_schema() -> Schema {
    OTAP_EXEMPLARS_SCHEMA.clone()
}

pub fn otap_summary_data_points_schema() -> Schema {
    OTAP_SUMMARY_DPS_SCHEMA.clone()
}

pub fn otap_quantile_schema() -> Schema {
    OTAP_QUANTILE_SCHEMA.clone()
}

pub fn otap_histogram_data_points_schema() -> Schema {
    OTAP_HISTOGRAM_DPS_SCHEMA.clone()
}

pub fn otap_exp_histogram_data_points_schema() -> Schema {
    OTAP_EXP_HISTOGRAM_DPS_SCHEMA.clone()
}

pub fn otap_span_events_schema() -> Schema {
    OTAP_SPAN_EVENTS_SCHEMA.clone()
}

pub fn otap_span_links_schema() -> Schema {
    OTAP_SPAN_LINKS_SCHEMA.clone()
}

fn attrs_schema(parent_type: DataType) -> Schema {
    Schema::new(vec![
        Field::new("parent_id", parent_type, false),
        utf8("key", false),
        u8_field("type", false),
        utf8("str", true),
        i64_field("int", true),
        f64_field("double", true),
        bool_field("bool", true),
        binary("bytes", true),
        binary("ser", true),
    ])
}

fn ts_ns(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        nullable,
    )
}

fn duration_ns(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Duration(TimeUnit::Nanosecond), nullable)
}

fn utf8(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Utf8, nullable)
}

fn binary(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Binary, nullable)
}

fn fixed(name: &str, size: i32, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(size), nullable)
}

fn u8_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt8, nullable)
}

fn u16_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt16, nullable)
}

fn u32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt32, nullable)
}

fn u64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt64, nullable)
}

fn i32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int32, nullable)
}

fn i64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int64, nullable)
}

fn f64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Float64, nullable)
}

fn bool_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Boolean, nullable)
}

fn u64_list(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::List(Field::new("item", DataType::UInt64, true).into()),
        nullable,
    )
}

fn f64_list(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::List(Field::new("item", DataType::Float64, true).into()),
        nullable,
    )
}

fn quantile_list(name: &str, nullable: bool) -> Field {
    let item = Field::new(
        "item",
        DataType::Struct(
            vec![
                Field::new("quantile", DataType::Float64, true),
                Field::new("value", DataType::Float64, true),
            ]
            .into(),
        ),
        true,
    );
    Field::new(name, DataType::List(item.into()), nullable)
}
