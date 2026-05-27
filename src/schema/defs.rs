//! Schema definitions for public schema inspection.

/// The data type of a schema field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    Timestamp,
    Duration,
    Int64,
    Int32,
    UInt64,
    UInt32,
    Float64,
    Bool,
    String,
    Json,
    FixedSizeBinary16,
    FixedSizeBinary8,
    ListUInt64,
    ListFloat64,
}

impl FieldType {
    /// Return the lowercase string name of this field type.
    pub fn as_str(self) -> &'static str {
        match self {
            FieldType::Timestamp => "timestamp_ns",
            FieldType::Duration => "duration_ns",
            FieldType::Int64 => "int64",
            FieldType::Int32 => "int32",
            FieldType::UInt64 => "uint64",
            FieldType::UInt32 => "uint32",
            FieldType::Float64 => "float64",
            FieldType::Bool => "bool",
            FieldType::String => "string",
            FieldType::Json => "json",
            FieldType::FixedSizeBinary16 => "fixed_size_binary_16",
            FieldType::FixedSizeBinary8 => "fixed_size_binary_8",
            FieldType::ListUInt64 => "list_uint64",
            FieldType::ListFloat64 => "list_float64",
        }
    }
}

/// A single schema field definition.
#[derive(Clone, Copy, Debug)]
pub struct SchemaField {
    pub name: &'static str,
    pub field_type: FieldType,
    pub required: bool,
}

/// A schema definition.
#[derive(Clone, Copy, Debug)]
pub struct SchemaDef {
    pub name: &'static str,
    pub fields: &'static [SchemaField],
}

use FieldType::{
    Bool, Duration, FixedSizeBinary16, FixedSizeBinary8, Float64, Int32, Int64, Json, ListFloat64,
    ListUInt64, String, Timestamp, UInt32, UInt64,
};

const LOG_FIELDS: &[SchemaField] = &[
    field("time_unix_nano", Timestamp, false),
    field("observed_time_unix_nano", Timestamp, false),
    field("trace_id", FixedSizeBinary16, false),
    field("span_id", FixedSizeBinary8, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("severity_number", Int32, false),
    field("severity_text", String, false),
    field("event_name", String, false),
    field("body", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("log_attributes", Json, false),
    field("dropped_attributes_count", UInt32, false),
    field("flags", UInt32, false),
];

const TRACE_FIELDS: &[SchemaField] = &[
    field("start_time_unix_nano", Timestamp, true),
    field("duration_time_unix_nano", Duration, true),
    field("trace_id", FixedSizeBinary16, true),
    field("span_id", FixedSizeBinary8, true),
    field("parent_span_id", FixedSizeBinary8, false),
    field("trace_state", String, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("name", String, true),
    field("kind", Int32, false),
    field("status_code", Int32, false),
    field("status_status_message", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("span_attributes", Json, false),
    field("events_json", Json, false),
    field("links_json", Json, false),
    field("dropped_attributes_count", UInt32, false),
    field("dropped_events_count", UInt32, false),
    field("dropped_links_count", UInt32, false),
    field("flags", UInt32, false),
];

const GAUGE_FIELDS: &[SchemaField] = &[
    field("time_unix_nano", Timestamp, true),
    field("start_time_unix_nano", Timestamp, false),
    field("name", String, true),
    field("description", String, false),
    field("unit", String, false),
    field("int_value", Int64, false),
    field("double_value", Float64, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", UInt32, false),
    field("exemplars_json", Json, false),
];

const SUM_FIELDS: &[SchemaField] = &[
    field("time_unix_nano", Timestamp, true),
    field("start_time_unix_nano", Timestamp, false),
    field("name", String, true),
    field("description", String, false),
    field("unit", String, false),
    field("int_value", Int64, false),
    field("double_value", Float64, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", UInt32, false),
    field("exemplars_json", Json, false),
    field("aggregation_temporality", Int32, false),
    field("is_monotonic", Bool, false),
];

const HISTOGRAM_FIELDS: &[SchemaField] = &[
    field("time_unix_nano", Timestamp, true),
    field("start_time_unix_nano", Timestamp, false),
    field("name", String, true),
    field("description", String, false),
    field("unit", String, false),
    field("count", UInt64, false),
    field("sum", Float64, false),
    field("min", Float64, false),
    field("max", Float64, false),
    field("bucket_counts", ListUInt64, false),
    field("explicit_bounds", ListFloat64, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", UInt32, false),
    field("exemplars_json", Json, false),
    field("aggregation_temporality", Int32, false),
];

const EXP_HISTOGRAM_FIELDS: &[SchemaField] = &[
    field("time_unix_nano", Timestamp, true),
    field("start_time_unix_nano", Timestamp, false),
    field("name", String, true),
    field("description", String, false),
    field("unit", String, false),
    field("count", UInt64, false),
    field("sum", Float64, false),
    field("min", Float64, false),
    field("max", Float64, false),
    field("scale", Int32, false),
    field("zero_count", UInt64, false),
    field("zero_threshold", Float64, false),
    field("positive_offset", Int32, false),
    field("positive_bucket_counts", ListUInt64, false),
    field("negative_offset", Int32, false),
    field("negative_bucket_counts", ListUInt64, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", UInt32, false),
    field("exemplars_json", Json, false),
    field("aggregation_temporality", Int32, false),
];

const ALL_SCHEMA_DEFS: &[SchemaDef] = &[
    SchemaDef {
        name: "logs",
        fields: LOG_FIELDS,
    },
    SchemaDef {
        name: "spans",
        fields: TRACE_FIELDS,
    },
    SchemaDef {
        name: "gauge",
        fields: GAUGE_FIELDS,
    },
    SchemaDef {
        name: "sum",
        fields: SUM_FIELDS,
    },
    SchemaDef {
        name: "histogram",
        fields: HISTOGRAM_FIELDS,
    },
    SchemaDef {
        name: "exp_histogram",
        fields: EXP_HISTOGRAM_FIELDS,
    },
];

pub fn schema_defs() -> &'static [SchemaDef] {
    ALL_SCHEMA_DEFS
}

pub fn schema_def(name: &str) -> Option<&'static SchemaDef> {
    ALL_SCHEMA_DEFS.iter().find(|schema| schema.name == name)
}

const fn field(name: &'static str, field_type: FieldType, required: bool) -> SchemaField {
    SchemaField {
        name,
        field_type,
        required,
    }
}
