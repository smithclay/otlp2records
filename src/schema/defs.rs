//! Schema definitions for public schema inspection.

/// The data type of a schema field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldType {
    Timestamp,
    Int64,
    Int32,
    Float64,
    Bool,
    String,
    Json,
}

impl FieldType {
    /// Return the lowercase string name of this field type.
    pub fn as_str(self) -> &'static str {
        match self {
            FieldType::Timestamp => "timestamp",
            FieldType::Int64 => "int64",
            FieldType::Int32 => "int32",
            FieldType::Float64 => "float64",
            FieldType::Bool => "bool",
            FieldType::String => "string",
            FieldType::Json => "json",
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

use FieldType::{Bool, Float64, Int32, Int64, Json, String, Timestamp};

const LOG_FIELDS: &[SchemaField] = &[
    field("timestamp", Timestamp, true),
    field("observed_timestamp", Int64, true),
    field("trace_id", String, false),
    field("span_id", String, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("severity_number", Int32, true),
    field("severity_text", String, true),
    field("body", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("log_attributes", Json, false),
];

const TRACE_FIELDS: &[SchemaField] = &[
    field("timestamp", Timestamp, true),
    field("end_timestamp", Int64, true),
    field("duration", Int64, true),
    field("trace_id", String, false),
    field("span_id", String, false),
    field("parent_span_id", String, false),
    field("trace_state", String, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("span_name", String, true),
    field("span_kind", Int32, true),
    field("status_code", Int32, true),
    field("status_message", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("span_attributes", Json, false),
    field("events_json", Json, false),
    field("links_json", Json, false),
    field("dropped_attributes_count", Int32, false),
    field("dropped_events_count", Int32, false),
    field("dropped_links_count", Int32, false),
    field("flags", Int32, false),
];

const GAUGE_FIELDS: &[SchemaField] = &[
    field("timestamp", Timestamp, true),
    field("start_timestamp", Int64, false),
    field("metric_name", String, true),
    field("metric_description", String, false),
    field("metric_unit", String, false),
    field("value", Float64, true),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", Int32, false),
    field("exemplars_json", Json, false),
];

const SUM_FIELDS: &[SchemaField] = &[
    field("timestamp", Timestamp, true),
    field("start_timestamp", Int64, false),
    field("metric_name", String, true),
    field("metric_description", String, false),
    field("metric_unit", String, false),
    field("value", Float64, true),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", Int32, false),
    field("exemplars_json", Json, false),
    field("aggregation_temporality", Int32, true),
    field("is_monotonic", Bool, true),
];

const HISTOGRAM_FIELDS: &[SchemaField] = &[
    field("timestamp", Timestamp, true),
    field("start_timestamp", Int64, false),
    field("metric_name", String, true),
    field("metric_description", String, false),
    field("metric_unit", String, false),
    field("count", Int64, true),
    field("sum", Float64, false),
    field("min", Float64, false),
    field("max", Float64, false),
    field("bucket_counts", Json, true),
    field("explicit_bounds", Json, true),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", Int32, false),
    field("exemplars_json", Json, false),
    field("aggregation_temporality", Int32, true),
];

const EXP_HISTOGRAM_FIELDS: &[SchemaField] = &[
    field("timestamp", Timestamp, true),
    field("start_timestamp", Int64, false),
    field("metric_name", String, true),
    field("metric_description", String, false),
    field("metric_unit", String, false),
    field("count", Int64, true),
    field("sum", Float64, false),
    field("min", Float64, false),
    field("max", Float64, false),
    field("scale", Int32, true),
    field("zero_count", Int64, true),
    field("zero_threshold", Float64, false),
    field("positive_offset", Int32, false),
    field("positive_bucket_counts", Json, false),
    field("negative_offset", Int32, false),
    field("negative_bucket_counts", Json, false),
    field("service_name", String, true),
    field("service_namespace", String, false),
    field("service_instance_id", String, false),
    field("resource_attributes", Json, false),
    field("scope_name", String, false),
    field("scope_version", String, false),
    field("scope_attributes", Json, false),
    field("metric_attributes", Json, false),
    field("flags", Int32, false),
    field("exemplars_json", Json, false),
    field("aggregation_temporality", Int32, true),
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
