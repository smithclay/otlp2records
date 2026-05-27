//! Arrow schema accessors.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use once_cell::sync::Lazy;

static LOGS_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ts_ns("time_unix_nano", true),
        ts_ns("observed_time_unix_nano", true),
        fixed("trace_id", 16, true),
        fixed("span_id", 8, true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        i32_field("severity_number", true),
        utf8("severity_text", true),
        utf8("event_name", true),
        utf8("body", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("log_attributes", true),
        u32_field("dropped_attributes_count", true),
        u32_field("flags", true),
    ]))
});

static TRACES_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ts_ns("start_time_unix_nano", false),
        duration_ns("duration_time_unix_nano", false),
        fixed("trace_id", 16, false),
        fixed("span_id", 8, false),
        fixed("parent_span_id", 8, true),
        utf8("trace_state", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("name", false),
        i32_field("kind", true),
        i32_field("status_code", true),
        utf8("status_status_message", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("span_attributes", true),
        utf8("events_json", true),
        utf8("links_json", true),
        u32_field("dropped_attributes_count", true),
        u32_field("dropped_events_count", true),
        u32_field("dropped_links_count", true),
        u32_field("flags", true),
    ]))
});

static GAUGE_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ts_ns("time_unix_nano", false),
        ts_ns("start_time_unix_nano", true),
        utf8("name", false),
        utf8("description", true),
        utf8("unit", true),
        i64_field("int_value", true),
        f64_field("double_value", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        u32_field("flags", true),
        utf8("exemplars_json", true),
    ]))
});

static SUM_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ts_ns("time_unix_nano", false),
        ts_ns("start_time_unix_nano", true),
        utf8("name", false),
        utf8("description", true),
        utf8("unit", true),
        i64_field("int_value", true),
        f64_field("double_value", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        u32_field("flags", true),
        utf8("exemplars_json", true),
        i32_field("aggregation_temporality", true),
        bool_field("is_monotonic", true),
    ]))
});

static HISTOGRAM_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ts_ns("time_unix_nano", false),
        ts_ns("start_time_unix_nano", true),
        utf8("name", false),
        utf8("description", true),
        utf8("unit", true),
        u64_field("count", true),
        f64_field("sum", true),
        f64_field("min", true),
        f64_field("max", true),
        u64_list("bucket_counts", true),
        f64_list("explicit_bounds", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        u32_field("flags", true),
        utf8("exemplars_json", true),
        i32_field("aggregation_temporality", true),
    ]))
});

static EXP_HISTOGRAM_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ts_ns("time_unix_nano", false),
        ts_ns("start_time_unix_nano", true),
        utf8("name", false),
        utf8("description", true),
        utf8("unit", true),
        u64_field("count", true),
        f64_field("sum", true),
        f64_field("min", true),
        f64_field("max", true),
        i32_field("scale", true),
        u64_field("zero_count", true),
        f64_field("zero_threshold", true),
        i32_field("positive_offset", true),
        u64_list("positive_bucket_counts", true),
        i32_field("negative_offset", true),
        u64_list("negative_bucket_counts", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        u32_field("flags", true),
        utf8("exemplars_json", true),
        i32_field("aggregation_temporality", true),
    ]))
});

pub fn logs_schema() -> Schema {
    (**LOGS_SCHEMA).clone()
}

pub fn traces_schema() -> Schema {
    (**TRACES_SCHEMA).clone()
}

pub fn gauge_schema() -> Schema {
    (**GAUGE_SCHEMA).clone()
}

pub fn sum_schema() -> Schema {
    (**SUM_SCHEMA).clone()
}

pub fn histogram_schema() -> Schema {
    (**HISTOGRAM_SCHEMA).clone()
}

pub fn exp_histogram_schema() -> Schema {
    (**EXP_HISTOGRAM_SCHEMA).clone()
}

pub(crate) fn logs_schema_arc() -> Arc<Schema> {
    Arc::clone(&LOGS_SCHEMA)
}

pub(crate) fn traces_schema_arc() -> Arc<Schema> {
    Arc::clone(&TRACES_SCHEMA)
}

pub(crate) fn gauge_schema_arc() -> Arc<Schema> {
    Arc::clone(&GAUGE_SCHEMA)
}

pub(crate) fn sum_schema_arc() -> Arc<Schema> {
    Arc::clone(&SUM_SCHEMA)
}

pub(crate) fn histogram_schema_arc() -> Arc<Schema> {
    Arc::clone(&HISTOGRAM_SCHEMA)
}

pub(crate) fn exp_histogram_schema_arc() -> Arc<Schema> {
    Arc::clone(&EXP_HISTOGRAM_SCHEMA)
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

fn i64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int64, nullable)
}

fn i32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Int32, nullable)
}

fn u32_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt32, nullable)
}

fn u64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::UInt64, nullable)
}

fn fixed(name: &str, size: i32, nullable: bool) -> Field {
    Field::new(name, DataType::FixedSizeBinary(size), nullable)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logs_schema_has_expected_fields() {
        let schema = logs_schema();
        let ts_field = schema.field_with_name("time_unix_nano").unwrap();
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert!(ts_field.is_nullable());
        assert_eq!(
            schema.field_with_name("service_name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("trace_id").unwrap().data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(schema.field_with_name("trace_id").unwrap().is_nullable());
        assert_eq!(
            schema
                .field_with_name("severity_number")
                .unwrap()
                .data_type(),
            &DataType::Int32
        );
        assert!(schema
            .field_with_name("severity_number")
            .unwrap()
            .is_nullable());
        assert_eq!(
            schema.field_with_name("flags").unwrap().data_type(),
            &DataType::UInt32
        );
    }

    #[test]
    fn traces_schema_has_expected_fields() {
        let schema = traces_schema();
        assert_eq!(
            schema
                .field_with_name("start_time_unix_nano")
                .unwrap()
                .data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            schema.field_with_name("kind").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema
                .field_with_name("duration_time_unix_nano")
                .unwrap()
                .data_type(),
            &DataType::Duration(TimeUnit::Nanosecond)
        );
        assert_eq!(
            schema.field_with_name("span_id").unwrap().data_type(),
            &DataType::FixedSizeBinary(8)
        );
    }

    #[test]
    fn metric_schemas_have_expected_fields() {
        assert_eq!(
            gauge_schema()
                .field_with_name("double_value")
                .unwrap()
                .data_type(),
            &DataType::Float64
        );
        assert_eq!(
            gauge_schema()
                .field_with_name("int_value")
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
        assert!(sum_schema()
            .field_with_name("aggregation_temporality")
            .unwrap()
            .is_nullable());
        assert_eq!(
            sum_schema()
                .field_with_name("is_monotonic")
                .unwrap()
                .data_type(),
            &DataType::Boolean
        );
        assert_eq!(
            histogram_schema()
                .field_with_name("bucket_counts")
                .unwrap()
                .data_type(),
            &DataType::List(Field::new("item", DataType::UInt64, true).into())
        );
        assert_eq!(
            exp_histogram_schema()
                .field_with_name("zero_count")
                .unwrap()
                .data_type(),
            &DataType::UInt64
        );
    }
}
