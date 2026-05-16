//! Arrow schema accessors.

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use once_cell::sync::Lazy;

static LOGS_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        ts("timestamp", false),
        i64_field("observed_timestamp", false),
        utf8("trace_id", true),
        utf8("span_id", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        i32_field("severity_number", false),
        utf8("severity_text", false),
        utf8("body", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("log_attributes", true),
    ])
});

static TRACES_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        ts("timestamp", false),
        i64_field("end_timestamp", false),
        i64_field("duration", false),
        utf8("trace_id", true),
        utf8("span_id", true),
        utf8("parent_span_id", true),
        utf8("trace_state", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("span_name", false),
        i32_field("span_kind", false),
        i32_field("status_code", false),
        utf8("status_message", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("span_attributes", true),
        utf8("events_json", true),
        utf8("links_json", true),
        i32_field("dropped_attributes_count", true),
        i32_field("dropped_events_count", true),
        i32_field("dropped_links_count", true),
        i32_field("flags", true),
    ])
});

static GAUGE_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        ts("timestamp", false),
        i64_field("start_timestamp", true),
        utf8("metric_name", false),
        utf8("metric_description", true),
        utf8("metric_unit", true),
        f64_field("value", false),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        i32_field("flags", true),
        utf8("exemplars_json", true),
    ])
});

static SUM_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        ts("timestamp", false),
        i64_field("start_timestamp", true),
        utf8("metric_name", false),
        utf8("metric_description", true),
        utf8("metric_unit", true),
        f64_field("value", false),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        i32_field("flags", true),
        utf8("exemplars_json", true),
        i32_field("aggregation_temporality", false),
        bool_field("is_monotonic", false),
    ])
});

static HISTOGRAM_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        ts("timestamp", false),
        i64_field("start_timestamp", true),
        utf8("metric_name", false),
        utf8("metric_description", true),
        utf8("metric_unit", true),
        i64_field("count", false),
        f64_field("sum", true),
        f64_field("min", true),
        f64_field("max", true),
        utf8("bucket_counts", false),
        utf8("explicit_bounds", false),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        i32_field("flags", true),
        utf8("exemplars_json", true),
        i32_field("aggregation_temporality", false),
    ])
});

static EXP_HISTOGRAM_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        ts("timestamp", false),
        i64_field("start_timestamp", true),
        utf8("metric_name", false),
        utf8("metric_description", true),
        utf8("metric_unit", true),
        i64_field("count", false),
        f64_field("sum", true),
        f64_field("min", true),
        f64_field("max", true),
        i32_field("scale", false),
        i64_field("zero_count", false),
        f64_field("zero_threshold", true),
        i32_field("positive_offset", true),
        utf8("positive_bucket_counts", true),
        i32_field("negative_offset", true),
        utf8("negative_bucket_counts", true),
        utf8("service_name", false),
        utf8("service_namespace", true),
        utf8("service_instance_id", true),
        utf8("resource_attributes", true),
        utf8("scope_name", true),
        utf8("scope_version", true),
        utf8("scope_attributes", true),
        utf8("metric_attributes", true),
        i32_field("flags", true),
        utf8("exemplars_json", true),
        i32_field("aggregation_temporality", false),
    ])
});

pub fn logs_schema() -> Schema {
    LOGS_SCHEMA.clone()
}

pub fn traces_schema() -> Schema {
    TRACES_SCHEMA.clone()
}

pub fn gauge_schema() -> Schema {
    GAUGE_SCHEMA.clone()
}

pub fn sum_schema() -> Schema {
    SUM_SCHEMA.clone()
}

pub fn histogram_schema() -> Schema {
    HISTOGRAM_SCHEMA.clone()
}

pub fn exp_histogram_schema() -> Schema {
    EXP_HISTOGRAM_SCHEMA.clone()
}

fn ts(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        nullable,
    )
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

fn f64_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Float64, nullable)
}

fn bool_field(name: &str, nullable: bool) -> Field {
    Field::new(name, DataType::Boolean, nullable)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logs_schema_has_expected_fields() {
        let schema = logs_schema();
        let ts_field = schema.field_with_name("timestamp").unwrap();
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(!ts_field.is_nullable());
        assert_eq!(
            schema.field_with_name("service_name").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(schema.field_with_name("trace_id").unwrap().is_nullable());
        assert_eq!(
            schema
                .field_with_name("severity_number")
                .unwrap()
                .data_type(),
            &DataType::Int32
        );
    }

    #[test]
    fn traces_schema_has_expected_fields() {
        let schema = traces_schema();
        assert_eq!(
            schema.field_with_name("timestamp").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field_with_name("span_kind").unwrap().data_type(),
            &DataType::Int32
        );
        assert_eq!(
            schema.field_with_name("duration").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn metric_schemas_have_expected_fields() {
        assert_eq!(
            gauge_schema().field_with_name("value").unwrap().data_type(),
            &DataType::Float64
        );
        assert!(!sum_schema()
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
            &DataType::Utf8
        );
        assert_eq!(
            exp_histogram_schema()
                .field_with_name("scale")
                .unwrap()
                .data_type(),
            &DataType::Int32
        );
    }
}
