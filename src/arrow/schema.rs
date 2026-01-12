//! Arrow schema accessors
//!
//! Provides public access to the Arrow schemas generated from VRL @schema annotations.
//! These schemas are generated at build time by build.rs.

use arrow::datatypes::Schema;
use once_cell::sync::Lazy;

// Include compiled VRL schemas from build.rs
include!(concat!(env!("OUT_DIR"), "/compiled_vrl.rs"));

/// Returns the Arrow schema for OTLP logs.
///
/// Schema fields:
/// - timestamp: TimestampMillisecond (required)
/// - observed_timestamp: Int64 (required)
/// - trace_id: Utf8 (optional)
/// - span_id: Utf8 (optional)
/// - service_name: Utf8 (required)
/// - service_namespace: Utf8 (optional)
/// - service_instance_id: Utf8 (optional)
/// - severity_number: Int32 (required)
/// - severity_text: Utf8 (required)
/// - body: Utf8 (optional)
/// - resource_attributes: Utf8/JSON (optional)
/// - scope_name: Utf8 (optional)
/// - scope_version: Utf8 (optional)
/// - scope_attributes: Utf8/JSON (optional)
/// - log_attributes: Utf8/JSON (optional)
pub fn logs_schema() -> Schema {
    OTLP_LOGS_SCHEMA.clone()
}

/// Returns the Arrow schema for OTLP trace spans.
///
/// Schema fields:
/// - timestamp: TimestampMillisecond (required)
/// - end_timestamp: Int64 (required)
/// - duration: Int64 (required)
/// - trace_id: Utf8 (optional)
/// - span_id: Utf8 (optional)
/// - parent_span_id: Utf8 (optional)
/// - trace_state: Utf8 (optional)
/// - service_name: Utf8 (required)
/// - service_namespace: Utf8 (optional)
/// - service_instance_id: Utf8 (optional)
/// - span_name: Utf8 (required)
/// - span_kind: Int32 (required)
/// - status_code: Int32 (required)
/// - status_message: Utf8 (optional)
/// - resource_attributes: Utf8/JSON (optional)
/// - scope_name: Utf8 (optional)
/// - scope_version: Utf8 (optional)
/// - scope_attributes: Utf8/JSON (optional)
/// - span_attributes: Utf8/JSON (optional)
/// - events_json: Utf8/JSON (optional)
/// - links_json: Utf8/JSON (optional)
/// - dropped_attributes_count: Int32 (optional)
/// - dropped_events_count: Int32 (optional)
/// - dropped_links_count: Int32 (optional)
/// - flags: Int32 (optional)
pub fn traces_schema() -> Schema {
    OTLP_TRACES_SCHEMA.clone()
}

/// Returns the Arrow schema for OTLP gauge metrics.
///
/// Schema fields:
/// - timestamp: TimestampMillisecond (required)
/// - start_timestamp: Int64 (optional)
/// - metric_name: Utf8 (required)
/// - metric_description: Utf8 (optional)
/// - metric_unit: Utf8 (optional)
/// - value: Float64 (required)
/// - service_name: Utf8 (required)
/// - service_namespace: Utf8 (optional)
/// - service_instance_id: Utf8 (optional)
/// - resource_attributes: Utf8/JSON (optional)
/// - scope_name: Utf8 (optional)
/// - scope_version: Utf8 (optional)
/// - scope_attributes: Utf8/JSON (optional)
/// - metric_attributes: Utf8/JSON (optional)
/// - flags: Int32 (optional)
/// - exemplars_json: Utf8/JSON (optional)
pub fn gauge_schema() -> Schema {
    OTLP_GAUGE_SCHEMA.clone()
}

/// Returns the Arrow schema for OTLP sum metrics.
///
/// Schema fields:
/// - timestamp: TimestampMillisecond (required)
/// - start_timestamp: Int64 (optional)
/// - metric_name: Utf8 (required)
/// - metric_description: Utf8 (optional)
/// - metric_unit: Utf8 (optional)
/// - value: Float64 (required)
/// - service_name: Utf8 (required)
/// - service_namespace: Utf8 (optional)
/// - service_instance_id: Utf8 (optional)
/// - resource_attributes: Utf8/JSON (optional)
/// - scope_name: Utf8 (optional)
/// - scope_version: Utf8 (optional)
/// - scope_attributes: Utf8/JSON (optional)
/// - metric_attributes: Utf8/JSON (optional)
/// - flags: Int32 (optional)
/// - exemplars_json: Utf8/JSON (optional)
/// - aggregation_temporality: Int32 (required)
/// - is_monotonic: Boolean (required)
pub fn sum_schema() -> Schema {
    OTLP_SUM_SCHEMA.clone()
}

/// Returns the Arrow schema for OTLP histogram metrics.
///
/// Schema fields:
/// - timestamp: TimestampMillisecond (required)
/// - start_timestamp: Int64 (optional)
/// - metric_name: Utf8 (required)
/// - metric_description: Utf8 (optional)
/// - metric_unit: Utf8 (optional)
/// - count: Int64 (required)
/// - sum: Float64 (optional)
/// - min: Float64 (optional)
/// - max: Float64 (optional)
/// - bucket_counts: Utf8/JSON (required) - JSON array of u64
/// - explicit_bounds: Utf8/JSON (required) - JSON array of f64
/// - service_name: Utf8 (required)
/// - service_namespace: Utf8 (optional)
/// - service_instance_id: Utf8 (optional)
/// - resource_attributes: Utf8/JSON (optional)
/// - scope_name: Utf8 (optional)
/// - scope_version: Utf8 (optional)
/// - scope_attributes: Utf8/JSON (optional)
/// - metric_attributes: Utf8/JSON (optional)
/// - flags: Int32 (optional)
/// - exemplars_json: Utf8/JSON (optional)
/// - aggregation_temporality: Int32 (required)
pub fn histogram_schema() -> Schema {
    OTLP_HISTOGRAM_SCHEMA.clone()
}

/// Returns the Arrow schema for OTLP exponential histogram metrics.
///
/// Schema fields:
/// - timestamp: TimestampMillisecond (required)
/// - start_timestamp: Int64 (optional)
/// - metric_name: Utf8 (required)
/// - metric_description: Utf8 (optional)
/// - metric_unit: Utf8 (optional)
/// - count: Int64 (required)
/// - sum: Float64 (optional)
/// - min: Float64 (optional)
/// - max: Float64 (optional)
/// - scale: Int32 (required)
/// - zero_count: Int64 (required)
/// - zero_threshold: Float64 (optional)
/// - positive_offset: Int32 (optional)
/// - positive_bucket_counts: Utf8/JSON (optional) - JSON array of u64
/// - negative_offset: Int32 (optional)
/// - negative_bucket_counts: Utf8/JSON (optional) - JSON array of u64
/// - service_name: Utf8 (required)
/// - service_namespace: Utf8 (optional)
/// - service_instance_id: Utf8 (optional)
/// - resource_attributes: Utf8/JSON (optional)
/// - scope_name: Utf8 (optional)
/// - scope_version: Utf8 (optional)
/// - scope_attributes: Utf8/JSON (optional)
/// - metric_attributes: Utf8/JSON (optional)
/// - flags: Int32 (optional)
/// - exemplars_json: Utf8/JSON (optional)
/// - aggregation_temporality: Int32 (required)
pub fn exp_histogram_schema() -> Schema {
    OTLP_EXP_HISTOGRAM_SCHEMA.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn test_logs_schema_has_expected_fields() {
        let schema = logs_schema();

        // Check timestamp field
        let ts_field = schema.field_with_name("timestamp").unwrap();
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert!(!ts_field.is_nullable());

        // Check service_name field
        let service_field = schema.field_with_name("service_name").unwrap();
        assert_eq!(service_field.data_type(), &DataType::Utf8);
        assert!(!service_field.is_nullable());

        // Check trace_id is optional
        let trace_id_field = schema.field_with_name("trace_id").unwrap();
        assert!(trace_id_field.is_nullable());

        // Check severity_number field
        let severity_field = schema.field_with_name("severity_number").unwrap();
        assert_eq!(severity_field.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_traces_schema_has_expected_fields() {
        let schema = traces_schema();

        // Check timestamp field
        let ts_field = schema.field_with_name("timestamp").unwrap();
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        // Check span_kind field
        let kind_field = schema.field_with_name("span_kind").unwrap();
        assert_eq!(kind_field.data_type(), &DataType::Int32);

        // Check duration field
        let duration_field = schema.field_with_name("duration").unwrap();
        assert_eq!(duration_field.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_gauge_schema_has_expected_fields() {
        let schema = gauge_schema();

        // Check value field
        let value_field = schema.field_with_name("value").unwrap();
        assert_eq!(value_field.data_type(), &DataType::Float64);
        assert!(!value_field.is_nullable());

        // Check metric_name field
        let name_field = schema.field_with_name("metric_name").unwrap();
        assert_eq!(name_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_sum_schema_has_expected_fields() {
        let schema = sum_schema();

        // Check aggregation_temporality field
        let agg_field = schema.field_with_name("aggregation_temporality").unwrap();
        assert_eq!(agg_field.data_type(), &DataType::Int32);
        assert!(!agg_field.is_nullable());

        // Check is_monotonic field
        let mono_field = schema.field_with_name("is_monotonic").unwrap();
        assert_eq!(mono_field.data_type(), &DataType::Boolean);
        assert!(!mono_field.is_nullable());
    }

    #[test]
    fn test_schemas_are_cloneable() {
        // Multiple calls should return independent clones
        let schema1 = logs_schema();
        let schema2 = logs_schema();

        assert_eq!(schema1.fields().len(), schema2.fields().len());
    }
}
