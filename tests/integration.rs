//! Integration tests for otlp2records
//!
//! These tests use real OTLP fixtures to verify the complete transformation pipeline.

use otlp2records::{
    to_ipc, to_json, transform_logs, transform_metrics, transform_traces, InputFormat,
};

// ============================================================================
// Logs Integration Tests
// ============================================================================

#[test]
fn test_full_pipeline_logs_json() {
    let json = include_bytes!("fixtures/sample_otlp.json");
    let batch = transform_logs(json, InputFormat::Json).unwrap();

    assert!(batch.num_rows() > 0, "Expected at least one log record");

    // Verify schema has expected fields
    let schema = batch.schema();
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("service_name").is_ok());
    assert!(schema.field_with_name("severity_number").is_ok());
    assert!(schema.field_with_name("body").is_ok());

    // Verify NDJSON output
    let ndjson = to_json(&batch).unwrap();
    let ndjson_str = String::from_utf8(ndjson).unwrap();
    assert!(!ndjson_str.is_empty(), "Expected non-empty NDJSON output");

    // Should contain the service name from fixture
    assert!(
        ndjson_str.contains("my-service"),
        "Expected service name in output"
    );
}

#[test]
fn test_logs_to_ipc() {
    let json = include_bytes!("fixtures/sample_otlp.json");
    let batch = transform_logs(json, InputFormat::Json).unwrap();

    let ipc = to_ipc(&batch).unwrap();
    assert!(!ipc.is_empty(), "Expected non-empty IPC output");

    // IPC format starts with magic bytes "ARROW1"
    assert!(ipc.len() > 6, "IPC output too short");
}

// ============================================================================
// Traces Integration Tests
// ============================================================================

#[test]
fn test_full_pipeline_traces_json() {
    let json = include_bytes!("fixtures/sample_otlp_traces.json");
    let batch = transform_traces(json, InputFormat::Json).unwrap();

    assert!(batch.num_rows() > 0, "Expected at least one span");

    // Verify schema has expected fields
    let schema = batch.schema();
    assert!(schema.field_with_name("timestamp").is_ok());
    assert!(schema.field_with_name("trace_id").is_ok());
    assert!(schema.field_with_name("span_id").is_ok());
    assert!(schema.field_with_name("span_name").is_ok());
    assert!(schema.field_with_name("service_name").is_ok());

    // Verify NDJSON output
    let ndjson = to_json(&batch).unwrap();
    let ndjson_str = String::from_utf8(ndjson).unwrap();
    assert!(!ndjson_str.is_empty(), "Expected non-empty NDJSON output");

    // Should contain the span name from fixture
    assert!(
        ndjson_str.contains("HTTP GET /api/users") || ndjson_str.contains("api/users"),
        "Expected span name in output"
    );
}

#[test]
fn test_traces_to_ipc() {
    let json = include_bytes!("fixtures/sample_otlp_traces.json");
    let batch = transform_traces(json, InputFormat::Json).unwrap();

    let ipc = to_ipc(&batch).unwrap();
    assert!(!ipc.is_empty(), "Expected non-empty IPC output");
}

// ============================================================================
// Metrics Integration Tests (JSON)
// ============================================================================

#[test]
fn test_full_pipeline_metrics_json() {
    let json = include_bytes!("fixtures/sample_otlp_metrics.json");
    let batches = transform_metrics(json, InputFormat::Json).unwrap();

    // Fixture contains both gauge and sum metrics
    assert!(
        batches.gauge.is_some() || batches.sum.is_some(),
        "Expected at least one metric type"
    );

    // Verify gauge metrics
    if let Some(gauge) = &batches.gauge {
        assert!(gauge.num_rows() > 0, "Expected at least one gauge metric");

        let schema = gauge.schema();
        assert!(schema.field_with_name("metric_name").is_ok());
        assert!(schema.field_with_name("value").is_ok());
        assert!(schema.field_with_name("timestamp").is_ok());

        let ndjson = to_json(gauge).unwrap();
        let ndjson_str = String::from_utf8(ndjson).unwrap();
        assert!(!ndjson_str.is_empty());
    }

    // Verify sum metrics
    if let Some(sum) = &batches.sum {
        assert!(sum.num_rows() > 0, "Expected at least one sum metric");

        let schema = sum.schema();
        assert!(schema.field_with_name("metric_name").is_ok());
        assert!(schema.field_with_name("value").is_ok());
        assert!(schema.field_with_name("aggregation_temporality").is_ok());
        assert!(schema.field_with_name("is_monotonic").is_ok());

        let ndjson = to_json(sum).unwrap();
        let ndjson_str = String::from_utf8(ndjson).unwrap();
        assert!(!ndjson_str.is_empty());
    }
}

// ============================================================================
// Metrics Integration Tests (Protobuf)
// ============================================================================

#[test]
fn test_full_pipeline_metrics_gauge_protobuf() {
    let pb = include_bytes!("fixtures/metrics_gauge.pb");
    let batches = transform_metrics(pb, InputFormat::Protobuf).unwrap();

    assert!(batches.gauge.is_some(), "Expected gauge metrics");
    assert!(
        batches.sum.is_none(),
        "Expected no sum metrics in gauge-only file"
    );

    let gauge = batches.gauge.unwrap();
    assert!(
        gauge.num_rows() > 0,
        "Expected at least one gauge data point"
    );

    // Verify output
    let ndjson = to_json(&gauge).unwrap();
    assert!(!ndjson.is_empty());
}

#[test]
fn test_full_pipeline_metrics_sum_protobuf() {
    let pb = include_bytes!("fixtures/metrics_sum.pb");
    let batches = transform_metrics(pb, InputFormat::Protobuf).unwrap();

    assert!(batches.sum.is_some(), "Expected sum metrics");
    assert!(
        batches.gauge.is_none(),
        "Expected no gauge metrics in sum-only file"
    );

    let sum = batches.sum.unwrap();
    assert!(sum.num_rows() > 0, "Expected at least one sum data point");

    // Verify schema has sum-specific fields
    let schema = sum.schema();
    assert!(schema.field_with_name("aggregation_temporality").is_ok());
    assert!(schema.field_with_name("is_monotonic").is_ok());

    // Verify output
    let ndjson = to_json(&sum).unwrap();
    assert!(!ndjson.is_empty());
}

#[test]
fn test_full_pipeline_metrics_mixed_protobuf() {
    let pb = include_bytes!("fixtures/metrics_mixed.pb");
    let batches = transform_metrics(pb, InputFormat::Protobuf).unwrap();

    // Mixed file should have both types
    assert!(
        batches.gauge.is_some() || batches.sum.is_some(),
        "Expected at least one metric type in mixed file"
    );

    // Verify each type if present
    if let Some(gauge) = &batches.gauge {
        assert!(gauge.num_rows() > 0);
        let ndjson = to_json(gauge).unwrap();
        assert!(!ndjson.is_empty());
    }

    if let Some(sum) = &batches.sum {
        assert!(sum.num_rows() > 0);
        let ndjson = to_json(sum).unwrap();
        assert!(!ndjson.is_empty());
    }
}

// ============================================================================
// Cross-format tests
// ============================================================================

#[test]
fn test_logs_schema_consistency() {
    // Transform from JSON fixture
    let json_batch = transform_logs(
        include_bytes!("fixtures/sample_otlp.json"),
        InputFormat::Json,
    )
    .unwrap();

    // The schema should have consistent fields regardless of input format
    let schema = json_batch.schema();

    // Required fields in logs schema
    let required_fields = [
        "timestamp",
        "observed_timestamp",
        "severity_number",
        "severity_text",
        "body",
        "trace_id",
        "span_id",
        "service_name",
        "scope_name",
    ];

    for field in required_fields {
        assert!(
            schema.field_with_name(field).is_ok(),
            "Missing required field: {field}"
        );
    }
}

#[test]
fn test_traces_schema_consistency() {
    let json_batch = transform_traces(
        include_bytes!("fixtures/sample_otlp_traces.json"),
        InputFormat::Json,
    )
    .unwrap();

    let schema = json_batch.schema();

    // Required fields in traces schema
    let required_fields = [
        "timestamp",
        "trace_id",
        "span_id",
        "parent_span_id",
        "span_name",
        "span_kind",
        "service_name",
        "scope_name",
        "duration",
    ];

    for field in required_fields {
        assert!(
            schema.field_with_name(field).is_ok(),
            "Missing required field: {field}"
        );
    }
}

#[test]
fn test_metrics_schema_consistency() {
    let json_batches = transform_metrics(
        include_bytes!("fixtures/sample_otlp_metrics.json"),
        InputFormat::Json,
    )
    .unwrap();

    // Gauge schema
    if let Some(gauge) = &json_batches.gauge {
        let schema = gauge.schema();
        let required_fields = ["timestamp", "metric_name", "value", "service_name"];
        for field in required_fields {
            assert!(
                schema.field_with_name(field).is_ok(),
                "Missing required gauge field: {field}"
            );
        }
    }

    // Sum schema
    if let Some(sum) = &json_batches.sum {
        let schema = sum.schema();
        let required_fields = [
            "timestamp",
            "metric_name",
            "value",
            "service_name",
            "aggregation_temporality",
            "is_monotonic",
        ];
        for field in required_fields {
            assert!(
                schema.field_with_name(field).is_ok(),
                "Missing required sum field: {field}"
            );
        }
    }
}

// ============================================================================
// Round-trip tests
// ============================================================================

#[test]
fn test_logs_to_json_is_valid_ndjson() {
    let batch = transform_logs(
        include_bytes!("fixtures/sample_otlp.json"),
        InputFormat::Json,
    )
    .unwrap();

    let ndjson = to_json(&batch).unwrap();
    let ndjson_str = String::from_utf8(ndjson).unwrap();

    // Each line should be valid JSON
    for line in ndjson_str.lines() {
        if !line.trim().is_empty() {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
            assert!(parsed.is_ok(), "Invalid JSON line: {line}");
        }
    }
}

#[test]
fn test_traces_to_json_is_valid_ndjson() {
    let batch = transform_traces(
        include_bytes!("fixtures/sample_otlp_traces.json"),
        InputFormat::Json,
    )
    .unwrap();

    let ndjson = to_json(&batch).unwrap();
    let ndjson_str = String::from_utf8(ndjson).unwrap();

    // Each line should be valid JSON
    for line in ndjson_str.lines() {
        if !line.trim().is_empty() {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
            assert!(parsed.is_ok(), "Invalid JSON line: {line}");
        }
    }
}
