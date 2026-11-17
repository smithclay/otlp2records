//! `ArrowProfile` over real transform output: the DuckDb profile carries no
//! type DuckDB's stock Arrow importer would map differently from the
//! duckdb-otlp column types, preserves every value, and the canonical profile
//! is a no-op.

use arrow_array::cast::AsArray;
use arrow_array::types::{DurationNanosecondType, Int64Type, UInt32Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};
use otlp2records::{
    exp_histogram_schema, gauge_schema, histogram_schema, logs_schema, sum_schema, to_ipc,
    traces_schema, transform_logs, transform_metrics, transform_traces, ArrowProfile, InputFormat,
};

/// The only types the DuckDb profile may emit for the normalized schemas.
fn assert_duckdb_type(column: &str, data_type: &DataType) {
    match data_type {
        DataType::Utf8
        | DataType::Int32
        | DataType::Int64
        | DataType::Float64
        | DataType::Boolean
        | DataType::Timestamp(TimeUnit::Nanosecond, None) => {}
        DataType::List(item) => assert_duckdb_type(column, item.data_type()),
        other => panic!("column {column}: {other} is not a DuckDb profile type"),
    }
}

fn all_schemas() -> Vec<(&'static str, Schema)> {
    vec![
        ("logs", logs_schema()),
        ("traces", traces_schema()),
        ("gauge", gauge_schema()),
        ("sum", sum_schema()),
        ("histogram", histogram_schema()),
        ("exp_histogram", exp_histogram_schema()),
    ]
}

#[test]
fn duckdb_schema_for_every_signal_has_only_duckdb_types() {
    for (signal, canonical) in all_schemas() {
        let mapped = ArrowProfile::DuckDb.schema(&canonical);
        assert_eq!(mapped.fields().len(), canonical.fields().len(), "{signal}");
        for (m, c) in mapped.fields().iter().zip(canonical.fields()) {
            assert_eq!(m.name(), c.name(), "{signal}");
            assert_eq!(m.is_nullable(), c.is_nullable(), "{signal}.{}", c.name());
            assert_duckdb_type(&format!("{signal}.{}", m.name()), m.data_type());
        }
    }
}

#[test]
fn duckdb_schema_spot_checks() {
    let ty = |schema: &Schema, name: &str| {
        ArrowProfile::DuckDb
            .schema(schema)
            .field_with_name(name)
            .unwrap()
            .data_type()
            .clone()
    };
    let traces = traces_schema();
    assert_eq!(ty(&traces, "trace_id"), DataType::Utf8);
    assert_eq!(ty(&traces, "span_id"), DataType::Utf8);
    assert_eq!(ty(&traces, "parent_span_id"), DataType::Utf8);
    assert_eq!(ty(&traces, "duration_time_unix_nano"), DataType::Int64);
    assert_eq!(ty(&traces, "dropped_events_count"), DataType::Int32);
    assert_eq!(
        ty(&traces, "start_time_unix_nano"),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    let logs = logs_schema();
    assert_eq!(ty(&logs, "trace_id"), DataType::Utf8);
    assert_eq!(ty(&logs, "flags"), DataType::Int32);
    let histogram = histogram_schema();
    assert_eq!(ty(&histogram, "count"), DataType::Int64);
    match ty(&histogram, "bucket_counts") {
        DataType::List(item) => assert_eq!(item.data_type(), &DataType::Int64),
        other => panic!("bucket_counts is {other}"),
    }
}

#[test]
fn canonical_schemas_are_unchanged() {
    for (signal, canonical) in all_schemas() {
        assert_eq!(
            ArrowProfile::Canonical.schema(&canonical),
            canonical,
            "{signal}"
        );
    }
    // Pin the canonical types the DuckDb profile rewrites, so a change to the
    // default schema is caught here rather than downstream.
    let traces = traces_schema();
    assert_eq!(
        traces.field_with_name("trace_id").unwrap().data_type(),
        &DataType::FixedSizeBinary(16)
    );
    assert_eq!(
        traces
            .field_with_name("duration_time_unix_nano")
            .unwrap()
            .data_type(),
        &DataType::Duration(TimeUnit::Nanosecond)
    );
    assert_eq!(
        traces.field_with_name("flags").unwrap().data_type(),
        &DataType::UInt32
    );
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Every FixedSizeBinary column became hex of the same bytes, null for null.
fn assert_ids_hex(canonical: &RecordBatch, mapped: &RecordBatch) {
    for (i, field) in canonical.schema().fields().iter().enumerate() {
        if !matches!(field.data_type(), DataType::FixedSizeBinary(_)) {
            continue;
        }
        let src = canonical.column(i).as_fixed_size_binary();
        let out = mapped.column(i).as_string::<i32>();
        for row in 0..src.len() {
            assert_eq!(
                src.is_null(row),
                out.is_null(row),
                "{}[{row}]",
                field.name()
            );
            if !src.is_null(row) {
                assert_eq!(
                    out.value(row),
                    hex(src.value(row)),
                    "{}[{row}]",
                    field.name()
                );
            }
        }
    }
}

/// Every unsigned / duration column holds the same values as signed ints.
fn assert_numbers_preserved(canonical: &RecordBatch, mapped: &RecordBatch) {
    for (i, field) in canonical.schema().fields().iter().enumerate() {
        let out = mapped.column(i);
        match field.data_type() {
            DataType::UInt32 => {
                let src = canonical.column(i).as_primitive::<UInt32Type>();
                let out = out.as_primitive::<arrow_array::types::Int32Type>();
                for row in 0..src.len() {
                    assert_eq!(src.is_null(row), out.is_null(row));
                    assert_eq!(i64::from(src.value(row)), i64::from(out.value(row)));
                }
            }
            DataType::UInt64 => {
                let src = canonical.column(i).as_primitive::<UInt64Type>();
                let out = out.as_primitive::<Int64Type>();
                for row in 0..src.len() {
                    assert_eq!(src.is_null(row), out.is_null(row));
                    assert_eq!(src.value(row) as i64, out.value(row));
                }
            }
            DataType::Duration(_) => {
                let src = canonical.column(i).as_primitive::<DurationNanosecondType>();
                let out = out.as_primitive::<Int64Type>();
                assert_eq!(src.values().as_ref(), out.values().as_ref());
                assert_eq!(src.nulls(), out.nulls());
            }
            DataType::List(item) if item.data_type() == &DataType::UInt64 => {
                let src = canonical.column(i).as_list::<i32>();
                let out = out.as_list::<i32>();
                assert_eq!(src.offsets(), out.offsets());
                assert_eq!(src.nulls(), out.nulls());
                let src = src.values().as_primitive::<UInt64Type>();
                let out = out.values().as_primitive::<Int64Type>();
                for row in 0..src.len() {
                    assert_eq!(src.is_null(row), out.is_null(row));
                    assert_eq!(src.value(row) as i64, out.value(row));
                }
            }
            // Checked by assert_ids_hex.
            DataType::FixedSizeBinary(_) => {}
            _ => assert_eq!(
                canonical.column(i).to_data(),
                out.to_data(),
                "{}",
                field.name()
            ),
        }
    }
}

fn check(canonical: RecordBatch, canonical_schema: &Schema) {
    assert!(canonical.num_rows() > 0);
    let mapped = ArrowProfile::DuckDb.apply(canonical.clone()).unwrap();
    // The batch schema is exactly what get_schema reports under the profile, so
    // a consumer binding against the schema sees the same types at scan time.
    assert_eq!(
        mapped.schema().as_ref(),
        &ArrowProfile::DuckDb.schema(canonical_schema)
    );
    assert_eq!(mapped.num_rows(), canonical.num_rows());
    assert_ids_hex(&canonical, &mapped);
    assert_numbers_preserved(&canonical, &mapped);

    // The canonical profile is byte-for-byte the input.
    let same = ArrowProfile::Canonical.apply(canonical.clone()).unwrap();
    assert_eq!(to_ipc(&same).unwrap(), to_ipc(&canonical).unwrap());
}

#[test]
fn logs_values_survive_the_duckdb_profile() {
    let pb = include_bytes!("../testdata/logs_large.pb");
    check(
        transform_logs(pb, InputFormat::Protobuf).unwrap(),
        &logs_schema(),
    );
}

#[test]
fn traces_values_survive_the_duckdb_profile() {
    let pb = include_bytes!("../testdata/traces_large.pb");
    let canonical = transform_traces(pb, InputFormat::Protobuf).unwrap();
    // The fixture must exercise both present and absent parent ids.
    let parent = canonical
        .column_by_name("parent_span_id")
        .unwrap()
        .as_fixed_size_binary();
    assert!(parent.null_count() > 0 && parent.null_count() < parent.len());
    check(canonical, &traces_schema());

    let json = include_bytes!("fixtures/sample_otlp_traces.json");
    check(
        transform_traces(json, InputFormat::Json).unwrap(),
        &traces_schema(),
    );
}

#[test]
fn metric_values_survive_the_duckdb_profile() {
    for pb in [
        &include_bytes!("../testdata/metrics_mixed.pb")[..],
        &include_bytes!("../testdata/metrics_histogram.pb")[..],
        &include_bytes!("../testdata/metrics_exponential_histogram.pb")[..],
    ] {
        let batches = transform_metrics(pb, InputFormat::Protobuf).unwrap();
        let mapped = ArrowProfile::DuckDb
            .apply_metrics(transform_metrics(pb, InputFormat::Protobuf).unwrap())
            .unwrap();
        for (canonical, out, schema) in [
            (batches.gauge, mapped.gauge, gauge_schema()),
            (batches.sum, mapped.sum, sum_schema()),
            (batches.histogram, mapped.histogram, histogram_schema()),
            (
                batches.exp_histogram,
                mapped.exp_histogram,
                exp_histogram_schema(),
            ),
        ] {
            assert_eq!(canonical.is_some(), out.is_some());
            if let Some(canonical) = canonical {
                check(canonical, &schema);
            }
        }
    }
}
