//! `TransformOptions`: a narrowed transform equals the full transform, filtered
//! by the same predicates and projected to the requested columns.

use std::collections::BTreeSet;

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_select::filter::filter_record_batch;
use otlp2records::{
    otap::OtapDecoder, transform_logs, transform_metrics, transform_traces, Error, InputFormat,
    MetricBatches, MetricShape, MetricShapeBatch, TransformOptions, Transformer,
};

const LOGS: &[u8] = include_bytes!("../testdata/logs_large.pb");
const TRACES: &[u8] = include_bytes!("../testdata/traces_large.pb");
const METRICS: &[u8] = include_bytes!("../testdata/metrics_mixed.pb");
const LOGS_JSON: &[u8] = include_bytes!("fixtures/sample_otlp.json");

const SHAPES: [MetricShape; 4] = [
    MetricShape::Gauge,
    MetricShape::Sum,
    MetricShape::Histogram,
    MetricShape::ExponentialHistogram,
];

/// The predicates a test narrows by, applied the slow way.
#[derive(Clone, Default)]
struct Predicates {
    columns: Option<Vec<usize>>,
    services: Option<Vec<String>>,
    ranges: Vec<(usize, i64, i64)>,
}

impl Predicates {
    fn options(&self) -> TransformOptions {
        let mut options = TransformOptions::new();
        if let Some(columns) = &self.columns {
            options = options.with_columns(columns.iter().copied());
        }
        if let Some(services) = self.services.clone() {
            options = options.with_service_filter(move |name| services.iter().any(|s| s == name));
        }
        for &(column, min, max) in &self.ranges {
            options = options.with_time_range(column, min, max);
        }
        options
    }

    /// `full` filtered by the predicates (SQL semantics: NULL fails a range)
    /// and projected to the requested columns.
    fn expected(&self, full: &RecordBatch) -> RecordBatch {
        let rows = full.num_rows();
        let mut keep = vec![true; rows];
        if let Some(services) = &self.services {
            let column = string_column(full, "service_name");
            for (row, keep) in keep.iter_mut().enumerate() {
                *keep &= services.iter().any(|s| s == column.value(row));
            }
        }
        for &(column, min, max) in &self.ranges {
            let values = full
                .column(column)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp column");
            for (row, keep) in keep.iter_mut().enumerate() {
                *keep &= values.is_valid(row) && (min..=max).contains(&values.value(row));
            }
        }
        let filtered = filter_record_batch(full, &BooleanArray::from(keep)).unwrap();
        match &self.columns {
            Some(columns) => filtered.project(columns).unwrap(),
            None => filtered,
        }
    }

    fn check(&self, full: &RecordBatch, actual: &RecordBatch) {
        let expected = self.expected(full);
        assert_eq!(actual.num_rows(), expected.num_rows(), "row count");
        assert_eq!(actual, &expected);
    }
}

/// A narrowed transform of `format` input.
fn narrowed(format: InputFormat, options: TransformOptions) -> Transformer<'static> {
    Transformer::new(format).options(options)
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
}

fn index(batch: &RecordBatch, name: &str) -> usize {
    batch.schema().index_of(name).unwrap()
}

/// A service present in the batch, and its share of rows (to make sure the
/// filter is selective).
fn some_service(batch: &RecordBatch) -> String {
    let column = string_column(batch, "service_name");
    let services: BTreeSet<&str> = (0..column.len()).map(|row| column.value(row)).collect();
    services.into_iter().next().unwrap().to_owned()
}

/// A range covering the middle half of the column's values.
fn middle_range(batch: &RecordBatch, name: &str) -> (usize, i64, i64) {
    let column = index(batch, name);
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let mut sorted: Vec<i64> = values.iter().flatten().collect();
    sorted.sort_unstable();
    assert!(!sorted.is_empty(), "{name} has no values");
    (
        column,
        sorted[sorted.len() / 4],
        sorted[sorted.len() * 3 / 4],
    )
}

fn shape_batch(batches: &MetricBatches, shape: MetricShape) -> Option<&RecordBatch> {
    match shape {
        MetricShape::Gauge => batches.gauge.as_ref(),
        MetricShape::Sum => batches.sum.as_ref(),
        MetricShape::Histogram => batches.histogram.as_ref(),
        MetricShape::ExponentialHistogram => batches.exp_histogram.as_ref(),
    }
}

/// Every single-column projection, no projection, a service filter, each
/// time range, and all of them together.
fn cases(full: &RecordBatch, time_columns: &[&str]) -> Vec<Predicates> {
    let mut cases = vec![Predicates::default()];
    cases.push(Predicates {
        columns: Some(vec![]),
        ..Default::default()
    });
    for column in 0..full.num_columns() {
        cases.push(Predicates {
            columns: Some(vec![column]),
            ..Default::default()
        });
    }
    let service = some_service(full);
    cases.push(Predicates {
        services: Some(vec![service.clone()]),
        ..Default::default()
    });
    cases.push(Predicates {
        services: Some(vec!["no-such-service".into()]),
        ..Default::default()
    });
    for name in time_columns {
        cases.push(Predicates {
            ranges: vec![middle_range(full, name)],
            ..Default::default()
        });
    }
    // Columns come back in the order asked, and may repeat.
    cases.push(Predicates {
        columns: Some(vec![full.num_columns() - 1, 0, full.num_columns() - 1]),
        ..Default::default()
    });
    cases.push(Predicates {
        columns: Some(vec![index(full, "service_name"), full.num_columns() - 1]),
        services: Some(vec![service]),
        ranges: time_columns
            .iter()
            .map(|name| middle_range(full, name))
            .collect(),
    });
    cases
}

#[test]
fn logs_options_match_the_filtered_full_transform() {
    let full = transform_logs(LOGS, InputFormat::Protobuf).unwrap();
    for case in cases(&full, &["time_unix_nano"]) {
        let actual = narrowed(InputFormat::Protobuf, case.options())
            .logs(LOGS)
            .unwrap();
        case.check(&full, &actual);
    }
}

#[test]
fn logs_options_apply_to_json_input() {
    let full = transform_logs(LOGS_JSON, InputFormat::Auto).unwrap();
    for case in cases(&full, &[]) {
        let actual = narrowed(InputFormat::Auto, case.options())
            .logs(LOGS_JSON)
            .unwrap();
        case.check(&full, &actual);
    }
}

#[test]
fn traces_options_match_the_filtered_full_transform() {
    let full = transform_traces(TRACES, InputFormat::Protobuf).unwrap();
    for case in cases(&full, &["start_time_unix_nano"]) {
        let actual = narrowed(InputFormat::Protobuf, case.options())
            .traces(TRACES)
            .unwrap();
        case.check(&full, &actual);
    }
}

#[test]
fn metric_options_build_only_their_shape() {
    let all = transform_metrics(METRICS, InputFormat::Protobuf).unwrap();
    let mut checked = 0;
    for shape in SHAPES {
        let Some(full) = shape_batch(&all, shape) else {
            continue;
        };
        for case in cases(full, &["time_unix_nano"]) {
            let result = narrowed(InputFormat::Protobuf, case.options())
                .metric_shape(shape, METRICS)
                .unwrap();
            assert_eq!(result.skipped.summaries, all.skipped.summaries);
            case.check(full, &result.batch);
        }
        checked += 1;
    }
    assert!(checked >= 2, "fixture should hold several shapes");
}

#[test]
fn otap_options_match_the_equivalent_otlp() {
    let logs_bar = include_bytes!("fixtures/otap/logs-initial.bar");
    let logs_otlp = include_bytes!("fixtures/otap/logs-initial.otlp");
    let full = transform_logs(logs_otlp, InputFormat::Protobuf).unwrap();
    for case in cases(&full, &[]) {
        let actual = OtapDecoder::new()
            .decode_logs_with_options(logs_bar, &case.options())
            .unwrap();
        case.check(&full, &actual);
    }

    let traces_bar = include_bytes!("fixtures/otap/traces-initial.bar");
    let traces_otlp = include_bytes!("fixtures/otap/traces-initial.otlp");
    let full = transform_traces(traces_otlp, InputFormat::Protobuf).unwrap();
    for case in cases(&full, &["start_time_unix_nano"]) {
        let actual = OtapDecoder::new()
            .decode_traces_with_options(traces_bar, &case.options())
            .unwrap();
        case.check(&full, &actual);
    }

    let metrics_bar = include_bytes!("fixtures/otap/metrics-initial.bar");
    let metrics_otlp = include_bytes!("fixtures/otap/metrics-initial.otlp");
    let all = transform_metrics(metrics_otlp, InputFormat::Protobuf).unwrap();
    for shape in SHAPES {
        let Some(full) = shape_batch(&all, shape) else {
            continue;
        };
        for case in cases(full, &["time_unix_nano"]) {
            let result = OtapDecoder::new()
                .decode_metric_shape(metrics_bar, shape, &case.options())
                .unwrap();
            case.check(full, &result.batch);
        }
    }
}

#[test]
fn default_options_are_the_plain_transform() {
    let options = TransformOptions::new();
    assert_eq!(
        narrowed(InputFormat::Protobuf, options.clone())
            .logs(LOGS)
            .unwrap(),
        transform_logs(LOGS, InputFormat::Protobuf).unwrap()
    );
}

#[test]
fn invalid_options_are_rejected_before_decoding() {
    let width = otlp2records::logs_schema().fields().len();
    let body = otlp2records::logs_schema().index_of("body").unwrap();
    for options in [
        TransformOptions::new().with_columns([width]),
        TransformOptions::new().with_columns([200]),
        TransformOptions::new().with_time_range(body, 0, 1),
        TransformOptions::new().with_time_range(width, 0, 1),
    ] {
        // Garbage input: the options error must win over the decode error.
        let error = narrowed(InputFormat::Auto, options)
            .logs(b"\xff\xff")
            .unwrap_err();
        assert!(matches!(&error, Error::InvalidOptions(_)), "{error}");
    }
}

/// Two resources (one without `service.name`), each with a log that has an
/// observed time and one that does not.
const NULLS_AND_UNKNOWN: &[u8] = br#"{"resourceLogs":[
  {"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},
   "scopeLogs":[{"logRecords":[
     {"timeUnixNano":"10","observedTimeUnixNano":"15","body":{"stringValue":"a"}},
     {"timeUnixNano":"20","body":{"stringValue":"b"}}]}]},
  {"resource":{},
   "scopeLogs":[{"logRecords":[
     {"timeUnixNano":"30","observedTimeUnixNano":"35","body":{"stringValue":"c"}},
     {"timeUnixNano":"40","body":{"stringValue":"d"}}]}]}]}"#;

#[test]
fn a_range_rejects_null_timestamps() {
    let full = transform_logs(NULLS_AND_UNKNOWN, InputFormat::Json).unwrap();
    let column = index(&full, "observed_time_unix_nano");
    assert_eq!(full.column(column).null_count(), 2);
    let case = Predicates {
        ranges: vec![(column, i64::MIN, i64::MAX)],
        ..Default::default()
    };
    let actual = narrowed(InputFormat::Json, case.options())
        .logs(NULLS_AND_UNKNOWN)
        .unwrap();
    assert_eq!(actual.num_rows(), 2);
    case.check(&full, &actual);
}

#[test]
fn a_resource_without_service_name_matches_unknown() {
    let full = transform_logs(NULLS_AND_UNKNOWN, InputFormat::Json).unwrap();
    for (service, rows) in [("unknown", 2), ("api", 2), ("other", 0)] {
        let case = Predicates {
            services: Some(vec![service.into()]),
            ..Default::default()
        };
        let actual = narrowed(InputFormat::Json, case.options())
            .logs(NULLS_AND_UNKNOWN)
            .unwrap();
        assert_eq!(actual.num_rows(), rows, "{service}");
        case.check(&full, &actual);
    }
}

#[test]
fn a_shape_the_input_lacks_is_an_empty_projected_batch() {
    let gauges = include_bytes!("../testdata/metrics_gauge.pb");
    let schema = MetricShape::Histogram.schema();
    let bounds = schema.index_of("explicit_bounds").unwrap();
    let MetricShapeBatch { batch, .. } = narrowed(
        InputFormat::Protobuf,
        TransformOptions::new().with_columns([bounds]),
    )
    .metric_shape(MetricShape::Histogram, gauges)
    .unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.schema().fields().len(), 1);
    assert_eq!(batch.schema().field(0).name(), "explicit_bounds");
}

#[test]
fn every_shape_at_once_cannot_be_narrowed() {
    let options = TransformOptions::new().with_time_range(0, 0, 1);
    let error = narrowed(InputFormat::Protobuf, options)
        .metrics(METRICS)
        .unwrap_err();
    assert!(matches!(error, Error::InvalidOptions(_)), "{error}");
}

#[test]
fn options_and_an_observer_combine() {
    #[derive(Default)]
    struct Phases(usize);
    impl otlp2records::TransformObserver for Phases {
        fn on_phase(&mut self, _: otlp2records::TransformPhaseTiming) {
            self.0 += 1;
        }
    }
    let schema = otlp2records::logs_schema();
    let options = TransformOptions::new().with_columns([schema.index_of("body").unwrap()]);
    let mut phases = Phases::default();
    let batch = Transformer::new(InputFormat::Protobuf)
        .options(options)
        .observer(&mut phases)
        .logs(LOGS)
        .unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert!(phases.0 > 0);
}
