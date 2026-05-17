use std::hint::black_box;
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use otlp2records::{
    transform_traces_with_observer, InputFormat, TransformCounter, TransformCounterValue,
    TransformObserver, TransformPhase, TransformPhaseTiming,
};
use prost::Message;

const WARMUP_ITERS: usize = 5;
const MEASURE_ITERS: usize = 50;
const SPANS_PER_REQUEST: usize = 256;

struct Workload {
    name: &'static str,
    spans_per_request: usize,
    span_attribute_bytes: usize,
    resource_attribute_bytes: usize,
    scope_attribute_bytes: usize,
}

struct Measurement {
    total: Duration,
    rows_per_iter: usize,
    phases: PhaseTotals,
}

#[derive(Default)]
struct PhaseTotals {
    decode: Duration,
    row_count: Duration,
    builder_init: Duration,
    resource_context: Duration,
    resource_attrs_json: Duration,
    scope_context: Duration,
    scope_attrs_json: Duration,
    arrow_append: Duration,
    resource_attrs_append: Duration,
    scope_attrs_append: Duration,
    span_attrs_json: Duration,
    events_json: Duration,
    links_json: Duration,
    finish: Duration,
    output_rows: u64,
    resource_context_hits: u64,
    resource_context_misses: u64,
    scope_context_hits: u64,
    scope_context_misses: u64,
    resource_attr_row_copy_bytes: u64,
    scope_attr_row_copy_bytes: u64,
}

impl TransformObserver for PhaseTotals {
    fn on_phase(&mut self, timing: TransformPhaseTiming) {
        match timing.phase {
            TransformPhase::ProtobufDecode => self.decode += timing.elapsed,
            TransformPhase::RowCount => self.row_count += timing.elapsed,
            TransformPhase::BuilderInit => self.builder_init += timing.elapsed,
            TransformPhase::ResourceContextBuild => self.resource_context += timing.elapsed,
            TransformPhase::ResourceAttributesJson => self.resource_attrs_json += timing.elapsed,
            TransformPhase::ScopeContextBuild => self.scope_context += timing.elapsed,
            TransformPhase::ScopeAttributesJson => self.scope_attrs_json += timing.elapsed,
            TransformPhase::ArrowAppend => self.arrow_append += timing.elapsed,
            TransformPhase::ResourceAttributesAppend => {
                self.resource_attrs_append += timing.elapsed
            }
            TransformPhase::ScopeAttributesAppend => self.scope_attrs_append += timing.elapsed,
            TransformPhase::SpanAttributesJson => self.span_attrs_json += timing.elapsed,
            TransformPhase::EventsJson => self.events_json += timing.elapsed,
            TransformPhase::LinksJson => self.links_json += timing.elapsed,
            TransformPhase::ArrowFinalize => self.finish += timing.elapsed,
            _ => {}
        }
    }

    fn on_counter(&mut self, counter: TransformCounterValue) {
        match counter.counter {
            TransformCounter::OutputRows => self.output_rows += counter.value,
            TransformCounter::ResourceContextDuplicateHit => {
                self.resource_context_hits += counter.value
            }
            TransformCounter::ResourceContextDuplicateMiss => {
                self.resource_context_misses += counter.value
            }
            TransformCounter::ScopeContextDuplicateHit => self.scope_context_hits += counter.value,
            TransformCounter::ScopeContextDuplicateMiss => {
                self.scope_context_misses += counter.value
            }
            TransformCounter::ResourceAttributesRowCopyBytes => {
                self.resource_attr_row_copy_bytes += counter.value
            }
            TransformCounter::ScopeAttributesRowCopyBytes => {
                self.scope_attr_row_copy_bytes += counter.value
            }
            _ => {}
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workloads = [
        Workload {
            name: "trace_attrs_256",
            spans_per_request: SPANS_PER_REQUEST,
            span_attribute_bytes: 256,
            resource_attribute_bytes: 96,
            scope_attribute_bytes: 48,
        },
        Workload {
            name: "trace_attrs_0",
            spans_per_request: SPANS_PER_REQUEST,
            span_attribute_bytes: 0,
            resource_attribute_bytes: 96,
            scope_attribute_bytes: 48,
        },
        Workload {
            name: "repeated_resource_scope",
            spans_per_request: 1024,
            span_attribute_bytes: 0,
            resource_attribute_bytes: 512,
            scope_attribute_bytes: 256,
        },
    ];

    println!(
        "profile_header,workload,fixture_bytes,rows_per_iter,iterations,total_ms,ms_per_iter,spans_per_sec,requests_per_sec,mib_per_sec,decode_ms,row_count_ms,builder_init_ms,resource_context_ms,resource_attrs_json_ms,scope_context_ms,scope_attrs_json_ms,arrow_append_ms,resource_attrs_append_ms,scope_attrs_append_ms,span_attrs_json_ms,events_json_ms,links_json_ms,finish_ms,output_rows,resource_context_hits,resource_context_misses,scope_context_hits,scope_context_misses,resource_attr_row_copy_bytes,scope_attr_row_copy_bytes"
    );

    for workload in workloads {
        let request = build_request(&workload);
        let bytes = request.encode_to_vec();
        let measurement = measure(&bytes)?;
        print_measurement(&workload, bytes.len(), &measurement);
    }

    Ok(())
}

fn measure(bytes: &[u8]) -> Result<Measurement, Box<dyn std::error::Error>> {
    for _ in 0..WARMUP_ITERS {
        let mut phases = PhaseTotals::default();
        let batch = transform_traces_with_observer(bytes, InputFormat::Protobuf, &mut phases)?;
        black_box(batch.num_rows());
        black_box(phases);
    }

    let mut rows_per_iter = 0;
    let mut phases = PhaseTotals::default();
    let start = Instant::now();
    for _ in 0..MEASURE_ITERS {
        let batch = transform_traces_with_observer(bytes, InputFormat::Protobuf, &mut phases)?;
        rows_per_iter = batch.num_rows();
        black_box(batch);
    }
    let total = start.elapsed();

    Ok(Measurement {
        total,
        rows_per_iter,
        phases,
    })
}

fn print_measurement(workload: &Workload, fixture_bytes: usize, measurement: &Measurement) {
    let total_ms = duration_ms(measurement.total);
    let ms_per_iter = total_ms / MEASURE_ITERS as f64;
    let spans_per_sec =
        (measurement.rows_per_iter * MEASURE_ITERS) as f64 / measurement.total.as_secs_f64();
    let requests_per_sec = MEASURE_ITERS as f64 / measurement.total.as_secs_f64();
    let mib_per_sec =
        (fixture_bytes * MEASURE_ITERS) as f64 / 1024.0 / 1024.0 / measurement.total.as_secs_f64();
    let phases = &measurement.phases;

    println!(
        "profile,{},{},{},{},{:.3},{:.3},{:.0},{:.1},{:.1},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{},{},{},{},{}",
        workload.name,
        fixture_bytes,
        measurement.rows_per_iter,
        MEASURE_ITERS,
        total_ms,
        ms_per_iter,
        spans_per_sec,
        requests_per_sec,
        mib_per_sec,
        duration_ms(phases.decode),
        duration_ms(phases.row_count),
        duration_ms(phases.builder_init),
        duration_ms(phases.resource_context),
        duration_ms(phases.resource_attrs_json),
        duration_ms(phases.scope_context),
        duration_ms(phases.scope_attrs_json),
        duration_ms(phases.arrow_append),
        duration_ms(phases.resource_attrs_append),
        duration_ms(phases.scope_attrs_append),
        duration_ms(phases.span_attrs_json),
        duration_ms(phases.events_json),
        duration_ms(phases.links_json),
        duration_ms(phases.finish),
        phases.output_rows,
        phases.resource_context_hits,
        phases.resource_context_misses,
        phases.scope_context_hits,
        phases.scope_context_misses,
        phases.resource_attr_row_copy_bytes,
        phases.scope_attr_row_copy_bytes,
    );
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn build_request(workload: &Workload) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: build_attrs("resource.attr", workload.resource_attribute_bytes, 8),
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "bench-scope".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: build_attrs("scope.attr", workload.scope_attribute_bytes, 4),
                    ..Default::default()
                }),
                spans: (0..workload.spans_per_request)
                    .map(|idx| build_span(idx, workload.span_attribute_bytes))
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn build_span(idx: usize, attribute_bytes: usize) -> Span {
    Span {
        trace_id: id_bytes(idx as u64 * 17 + 3, 16),
        span_id: id_bytes(idx as u64, 8),
        parent_span_id: if idx.is_multiple_of(8) {
            Vec::new()
        } else {
            id_bytes(idx as u64 - 1, 8)
        },
        name: format!("GET /api/items/{}", idx % 128),
        kind: 2,
        start_time_unix_nano: 1_700_000_000_000_000_000 + idx as u64 * 1_000_000,
        end_time_unix_nano: 1_700_000_000_001_000_000 + idx as u64 * 1_000_000,
        attributes: build_attrs("span.attr", attribute_bytes, 8),
        ..Default::default()
    }
}

fn build_attrs(prefix: &str, approximate_value_bytes: usize, count: usize) -> Vec<KeyValue> {
    if approximate_value_bytes == 0 {
        return Vec::new();
    }

    let value_len = approximate_value_bytes.div_ceil(count).max(1);
    (0..count)
        .map(|idx| string_kv(&format!("{prefix}.{idx:03}"), &attr_value(idx, value_len)))
        .collect()
}

fn attr_value(seed: usize, len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..len)
        .map(|idx| ALPHABET[(idx + seed * 7) % ALPHABET.len()] as char)
        .collect()
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn id_bytes(seed: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|idx| ((seed.wrapping_mul(31) + idx as u64 * 17) & 0xff) as u8)
        .collect()
}
