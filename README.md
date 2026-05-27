# otlp2records

[![Crates.io](https://img.shields.io/crates/v/otlp2records.svg)](https://crates.io/crates/otlp2records)

Transform OTLP telemetry (logs, traces, metrics) into Arrow RecordBatches.

A high-performance, WASM-compatible library for converting OpenTelemetry Protocol (OTLP) data to Apache Arrow format for efficient storage and querying.

Currently consumed by [duckdb-otlp](https://github.com/smithclay/duckdb-otlp), [otlp2parquet](https://github.com/smithclay/otlp2parquet) and [otlp2pipeline](https://github.com/smithclay/otlp2pipeline).

## Design Principles

- **No I/O**: Core never touches network or filesystem
- **No async**: Pure synchronous transforms
- **WASM-first**: All dependencies compile to wasm32
- **Arrow-native**: RecordBatch is the canonical output format

## Features

- Transform OTLP logs, traces, and metrics to Arrow RecordBatches
- Support for both Protobuf and JSON input formats
- Output to NDJSON, Arrow IPC, or Parquet
- Direct OTLP-to-Arrow hot path for high-throughput ingestion
- JSON/JSONL support through OTLP request normalization into the same Arrow builders

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
otlp2records = "0.8"

# Optional: Enable Parquet output
otlp2records = { version = "0.8", features = ["parquet"] }

# Optional: Enable WASM bindings
otlp2records = { version = "0.8", features = ["wasm"] }
```

## Usage

### Rust API

#### High-level API (Recommended)

```rust
use otlp2records::{transform_logs, transform_traces, transform_metrics, InputFormat};

// Transform OTLP logs
let bytes: &[u8] = /* OTLP log data */;
let batch = transform_logs(bytes, InputFormat::Protobuf)?;
println!("Transformed {} log records", batch.num_rows());

// Transform OTLP traces
let batch = transform_traces(bytes, InputFormat::Json)?;
println!("Transformed {} spans", batch.num_rows());

// Transform OTLP metrics (returns separate batches by type)
let batches = transform_metrics(bytes, InputFormat::Protobuf)?;
if let Some(gauge) = batches.gauge {
    println!("Transformed {} gauge metrics", gauge.num_rows());
}
if let Some(sum) = batches.sum {
    println!("Transformed {} sum metrics", sum.num_rows());
}
```

#### Output Formats

```rust
use otlp2records::{transform_logs, to_json, to_ipc, InputFormat};

let batch = transform_logs(bytes, InputFormat::Protobuf)?;

// Output as NDJSON
let ndjson: Vec<u8> = to_json(&batch)?;

// Output as Arrow IPC (streaming format)
let ipc: Vec<u8> = to_ipc(&batch)?;

// Output as Parquet (requires "parquet" feature)
#[cfg(feature = "parquet")]
let parquet: Vec<u8> = otlp2records::to_parquet(&batch)?;
```

### WASM Usage

Build with the `wasm` feature for browser/Node.js environments:

```bash
cargo build --target wasm32-unknown-unknown --features wasm
```

```javascript
import init, { transform_logs_wasm } from './otlp2records.js';

await init();

// Transform OTLP logs (Uint8Array) to Arrow IPC
const otlpBytes = new Uint8Array(/* ... */);
const arrowIpc = transform_logs_wasm(otlpBytes, "protobuf");
```

## API Overview

### Input Formats

| Format | Description |
|--------|-------------|
| `InputFormat::Protobuf` | Standard OTLP protobuf encoding |
| `InputFormat::Json` | OTLP JSON encoding (camelCase field names) |
| `InputFormat::Jsonl` | Newline-delimited OTLP JSON envelopes |
| `InputFormat::Auto` | Auto-detect JSON vs protobuf with fallback decoding |

### High-level Functions

| Function | Description |
|----------|-------------|
| `transform_logs(bytes, format)` | Transform OTLP logs to Arrow RecordBatch |
| `transform_traces(bytes, format)` | Transform OTLP traces to Arrow RecordBatch |
| `transform_metrics(bytes, format)` | Transform OTLP metrics to MetricBatches |

### Schema Output Selection

The default output is `SchemaOutput::Normalized`, the flattened ClickStack-compatible
schema used by the existing `transform_logs`, `transform_traces`, and
`transform_metrics` APIs. The aliases `"normalized"`, `"clickstack"`,
`"clickstack-mode"`, `""`, and `"default"` all parse to this default.

Rust callers can opt into `SchemaOutput::OtapStar` with explicit APIs:

| Function | Description |
|----------|-------------|
| `transform_logs_with_schema(bytes, format, schema_output)` | Transform logs to `LogsOutput::Normalized` or `LogsOutput::OtapStar` |
| `transform_traces_with_schema(bytes, format, schema_output)` | Transform traces to `TracesOutput::Normalized` or `TracesOutput::OtapStar` |
| `transform_metrics_with_schema(bytes, format, schema_output)` | Transform metrics to `MetricsOutput::Normalized` or `MetricsOutput::OtapStar` |

`otap-star` / `otap_star` emits multi-table Arrow batches modeled after the
OpenTelemetry otel-arrow data model. Instead of flattened JSON columns such as
`events_json`, `links_json`, `metric_attributes`, or `exemplars_json`, child
entities are emitted as separate tables keyed by deterministic `id` and
`parent_id` columns. Use `iter_named_batches()` on `OtapLogsBatches`,
`OtapTracesBatches`, or `OtapMetricsBatches` to serialize each named table.

The FFI and WASM bindings continue to expose the normalized single-batch shape in
this release. `otap-star` is Rust API only to avoid changing those ABIs.

### Breaking Changes In 0.8.0

The 0.7 to 0.8 release intentionally changes the default normalized schema. The
existing `transform_logs`, `transform_traces`, and `transform_metrics` APIs still
return flattened batches by default, but downstream code that selects columns by
name or expects specific Arrow physical types must be updated.

Key normalized-schema changes:

- OTLP/OTAP field names replace older ClickStack-style names: for example,
  `timestamp` becomes `time_unix_nano` for logs and metrics,
  trace `timestamp` becomes `start_time_unix_nano`, `span_name` becomes `name`,
  `span_kind` becomes `kind`, and metric `metric_name`/`metric_description`/
  `metric_unit` become `name`/`description`/`unit`.
- Timestamps now use Arrow `Timestamp(Nanosecond)` instead of microsecond or
  millisecond-scaled integer columns. Span duration is
  `duration_time_unix_nano` with Arrow `Duration(Nanosecond)`.
- Trace and span identifiers are Arrow `FixedSizeBinary(16)` and
  `FixedSizeBinary(8)` instead of hex strings.
- Metric number values are split into nullable `int_value` and `double_value`
  columns instead of a single `Float64` `value` column.
- Histogram bucket columns now use typed Arrow list columns instead of JSON
  strings, and dropped counts/flags/count fields use unsigned Arrow integer
  types where OTAP does.

The flattened JSON convenience columns remain for now: `resource_attributes`,
`scope_attributes`, signal attribute JSON columns, `events_json`, `links_json`,
and `exemplars_json`. The new `otap-star` output is the more relational
multi-table shape for callers that want child tables instead of flattened JSON.

### Transform Observation

Production callers can opt into phase timings and counters without changing output semantics:

| Function | Description |
|----------|-------------|
| `transform_logs_with_observer(bytes, format, observer)` | Transform logs and report decode/build/append/finalize phases |
| `transform_traces_with_observer(bytes, format, observer)` | Transform traces and report decode/build/attribute JSON/append/finalize phases |
| `transform_metrics_with_observer(bytes, format, observer)` | Transform metrics and report decode/capacity/context/append/finalize phases |

Implement `TransformObserver` to receive `TransformPhaseTiming` and `TransformCounterValue`
events. Counters include duplicate resource/scope context hits and misses plus repeated
resource/scope attribute row-copy counts and bytes.

To observe an OTAP star transform, use the `*_with_schema_and_observer` entry
points, which route to either schema and thread the observer through:

| Function | Description |
|----------|-------------|
| `transform_logs_with_schema_and_observer(bytes, format, schema_output, observer)` | Logs transform with both schema selection and observer |
| `transform_traces_with_schema_and_observer(bytes, format, schema_output, observer)` | Traces transform with both schema selection and observer |
| `transform_metrics_with_schema_and_observer(bytes, format, schema_output, observer)` | Metrics transform with both schema selection and observer |

The OTAP path emits the same phase enum (`ProtobufDecode`, `JsonDecode`,
`JsonlDecode`, `BuilderInit`, `ResourceLogsBuild` / `ResourceSpansBuild` /
`ResourceMetricsBuild`, the matching `Scope*Build` and per-record
`LogRecordBuild` / `SpanBuild` / `MetricBuild`, and `ArrowFinalize`) plus the
`OutputRows`, `Resource/ScopeContextDuplicateHit`, and
`Resource/ScopeContextDuplicateMiss` counters. The
`Resource/ScopeAttributesRowCopies*` counters are normalized-only — OTAP
emits attributes as their own child tables, so no row replication happens.

### Output Functions

| Function | Description |
|----------|-------------|
| `to_json(&batch)` | Convert RecordBatch to NDJSON bytes |
| `to_ipc(&batch)` | Convert RecordBatch to Arrow IPC format |
| `to_parquet(&batch)` | Convert RecordBatch to Parquet (requires feature) |

These serializers operate on one `RecordBatch` at a time. For `otap-star`, call
them per table by iterating named batches.

### Schemas

| Function | Description |
|----------|-------------|
| `logs_schema()` | Arrow schema for log records |
| `traces_schema()` | Arrow schema for trace spans |
| `gauge_schema()` | Arrow schema for gauge metrics |
| `sum_schema()` | Arrow schema for sum metrics |

## Architecture

```
                              +-------------------+
                              |   OTLP Input      |
                              | (Protobuf / JSON) |
                              +---------+---------+
                                        |
                                        v
                              +---------+---------+
                              |   Format Dispatch |
                              | (protobuf/jsonl)  |
                              +---------+---------+
                                        |
                                        v
                              +---------+---------+
                              | OTLP Request      |
                              | (prost structs)   |
                              +---------+---------+
                                        |
                                        v
                              +---------+---------+
                              | Arrow Builders    |
                              | (direct columns)  |
                              +---------+---------+
                                        |
                                        v
                              +---------+---------+
                              |   RecordBatch     |
                              +---------+---------+
                                        |
                  +---------------------+---------------------+
                  |                     |                     |
                  v                     v                     v
          +-------+-------+     +-------+-------+     +-------+-------+
          |    NDJSON     |     |   Arrow IPC   |     |    Parquet    |
          +---------------+     +---------------+     +---------------+
```

### Public Surface

- **transform functions**: Convert OTLP logs, traces, and metrics to Arrow batches
- **schema functions**: Return the Arrow schemas used by the transform functions
- **partition helpers**: Group transformed batches by service
- **output helpers**: Serialize RecordBatches to NDJSON, Arrow IPC, or Parquet
- **wasm**: WASM bindings (optional)

## Output Schemas

`SchemaOutput::Normalized` is the default flattened schema. In 0.8.0 it uses
OTAP-compatible field names and high-value Arrow physical types while keeping
the flattened resource/scope/attribute convenience columns. The `clickstack`
and `clickstack-mode` schema aliases still select this normalized output.

### Logs Schema

| Field | Type | Description |
|-------|------|-------------|
| time_unix_nano | TimestampNanosecond | Log record timestamp |
| observed_time_unix_nano | TimestampNanosecond | When log was observed |
| trace_id | FixedSizeBinary(16) | Trace correlation ID |
| span_id | FixedSizeBinary(8) | Span correlation ID |
| service_name | String | Service name from resource |
| service_namespace | String | Service namespace |
| service_instance_id | String | Service instance ID |
| severity_number | Int32 | Numeric severity (1-24) |
| severity_text | String | Severity string (DEBUG, INFO, etc.) |
| event_name | String | Log event name |
| body | String | Log message body |
| resource_attributes | String | JSON-encoded resource attributes |
| scope_name | String | Instrumentation scope name |
| scope_version | String | Instrumentation scope version |
| scope_attributes | String | JSON-encoded scope attributes |
| log_attributes | String | JSON-encoded log attributes |
| dropped_attributes_count | UInt32 | Dropped log attributes |
| flags | UInt32 | Log flags |

### Traces Schema

| Field | Type | Description |
|-------|------|-------------|
| start_time_unix_nano | TimestampNanosecond | Span start time |
| duration_time_unix_nano | DurationNanosecond | Span duration |
| trace_id | FixedSizeBinary(16) | Trace ID |
| span_id | FixedSizeBinary(8) | Span ID |
| parent_span_id | FixedSizeBinary(8) | Parent span ID |
| trace_state | String | W3C trace state |
| name | String | Operation name |
| kind | Int32 | Span kind enum |
| status_code | Int32 | Status code |
| status_status_message | String | Status message |
| service_name | String | Service name from resource |
| service_namespace | String | Service namespace |
| service_instance_id | String | Service instance ID |
| scope_name | String | Instrumentation scope name |
| scope_version | String | Instrumentation scope version |
| scope_attributes | String | JSON-encoded scope attributes |
| span_attributes | String | JSON-encoded span attributes |
| resource_attributes | String | JSON-encoded resource attributes |
| events_json | String | JSON-encoded span events |
| links_json | String | JSON-encoded span links |
| dropped_attributes_count | UInt32 | Dropped attributes count |
| dropped_events_count | UInt32 | Dropped events count |
| dropped_links_count | UInt32 | Dropped links count |
| flags | UInt32 | Span flags |

### Gauge Metrics Schema

| Field | Type | Description |
|-------|------|-------------|
| time_unix_nano | TimestampNanosecond | Data point timestamp |
| start_time_unix_nano | TimestampNanosecond | Start of measurement window |
| name | String | Metric name |
| description | String | Metric description |
| unit | String | Unit of measurement |
| int_value | Int64 | Integer metric value |
| double_value | Float64 | Floating-point metric value |
| service_name | String | Service name from resource |
| service_namespace | String | Service namespace |
| service_instance_id | String | Service instance ID |
| resource_attributes | String | JSON-encoded resource attributes |
| scope_name | String | Instrumentation scope name |
| scope_version | String | Instrumentation scope version |
| scope_attributes | String | JSON-encoded scope attributes |
| metric_attributes | String | JSON-encoded metric attributes |
| flags | UInt32 | Data point flags |
| exemplars_json | String | JSON-encoded exemplars |

### Sum Metrics Schema

Includes all gauge fields plus:

| Field | Type | Description |
|-------|------|-------------|
| aggregation_temporality | Int32 | 1=Delta, 2=Cumulative |
| is_monotonic | Boolean | Whether sum is monotonic |

### Histogram Metrics Schema

Histogram metrics use the common metric context fields above, plus `count`
(`UInt64`), `sum`, `min`, `max`, typed `bucket_counts` (`List<UInt64>`),
typed `explicit_bounds` (`List<Float64>`), `flags`, `exemplars_json`, and
`aggregation_temporality`.

### Exponential Histogram Metrics Schema

Exponential histograms use the common metric context fields above, plus
`count` (`UInt64`), `sum`, `min`, `max`, `scale`, `zero_count` (`UInt64`),
`zero_threshold`, typed positive/negative bucket-count lists, `flags`,
`exemplars_json`, and `aggregation_temporality`.

## Cargo Features

| Feature | Description | Default |
|---------|-------------|---------|
| `default` | Core functionality | Yes |
| `parquet` | Enable Parquet output | No |
| `wasm` | Enable WASM bindings | No |

## Performance

- Transforms are plain Rust functions with no interpreter or runtime overhead
- Arc-shared resource/scope values reduce memory allocations
- Arrow columnar format enables efficient compression
- Release builds use LTO and size optimization

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions welcome! Please ensure:

1. All tests pass: `cargo test`
2. Code is formatted: `cargo fmt`
3. No clippy warnings: `cargo clippy -- -D warnings`
