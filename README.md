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
- VRL (Vector Remap Language) transformations built-in
- Efficient memory usage with Arc-shared resource/scope values

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
otlp2records = "0.1"

# Optional: Enable Parquet output
otlp2records = { version = "0.1", features = ["parquet"] }

# Optional: Enable WASM bindings
otlp2records = { version = "0.1", features = ["wasm"] }
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

#### Lower-level API

For more control over the transformation pipeline:

```rust
use otlp2records::{
    decode_logs, apply_log_transform, values_to_arrow, logs_schema, InputFormat
};

// Step 1: Decode OTLP bytes to VRL Values
let values = decode_logs(bytes, InputFormat::Protobuf)?;

// Step 2: Apply VRL transformation
let transformed = apply_log_transform(values)?;

// Step 3: Convert to Arrow RecordBatch
let batch = values_to_arrow(&transformed, &logs_schema())?;
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
| `InputFormat::Auto` | Auto-detect JSON vs protobuf with fallback decoding |

### High-level Functions

| Function | Description |
|----------|-------------|
| `transform_logs(bytes, format)` | Transform OTLP logs to Arrow RecordBatch |
| `transform_traces(bytes, format)` | Transform OTLP traces to Arrow RecordBatch |
| `transform_metrics(bytes, format)` | Transform OTLP metrics to MetricBatches |

### Output Functions

| Function | Description |
|----------|-------------|
| `to_json(&batch)` | Convert RecordBatch to NDJSON bytes |
| `to_ipc(&batch)` | Convert RecordBatch to Arrow IPC format |
| `to_parquet(&batch)` | Convert RecordBatch to Parquet (requires feature) |

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
                              |      Decode       |
                              | (prost / serde)   |
                              +---------+---------+
                                        |
                                        v
                              +---------+---------+
                              |   VRL Transform   |
                              | (field mapping)   |
                              +---------+---------+
                                        |
                                        v
                              +---------+---------+
                              |   Arrow Builder   |
                              | (RecordBatch)     |
                              +---------+---------+
                                        |
                  +---------------------+---------------------+
                  |                     |                     |
                  v                     v                     v
          +-------+-------+     +-------+-------+     +-------+-------+
          |    NDJSON     |     |   Arrow IPC   |     |    Parquet    |
          +---------------+     +---------------+     +---------------+
```

### Module Structure

- **decode**: Parse OTLP protobuf/JSON into VRL Values
- **transform**: Apply VRL programs to normalize data
- **arrow**: Convert VRL Values to Arrow RecordBatches
- **output**: Serialize RecordBatches to various formats
- **wasm**: WASM bindings (optional)

## Output Schemas

### Logs Schema

| Field | Type | Description |
|-------|------|-------------|
| timestamp | TimestampMs | Log record timestamp |
| observed_timestamp | Int64 | When log was observed (ms) |
| trace_id | String | Trace correlation ID (hex) |
| span_id | String | Span correlation ID (hex) |
| service_name | String | Service name from resource |
| service_namespace | String | Service namespace |
| service_instance_id | String | Service instance ID |
| severity_number | Int32 | Numeric severity (1-24) |
| severity_text | String | Severity string (DEBUG, INFO, etc.) |
| body | String | Log message body |
| resource_attributes | String | JSON-encoded resource attributes |
| scope_name | String | Instrumentation scope name |
| scope_version | String | Instrumentation scope version |
| scope_attributes | String | JSON-encoded scope attributes |
| log_attributes | String | JSON-encoded log attributes |

### Traces Schema

| Field | Type | Description |
|-------|------|-------------|
| timestamp | TimestampMs | Span start time |
| end_timestamp | Int64 | Span end time (ms) |
| duration | Int64 | Duration in milliseconds |
| trace_id | String | Trace ID (hex) |
| span_id | String | Span ID (hex) |
| parent_span_id | String | Parent span ID (hex) |
| trace_state | String | W3C trace state |
| span_name | String | Operation name |
| span_kind | Int32 | Span kind enum |
| status_code | Int32 | Status code |
| status_message | String | Status message |
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
| dropped_attributes_count | Int32 | Dropped attributes count |
| dropped_events_count | Int32 | Dropped events count |
| dropped_links_count | Int32 | Dropped links count |
| flags | Int32 | Span flags |

### Gauge Metrics Schema

| Field | Type | Description |
|-------|------|-------------|
| timestamp | TimestampMs | Data point timestamp |
| start_timestamp | Int64 | Start of measurement window (ms) |
| metric_name | String | Metric name |
| metric_description | String | Metric description |
| metric_unit | String | Unit of measurement |
| value | Float64 | Metric value |
| service_name | String | Service name from resource |
| service_namespace | String | Service namespace |
| service_instance_id | String | Service instance ID |
| resource_attributes | String | JSON-encoded resource attributes |
| scope_name | String | Instrumentation scope name |
| scope_version | String | Instrumentation scope version |
| scope_attributes | String | JSON-encoded scope attributes |
| metric_attributes | String | JSON-encoded metric attributes |
| flags | Int32 | Data point flags |
| exemplars_json | String | JSON-encoded exemplars |

### Sum Metrics Schema

Includes all gauge fields plus:

| Field | Type | Description |
|-------|------|-------------|
| aggregation_temporality | Int32 | 1=Delta, 2=Cumulative |
| is_monotonic | Boolean | Whether sum is monotonic |

## Cargo Features

| Feature | Description | Default |
|---------|-------------|---------|
| `default` | Core functionality | Yes |
| `parquet` | Enable Parquet output | No |
| `wasm` | Enable WASM bindings | No |

## Performance

- Compiled VRL programs cached for reuse
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
