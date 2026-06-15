# Native OTAP Input

Native OTAP input decodes canonical `BatchArrowRecords` envelopes into the same
normalized, flattened logs, traces, and metric batches produced by OTLP
protobuf and JSON inputs. It is available in the default build.

```rust,ignore
use otlp2records::otap::OtapDecoder;

let mut decoder = OtapDecoder::new();
let first = decoder.decode_logs(&first_batch_arrow_records)?;
let next = decoder.decode_logs(&next_batch_arrow_records)?;
let traces = decoder.decode_traces(&trace_batch_arrow_records)?;
let metrics = decoder.decode_metrics(&metric_batch_arrow_records)?;
```

Keep one decoder per OTAP stream. Arrow schemas and dictionaries can be omitted
from later messages and reused by `schema_id`; decoding such a message with a
new decoder returns an error.

## Supported Surface

The decoder:

- uses the canonical `BatchArrowRecords`, `ArrowPayload`, and
  `ArrowPayloadType` wire field numbers and enum values;
- accepts canonical logs, traces, univariate metrics, attributes, events,
  links, data point, and exemplar payloads;
- validates required root columns and canonical attribute shapes;
- supports native and legal 8-bit or 16-bit dictionary columns;
- restores transport delta-encoded root, data point, event, link, and exemplar
  IDs;
- restores quasi-delta-encoded attribute and child parent IDs;
- supports string, integer, double, Boolean, bytes, map, and slice values;
- preserves resource, scope, signal, event, link, data point, and exemplar
  attributes plus trace/span identifiers;
- emits normalized gauge, sum, histogram, and exponential histogram batches;
- counts summaries as skipped, matching the existing normalized metrics API;
- rejects unknown payload types, missing roots, invalid schemas, and
  multivariate metrics, whose upstream canonical schema is currently empty.

The default build supports uncompressed and LZ4 Arrow IPC on native and
`wasm32-unknown-unknown` targets. Upstream `Producer` defaults to Zstandard;
enable `otap-zstd` to accept that output:

```toml
otlp2records = {
  version = "0.10",
  features = ["otap-zstd"]
}
```

Arrow's Zstandard backend compiles bundled C. It therefore requires a C
toolchain capable of targeting the selected platform; it is not part of the
WASM portability guarantee.

## Architecture And Provenance

OTLP and OTAP converge at semantic view traits compatible with
`otap-df-pdata-views`; neither path owns a separate normalization policy.
Scalar OTAP values remain borrowed from Arrow arrays. The adapter allocates
hierarchy indexes, decoded transport ID columns, owned CBOR containers for
map/slice values, and the small lists required by normalized histogram output.

The copied stable interfaces and wire definitions are pinned to
open-telemetry/otel-arrow commit
`f8cd17f084c1a766f887530531ad06f546080c90`:

- semantic telemetry traits: `src/views/pdata.rs`;
- canonical OTAP envelope types and enum values: `src/otap/wire.rs`.

The schema, transport, and view adapters are intentionally local while upstream
crate boundaries stabilize. They are not a general OTAP implementation and do
not produce OTAP star tables or convert OTAP back to OTLP.

Interoperability fixtures in `tests/fixtures/otap` are generated with the
upstream `Producer`. Regenerate them from the repository root with:

```sh
cargo +stable run \
  --manifest-path tools/otap-fixture-gen/Cargo.toml -- \
  tests/fixtures/otap
```

The generator is excluded from crates.io packages because it intentionally
depends on the adjacent authoritative upstream checkout and its heavy dataflow
dependency graph. No upstream crate enters this crate's product dependency
graph.

## Remaining Boundary

Multivariate metrics remain unsupported because upstream does not yet define a
canonical payload schema or production path. Summary payloads are decoded and
counted but remain absent from normalized output, matching the existing OTLP
normalization policy.

The preferred upstream evolution is still a publishable, WASM-compatible crate
containing canonical wire definitions, schema validation, stateful IPC
consumption, transport decoding, and complete Arrow-backed semantic views
without DataFusion, Tokio, Nix, or other dataflow runtime dependencies.
