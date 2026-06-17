//! C FFI for an embedded gRPC ingest server.
//!
//! This module hosts a [`tonic`] gRPC server on its own [`tokio`] runtime and
//! hands each decoded Arrow batch back to the C/C++ caller through a callback.
//! It is the live-ingest counterpart to the one-shot transforms in
//! [`crate::ffi`]: instead of the caller pushing bytes in and pulling Arrow out,
//! the caller starts a server and Arrow batches are pushed out as telemetry
//! arrives over the wire.
//!
//! # Scope
//!
//! All six signals over two service families on one listener: standard
//! **OTLP/gRPC unary** `Export` (`{Logs,Trace,Metrics}Service`) and the
//! canonical **OTAP/Arrow** bidirectional streams (`Arrow{Logs,Traces,Metrics}
//! Service`). The C ABI carries `stream_id`/`batch_id` so the unary and
//! streaming paths share one callback signature; a metrics message fans out into
//! up to four shape-specific batches.
//!
//! # Threading & ownership
//!
//! - The runtime owns dedicated worker threads. `otlp_grpc_server_start` binds
//!   the listener synchronously (so address-in-use surfaces to the caller) and
//!   then serves on the runtime; `otlp_grpc_server_stop` triggers a graceful
//!   shutdown, drains in-flight requests up to a deadline, and joins the
//!   runtime threads. `stop` must NOT be called from inside the callback (it
//!   would block a runtime worker on its own shutdown).
//! - Arrow batches handed to the callback are **borrowed for the duration of
//!   the call only**: the callee must finish copying before returning, and this
//!   module releases the Arrow C Data structures immediately after the callback
//!   returns. The callee must not call their `release` callbacks.

use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StructArray};
use prost::Message as _;
use tonic::metadata::MetadataMap;

use crate::ffi::OtlpSignalType;
use crate::otap::grpc_service::{
    ArrowLogsService, ArrowLogsServiceServer, ArrowMetricsService, ArrowMetricsServiceServer,
    ArrowTracesService, ArrowTracesServiceServer,
};
use crate::otap::{BatchArrowRecords, BatchStatus, OtapDecoder, StatusCode};

use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::{LogsService, LogsServiceServer},
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::{MetricsService, MetricsServiceServer},
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::{TraceService, TraceServiceServer},
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};

// ============================================================================
// C-compatible types
// ============================================================================

/// Outcome the host's batch callback reports back to the server, mapped to a
/// gRPC status (and, for streaming, an OTAP `BatchStatus`).
///
/// C names: OTLP_INGEST_OK, OTLP_INGEST_RESOURCE_EXHAUSTED, etc.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpIngestStatus {
    /// Buffered successfully (C: OTLP_INGEST_OK) -> gRPC OK.
    Ok = 0,
    /// Backpressure: ingest buffer full (C: OTLP_INGEST_RESOURCE_EXHAUSTED)
    /// -> gRPC RESOURCE_EXHAUSTED.
    ResourceExhausted = 1,
    /// Malformed/unconvertible payload (C: OTLP_INGEST_INVALID)
    /// -> gRPC INVALID_ARGUMENT.
    Invalid = 2,
    /// Host-side internal error (C: OTLP_INGEST_INTERNAL) -> gRPC INTERNAL.
    Internal = 3,
    /// Rejected by auth (C: OTLP_INGEST_UNAUTHENTICATED)
    /// -> gRPC UNAUTHENTICATED.
    Unauthenticated = 4,
}

/// Per-batch callback. Invoked on a runtime worker thread with one decoded
/// Arrow batch destined for `signal_type`'s buffer.
///
/// `stream_id`/`batch_id` are 0 for unary requests; for streaming they identify
/// the source stream and the message within it. `input_bytes` is the wire size
/// of the source payload, charged against the host's admission/backpressure
/// budget. `array`/`schema` are borrowed: valid only for the duration of the
/// call, and released by the server after the call returns — the callee must
/// copy what it needs and must NOT release them. The callee must not unwind
/// across this boundary; it must translate any error into an `OtlpIngestStatus`.
pub type OtlpBatchCallback = extern "C" fn(
    user_data: *mut c_void,
    signal_type: OtlpSignalType,
    stream_id: u64,
    batch_id: i64,
    input_bytes: u64,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> OtlpIngestStatus;

/// Optional auth callback. Given the raw `authorization` metadata value
/// (possibly empty), returns non-zero to accept and 0 to reject. When the host
/// passes a null callback, every request is accepted.
pub type OtlpAuthCallback =
    extern "C" fn(user_data: *mut c_void, metadata_token: *const c_char, len: usize) -> c_int;

// ============================================================================
// Internal callback context
// ============================================================================

/// Raw host callbacks + opaque pointer, shared across all services and tasks.
///
/// The pointers are owned by the host (C++), which guarantees they outlive the
/// server (it tears the server down before freeing them). They are only ever
/// invoked, never dereferenced by Rust, so the struct is `Send`/`Sync` by the
/// host's contract.
struct CallbackCtx {
    user_data: *mut c_void,
    on_batch: OtlpBatchCallback,
    on_auth: Option<OtlpAuthCallback>,
}

// SAFETY: `user_data` is an opaque handle the host keeps alive for the server's
// lifetime; Rust never dereferences it. The callbacks are plain function
// pointers. The host serializes lifetime via start/stop, so sharing the context
// across worker threads is sound.
unsafe impl Send for CallbackCtx {}
unsafe impl Sync for CallbackCtx {}

impl CallbackCtx {
    fn auth_ok(&self, metadata: &MetadataMap) -> bool {
        let Some(cb) = self.on_auth else {
            return true;
        };
        // Pass the raw `authorization` value through to the host, which owns the
        // canonical token comparison. Non-ASCII / binary metadata is treated as
        // an empty token (and thus rejected when a token is configured).
        let value = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        cb(self.user_data, value.as_ptr() as *const c_char, value.len()) != 0
    }

    /// Export `batch` to the Arrow C Data Interface and hand it to the host's
    /// batch callback. The FFI structures are released here, after the callback
    /// returns (the host copies but never releases).
    fn deliver(
        &self,
        signal_type: OtlpSignalType,
        stream_id: u64,
        batch_id: i64,
        input_bytes: u64,
        batch: RecordBatch,
    ) -> OtlpIngestStatus {
        // Fence panics from the Arrow C-Data export (or the callback's Rust
        // frames) so a bug becomes a clean INTERNAL for this one batch instead of
        // unwinding the connection task and tearing down every stream multiplexed
        // on it. Mirrors the catch_unwind in `otlp_grpc_server_start`.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let ffi_schema = match FFI_ArrowSchema::try_from(batch.schema().as_ref()) {
                Ok(schema) => schema,
                Err(_) => return OtlpIngestStatus::Internal,
            };
            let struct_array: StructArray = batch.into();
            let mut ffi_array = FFI_ArrowArray::new(&struct_array.into_data());
            let mut ffi_schema = ffi_schema;

            (self.on_batch)(
                self.user_data,
                signal_type,
                stream_id,
                batch_id,
                input_bytes,
                &mut ffi_array as *mut FFI_ArrowArray,
                &mut ffi_schema as *mut FFI_ArrowSchema,
            )
            // ffi_array / ffi_schema drop here, invoking their release callbacks.
        }));
        outcome.unwrap_or(OtlpIngestStatus::Internal)
    }
}

impl OtlpIngestStatus {
    /// Canonical `(OTAP status code, human message)` for an outcome — the single
    /// source of truth shared by the unary (`ingest_status_to_grpc`) and the
    /// streaming (`ingest_to_batch_status`) mappers so their messages can't drift.
    /// `Ok` carries an empty message.
    fn detail(self) -> (StatusCode, &'static str) {
        match self {
            OtlpIngestStatus::Ok => (StatusCode::Ok, ""),
            OtlpIngestStatus::ResourceExhausted => {
                (StatusCode::ResourceExhausted, "ingest buffer full")
            }
            OtlpIngestStatus::Invalid => (StatusCode::InvalidArgument, "invalid payload"),
            OtlpIngestStatus::Internal => (StatusCode::Internal, "ingest error"),
            OtlpIngestStatus::Unauthenticated => (StatusCode::Unauthenticated, "unauthorized"),
        }
    }
}

fn ingest_status_to_grpc(status: OtlpIngestStatus) -> Result<(), tonic::Status> {
    // Exhaustive over OtlpIngestStatus on purpose: a new variant must force a
    // decision here. The message text comes from `detail()` (shared with the
    // streaming mapper); only the gRPC constructor is chosen per variant.
    let msg = status.detail().1;
    match status {
        OtlpIngestStatus::Ok => Ok(()),
        OtlpIngestStatus::ResourceExhausted => Err(tonic::Status::resource_exhausted(msg)),
        OtlpIngestStatus::Invalid => Err(tonic::Status::invalid_argument(msg)),
        OtlpIngestStatus::Internal => Err(tonic::Status::internal(msg)),
        OtlpIngestStatus::Unauthenticated => Err(tonic::Status::unauthenticated(msg)),
    }
}

// ============================================================================
// Service implementations
// ============================================================================

struct LogsServiceImpl {
    ctx: Arc<CallbackCtx>,
}

#[tonic::async_trait]
impl LogsService for LogsServiceImpl {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        if !self.ctx.auth_ok(request.metadata()) {
            return Err(tonic::Status::unauthenticated("invalid token"));
        }
        let req = request.into_inner();
        // `encoded_len` is the wire size of the request, used both to pre-size the
        // Arrow builders and as the admission charge on the host side. It avoids
        // re-encoding the already-decoded request.
        let input_bytes = req.encoded_len();
        let batch = crate::batch::transform_logs_request_observed(req, input_bytes, &mut None)
            .map_err(|e| {
                tonic::Status::invalid_argument(format!("failed to transform logs: {e}"))
            })?;
        ingest_status_to_grpc(self.ctx.deliver(
            OtlpSignalType::Logs,
            0,
            0,
            input_bytes as u64,
            batch,
        ))?;
        Ok(tonic::Response::new(ExportLogsServiceResponse::default()))
    }
}

struct TraceServiceImpl {
    ctx: Arc<CallbackCtx>,
}

#[tonic::async_trait]
impl TraceService for TraceServiceImpl {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        if !self.ctx.auth_ok(request.metadata()) {
            return Err(tonic::Status::unauthenticated("invalid token"));
        }
        let req = request.into_inner();
        let input_bytes = req.encoded_len();
        let batch = crate::batch::transform_traces_request_observed(req, input_bytes, &mut None)
            .map_err(|e| {
                tonic::Status::invalid_argument(format!("failed to transform traces: {e}"))
            })?;
        ingest_status_to_grpc(self.ctx.deliver(
            OtlpSignalType::Traces,
            0,
            0,
            input_bytes as u64,
            batch,
        ))?;
        Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
    }
}

struct MetricsServiceImpl {
    ctx: Arc<CallbackCtx>,
}

#[tonic::async_trait]
impl MetricsService for MetricsServiceImpl {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        if !self.ctx.auth_ok(request.metadata()) {
            return Err(tonic::Status::unauthenticated("invalid token"));
        }
        let req = request.into_inner();
        let input_bytes = req.encoded_len() as u64;
        let batches =
            crate::batch::transform_metrics_request_observed(req, &mut None).map_err(|e| {
                tonic::Status::invalid_argument(format!("failed to transform metrics: {e}"))
            })?;
        ingest_status_to_grpc(deliver_metric_batches(
            &self.ctx,
            0,
            0,
            input_bytes,
            batches,
        ))?;
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

/// Deliver each present metric shape from one OTLP/OTAP message to its buffer,
/// returning the first non-OK outcome (or OK). The wire bytes are charged once,
/// on the first present shape; the others take the host's admission floor.
///
/// NOTE: the shapes are buffered independently (one host call each), so a
/// backpressure nack partway through can leave earlier shapes buffered. A client
/// retry of the whole message would then re-buffer them. This matches the
/// approximate admission model and keeps the per-batch FFI ABI unchanged.
fn deliver_metric_batches(
    ctx: &CallbackCtx,
    stream_id: u64,
    batch_id: i64,
    input_bytes: u64,
    batches: crate::MetricBatches,
) -> OtlpIngestStatus {
    let mut remaining = input_bytes;
    for (signal, maybe_batch) in [
        (OtlpSignalType::MetricsGauge, batches.gauge),
        (OtlpSignalType::MetricsSum, batches.sum),
        (OtlpSignalType::MetricsHistogram, batches.histogram),
        (OtlpSignalType::MetricsExpHistogram, batches.exp_histogram),
    ] {
        if let Some(batch) = maybe_batch {
            let charge = std::mem::take(&mut remaining);
            let status = ctx.deliver(signal, stream_id, batch_id, charge, batch);
            if status != OtlpIngestStatus::Ok {
                return status;
            }
        }
    }
    OtlpIngestStatus::Ok
}

/// Map a host ingest outcome to the OTAP `BatchStatus` echoed back on the stream.
fn ingest_to_batch_status(batch_id: i64, status: OtlpIngestStatus) -> BatchStatus {
    let (code, message) = status.detail();
    BatchStatus {
        batch_id,
        status_code: code as i32,
        status_message: message.to_string(),
    }
}

/// A streamed response: one `BatchStatus` per received `BatchArrowRecords`.
type BatchStatusStream = tokio_stream::wrappers::ReceiverStream<Result<BatchStatus, tonic::Status>>;

/// Single-shape OTAP decode entry point (logs or traces): one envelope -> one batch.
type DecodeOne = fn(&mut OtapDecoder, BatchArrowRecords, usize) -> crate::Result<RecordBatch>;

/// Nack for a message whose envelope failed to decode. A decode error poisons the
/// stateful decoder, so the caller closes the stream after sending this.
fn decode_error_status(batch_id: i64, error: &crate::Error) -> BatchStatus {
    BatchStatus {
        batch_id,
        status_code: StatusCode::InvalidArgument as i32,
        status_message: format!("OTAP decode error: {error}"),
    }
}

/// Drive a single-shape (logs/traces) OTAP stream: one stateful decoder for the
/// whole stream, decode each envelope, buffer it, and ack with a `BatchStatus`. A
/// decode error nacks and closes the stream (the decoder is poisoned); a
/// backpressure nack keeps it open.
fn spawn_single_shape_stream(
    ctx: Arc<CallbackCtx>,
    stream_id: u64,
    signal: OtlpSignalType,
    decode: DecodeOne,
    mut input: tonic::Streaming<BatchArrowRecords>,
) -> BatchStatusStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<BatchStatus, tonic::Status>>(64);
    tokio::spawn(async move {
        let mut decoder = OtapDecoder::new();
        loop {
            match input.message().await {
                Ok(Some(envelope)) => {
                    let batch_id = envelope.batch_id;
                    let input_bytes = envelope.encoded_len();
                    match decode(&mut decoder, envelope, input_bytes) {
                        Ok(batch) => {
                            let ingest =
                                ctx.deliver(signal, stream_id, batch_id, input_bytes as u64, batch);
                            if tx
                                .send(Ok(ingest_to_batch_status(batch_id, ingest)))
                                .await
                                .is_err()
                            {
                                break; // response stream dropped (client gone)
                            }
                        }
                        Err(error) => {
                            let _ = tx.send(Ok(decode_error_status(batch_id, &error))).await;
                            break;
                        }
                    }
                }
                Ok(None) => break, // client half-closed the request stream
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    break;
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Drive an OTAP metrics stream: like `spawn_single_shape_stream`, but each
/// envelope decodes into up to four metric shapes, each buffered separately.
fn spawn_metrics_stream(
    ctx: Arc<CallbackCtx>,
    stream_id: u64,
    mut input: tonic::Streaming<BatchArrowRecords>,
) -> BatchStatusStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<BatchStatus, tonic::Status>>(64);
    tokio::spawn(async move {
        let mut decoder = OtapDecoder::new();
        loop {
            match input.message().await {
                Ok(Some(envelope)) => {
                    let batch_id = envelope.batch_id;
                    let input_bytes = envelope.encoded_len() as u64;
                    match decoder.decode_metrics_message(envelope) {
                        Ok(batches) => {
                            let ingest = deliver_metric_batches(
                                &ctx,
                                stream_id,
                                batch_id,
                                input_bytes,
                                batches,
                            );
                            if tx
                                .send(Ok(ingest_to_batch_status(batch_id, ingest)))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(error) => {
                            let _ = tx.send(Ok(decode_error_status(batch_id, &error))).await;
                            break;
                        }
                    }
                }
                Ok(None) => break,
                Err(status) => {
                    let _ = tx.send(Err(status)).await;
                    break;
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

// OTAP/Arrow bidirectional-streaming services. Each stream gets its own stateful
// OtapDecoder (one per ArrowLogs/ArrowTraces/ArrowMetrics RPC), so later messages
// can reuse Arrow dictionaries established by earlier ones.

struct ArrowLogsServiceImpl {
    ctx: Arc<CallbackCtx>,
    stream_counter: AtomicU64,
}

#[tonic::async_trait]
impl ArrowLogsService for ArrowLogsServiceImpl {
    type ArrowLogsStream = BatchStatusStream;
    async fn arrow_logs(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowLogsStream>, tonic::Status> {
        if !self.ctx.auth_ok(request.metadata()) {
            return Err(tonic::Status::unauthenticated("invalid token"));
        }
        let stream_id = self.stream_counter.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(tonic::Response::new(spawn_single_shape_stream(
            self.ctx.clone(),
            stream_id,
            OtlpSignalType::Logs,
            OtapDecoder::decode_logs_message,
            request.into_inner(),
        )))
    }
}

struct ArrowTracesServiceImpl {
    ctx: Arc<CallbackCtx>,
    stream_counter: AtomicU64,
}

#[tonic::async_trait]
impl ArrowTracesService for ArrowTracesServiceImpl {
    type ArrowTracesStream = BatchStatusStream;
    async fn arrow_traces(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowTracesStream>, tonic::Status> {
        if !self.ctx.auth_ok(request.metadata()) {
            return Err(tonic::Status::unauthenticated("invalid token"));
        }
        let stream_id = self.stream_counter.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(tonic::Response::new(spawn_single_shape_stream(
            self.ctx.clone(),
            stream_id,
            OtlpSignalType::Traces,
            OtapDecoder::decode_traces_message,
            request.into_inner(),
        )))
    }
}

struct ArrowMetricsServiceImpl {
    ctx: Arc<CallbackCtx>,
    stream_counter: AtomicU64,
}

#[tonic::async_trait]
impl ArrowMetricsService for ArrowMetricsServiceImpl {
    type ArrowMetricsStream = BatchStatusStream;
    async fn arrow_metrics(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowMetricsStream>, tonic::Status> {
        if !self.ctx.auth_ok(request.metadata()) {
            return Err(tonic::Status::unauthenticated("invalid token"));
        }
        let stream_id = self.stream_counter.fetch_add(1, Ordering::Relaxed) + 1;
        Ok(tonic::Response::new(spawn_metrics_stream(
            self.ctx.clone(),
            stream_id,
            request.into_inner(),
        )))
    }
}

// ============================================================================
// Server handle + lifecycle
// ============================================================================

/// Opaque, heap-owned running gRPC server. Created by
/// `otlp_grpc_server_start`, torn down by `otlp_grpc_server_stop` and freed by
/// `otlp_grpc_server_free`.
pub struct OtlpGrpcServer {
    runtime: Option<tokio::runtime::Runtime>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

fn write_err(err_buf: *mut c_char, err_buf_len: usize, msg: &str) {
    if err_buf.is_null() || err_buf_len == 0 {
        return;
    }
    // Copy up to err_buf_len-1 bytes and null-terminate.
    let bytes = msg.as_bytes();
    let n = bytes.len().min(err_buf_len - 1);
    // SAFETY: err_buf is valid for err_buf_len bytes per the contract.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, err_buf, n);
        *err_buf.add(n) = 0;
    }
}

fn worker_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().clamp(1, 4))
        .unwrap_or(2)
}

/// Start a gRPC ingest server bound to `bind_addr` ("host:port").
///
/// On success returns a non-null handle the caller owns. On failure returns
/// null and writes a message into `err_buf` (truncated, null-terminated).
///
/// # Safety
///
/// - `bind_addr` must be valid for `bind_addr_len` bytes (UTF-8 "host:port").
/// - `on_batch` must be a valid function pointer for the server's lifetime.
/// - `on_auth` may be null to disable auth; otherwise valid for the lifetime.
/// - `user_data` is passed verbatim to the callbacks; the caller must keep it
///   valid until `otlp_grpc_server_stop` returns.
/// - `err_buf` may be null; otherwise valid for `err_buf_len` bytes.
#[no_mangle]
pub unsafe extern "C" fn otlp_grpc_server_start(
    bind_addr: *const c_char,
    bind_addr_len: usize,
    on_batch: Option<OtlpBatchCallback>,
    on_auth: Option<OtlpAuthCallback>,
    user_data: *mut c_void,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut OtlpGrpcServer {
    let Some(on_batch) = on_batch else {
        write_err(err_buf, err_buf_len, "on_batch callback is null");
        return std::ptr::null_mut();
    };
    if bind_addr.is_null() {
        write_err(err_buf, err_buf_len, "bind_addr is null");
        return std::ptr::null_mut();
    }

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let addr_slice = std::slice::from_raw_parts(bind_addr as *const u8, bind_addr_len);
        let addr = std::str::from_utf8(addr_slice)
            .map_err(|_| "bind_addr is not valid UTF-8".to_string())?;

        let ctx = Arc::new(CallbackCtx {
            user_data,
            on_batch,
            on_auth,
        });

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(worker_threads())
            .thread_name("otlp-grpc")
            .enable_all()
            .build()
            .map_err(|e| format!("failed to start runtime: {e}"))?;

        // Bind synchronously so address-in-use surfaces to the caller now.
        let std_listener =
            std::net::TcpListener::bind(addr).map_err(|e| format!("failed to bind {addr}: {e}"))?;
        std_listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to configure listener: {e}"))?;

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // All six signals on one listener: OTLP/gRPC unary Export (logs/traces/
        // metrics) and OTAP/Arrow bidirectional streaming (logs/traces/metrics).
        let logs = LogsServiceServer::new(LogsServiceImpl { ctx: ctx.clone() });
        let traces = TraceServiceServer::new(TraceServiceImpl { ctx: ctx.clone() });
        let metrics = MetricsServiceServer::new(MetricsServiceImpl { ctx: ctx.clone() });
        let arrow_logs = ArrowLogsServiceServer::new(ArrowLogsServiceImpl {
            ctx: ctx.clone(),
            stream_counter: AtomicU64::new(0),
        });
        let arrow_traces = ArrowTracesServiceServer::new(ArrowTracesServiceImpl {
            ctx: ctx.clone(),
            stream_counter: AtomicU64::new(0),
        });
        let arrow_metrics = ArrowMetricsServiceServer::new(ArrowMetricsServiceImpl {
            ctx: ctx.clone(),
            stream_counter: AtomicU64::new(0),
        });

        let join = {
            let _guard = runtime.enter();
            let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
                .map_err(|e| format!("failed to register listener: {e}"))?;
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(tokio_listener);
            runtime.spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(logs)
                    .add_service(traces)
                    .add_service(metrics)
                    .add_service(arrow_logs)
                    .add_service(arrow_traces)
                    .add_service(arrow_metrics)
                    .serve_with_incoming_shutdown(incoming, async move {
                        let _ = shutdown_rx.await;
                    })
                    .await
            })
        };

        Ok::<_, String>(OtlpGrpcServer {
            runtime: Some(runtime),
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
        })
    }));

    match result {
        Ok(Ok(server)) => Box::into_raw(Box::new(server)),
        Ok(Err(msg)) => {
            write_err(err_buf, err_buf_len, &msg);
            std::ptr::null_mut()
        }
        Err(_) => {
            write_err(err_buf, err_buf_len, "panic while starting gRPC server");
            std::ptr::null_mut()
        }
    }
}

/// Stop the server: signal graceful shutdown, drain in-flight work up to
/// `drain_deadline_ms`, then shut down the runtime threads. Idempotent.
///
/// # Safety
///
/// `server` must be a handle from `otlp_grpc_server_start` (not yet freed), or
/// null (a no-op). Must NOT be called from within a batch/auth callback.
#[no_mangle]
pub unsafe extern "C" fn otlp_grpc_server_stop(
    server: *mut OtlpGrpcServer,
    drain_deadline_ms: u64,
) {
    if server.is_null() {
        return;
    }
    let server = &mut *server;
    // Fence any panic (e.g. block_on re-entrancy if the "not from a callback"
    // contract is violated) so it cannot unwind across the extern "C" boundary,
    // which would be UB. Mirrors the catch_unwind in `otlp_grpc_server_start`.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let deadline = std::time::Duration::from_millis(drain_deadline_ms.max(1));
        let start = std::time::Instant::now();

        if let Some(tx) = server.shutdown_tx.take() {
            let _ = tx.send(());
        }
        // Wait up to `deadline` for the server task to drain in-flight requests.
        if let (Some(runtime), Some(join)) = (server.runtime.as_ref(), server.join.take()) {
            runtime.block_on(async move {
                let _ = tokio::time::timeout(deadline, join).await;
            });
        }
        // Spend only the *remaining* budget joining the runtime threads, so total
        // shutdown stays bounded by `deadline` rather than up to 2x.
        if let Some(runtime) = server.runtime.take() {
            let remaining = deadline
                .saturating_sub(start.elapsed())
                .max(std::time::Duration::from_millis(1));
            runtime.shutdown_timeout(remaining);
        }
    }));
}

/// Free a server handle. Call `otlp_grpc_server_stop` first.
///
/// # Safety
///
/// `server` must be a handle from `otlp_grpc_server_start` that has not been
/// freed, or null (a no-op).
#[no_mangle]
pub unsafe extern "C" fn otlp_grpc_server_free(server: *mut OtlpGrpcServer) {
    if !server.is_null() {
        drop(Box::from_raw(server));
    }
}
