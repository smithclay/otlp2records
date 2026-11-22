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
use arrow_array::RecordBatch;
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataMap;

use crate::ffi::{OtlpArrowProfile, OtlpSignalType};
use crate::ffi_export::{ffi_boundary, ExportError, ExportedBatch, ExportedMetrics};
use crate::otap::grpc_service::{
    ArrowLogsService, ArrowLogsServiceServer, ArrowMetricsService, ArrowMetricsServiceServer,
    ArrowTracesService, ArrowTracesServiceServer,
};
use crate::otap::{BatchArrowRecords, BatchStatus, OtapDecoder, StatusCode};
use crate::ArrowProfile;

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
    /// Applied to every batch before it is exported to the host.
    profile: ArrowProfile,
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

    fn deliver(
        &self,
        signal_type: OtlpSignalType,
        stream_id: u64,
        batch_id: i64,
        input_bytes: u64,
        batch: ExportedBatch,
    ) -> OtlpIngestStatus {
        batch.lend(|array, schema| {
            (self.on_batch)(
                self.user_data,
                signal_type,
                stream_id,
                batch_id,
                input_bytes,
                array,
                schema,
            )
        })
    }

    fn deliver_one(
        &self,
        signal_type: OtlpSignalType,
        stream_id: u64,
        batch_id: i64,
        input_bytes: u64,
        batch: RecordBatch,
    ) -> OtlpIngestStatus {
        ffi_boundary(OtlpIngestStatus::Internal, || {
            match ExportedBatch::new(self.profile, batch).map_err(OtlpIngestStatus::from) {
                Ok(batch) => self.deliver(signal_type, stream_id, batch_id, input_bytes, batch),
                Err(status) => status,
            }
        })
    }

    /// Delivers each present metric shape of one message, charging the wire
    /// bytes to the first. Every shape is exported before any is delivered, so a
    /// profile error delivers nothing; a host nack part-way leaves the earlier
    /// shapes buffered.
    fn deliver_metrics(
        &self,
        stream_id: u64,
        batch_id: i64,
        input_bytes: u64,
        batches: crate::MetricBatches,
    ) -> OtlpIngestStatus {
        ffi_boundary(OtlpIngestStatus::Internal, || {
            let exported = match ExportedMetrics::new(self.profile, batches) {
                Ok(exported) => exported,
                Err(error) => return error.into(),
            };
            let mut charge = input_bytes;
            for (signal, batch) in exported.into_shapes() {
                let charge = std::mem::take(&mut charge);
                let status = self.deliver(signal, stream_id, batch_id, charge, batch);
                if status != OtlpIngestStatus::Ok {
                    return status;
                }
            }
            OtlpIngestStatus::Ok
        })
    }
}

impl From<ExportError> for OtlpIngestStatus {
    fn from(error: ExportError) -> Self {
        match error {
            ExportError::Unrepresentable => OtlpIngestStatus::Invalid,
            ExportError::SchemaNotExportable => OtlpIngestStatus::Internal,
        }
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
        ingest_status_to_grpc(self.ctx.deliver_one(
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
        ingest_status_to_grpc(self.ctx.deliver_one(
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
        ingest_status_to_grpc(self.ctx.deliver_metrics(0, 0, input_bytes, batches))?;
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
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
    cancel: CancellationToken,
    tracker: &TaskTracker,
    stream_id: u64,
    signal: OtlpSignalType,
    decode: DecodeOne,
    mut input: tonic::Streaming<BatchArrowRecords>,
) -> BatchStatusStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<BatchStatus, tonic::Status>>(64);
    // Spawn into the server's TaskTracker so shutdown can await this task; the
    // CancellationToken (checked first via `biased`) lets `otlp_grpc_server_stop`
    // break the loop at the next await instead of leaving it detached.
    tracker.spawn(async move {
        let mut decoder = OtapDecoder::new();
        loop {
            let message = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                message = input.message() => message,
            };
            match message {
                Ok(Some(envelope)) => {
                    let batch_id = envelope.batch_id;
                    let input_bytes = envelope.encoded_len();
                    match decode(&mut decoder, envelope, input_bytes) {
                        Ok(batch) => {
                            let ingest = ctx.deliver_one(
                                signal,
                                stream_id,
                                batch_id,
                                input_bytes as u64,
                                batch,
                            );
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
    cancel: CancellationToken,
    tracker: &TaskTracker,
    stream_id: u64,
    mut input: tonic::Streaming<BatchArrowRecords>,
) -> BatchStatusStream {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<BatchStatus, tonic::Status>>(64);
    tracker.spawn(async move {
        let mut decoder = OtapDecoder::new();
        loop {
            let message = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                message = input.message() => message,
            };
            match message {
                Ok(Some(envelope)) => {
                    let batch_id = envelope.batch_id;
                    let input_bytes = envelope.encoded_len() as u64;
                    match decoder.decode_metrics_message(envelope) {
                        Ok(batches) => {
                            let ingest =
                                ctx.deliver_metrics(stream_id, batch_id, input_bytes, batches);
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
    cancel: CancellationToken,
    tracker: TaskTracker,
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
            self.cancel.clone(),
            &self.tracker,
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
    cancel: CancellationToken,
    tracker: TaskTracker,
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
            self.cancel.clone(),
            &self.tracker,
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
    cancel: CancellationToken,
    tracker: TaskTracker,
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
            self.cancel.clone(),
            &self.tracker,
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
    /// Cancels in-flight OTAP stream loops at their next await on shutdown.
    cancel: CancellationToken,
    /// Tracks the spawned OTAP stream tasks so shutdown can await them, so no host
    /// callback runs after `otlp_grpc_server_stop` returns.
    tracker: TaskTracker,
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

/// `service_flags` bit: register the OTLP/gRPC unary `Export` services
/// (`{Logs,Trace,Metrics}Service`).
pub const OTLP_GRPC_SERVICE_OTLP_UNARY: u32 = 1 << 0;
/// `service_flags` bit: register the OTAP/Arrow bidirectional-streaming services
/// (`Arrow{Logs,Traces,Metrics}Service`).
pub const OTLP_GRPC_SERVICE_OTAP_ARROW: u32 = 1 << 1;

/// Start a gRPC ingest server bound to `bind_addr` ("host:port").
///
/// On success returns a non-null handle the caller owns. On failure returns
/// null and writes a message into `err_buf` (truncated, null-terminated).
///
/// `service_flags` selects which gRPC service families to register, OR-ing
/// [`OTLP_GRPC_SERVICE_OTLP_UNARY`] and/or [`OTLP_GRPC_SERVICE_OTAP_ARROW`]; 0
/// registers both. This is how the host keeps `otlp:` (OTLP/gRPC unary) and
/// `otap:` (OTAP/Arrow streaming) as separate listeners with disjoint services.
///
/// `max_decoding_message_bytes` caps a single received gRPC message (one OTLP
/// Export request or one OTAP `BatchArrowRecords`) on every service; pass 0 for
/// tonic's 4 MiB default. Raise it when producers send batches larger than 4 MiB.
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
    service_flags: u32,
    max_decoding_message_bytes: u64,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut OtlpGrpcServer {
    otlp_grpc_server_start_with_profile(
        bind_addr,
        bind_addr_len,
        on_batch,
        on_auth,
        user_data,
        service_flags,
        max_decoding_message_bytes,
        OtlpArrowProfile::Canonical,
        err_buf,
        err_buf_len,
    )
}

/// Start a gRPC ingest server whose batches use the Arrow output `profile`.
///
/// Identical to [`otlp_grpc_server_start`] except that every batch handed to
/// `on_batch` (OTLP/gRPC unary and OTAP/Arrow streaming, every signal and
/// metric shape) is re-typed for `profile` first.
/// `OTLP_ARROW_PROFILE_CANONICAL` is identical to `otlp_grpc_server_start`.
/// Under `OTLP_ARROW_PROFILE_DUCKDB`, a batch the profile cannot represent is
/// answered `INVALID_ARGUMENT` without invoking `on_batch`.
///
/// # Safety
///
/// Same contract as [`otlp_grpc_server_start`].
#[no_mangle]
pub unsafe extern "C" fn otlp_grpc_server_start_with_profile(
    bind_addr: *const c_char,
    bind_addr_len: usize,
    on_batch: Option<OtlpBatchCallback>,
    on_auth: Option<OtlpAuthCallback>,
    user_data: *mut c_void,
    service_flags: u32,
    max_decoding_message_bytes: u64,
    profile: OtlpArrowProfile,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut OtlpGrpcServer {
    let started = ffi_boundary(Err("panic while starting gRPC server".into()), || {
        start_server(
            bind_addr,
            bind_addr_len,
            on_batch,
            on_auth,
            user_data,
            service_flags,
            max_decoding_message_bytes,
            profile.into(),
        )
    });
    match started {
        Ok(server) => Box::into_raw(Box::new(server)),
        Err(msg) => {
            write_err(err_buf, err_buf_len, &msg);
            std::ptr::null_mut()
        }
    }
}

#[allow(clippy::too_many_arguments)]
unsafe fn start_server(
    bind_addr: *const c_char,
    bind_addr_len: usize,
    on_batch: Option<OtlpBatchCallback>,
    on_auth: Option<OtlpAuthCallback>,
    user_data: *mut c_void,
    service_flags: u32,
    max_decoding_message_bytes: u64,
    profile: ArrowProfile,
) -> Result<OtlpGrpcServer, String> {
    let on_batch = on_batch.ok_or("on_batch callback is null")?;
    if bind_addr.is_null() {
        return Err("bind_addr is null".into());
    }
    let addr_slice = std::slice::from_raw_parts(bind_addr as *const u8, bind_addr_len);
    let addr =
        std::str::from_utf8(addr_slice).map_err(|_| "bind_addr is not valid UTF-8".to_string())?;

    let ctx = Arc::new(CallbackCtx {
        user_data,
        on_batch,
        on_auth,
        profile,
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
    // Shared across the three OTAP streaming services: the token signals their
    // spawned loops to stop, the tracker lets shutdown await them.
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();

    // Per-message decode cap applied uniformly to all six services. 0 means
    // "tonic's default"; resolve it to that concrete value (4 MiB) so the cap
    // is explicit rather than relying on tonic's implicit default.
    const TONIC_DEFAULT_MAX_DECODING: usize = 4 * 1024 * 1024;
    let max_decoding = if max_decoding_message_bytes == 0 {
        TONIC_DEFAULT_MAX_DECODING
    } else {
        max_decoding_message_bytes as usize
    };

    // Which service families to register. 0 = both, so each named bit (or the
    // 0 default) opts a family in.
    let serve_otlp = service_flags == 0 || (service_flags & OTLP_GRPC_SERVICE_OTLP_UNARY) != 0;
    let serve_otap = service_flags == 0 || (service_flags & OTLP_GRPC_SERVICE_OTAP_ARROW) != 0;

    // The listener serves the OTLP/gRPC unary Export services and/or the
    // OTAP/Arrow bidirectional-streaming services, per `service_flags`.
    // accept_compressed(Zstd): decompress zstd-compressed gRPC requests. The
    // otel-arrow OTAP exporter sends `grpc-encoding: zstd` by default; without
    // this every stream is rejected. Responses are left uncompressed (the
    // BatchStatus/ExportResponse acks are tiny). Applied to all six services so
    // OTLP/gRPC unary and OTAP/Arrow streaming both accept the wire default.
    let logs = LogsServiceServer::new(LogsServiceImpl { ctx: ctx.clone() })
        .max_decoding_message_size(max_decoding)
        .accept_compressed(CompressionEncoding::Zstd);
    let traces = TraceServiceServer::new(TraceServiceImpl { ctx: ctx.clone() })
        .max_decoding_message_size(max_decoding)
        .accept_compressed(CompressionEncoding::Zstd);
    let metrics = MetricsServiceServer::new(MetricsServiceImpl { ctx: ctx.clone() })
        .max_decoding_message_size(max_decoding)
        .accept_compressed(CompressionEncoding::Zstd);
    let arrow_logs = ArrowLogsServiceServer::new(ArrowLogsServiceImpl {
        ctx: ctx.clone(),
        stream_counter: AtomicU64::new(0),
        cancel: cancel.clone(),
        tracker: tracker.clone(),
    })
    .max_decoding_message_size(max_decoding)
    .accept_compressed(CompressionEncoding::Zstd);
    let arrow_traces = ArrowTracesServiceServer::new(ArrowTracesServiceImpl {
        ctx: ctx.clone(),
        stream_counter: AtomicU64::new(0),
        cancel: cancel.clone(),
        tracker: tracker.clone(),
    })
    .max_decoding_message_size(max_decoding)
    .accept_compressed(CompressionEncoding::Zstd);
    let arrow_metrics = ArrowMetricsServiceServer::new(ArrowMetricsServiceImpl {
        ctx: ctx.clone(),
        stream_counter: AtomicU64::new(0),
        cancel: cancel.clone(),
        tracker: tracker.clone(),
    })
    .max_decoding_message_size(max_decoding)
    .accept_compressed(CompressionEncoding::Zstd);

    let join = {
        let _guard = runtime.enter();
        let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
            .map_err(|e| format!("failed to register listener: {e}"))?;
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(tokio_listener);
        runtime.spawn(async move {
            // add_optional_service registers a family only when its flag is set;
            // a None arm is a no-op, so an `otlp:`-only or `otap:`-only listener
            // simply doesn't route the other family's methods.
            tonic::transport::Server::builder()
                .add_optional_service(serve_otlp.then_some(logs))
                .add_optional_service(serve_otlp.then_some(traces))
                .add_optional_service(serve_otlp.then_some(metrics))
                .add_optional_service(serve_otap.then_some(arrow_logs))
                .add_optional_service(serve_otap.then_some(arrow_traces))
                .add_optional_service(serve_otap.then_some(arrow_metrics))
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = shutdown_rx.await;
                })
                .await
        })
    };

    Ok(OtlpGrpcServer {
        runtime: Some(runtime),
        shutdown_tx: Some(shutdown_tx),
        join: Some(join),
        cancel,
        tracker,
    })
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
    ffi_boundary((), || {
        let Some(server) = server.as_mut() else {
            return;
        };
        let deadline = std::time::Duration::from_millis(drain_deadline_ms.max(1));
        let start = std::time::Instant::now();

        // Stop accepting new connections/streams, tell in-flight OTAP stream loops
        // to stop at their next await, and close the tracker so its `wait()` can
        // resolve once the already-spawned tasks finish.
        if let Some(tx) = server.shutdown_tx.take() {
            let _ = tx.send(());
        }
        server.cancel.cancel();
        server.tracker.close();

        // Within `deadline`, drain the server task AND every spawned stream task,
        // so no host callback (and thus no use of `user_data`) runs after this
        // returns.
        if let Some(runtime) = server.runtime.as_ref() {
            let join = server.join.take();
            let tracker = server.tracker.clone();
            runtime.block_on(async move {
                let _ = tokio::time::timeout(deadline, async move {
                    if let Some(join) = join {
                        let _ = join.await;
                    }
                    tracker.wait().await;
                })
                .await;
            });
        }
        // Spend only the *remaining* budget hard-stopping the runtime threads, so
        // total shutdown stays bounded by `deadline` rather than up to 2x.
        if let Some(runtime) = server.runtime.take() {
            let remaining = deadline
                .saturating_sub(start.elapsed())
                .max(std::time::Duration::from_millis(1));
            runtime.shutdown_timeout(remaining);
        }
    })
}

/// Free a server handle. Call `otlp_grpc_server_stop` first.
///
/// # Safety
///
/// `server` must be a handle from `otlp_grpc_server_start` that has not been
/// freed, or null (a no-op).
#[no_mangle]
pub unsafe extern "C" fn otlp_grpc_server_free(server: *mut OtlpGrpcServer) {
    ffi_boundary((), || {
        if !server.is_null() {
            drop(Box::from_raw(server));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    /// `(column, Arrow C format)` of each top-level column the callback saw,
    /// one entry per delivered batch.
    type Seen = Mutex<Vec<Vec<(String, String)>>>;

    extern "C" fn record_formats(
        user_data: *mut c_void,
        _signal_type: OtlpSignalType,
        _stream_id: u64,
        _batch_id: i64,
        _input_bytes: u64,
        _array: *mut FFI_ArrowArray,
        schema: *mut FFI_ArrowSchema,
    ) -> OtlpIngestStatus {
        // SAFETY: user_data is the `Seen` owned by `export_one_span`, alive
        // until the server is stopped; the schema is borrowed for this call.
        let (seen, schema) = unsafe { (&*(user_data as *const Seen), &*schema) };
        let formats = schema
            .children()
            .map(|c| {
                (
                    c.name().unwrap_or_default().to_string(),
                    c.format().to_string(),
                )
            })
            .collect();
        seen.lock().unwrap().push(formats);
        OtlpIngestStatus::Ok
    }

    /// Start a unary OTLP/gRPC server (with `profile`, or through the
    /// profile-less entry point for `None`), export one span to it, and return
    /// the column formats the batch callback received.
    fn export_one_span(profile: Option<OtlpArrowProfile>) -> Vec<(String, String)> {
        let seen: Seen = Mutex::new(Vec::new());
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr = format!("127.0.0.1:{port}");
        let user_data = &seen as *const Seen as *mut c_void;
        let server = unsafe {
            match profile {
                None => otlp_grpc_server_start(
                    addr.as_ptr() as *const c_char,
                    addr.len(),
                    Some(record_formats),
                    None,
                    user_data,
                    OTLP_GRPC_SERVICE_OTLP_UNARY,
                    0,
                    std::ptr::null_mut(),
                    0,
                ),
                Some(profile) => otlp_grpc_server_start_with_profile(
                    addr.as_ptr() as *const c_char,
                    addr.len(),
                    Some(record_formats),
                    None,
                    user_data,
                    OTLP_GRPC_SERVICE_OTLP_UNARY,
                    0,
                    profile,
                    std::ptr::null_mut(),
                    0,
                ),
            }
        };
        assert!(!server.is_null());

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![0xab; 16],
                        span_id: vec![0xcd; 8],
                        name: "span".to_string(),
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_000_000_001_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut client = TraceServiceClient::connect(format!("http://{addr}"))
                .await
                .expect("connect");
            client.export(request).await.expect("export");
        });
        unsafe {
            otlp_grpc_server_stop(server, 1_000);
            otlp_grpc_server_free(server);
        }

        let mut batches = seen.into_inner().unwrap();
        assert_eq!(batches.len(), 1);
        batches.pop().unwrap()
    }

    fn format_of<'a>(formats: &'a [(String, String)], column: &str) -> &'a str {
        &formats.iter().find(|(name, _)| name == column).unwrap().1
    }

    #[test]
    fn grpc_server_applies_the_duckdb_profile_to_delivered_batches() {
        let formats = export_one_span(Some(OtlpArrowProfile::DuckDb));
        assert_eq!(format_of(&formats, "trace_id"), "u");
        assert_eq!(format_of(&formats, "span_id"), "u");
        assert_eq!(format_of(&formats, "duration_time_unix_nano"), "l");
        assert_eq!(format_of(&formats, "flags"), "i");
        assert_eq!(format_of(&formats, "start_time_unix_nano"), "tsn:");
    }

    #[test]
    fn grpc_server_default_entry_point_keeps_the_canonical_schema() {
        let formats = export_one_span(None);
        assert_eq!(format_of(&formats, "trace_id"), "w:16");
        assert_eq!(format_of(&formats, "duration_time_unix_nano"), "tDn");
        assert_eq!(format_of(&formats, "flags"), "I");
        assert_eq!(export_one_span(Some(OtlpArrowProfile::Canonical)), formats);
    }
}
