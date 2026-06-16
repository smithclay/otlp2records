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
//! **Logs** only, over two services on one listener: standard **OTLP/gRPC
//! unary** `Export` and the canonical **OTAP/Arrow** bidirectional stream
//! (`ArrowLogsService`). Traces and metrics (unary and streaming) are added
//! later. The C ABI carries `stream_id`/`batch_id` so the unary and streaming
//! paths share one callback signature.
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
use crate::otap::grpc_service::{ArrowLogsService, ArrowLogsServiceServer};
use crate::otap::{BatchArrowRecords, BatchStatus, OtapDecoder, StatusCode};

use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::{LogsService, LogsServiceServer},
    ExportLogsServiceRequest, ExportLogsServiceResponse,
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
    }
}

fn ingest_status_to_grpc(status: OtlpIngestStatus) -> Result<(), tonic::Status> {
    match status {
        OtlpIngestStatus::Ok => Ok(()),
        OtlpIngestStatus::ResourceExhausted => {
            Err(tonic::Status::resource_exhausted("ingest buffer full"))
        }
        OtlpIngestStatus::Invalid => Err(tonic::Status::invalid_argument("invalid payload")),
        OtlpIngestStatus::Internal => Err(tonic::Status::internal("ingest error")),
        OtlpIngestStatus::Unauthenticated => Err(tonic::Status::unauthenticated("unauthorized")),
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

/// Map a host ingest outcome to the OTAP `BatchStatus` echoed back on the stream.
fn ingest_to_batch_status(batch_id: i64, status: OtlpIngestStatus) -> BatchStatus {
    let (code, message) = match status {
        OtlpIngestStatus::Ok => (StatusCode::Ok, ""),
        OtlpIngestStatus::ResourceExhausted => {
            (StatusCode::ResourceExhausted, "ingest buffer full")
        }
        OtlpIngestStatus::Invalid => (StatusCode::InvalidArgument, "invalid payload"),
        OtlpIngestStatus::Internal => (StatusCode::Internal, "ingest error"),
        OtlpIngestStatus::Unauthenticated => (StatusCode::Unauthenticated, "unauthorized"),
    };
    BatchStatus {
        batch_id,
        status_code: code as i32,
        status_message: message.to_string(),
    }
}

/// OTAP/Arrow bidirectional-streaming logs service. Each stream gets its own
/// stateful `OtapDecoder` so later messages can reuse Arrow dictionaries/schemas
/// established by earlier ones.
struct ArrowLogsServiceImpl {
    ctx: Arc<CallbackCtx>,
    stream_counter: AtomicU64,
}

#[tonic::async_trait]
impl ArrowLogsService for ArrowLogsServiceImpl {
    type ArrowLogsStream =
        tokio_stream::wrappers::ReceiverStream<Result<BatchStatus, tonic::Status>>;

    async fn arrow_logs(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowLogsStream>, tonic::Status> {
        if !self.ctx.auth_ok(request.metadata()) {
            return Err(tonic::Status::unauthenticated("invalid token"));
        }
        let stream_id = self.stream_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let ctx = self.ctx.clone();
        let mut input = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<BatchStatus, tonic::Status>>(64);

        tokio::spawn(async move {
            // One decoder per stream: it is &mut self / not thread-safe, and it
            // carries the cross-message dictionary state this stream depends on.
            let mut decoder = OtapDecoder::new();
            loop {
                match input.message().await {
                    Ok(Some(envelope)) => {
                        let batch_id = envelope.batch_id;
                        let input_bytes = envelope.encoded_len();
                        match decoder.decode_logs_message(envelope, input_bytes) {
                            Ok(batch) => {
                                let ingest = ctx.deliver(
                                    OtlpSignalType::Logs,
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
                                // The decoder is healthy; keep serving even when a batch was
                                // nacked for backpressure (the client may slow down and retry).
                            }
                            Err(error) => {
                                // A decode error leaves the stateful decoder poisoned, so nack
                                // this batch and close the stream rather than feeding it more.
                                let _ = tx
                                    .send(Ok(BatchStatus {
                                        batch_id,
                                        status_code: StatusCode::InvalidArgument as i32,
                                        status_message: format!("OTAP decode error: {error}"),
                                    }))
                                    .await;
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

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
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

        // OTLP/gRPC unary logs + OTAP/Arrow streaming logs on the same listener.
        let logs = LogsServiceServer::new(LogsServiceImpl { ctx: ctx.clone() });
        let arrow_logs = ArrowLogsServiceServer::new(ArrowLogsServiceImpl {
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
                    .add_service(arrow_logs)
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
    let deadline = std::time::Duration::from_millis(drain_deadline_ms.max(1));

    if let Some(tx) = server.shutdown_tx.take() {
        let _ = tx.send(());
    }
    if let (Some(runtime), Some(join)) = (server.runtime.as_ref(), server.join.take()) {
        runtime.block_on(async move {
            let _ = tokio::time::timeout(deadline, join).await;
        });
    }
    if let Some(runtime) = server.runtime.take() {
        runtime.shutdown_timeout(deadline);
    }
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
