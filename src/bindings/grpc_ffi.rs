//! C ABI for an embedded gRPC ingest server.
//!
//! A [`tonic`] server on its own [`tokio`] runtime serves all six signals over
//! two service families: OTLP/gRPC unary `Export` (`{Logs,Trace,Metrics}Service`)
//! and OTAP/Arrow bidirectional streams (`Arrow{Logs,Traces,Metrics}Service`).
//! Each decoded batch is lent to the host's callback for the duration of the
//! call; a metrics message fans out into up to four shape-specific batches.

use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::RecordBatch;
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataMap;
use tonic::Code;

use crate::ffi::{OtlpArrowProfile, OtlpSignalType};
use crate::ffi_export::{ffi_boundary, ExportError, ExportedBatch, ExportedMetrics};
use crate::grpc_gate::{in_host_call, CallbackGate};
use crate::otap::grpc_service::{
    ArrowLogsService, ArrowLogsServiceServer, ArrowMetricsService, ArrowMetricsServiceServer,
    ArrowTracesService, ArrowTracesServiceServer,
};
use crate::otap::{BatchArrowRecords, BatchStatus, OtapDecoder};
use crate::{ArrowProfile, MetricBatches};

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

/// Per-batch callback, invoked on a server worker thread.
///
/// `stream_id`/`batch_id` are 0 for unary requests. `input_bytes` is the wire
/// size of the source message, reported on the first batch delivered from it
/// and 0 on the rest, so a metrics message is charged once. `array`/`schema`
/// are borrowed for the call and released by the server when it returns.
pub type OtlpBatchCallback = extern "C" fn(
    user_data: *mut c_void,
    signal_type: OtlpSignalType,
    stream_id: u64,
    batch_id: i64,
    input_bytes: u64,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> OtlpIngestStatus;

/// Optional auth callback: given the raw `authorization` metadata value
/// (possibly empty), returns non-zero to accept and 0 to reject.
pub type OtlpAuthCallback =
    extern "C" fn(user_data: *mut c_void, metadata_token: *const c_char, len: usize) -> c_int;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Host(OtlpIngestStatus),
    Stopping,
}

impl From<ExportError> for Outcome {
    fn from(error: ExportError) -> Self {
        Outcome::Host(match error {
            ExportError::Unrepresentable => OtlpIngestStatus::Invalid,
            ExportError::SchemaNotExportable => OtlpIngestStatus::Internal,
        })
    }
}

impl Outcome {
    const OK: Outcome = Outcome::Host(OtlpIngestStatus::Ok);

    fn detail(self) -> (Code, &'static str) {
        match self {
            Outcome::Host(OtlpIngestStatus::Ok) => (Code::Ok, ""),
            Outcome::Host(OtlpIngestStatus::ResourceExhausted) => {
                (Code::ResourceExhausted, "ingest buffer full")
            }
            Outcome::Host(OtlpIngestStatus::Invalid) => (Code::InvalidArgument, "invalid payload"),
            Outcome::Host(OtlpIngestStatus::Internal) => (Code::Internal, "ingest error"),
            Outcome::Host(OtlpIngestStatus::Unauthenticated) => {
                (Code::Unauthenticated, "unauthorized")
            }
            Outcome::Stopping => (Code::Unavailable, "server is stopping"),
        }
    }

    fn into_grpc(self) -> Result<(), tonic::Status> {
        match self.detail() {
            (Code::Ok, _) => Ok(()),
            (code, message) => Err(tonic::Status::new(code, message)),
        }
    }

    fn into_batch_status(self, batch_id: i64) -> BatchStatus {
        let (code, message) = self.detail();
        BatchStatus {
            batch_id,
            status_code: code.into(),
            status_message: message.into(),
        }
    }
}

struct Host {
    user_data: *mut c_void,
    on_batch: OtlpBatchCallback,
    on_auth: Option<OtlpAuthCallback>,
    profile: ArrowProfile,
    gate: CallbackGate,
}

// SAFETY: Rust never dereferences `user_data`; it is only handed back to the
// host's callbacks, and the gate guarantees none runs after stop returns, which
// is as long as the host keeps it alive.
unsafe impl Send for Host {}
unsafe impl Sync for Host {}

impl Host {
    fn authorize(&self, metadata: &MetadataMap) -> Result<(), tonic::Status> {
        let Some(on_auth) = self.on_auth else {
            return Ok(());
        };
        let token = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        let accepted = self
            .gate
            .call(|| on_auth(self.user_data, token.as_ptr() as *const c_char, token.len()) != 0);
        match accepted {
            Some(true) => Ok(()),
            Some(false) => Err(tonic::Status::unauthenticated("invalid token")),
            None => Outcome::Stopping.into_grpc(),
        }
    }

    fn deliver(
        &self,
        signal_type: OtlpSignalType,
        stream_id: u64,
        batch_id: i64,
        input_bytes: u64,
        batch: ExportedBatch,
    ) -> Outcome {
        let status = self.gate.call(|| {
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
        });
        status.map_or(Outcome::Stopping, Outcome::Host)
    }

    fn deliver_one(
        &self,
        signal_type: OtlpSignalType,
        stream_id: u64,
        batch_id: i64,
        input_bytes: u64,
        batch: RecordBatch,
    ) -> Outcome {
        ffi_boundary(
            Outcome::Host(OtlpIngestStatus::Internal),
            || match ExportedBatch::new(self.profile, batch) {
                Ok(batch) => self.deliver(signal_type, stream_id, batch_id, input_bytes, batch),
                Err(error) => error.into(),
            },
        )
    }

    fn deliver_metrics(
        &self,
        stream_id: u64,
        batch_id: i64,
        input_bytes: u64,
        batches: MetricBatches,
    ) -> Outcome {
        ffi_boundary(Outcome::Host(OtlpIngestStatus::Internal), || {
            let exported = match ExportedMetrics::new(self.profile, batches) {
                Ok(exported) => exported,
                Err(error) => return error.into(),
            };
            let mut charge = input_bytes;
            for (signal, batch) in exported.into_shapes() {
                let charge = std::mem::take(&mut charge);
                let outcome = self.deliver(signal, stream_id, batch_id, charge, batch);
                if outcome != Outcome::OK {
                    return outcome;
                }
            }
            Outcome::OK
        })
    }
}

#[derive(Clone)]
struct OtlpIngest {
    host: Arc<Host>,
}

fn invalid(signal: &str, error: crate::Error) -> tonic::Status {
    tonic::Status::invalid_argument(format!("failed to transform {signal}: {error}"))
}

#[tonic::async_trait]
impl LogsService for OtlpIngest {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        self.host.authorize(request.metadata())?;
        let request = request.into_inner();
        let input_bytes = request.encoded_len();
        let batch = crate::batch::transform_logs_request_observed(request, input_bytes, &mut None)
            .map_err(|e| invalid("logs", e))?;
        self.host
            .deliver_one(OtlpSignalType::Logs, 0, 0, input_bytes as u64, batch)
            .into_grpc()?;
        Ok(tonic::Response::new(ExportLogsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl TraceService for OtlpIngest {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        self.host.authorize(request.metadata())?;
        let request = request.into_inner();
        let input_bytes = request.encoded_len();
        let batch =
            crate::batch::transform_traces_request_observed(request, input_bytes, &mut None)
                .map_err(|e| invalid("traces", e))?;
        self.host
            .deliver_one(OtlpSignalType::Traces, 0, 0, input_bytes as u64, batch)
            .into_grpc()?;
        Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpIngest {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        self.host.authorize(request.metadata())?;
        let request = request.into_inner();
        let input_bytes = request.encoded_len() as u64;
        let batches = crate::batch::transform_metrics_request_observed(request, &mut None)
            .map_err(|e| invalid("metrics", e))?;
        self.host
            .deliver_metrics(0, 0, input_bytes, batches)
            .into_grpc()?;
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

type DecodeAndDeliver =
    fn(&Host, &mut OtapDecoder, u64, BatchArrowRecords) -> crate::Result<Outcome>;

fn logs_message(
    host: &Host,
    decoder: &mut OtapDecoder,
    stream_id: u64,
    envelope: BatchArrowRecords,
) -> crate::Result<Outcome> {
    let (batch_id, input_bytes) = (envelope.batch_id, envelope.encoded_len());
    let batch = decoder.decode_logs_message(envelope, input_bytes)?;
    Ok(host.deliver_one(
        OtlpSignalType::Logs,
        stream_id,
        batch_id,
        input_bytes as u64,
        batch,
    ))
}

fn traces_message(
    host: &Host,
    decoder: &mut OtapDecoder,
    stream_id: u64,
    envelope: BatchArrowRecords,
) -> crate::Result<Outcome> {
    let (batch_id, input_bytes) = (envelope.batch_id, envelope.encoded_len());
    let batch = decoder.decode_traces_message(envelope, input_bytes)?;
    Ok(host.deliver_one(
        OtlpSignalType::Traces,
        stream_id,
        batch_id,
        input_bytes as u64,
        batch,
    ))
}

fn metrics_message(
    host: &Host,
    decoder: &mut OtapDecoder,
    stream_id: u64,
    envelope: BatchArrowRecords,
) -> crate::Result<Outcome> {
    let (batch_id, input_bytes) = (envelope.batch_id, envelope.encoded_len() as u64);
    let batches = decoder.decode_metrics_message(envelope)?;
    Ok(host.deliver_metrics(stream_id, batch_id, input_bytes, batches))
}

type BatchStatusStream = tokio_stream::wrappers::ReceiverStream<Result<BatchStatus, tonic::Status>>;

/// OTAP/Arrow streaming for all three signals. Each stream owns one stateful
/// decoder, so later messages can reuse dictionaries from earlier ones.
#[derive(Clone)]
struct OtapIngest {
    host: Arc<Host>,
    streams: Arc<AtomicU64>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl OtapIngest {
    fn serve(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
        decode_and_deliver: DecodeAndDeliver,
    ) -> Result<tonic::Response<BatchStatusStream>, tonic::Status> {
        self.host.authorize(request.metadata())?;
        let mut input = request.into_inner();
        let stream_id = self.streams.fetch_add(1, Ordering::Relaxed) + 1;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let (host, cancel) = (self.host.clone(), self.cancel.clone());
        self.tracker.spawn(async move {
            let mut decoder = OtapDecoder::new();
            loop {
                let message = tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    message = input.message() => message,
                };
                let envelope = match message {
                    Ok(Some(envelope)) => envelope,
                    Ok(None) => break,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                };
                let batch_id = envelope.batch_id;
                let (ack, keep_going) =
                    match decode_and_deliver(&host, &mut decoder, stream_id, envelope) {
                        Ok(outcome) => (
                            outcome.into_batch_status(batch_id),
                            outcome != Outcome::Stopping,
                        ),
                        Err(error) => (
                            BatchStatus {
                                batch_id,
                                status_code: Code::InvalidArgument.into(),
                                status_message: format!("OTAP decode error: {error}"),
                            },
                            false,
                        ),
                    };
                if tx.send(Ok(ack)).await.is_err() || !keep_going {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(BatchStatusStream::new(rx)))
    }
}

#[tonic::async_trait]
impl ArrowLogsService for OtapIngest {
    type ArrowLogsStream = BatchStatusStream;
    async fn arrow_logs(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowLogsStream>, tonic::Status> {
        self.serve(request, logs_message)
    }
}

#[tonic::async_trait]
impl ArrowTracesService for OtapIngest {
    type ArrowTracesStream = BatchStatusStream;
    async fn arrow_traces(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowTracesStream>, tonic::Status> {
        self.serve(request, traces_message)
    }
}

#[tonic::async_trait]
impl ArrowMetricsService for OtapIngest {
    type ArrowMetricsStream = BatchStatusStream;
    async fn arrow_metrics(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> Result<tonic::Response<Self::ArrowMetricsStream>, tonic::Status> {
        self.serve(request, metrics_message)
    }
}

/// Opaque, heap-owned running gRPC server. Created by
/// `otlp_grpc_server_start`, stopped by `otlp_grpc_server_stop` and freed by
/// `otlp_grpc_server_free`.
pub struct OtlpGrpcServer {
    host: Arc<Host>,
    running: Option<Running>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

struct Running {
    runtime: tokio::runtime::Runtime,
    join: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl OtlpGrpcServer {
    fn shutdown(&mut self, deadline: Duration) {
        let Some(Running { runtime, join }) = self.running.take() else {
            return;
        };
        let started = Instant::now();
        self.cancel.cancel();
        self.tracker.close();
        let tracker = self.tracker.clone();
        runtime.block_on(async move {
            let _ = tokio::time::timeout(deadline, async move {
                let _ = join.await;
                tracker.wait().await;
            })
            .await;
        });
        self.host.gate.close_and_wait();
        let remaining = deadline.saturating_sub(started.elapsed());
        runtime.shutdown_timeout(remaining.max(Duration::from_millis(1)));
    }
}

impl Drop for OtlpGrpcServer {
    fn drop(&mut self) {
        self.shutdown(Duration::from_millis(1));
    }
}

fn write_err(err_buf: *mut c_char, err_buf_len: usize, msg: &str) {
    if err_buf.is_null() || err_buf_len == 0 {
        return;
    }
    let n = msg.len().min(err_buf_len - 1);
    // SAFETY: err_buf is valid for err_buf_len bytes per the contract.
    unsafe {
        std::ptr::copy_nonoverlapping(msg.as_ptr() as *const c_char, err_buf, n);
        *err_buf.add(n) = 0;
    }
}

/// `service_flags` bit: register the OTLP/gRPC unary `Export` services
/// (`{Logs,Trace,Metrics}Service`).
pub const OTLP_GRPC_SERVICE_OTLP_UNARY: u32 = 1 << 0;
/// `service_flags` bit: register the OTAP/Arrow bidirectional-streaming services
/// (`Arrow{Logs,Traces,Metrics}Service`).
pub const OTLP_GRPC_SERVICE_OTAP_ARROW: u32 = 1 << 1;

const DEFAULT_MAX_DECODING: usize = 4 * 1024 * 1024;

/// Start a gRPC ingest server bound to `bind_addr` ("host:port").
///
/// On success returns a non-null handle the caller owns. On failure returns
/// null and writes a message into `err_buf` (truncated, null-terminated).
///
/// `service_flags` selects which gRPC service families to register, OR-ing
/// [`OTLP_GRPC_SERVICE_OTLP_UNARY`] and/or [`OTLP_GRPC_SERVICE_OTAP_ARROW`]; 0
/// registers both.
///
/// `max_decoding_message_bytes` caps a single received gRPC message (one OTLP
/// Export request or one OTAP `BatchArrowRecords`) on every service; pass 0 for
/// 4 MiB.
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
/// `on_batch` is re-typed for `profile` first. Under
/// `OTLP_ARROW_PROFILE_DUCKDB`, a batch the profile cannot represent is
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
        let on_batch = on_batch.ok_or("on_batch callback is null")?;
        if bind_addr.is_null() {
            return Err("bind_addr is null".into());
        }
        let addr = std::str::from_utf8(std::slice::from_raw_parts(
            bind_addr as *const u8,
            bind_addr_len,
        ))
        .map_err(|_| "bind_addr is not valid UTF-8")?;
        let host = Host {
            user_data,
            on_batch,
            on_auth,
            profile: profile.into(),
            gate: CallbackGate::default(),
        };
        let max_decoding = match max_decoding_message_bytes {
            0 => DEFAULT_MAX_DECODING,
            n => usize::try_from(n).unwrap_or(usize::MAX),
        };
        start_server(addr, host, service_flags, max_decoding)
    });
    match started {
        Ok(server) => Box::into_raw(Box::new(server)),
        Err(msg) => {
            write_err(err_buf, err_buf_len, &msg);
            std::ptr::null_mut()
        }
    }
}

fn start_server(
    addr: &str,
    host: Host,
    service_flags: u32,
    max_decoding: usize,
) -> Result<OtlpGrpcServer, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get().clamp(1, 4))
                .unwrap_or(2),
        )
        .thread_name("otlp-grpc")
        .enable_all()
        .build()
        .map_err(|e| format!("failed to start runtime: {e}"))?;
    let listener =
        std::net::TcpListener::bind(addr).map_err(|e| format!("failed to bind {addr}: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to configure listener: {e}"))?;

    let host = Arc::new(host);
    let cancel = CancellationToken::new();
    let tracker = TaskTracker::new();
    let otlp = OtlpIngest { host: host.clone() };
    let otap = OtapIngest {
        host: host.clone(),
        streams: Arc::default(),
        cancel: cancel.clone(),
        tracker: tracker.clone(),
    };
    let serve_otlp = service_flags == 0 || service_flags & OTLP_GRPC_SERVICE_OTLP_UNARY != 0;
    let serve_otap = service_flags == 0 || service_flags & OTLP_GRPC_SERVICE_OTAP_ARROW != 0;

    // Producers default to zstd (`grpc-encoding: zstd`); a service that does not
    // accept it rejects every stream from them.
    macro_rules! service {
        ($server:ident, $impl:expr) => {
            $server::new($impl)
                .max_decoding_message_size(max_decoding)
                .accept_compressed(CompressionEncoding::Zstd)
        };
    }
    let router = tonic::transport::Server::builder()
        .add_optional_service(serve_otlp.then(|| service!(LogsServiceServer, otlp.clone())))
        .add_optional_service(serve_otlp.then(|| service!(TraceServiceServer, otlp.clone())))
        .add_optional_service(serve_otlp.then(|| service!(MetricsServiceServer, otlp)))
        .add_optional_service(serve_otap.then(|| service!(ArrowLogsServiceServer, otap.clone())))
        .add_optional_service(serve_otap.then(|| service!(ArrowTracesServiceServer, otap.clone())))
        .add_optional_service(serve_otap.then(|| service!(ArrowMetricsServiceServer, otap)));

    let join = {
        let _guard = runtime.enter();
        let listener = tokio::net::TcpListener::from_std(listener)
            .map_err(|e| format!("failed to register listener: {e}"))?;
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        runtime
            .spawn(router.serve_with_incoming_shutdown(incoming, cancel.clone().cancelled_owned()))
    };

    Ok(OtlpGrpcServer {
        host,
        running: Some(Running { runtime, join }),
        cancel,
        tracker,
    })
}

/// Stop the server. Idempotent.
///
/// Stops accepting connections and drains in-flight requests for up to
/// `drain_deadline_ms`. Then no new host callback is started, and this waits
/// for callbacks already running to return, regardless of the deadline: once it
/// returns, no callback is running or will run, so `user_data` may be freed.
/// Called from inside a callback it returns immediately without stopping.
///
/// # Safety
///
/// `server` must be a handle from `otlp_grpc_server_start` (not yet freed), or
/// null (a no-op).
#[no_mangle]
pub unsafe extern "C" fn otlp_grpc_server_stop(
    server: *mut OtlpGrpcServer,
    drain_deadline_ms: u64,
) {
    ffi_boundary((), || {
        if let Some(server) = server.as_mut() {
            if !in_host_call() {
                server.shutdown(Duration::from_millis(drain_deadline_ms.max(1)));
            }
        }
    })
}

/// Free a server handle, stopping it first if it is still running. Called
/// from inside a callback, it returns at once and the server stops after that
/// callback returns.
///
/// # Safety
///
/// `server` must be a handle from `otlp_grpc_server_start` that has not been
/// freed, or null (a no-op).
#[no_mangle]
pub unsafe extern "C" fn otlp_grpc_server_free(server: *mut OtlpGrpcServer) {
    ffi_boundary((), || {
        if server.is_null() {
            return;
        }
        let server = Box::from_raw(server);
        if in_host_call() {
            std::thread::spawn(move || drop(server));
        } else {
            drop(server);
        }
    })
}
