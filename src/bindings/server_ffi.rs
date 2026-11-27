//! C ABI for an embedded OTLP/gRPC, OTAP/gRPC and OTLP/HTTP ingest server.

#[path = "server_http.rs"]
mod http;

use std::borrow::Cow;
use std::ffi::{c_char, c_int, c_void};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::RecordBatch;
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::codec::CompressionEncoding;
use tonic::codegen::InterceptedService;
use tonic::server::NamedService;
use tonic::service::Routes;
use tonic::transport::server::TcpIncoming;
use tonic::Code;

use crate::ffi::{OtlpArrowProfile, OtlpSignalType};
use crate::ffi_export::{ffi_boundary, ExportError, ExportedBatch, ExportedMetrics};
use crate::otap::grpc_service::{
    ArrowLogsService, ArrowLogsServiceServer, ArrowMetricsService, ArrowMetricsServiceServer,
    ArrowTracesService, ArrowTracesServiceServer,
};
use crate::otap::{BatchArrowRecords, BatchStatus, OtapDecoder};
use crate::server_gate::{in_host_call, CallbackGate};
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

/// Outcome the host reports for a decoded message.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpIngestStatus {
    /// Buffered successfully.
    Ok = 0,
    /// The ingest buffer is full.
    ResourceExhausted = 1,
    /// The host cannot accept this payload.
    Invalid = 2,
    /// Host-side failure.
    Internal = 3,
}

/// One decoded Arrow batch, borrowed for the message callback.
#[repr(C)]
pub struct OtlpIngestBatch {
    pub signal_type: OtlpSignalType,
    pub array: *mut FFI_ArrowArray,
    pub schema: *mut FFI_ArrowSchema,
}

/// One call per decoded message, with every batch and an optional error detail.
pub type OtlpIngestCallback = extern "C" fn(
    user_data: *mut c_void,
    input_bytes: u64,
    batches: *const OtlpIngestBatch,
    batch_count: usize,
    detail: *mut c_char,
    detail_len: usize,
) -> OtlpIngestStatus;

/// Returns non-zero when ready, or zero with an optional reason.
pub type OtlpReadyCallback =
    extern "C" fn(user_data: *mut c_void, reason: *mut c_char, reason_len: usize) -> c_int;

/// Host callbacks, authentication, Arrow profile and services for one listener.
#[repr(C)]
pub struct OtlpIngestServerConfig {
    pub user_data: *mut c_void,
    pub on_message: Option<OtlpIngestCallback>,
    pub on_ready: Option<OtlpReadyCallback>,
    pub auth_token: *const c_char,
    pub auth_token_len: usize,
    pub services: u32,
    pub profile: OtlpArrowProfile,
    pub max_message_bytes: u64,
}

struct Refusal {
    code: Code,
    message: Cow<'static, str>,
}

impl Refusal {
    const STOPPING: Self = Self::new(Code::Unavailable, "server is stopping");
    const INTERNAL: Self = Self::new(Code::Internal, "ingest error");
    const INVALID: Self = Self::new(Code::InvalidArgument, "invalid payload");

    const fn new(code: Code, message: &'static str) -> Self {
        Self {
            code,
            message: Cow::Borrowed(message),
        }
    }
}

impl From<Refusal> for tonic::Status {
    fn from(error: Refusal) -> Self {
        Self::new(error.code, error.message)
    }
}

impl From<ExportError> for Refusal {
    fn from(error: ExportError) -> Self {
        match error {
            ExportError::Unrepresentable => Self::INVALID,
            ExportError::SchemaNotExportable => Self::INTERNAL,
        }
    }
}

fn callback_text(buf: &[u8]) -> Cow<'_, str> {
    let end = buf.iter().position(|&byte| byte == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end])
}

struct Host {
    user_data: *mut c_void,
    on_message: OtlpIngestCallback,
    on_ready: Option<OtlpReadyCallback>,
    auth_token: Option<String>,
    profile: ArrowProfile,
    max_message_bytes: usize,
    cancel: CancellationToken,
    gate: CallbackGate,
}

// SAFETY: Rust never dereferences user_data. The host keeps it alive until
// stop returns, at which point the gate has closed and all callbacks have exited.
unsafe impl Send for Host {}
unsafe impl Sync for Host {}

impl Host {
    fn authorized(&self, authorization: &[u8], api_key: &[u8]) -> bool {
        let Some(token) = &self.auth_token else {
            return true;
        };
        let matches = |candidate: &[u8]| {
            token.len() == candidate.len()
                && std::hint::black_box(
                    token
                        .bytes()
                        .zip(candidate)
                        .fold(0, |diff, (a, b)| diff | (a ^ b)),
                ) == 0
        };
        let bearer_matches = authorization
            .split_at_checked(7)
            .is_some_and(|(prefix, bearer)| {
                prefix.eq_ignore_ascii_case(b"bearer ") && matches(bearer)
            });
        bearer_matches | matches(api_key)
    }

    fn readiness(&self) -> Result<(), Cow<'static, str>> {
        if self.cancel.is_cancelled() {
            return Err(Refusal::STOPPING.message);
        }
        let Some(on_ready) = self.on_ready else {
            return Ok(());
        };
        let mut reason = [0; 256];
        match self
            .gate
            .call(|| on_ready(self.user_data, reason.as_mut_ptr().cast(), reason.len()))
        {
            Some(0) if reason[0] == 0 => Err("not ready".into()),
            Some(0) => Err(callback_text(&reason).into_owned().into()),
            Some(_) => Ok(()),
            None => Err(Refusal::STOPPING.message),
        }
    }

    fn deliver(
        &self,
        input_bytes: u64,
        batches: &mut [(OtlpSignalType, ExportedBatch)],
    ) -> Result<(), Refusal> {
        let mut detail = [0; 256];
        let status = self.gate.call(|| {
            ExportedBatch::lend(batches, |batches| {
                let ptr = if batches.is_empty() {
                    std::ptr::null()
                } else {
                    batches.as_ptr()
                };
                (self.on_message)(
                    self.user_data,
                    input_bytes,
                    ptr,
                    batches.len(),
                    detail.as_mut_ptr().cast(),
                    detail.len(),
                )
            })
        });
        let mut refusal = match status {
            Some(OtlpIngestStatus::Ok) => return Ok(()),
            Some(OtlpIngestStatus::ResourceExhausted) => {
                Refusal::new(Code::ResourceExhausted, "ingest buffer full")
            }
            Some(OtlpIngestStatus::Invalid) => Refusal::INVALID,
            Some(OtlpIngestStatus::Internal) => Refusal::INTERNAL,
            None => return Err(Refusal::STOPPING),
        };
        if detail[0] != 0 {
            refusal.message = callback_text(&detail).into_owned().into();
        }
        Err(refusal)
    }

    fn deliver_one(
        &self,
        signal_type: OtlpSignalType,
        input_bytes: u64,
        batch: RecordBatch,
    ) -> Result<(), Refusal> {
        ffi_boundary(Err(Refusal::INTERNAL), || {
            let batch = ExportedBatch::new(self.profile, batch)?;
            self.deliver(input_bytes, &mut [(signal_type, batch)])
        })
    }

    fn deliver_metrics(&self, input_bytes: u64, batches: MetricBatches) -> Result<(), Refusal> {
        ffi_boundary(Err(Refusal::INTERNAL), || {
            let exported = ExportedMetrics::new(self.profile, batches)?;
            self.deliver(input_bytes, &mut exported.into_shapes().collect::<Vec<_>>())
        })
    }
}

#[derive(Clone)]
struct OtlpIngest {
    host: Arc<Host>,
}

fn invalid(signal: &str, error: crate::Error) -> Refusal {
    Refusal {
        code: Code::InvalidArgument,
        message: format!("failed to transform {signal}: {error}").into(),
    }
}

#[tonic::async_trait]
impl LogsService for OtlpIngest {
    async fn export(
        &self,
        request: tonic::Request<ExportLogsServiceRequest>,
    ) -> Result<tonic::Response<ExportLogsServiceResponse>, tonic::Status> {
        let request = request.into_inner();
        let input_bytes = request.encoded_len();
        let batch = crate::batch::transform_logs_request_observed(
            request,
            input_bytes,
            &mut crate::batch::Observation::none(crate::TransformSignal::Logs),
            &crate::options::Plan::all(&crate::schema::logs_schema_arc()),
        )
        .map_err(|e| invalid("logs", e))?;
        self.host
            .deliver_one(OtlpSignalType::Logs, input_bytes as u64, batch)?;
        Ok(tonic::Response::new(ExportLogsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl TraceService for OtlpIngest {
    async fn export(
        &self,
        request: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let request = request.into_inner();
        let input_bytes = request.encoded_len();
        let batch = crate::batch::transform_traces_request_observed(
            request,
            input_bytes,
            &mut crate::batch::Observation::none(crate::TransformSignal::Traces),
            &crate::options::Plan::all(&crate::schema::traces_schema_arc()),
        )
        .map_err(|e| invalid("traces", e))?;
        self.host
            .deliver_one(OtlpSignalType::Traces, input_bytes as u64, batch)?;
        Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpIngest {
    async fn export(
        &self,
        request: tonic::Request<ExportMetricsServiceRequest>,
    ) -> Result<tonic::Response<ExportMetricsServiceResponse>, tonic::Status> {
        let request = request.into_inner();
        let input_bytes = request.encoded_len() as u64;
        let batches = crate::batch::transform_metrics_request_observed(
            request,
            &mut crate::batch::Observation::none(crate::TransformSignal::Metrics),
            crate::batch::MetricsSelection::All,
        )
        .map_err(|e| invalid("metrics", e))?;
        self.host.deliver_metrics(input_bytes, batches)?;
        Ok(tonic::Response::new(ExportMetricsServiceResponse::default()))
    }
}

type DecodeAndDeliver =
    fn(&Host, &mut OtapDecoder, BatchArrowRecords) -> crate::Result<Result<(), Refusal>>;

fn logs_message(
    host: &Host,
    decoder: &mut OtapDecoder,
    envelope: BatchArrowRecords,
) -> crate::Result<Result<(), Refusal>> {
    let input_bytes = envelope.encoded_len();
    let batch = decoder.decode_logs_message(envelope, input_bytes)?;
    Ok(host.deliver_one(OtlpSignalType::Logs, input_bytes as u64, batch))
}

fn traces_message(
    host: &Host,
    decoder: &mut OtapDecoder,
    envelope: BatchArrowRecords,
) -> crate::Result<Result<(), Refusal>> {
    let input_bytes = envelope.encoded_len();
    let batch = decoder.decode_traces_message(envelope, input_bytes)?;
    Ok(host.deliver_one(OtlpSignalType::Traces, input_bytes as u64, batch))
}

fn metrics_message(
    host: &Host,
    decoder: &mut OtapDecoder,
    envelope: BatchArrowRecords,
) -> crate::Result<Result<(), Refusal>> {
    let input_bytes = envelope.encoded_len() as u64;
    let batches = decoder.decode_metrics_message(envelope)?;
    Ok(host.deliver_metrics(input_bytes, batches))
}

type BatchStatusStream = tokio_stream::wrappers::ReceiverStream<Result<BatchStatus, tonic::Status>>;

/// OTAP/Arrow streaming for all three signals. Each stream owns one stateful
/// decoder, so later messages can reuse dictionaries from earlier ones.
#[derive(Clone)]
struct OtapIngest {
    host: Arc<Host>,
    cancel: CancellationToken,
    tracker: TaskTracker,
}

impl OtapIngest {
    fn serve(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
        decode_and_deliver: DecodeAndDeliver,
    ) -> Result<tonic::Response<BatchStatusStream>, tonic::Status> {
        let mut input = request.into_inner();
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
                let (code, message, keep_going) =
                    match decode_and_deliver(&host, &mut decoder, envelope) {
                        Ok(Ok(())) => (Code::Ok, String::new(), true),
                        Ok(Err(refusal)) => (
                            refusal.code,
                            refusal.message.into_owned(),
                            refusal.code != Code::Unavailable,
                        ),
                        Err(error) => (
                            Code::InvalidArgument,
                            format!("OTAP decode error: {error}"),
                            false,
                        ),
                    };
                let ack = BatchStatus {
                    batch_id,
                    status_code: code.into(),
                    status_message: message,
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

/// Opaque, heap-owned running ingest server. Created by
/// `otlp_ingest_server_start`, stopped by `otlp_ingest_server_stop` and freed by
/// `otlp_ingest_server_free`.
pub struct OtlpIngestServer {
    host: Arc<Host>,
    running: Option<Running>,
    tracker: TaskTracker,
}

struct Running {
    runtime: tokio::runtime::Runtime,
    join: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl OtlpIngestServer {
    fn shutdown(&mut self, deadline: Duration) {
        let Some(Running { runtime, join }) = self.running.take() else {
            return;
        };
        let started = Instant::now();
        self.host.cancel.cancel();
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

impl Drop for OtlpIngestServer {
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

/// OTLP/gRPC unary Export services.
pub const OTLP_INGEST_SERVE_OTLP_GRPC: u32 = 1 << 0;
/// OTAP/Arrow bidirectional streaming services.
pub const OTLP_INGEST_SERVE_OTAP_GRPC: u32 = 1 << 1;
/// OTLP/HTTP ingest and health endpoints.
pub const OTLP_INGEST_SERVE_OTLP_HTTP: u32 = 1 << 2;

const DEFAULT_MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Start the ingest server on one listener ("host:port"). A zero service
/// mask enables OTLP/gRPC, OTAP/gRPC and OTLP/HTTP. A zero size limit uses 4 MiB.
///
/// On failure, returns null and writes a truncated, NUL-terminated error.
///
/// # Safety
///
/// - `bind_addr` must be valid for `bind_addr_len` bytes of UTF-8.
/// - `config` must point to a valid configuration. Its token bytes, when
///   present, must remain valid until start returns.
/// - The callbacks and `user_data` must remain valid until the server stops.
/// - `err_buf` may be null, or must be writable for `err_buf_len` bytes.
#[no_mangle]
pub unsafe extern "C" fn otlp_ingest_server_start(
    bind_addr: *const c_char,
    bind_addr_len: usize,
    config: *const OtlpIngestServerConfig,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut OtlpIngestServer {
    let started = ffi_boundary(Err("panic while starting ingest server".into()), || {
        let config = config.as_ref().ok_or("config is null")?;
        let on_message = config.on_message.ok_or("on_message callback is null")?;
        if bind_addr.is_null() {
            return Err("bind_addr is null".into());
        }
        let addr = std::str::from_utf8(std::slice::from_raw_parts(bind_addr.cast(), bind_addr_len))
            .map_err(|_| "bind_addr is not valid UTF-8")?;
        let auth_token = if config.auth_token.is_null() {
            None
        } else {
            if config.auth_token_len == 0 {
                return Err("auth_token is empty".into());
            }
            let token = std::slice::from_raw_parts(config.auth_token.cast(), config.auth_token_len);
            Some(
                std::str::from_utf8(token)
                    .map_err(|_| "auth_token is not valid UTF-8")?
                    .to_owned(),
            )
        };
        let host = Host {
            user_data: config.user_data,
            on_message,
            on_ready: config.on_ready,
            auth_token,
            profile: config.profile.into(),
            max_message_bytes: match config.max_message_bytes {
                0 => DEFAULT_MAX_MESSAGE_BYTES,
                n => usize::try_from(n).unwrap_or(usize::MAX),
            },
            cancel: CancellationToken::new(),
            gate: CallbackGate::default(),
        };
        start_server(addr, host, config.services)
    });
    match started {
        Ok(server) => Box::into_raw(Box::new(server)),
        Err(msg) => {
            write_err(err_buf, err_buf_len, &msg);
            std::ptr::null_mut()
        }
    }
}

fn start_server(addr: &str, host: Host, services: u32) -> Result<OtlpIngestServer, String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get().clamp(1, 4))
                .unwrap_or(2),
        )
        .thread_name("otlp-ingest")
        .enable_all()
        .build()
        .map_err(|e| format!("failed to start runtime: {e}"))?;
    let listener =
        std::net::TcpListener::bind(addr).map_err(|e| format!("failed to bind {addr}: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to configure listener: {e}"))?;

    let host = Arc::new(host);
    let cancel = host.cancel.clone();
    let tracker = TaskTracker::new();
    let otlp = OtlpIngest { host: host.clone() };
    let otap = OtapIngest {
        host: host.clone(),
        cancel: cancel.clone(),
        tracker: tracker.clone(),
    };
    let enabled = |bit| services == 0 || services & bit != 0;
    let serve_http = enabled(OTLP_INGEST_SERVE_OTLP_HTTP);

    // An interceptor runs before tonic reads or decodes the request body.
    let auth_host = host.clone();
    #[allow(clippy::result_large_err)]
    let authorize = move |request: tonic::Request<()>| {
        let get = |key: &'static str| {
            request
                .metadata()
                .get(key)
                .map_or(&[][..], |value| value.as_encoded_bytes())
        };
        if auth_host.authorized(get("authorization"), get("x-api-key")) {
            Ok(request)
        } else {
            Err(tonic::Status::unauthenticated("invalid token"))
        }
    };

    let mut grpc = Routes::default().into_axum_router();
    // Route exact methods so the interceptor runs only on implemented calls.
    // Arrow producers default to grpc-encoding: zstd.
    macro_rules! add_service {
        ($server:ident, $ingest:ty, $impl:expr, $method:literal) => {
            grpc = grpc.route_service(
                &format!("/{}/{}", <$server<$ingest>>::NAME, $method),
                InterceptedService::new(
                    $server::new($impl)
                        .max_decoding_message_size(host.max_message_bytes)
                        .accept_compressed(CompressionEncoding::Zstd),
                    authorize.clone(),
                ),
            );
        };
    }
    if enabled(OTLP_INGEST_SERVE_OTLP_GRPC) {
        add_service!(LogsServiceServer, OtlpIngest, otlp.clone(), "Export");
        add_service!(TraceServiceServer, OtlpIngest, otlp.clone(), "Export");
        add_service!(MetricsServiceServer, OtlpIngest, otlp, "Export");
    }
    if enabled(OTLP_INGEST_SERVE_OTAP_GRPC) {
        add_service!(
            ArrowLogsServiceServer,
            OtapIngest,
            otap.clone(),
            "ArrowLogs"
        );
        add_service!(
            ArrowTracesServiceServer,
            OtapIngest,
            otap.clone(),
            "ArrowTraces"
        );
        add_service!(ArrowMetricsServiceServer, OtapIngest, otap, "ArrowMetrics");
    }
    let routes = if serve_http {
        http::routes(grpc, host.clone())
    } else {
        grpc.into()
    };
    let router = tonic::transport::Server::builder()
        .accept_http1(serve_http)
        .add_routes(routes);

    let join = {
        let _guard = runtime.enter();
        let listener = tokio::net::TcpListener::from_std(listener)
            .map_err(|e| format!("failed to register listener: {e}"))?;
        let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
        runtime
            .spawn(router.serve_with_incoming_shutdown(incoming, cancel.clone().cancelled_owned()))
    };

    Ok(OtlpIngestServer {
        host,
        running: Some(Running { runtime, join }),
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
/// `server` must be a handle from `otlp_ingest_server_start` (not yet freed), or
/// null (a no-op).
#[no_mangle]
pub unsafe extern "C" fn otlp_ingest_server_stop(
    server: *mut OtlpIngestServer,
    drain_deadline_ms: u64,
) {
    ffi_boundary((), || {
        if !in_host_call() {
            if let Some(server) = server.as_mut() {
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
/// `server` must be a handle from `otlp_ingest_server_start` that has not been
/// freed, or null (a no-op).
#[no_mangle]
pub unsafe extern "C" fn otlp_ingest_server_free(server: *mut OtlpIngestServer) {
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
