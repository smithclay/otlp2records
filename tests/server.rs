#![cfg(feature = "server")]

use std::ffi::{c_char, c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use otlp2records::ffi::{OtlpArrowProfile, OtlpSignalType};
use otlp2records::server_ffi::{
    otlp_grpc_server_free, otlp_grpc_server_start_with_profile, otlp_grpc_server_stop,
    OtlpGrpcServer, OtlpIngestStatus, OTLP_GRPC_SERVICE_OTAP_ARROW, OTLP_GRPC_SERVICE_OTLP_UNARY,
};
use otlp2records::otap::{BatchArrowRecords, BatchStatus, StatusCode};
use prost::Message;
use tonic::codec::CompressionEncoding;
use tonic::Code;

const LOGS_PB: &[u8] = include_bytes!("../testdata/logs_large.pb");
const OTAP_LOGS_INITIAL: &[u8] = include_bytes!("fixtures/otap/logs-initial.bar");
const OTAP_LOGS_REUSE: &[u8] = include_bytes!("fixtures/otap/logs-reuse.bar");
const ARROW_LOGS: &str = "/opentelemetry.proto.experimental.arrow.v1.ArrowLogsService/ArrowLogs";

#[derive(Default)]
struct Host {
    batches: Mutex<Vec<Delivered>>,
    auth_calls: AtomicUsize,
    entered: AtomicUsize,
    returned: AtomicUsize,
    callback_sleep_ms: AtomicU64,
    reply: Option<OtlpIngestStatus>,
    token: Option<String>,
    stop_from_callback: AtomicPtr<OtlpGrpcServer>,
    free_from_callback: AtomicPtr<OtlpGrpcServer>,
}

#[derive(Debug, Clone)]
struct Delivered {
    signal: OtlpSignalType,
    stream_id: u64,
    batch_id: i64,
    input_bytes: u64,
    rows: i64,
    formats: Vec<(String, String)>,
}

impl Host {
    fn in_callback(&self) -> bool {
        self.entered.load(Ordering::SeqCst) != self.returned.load(Ordering::SeqCst)
    }

    fn delivered(&self) -> Vec<Delivered> {
        self.batches.lock().unwrap().clone()
    }
}

extern "C" fn on_batch(
    user_data: *mut c_void,
    signal: OtlpSignalType,
    stream_id: u64,
    batch_id: i64,
    input_bytes: u64,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> OtlpIngestStatus {
    let (host, array, schema) = unsafe { (&*(user_data as *const Host), &*array, &*schema) };
    host.entered.fetch_add(1, Ordering::SeqCst);
    unsafe {
        otlp_grpc_server_stop(host.stop_from_callback.load(Ordering::SeqCst), 1_000);
        otlp_grpc_server_free(
            host.free_from_callback
                .swap(std::ptr::null_mut(), Ordering::SeqCst),
        );
    }
    std::thread::sleep(Duration::from_millis(
        host.callback_sleep_ms.load(Ordering::SeqCst),
    ));
    host.batches.lock().unwrap().push(Delivered {
        signal,
        stream_id,
        batch_id,
        input_bytes,
        rows: array.len() as i64,
        formats: schema
            .children()
            .map(|c| (c.name().unwrap_or_default().into(), c.format().into()))
            .collect(),
    });
    host.returned.fetch_add(1, Ordering::SeqCst);
    host.reply.unwrap_or(OtlpIngestStatus::Ok)
}

extern "C" fn on_auth(user_data: *mut c_void, token: *const c_char, len: usize) -> c_int {
    let (host, token) = unsafe {
        (
            &*(user_data as *const Host),
            std::slice::from_raw_parts(token as *const u8, len),
        )
    };
    host.auth_calls.fetch_add(1, Ordering::SeqCst);
    c_int::from(Some(token) == host.token.as_deref().map(str::as_bytes))
}

struct Server {
    handle: *mut OtlpGrpcServer,
    addr: String,
    host: Box<Host>,
}

struct Options {
    services: u32,
    max_decoding: u64,
    profile: OtlpArrowProfile,
    auth: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            services: 0,
            max_decoding: 0,
            profile: OtlpArrowProfile::Canonical,
            auth: false,
        }
    }
}

impl Server {
    fn start(options: Options, host: Host) -> Server {
        let host = Box::new(host);
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr = format!("127.0.0.1:{port}");
        let mut err = [0 as c_char; 256];
        let handle = unsafe {
            otlp_grpc_server_start_with_profile(
                addr.as_ptr() as *const c_char,
                addr.len(),
                Some(on_batch),
                options.auth.then_some(on_auth as _),
                &*host as *const Host as *mut c_void,
                options.services,
                options.max_decoding,
                options.profile,
                err.as_mut_ptr(),
                err.len(),
            )
        };
        assert!(!handle.is_null(), "start failed: {:?}", unsafe {
            std::ffi::CStr::from_ptr(err.as_ptr())
        });
        Server { handle, addr, host }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn stop(&self, deadline_ms: u64) {
        unsafe { otlp_grpc_server_stop(self.handle, deadline_ms) };
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe {
            otlp_grpc_server_stop(self.handle, 1_000);
            otlp_grpc_server_free(self.handle);
        }
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn logs_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest::decode(LOGS_PB).unwrap()
}

fn span_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0xab; 16],
                    span_id: vec![0xcd; 8],
                    name: "span".into(),
                    start_time_unix_nano: 1_700_000_000_000_000_000,
                    end_time_unix_nano: 1_700_000_000_000_001_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

async fn export_logs(
    url: String,
    token: Option<&str>,
    compression: Option<CompressionEncoding>,
) -> Result<(), tonic::Status> {
    let mut client = LogsServiceClient::connect(url)
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    if let Some(encoding) = compression {
        client = client.send_compressed(encoding);
    }
    let mut request = tonic::Request::new(logs_request());
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", token.parse().unwrap());
    }
    client.export(request).await.map(|_| ())
}

fn envelope(batch_id: i64, bytes: &[u8]) -> BatchArrowRecords {
    let mut envelope = BatchArrowRecords::decode(bytes).unwrap();
    envelope.batch_id = batch_id;
    envelope
}

async fn arrow_logs(
    url: String,
    token: Option<&str>,
    compression: Option<CompressionEncoding>,
    messages: Vec<BatchArrowRecords>,
) -> Result<Vec<BatchStatus>, tonic::Status> {
    let channel = tonic::transport::Endpoint::from_shared(url)
        .unwrap()
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut grpc = tonic::client::Grpc::new(channel);
    if let Some(encoding) = compression {
        grpc = grpc.send_compressed(encoding);
    }
    grpc.ready()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut request = tonic::Request::new(tokio_stream::iter(messages));
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", token.parse().unwrap());
    }
    let codec = tonic_prost::ProstCodec::<BatchArrowRecords, BatchStatus>::default();
    let mut acks = grpc
        .streaming(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(ARROW_LOGS),
            codec,
        )
        .await?
        .into_inner();
    let mut out = Vec::new();
    while let Some(ack) = acks.message().await? {
        out.push(ack);
    }
    Ok(out)
}

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

#[test]
fn stop_does_not_return_while_a_host_callback_is_still_running() {
    let host = Host::default();
    host.callback_sleep_ms.store(400, Ordering::SeqCst);
    let server = Server::start(Options::default(), host);
    let rt = runtime();
    let url = server.url();
    let export = rt.spawn(async move { export_logs(url, None, None).await });

    wait_until("the callback to start", || {
        server.host.entered.load(Ordering::SeqCst) == 1
    });
    server.stop(10);

    assert!(
        !server.host.in_callback(),
        "otlp_grpc_server_stop returned while the host callback was still running; \
         the host frees user_data right after stop returns"
    );
    assert_eq!(server.host.returned.load(Ordering::SeqCst), 1);
    let _ = rt.block_on(export);
}

#[test]
fn no_host_callback_starts_after_stop_returns() {
    let server = Server::start(Options::default(), Host::default());
    let rt = runtime();
    let url = server.url();
    let senders: Vec<_> = (0..4)
        .map(|_| {
            let url = url.clone();
            rt.spawn(async move {
                loop {
                    let unary = export_logs(url.clone(), None, None);
                    let stream = arrow_logs(
                        url.clone(),
                        None,
                        None,
                        vec![envelope(1, OTAP_LOGS_INITIAL), envelope(2, OTAP_LOGS_REUSE)],
                    );
                    let (unary, stream) = tokio::join!(unary, stream);
                    if unary.is_err() && stream.is_err() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            })
        })
        .collect();

    wait_until("traffic", || {
        server.host.entered.load(Ordering::SeqCst) > 20
    });
    server.stop(50);
    let after_stop = server.host.entered.load(Ordering::SeqCst);
    assert!(!server.host.in_callback());
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(server.host.entered.load(Ordering::SeqCst), after_stop);
    for sender in senders {
        sender.abort();
    }
}

#[test]
fn stop_called_from_inside_a_callback_returns_instead_of_deadlocking() {
    let server = Server::start(Options::default(), Host::default());
    server
        .host
        .stop_from_callback
        .store(server.handle, Ordering::SeqCst);
    let rt = runtime();
    let (tx, rx) = std::sync::mpsc::channel();
    let url = server.url();
    rt.spawn(async move { tx.send(export_logs(url, None, None).await) });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("an export whose callback calls stop never completed")
        .expect("export");
    server
        .host
        .stop_from_callback
        .store(std::ptr::null_mut(), Ordering::SeqCst);
    server.stop(100);
}

#[test]
fn free_called_from_inside_a_callback_stops_the_server() {
    let mut server = Server::start(Options::default(), Host::default());
    server
        .host
        .free_from_callback
        .store(server.handle, Ordering::SeqCst);
    server.handle = std::ptr::null_mut();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if info.to_string().contains("Cannot drop a runtime") {
            RUNTIME_DROPPED_IN_CALLBACK.store(true, Ordering::SeqCst);
        }
        previous(info);
    }));
    let rt = runtime();
    let url = server.url();
    let _ = rt.block_on(export_logs(url.clone(), None, None));
    wait_until("the freed server stops accepting", || {
        rt.block_on(export_logs(url.clone(), None, None)).is_err()
    });
    assert!(!RUNTIME_DROPPED_IN_CALLBACK.load(Ordering::SeqCst));
}

static RUNTIME_DROPPED_IN_CALLBACK: AtomicBool = AtomicBool::new(false);

#[test]
fn requests_without_the_token_never_reach_the_batch_callback() {
    let host = Host {
        token: Some("Bearer s3cret".into()),
        ..Host::default()
    };
    let server = Server::start(
        Options {
            auth: true,
            ..Options::default()
        },
        host,
    );
    let rt = runtime();
    for token in [None, Some("Bearer wrong"), Some("")] {
        let unary = rt
            .block_on(export_logs(server.url(), token, None))
            .unwrap_err();
        assert_eq!(unary.code(), Code::Unauthenticated, "{token:?}");
        let stream = rt
            .block_on(arrow_logs(
                server.url(),
                token,
                None,
                vec![envelope(1, OTAP_LOGS_INITIAL)],
            ))
            .unwrap_err();
        assert_eq!(stream.code(), Code::Unauthenticated, "{token:?}");
    }
    assert!(server.host.auth_calls.load(Ordering::SeqCst) >= 6);
    assert!(server.host.delivered().is_empty());

    rt.block_on(export_logs(server.url(), Some("Bearer s3cret"), None))
        .unwrap();
    let acks = rt
        .block_on(arrow_logs(
            server.url(),
            Some("Bearer s3cret"),
            None,
            vec![envelope(7, OTAP_LOGS_INITIAL)],
        ))
        .unwrap();
    assert_eq!(acks.len(), 1);
    assert_eq!(acks[0].status_code, StatusCode::Ok as i32);
    assert_eq!(server.host.delivered().len(), 2);
}

#[test]
fn messages_over_the_decoding_cap_are_rejected_before_the_callback() {
    let server = Server::start(
        Options {
            max_decoding: 1024,
            ..Options::default()
        },
        Host::default(),
    );
    let rt = runtime();
    assert!(LOGS_PB.len() > 1024 && OTAP_LOGS_INITIAL.len() > 1024);
    let unary = rt
        .block_on(export_logs(server.url(), None, None))
        .unwrap_err();
    assert_eq!(unary.code(), Code::OutOfRange);
    let stream = rt
        .block_on(arrow_logs(
            server.url(),
            None,
            None,
            vec![envelope(1, OTAP_LOGS_INITIAL)],
        ))
        .unwrap_err();
    assert_eq!(stream.code(), Code::OutOfRange);
    assert!(server.host.delivered().is_empty());
}

#[test]
fn zstd_compressed_requests_are_accepted_on_both_families() {
    let server = Server::start(Options::default(), Host::default());
    let rt = runtime();
    rt.block_on(export_logs(
        server.url(),
        None,
        Some(CompressionEncoding::Zstd),
    ))
    .unwrap();
    let acks = rt
        .block_on(arrow_logs(
            server.url(),
            None,
            Some(CompressionEncoding::Zstd),
            vec![envelope(3, OTAP_LOGS_INITIAL)],
        ))
        .unwrap();
    assert_eq!(acks[0].status_code, StatusCode::Ok as i32);
    assert_eq!(server.host.delivered().len(), 2);
}

#[test]
fn service_flags_register_disjoint_service_families() {
    let rt = runtime();
    let otlp_only = Server::start(
        Options {
            services: OTLP_GRPC_SERVICE_OTLP_UNARY,
            ..Options::default()
        },
        Host::default(),
    );
    rt.block_on(export_logs(otlp_only.url(), None, None))
        .unwrap();
    let arrow = rt
        .block_on(arrow_logs(otlp_only.url(), None, None, vec![]))
        .unwrap_err();
    assert_eq!(arrow.code(), Code::Unimplemented);

    let otap_only = Server::start(
        Options {
            services: OTLP_GRPC_SERVICE_OTAP_ARROW,
            ..Options::default()
        },
        Host::default(),
    );
    let unary = rt
        .block_on(export_logs(otap_only.url(), None, None))
        .unwrap_err();
    assert_eq!(unary.code(), Code::Unimplemented);
    rt.block_on(arrow_logs(
        otap_only.url(),
        None,
        None,
        vec![envelope(1, OTAP_LOGS_INITIAL)],
    ))
    .unwrap();
}

#[test]
fn otap_stream_acks_each_message_reuses_state_and_closes_on_a_decode_error() {
    let server = Server::start(Options::default(), Host::default());
    let rt = runtime();
    let acks = rt
        .block_on(arrow_logs(
            server.url(),
            None,
            None,
            vec![
                envelope(10, OTAP_LOGS_INITIAL),
                envelope(11, OTAP_LOGS_REUSE),
                BatchArrowRecords {
                    batch_id: 12,
                    ..Default::default()
                },
                envelope(13, OTAP_LOGS_REUSE),
            ],
        ))
        .unwrap();
    let codes: Vec<_> = acks.iter().map(|a| (a.batch_id, a.status_code)).collect();
    assert_eq!(
        codes,
        [
            (10, StatusCode::Ok as i32),
            (11, StatusCode::Ok as i32),
            (12, StatusCode::InvalidArgument as i32),
        ]
    );
    let delivered = server.host.delivered();
    assert_eq!(delivered.len(), 2);
    assert!(delivered.iter().all(|d| d.signal == OtlpSignalType::Logs));
    assert!(delivered.iter().all(|d| d.rows > 0 && d.input_bytes > 0));
    assert_eq!(delivered[0].stream_id, delivered[1].stream_id);
    assert_ne!(delivered[0].stream_id, 0);
    assert_eq!((delivered[0].batch_id, delivered[1].batch_id), (10, 11));
}

#[test]
fn host_backpressure_nacks_without_closing_the_stream() {
    let host = Host {
        reply: Some(OtlpIngestStatus::ResourceExhausted),
        ..Host::default()
    };
    let server = Server::start(Options::default(), host);
    let rt = runtime();
    let unary = rt
        .block_on(export_logs(server.url(), None, None))
        .unwrap_err();
    assert_eq!(unary.code(), Code::ResourceExhausted);
    let acks = rt
        .block_on(arrow_logs(
            server.url(),
            None,
            None,
            vec![envelope(1, OTAP_LOGS_INITIAL), envelope(2, OTAP_LOGS_REUSE)],
        ))
        .unwrap();
    assert_eq!(acks.len(), 2);
    assert!(acks
        .iter()
        .all(|a| a.status_code == StatusCode::ResourceExhausted as i32));
}

fn export_one_span(profile: OtlpArrowProfile) -> Vec<(String, String)> {
    let server = Server::start(
        Options {
            profile,
            ..Options::default()
        },
        Host::default(),
    );
    runtime().block_on(async {
        TraceServiceClient::connect(server.url())
            .await
            .unwrap()
            .export(span_request())
            .await
            .unwrap();
    });
    let mut delivered = server.host.delivered();
    assert_eq!(delivered.len(), 1);
    delivered.pop().unwrap().formats
}

fn format_of<'a>(formats: &'a [(String, String)], column: &str) -> &'a str {
    &formats.iter().find(|(name, _)| name == column).unwrap().1
}

#[test]
fn delivered_batches_use_the_server_profile() {
    let canonical = export_one_span(OtlpArrowProfile::Canonical);
    assert_eq!(format_of(&canonical, "trace_id"), "w:16");
    assert_eq!(format_of(&canonical, "duration_time_unix_nano"), "tDn");
    let duckdb = export_one_span(OtlpArrowProfile::DuckDb);
    assert_eq!(format_of(&duckdb, "trace_id"), "u");
    assert_eq!(format_of(&duckdb, "duration_time_unix_nano"), "l");
    assert_eq!(format_of(&duckdb, "flags"), "i");
    assert_eq!(format_of(&duckdb, "start_time_unix_nano"), "tsn:");
}
