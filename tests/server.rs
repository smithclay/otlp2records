#![cfg(feature = "server")]

use std::ffi::{c_char, c_int, c_void};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_client::MetricsServiceClient, ExportMetricsServiceRequest,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use otlp2records::ffi::{OtlpArrowProfile, OtlpSignalType};
use otlp2records::otap::{BatchArrowRecords, BatchStatus, StatusCode};
use otlp2records::server_ffi::{
    otlp_ingest_server_free, otlp_ingest_server_start, otlp_ingest_server_stop, OtlpIngestBatch,
    OtlpIngestServer, OtlpIngestServerConfig, OtlpIngestStatus, OTLP_INGEST_SERVE_OTAP_GRPC,
    OTLP_INGEST_SERVE_OTLP_GRPC, OTLP_INGEST_SERVE_OTLP_HTTP,
};
use prost::Message;
use tonic::codec::CompressionEncoding;
use tonic::Code;

const LOGS_PB: &[u8] = include_bytes!("../testdata/logs_large.pb");
const OTAP_LOGS_INITIAL: &[u8] = include_bytes!("fixtures/otap/logs-initial.bar");
const OTAP_LOGS_REUSE: &[u8] = include_bytes!("fixtures/otap/logs-reuse.bar");
const ARROW_LOGS: &str = "/opentelemetry.proto.experimental.arrow.v1.ArrowLogsService/ArrowLogs";

#[derive(Default)]
struct Host {
    messages: Mutex<Vec<Delivered>>,
    readiness_calls: AtomicUsize,
    entered: AtomicUsize,
    returned: AtomicUsize,
    callback_sleep_ms: AtomicU64,
    reply: Option<OtlpIngestStatus>,
    detail: &'static [u8],
    readiness: Option<(bool, &'static [u8])>,
    stop_from_callback: AtomicPtr<OtlpIngestServer>,
    free_from_callback: AtomicPtr<OtlpIngestServer>,
}

#[derive(Debug, Clone)]
struct Delivered {
    input_bytes: u64,
    batches: Vec<DeliveredBatch>,
}

#[derive(Debug, Clone)]
struct DeliveredBatch {
    signal: OtlpSignalType,
    rows: usize,
    formats: Vec<(String, String)>,
}

impl Host {
    fn in_callback(&self) -> bool {
        self.entered.load(Ordering::SeqCst) != self.returned.load(Ordering::SeqCst)
    }

    fn delivered(&self) -> Vec<Delivered> {
        self.messages.lock().unwrap().clone()
    }
}

extern "C" fn on_message(
    user_data: *mut c_void,
    input_bytes: u64,
    batches: *const OtlpIngestBatch,
    batch_count: usize,
    detail: *mut c_char,
    detail_len: usize,
) -> OtlpIngestStatus {
    let host = unsafe { &*user_data.cast::<Host>() };
    host.entered.fetch_add(1, Ordering::SeqCst);
    unsafe {
        otlp_ingest_server_stop(host.stop_from_callback.load(Ordering::SeqCst), 1_000);
        otlp_ingest_server_free(
            host.free_from_callback
                .swap(std::ptr::null_mut(), Ordering::SeqCst),
        );
    }
    std::thread::sleep(Duration::from_millis(
        host.callback_sleep_ms.load(Ordering::SeqCst),
    ));
    let batches = if batch_count == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(batches, batch_count) }
    };
    host.messages.lock().unwrap().push(Delivered {
        input_bytes,
        batches: batches
            .iter()
            .map(|batch| {
                let (array, schema) = unsafe { (&*batch.array, &*batch.schema) };
                DeliveredBatch {
                    signal: batch.signal_type,
                    rows: array.len(),
                    formats: schema
                        .children()
                        .map(|c| (c.name().unwrap_or_default().into(), c.format().into()))
                        .collect(),
                }
            })
            .collect(),
    });
    write_callback_text(detail, detail_len, host.detail);
    host.returned.fetch_add(1, Ordering::SeqCst);
    host.reply.unwrap_or(OtlpIngestStatus::Ok)
}

fn write_callback_text(output: *mut c_char, output_len: usize, text: &[u8]) {
    let output = unsafe { std::slice::from_raw_parts_mut(output.cast::<u8>(), output_len) };
    assert!(output.iter().all(|&b| b == 0));
    let len = text.len().min(output_len);
    output[..len].copy_from_slice(&text[..len]);
}

extern "C" fn on_ready(user_data: *mut c_void, reason: *mut c_char, reason_len: usize) -> c_int {
    let host = unsafe { &*user_data.cast::<Host>() };
    host.readiness_calls.fetch_add(1, Ordering::SeqCst);
    let (ready, text) = host.readiness.unwrap();
    write_callback_text(reason, reason_len, text);
    c_int::from(ready)
}

struct Server {
    handle: *mut OtlpIngestServer,
    addr: String,
    host: Box<Host>,
}

struct Options<'a> {
    services: u32,
    max_decoding: u64,
    profile: OtlpArrowProfile,
    token: Option<&'a [u8]>,
}

impl Default for Options<'_> {
    fn default() -> Self {
        Self {
            services: 0,
            max_decoding: 0,
            profile: OtlpArrowProfile::Canonical,
            token: None,
        }
    }
}

impl Server {
    fn start(options: Options<'_>, host: Host) -> Server {
        let host = Box::new(host);
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let addr = format!("127.0.0.1:{port}");
        let mut err = [0 as c_char; 256];
        let handle = start(&addr, &configuration(&host, options), &mut err);
        assert!(!handle.is_null(), "start failed: {:?}", unsafe {
            std::ffi::CStr::from_ptr(err.as_ptr())
        });
        Server { handle, addr, host }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn stop(&self, deadline_ms: u64) {
        unsafe { otlp_ingest_server_stop(self.handle, deadline_ms) };
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        unsafe {
            otlp_ingest_server_stop(self.handle, 1_000);
            otlp_ingest_server_free(self.handle);
        }
    }
}

fn configuration(host: &Host, options: Options<'_>) -> OtlpIngestServerConfig {
    OtlpIngestServerConfig {
        user_data: (host as *const Host).cast_mut().cast(),
        on_message: Some(on_message),
        on_ready: host.readiness.map(|_| on_ready as _),
        auth_token: options
            .token
            .map_or(std::ptr::null(), |token| token.as_ptr().cast()),
        auth_token_len: options.token.map_or(0, <[u8]>::len),
        services: options.services,
        profile: options.profile,
        max_message_bytes: options.max_decoding,
    }
}

fn start(addr: &str, config: &OtlpIngestServerConfig, err: &mut [c_char]) -> *mut OtlpIngestServer {
    unsafe {
        otlp_ingest_server_start(
            addr.as_ptr().cast(),
            addr.len(),
            config,
            err.as_mut_ptr(),
            err.len(),
        )
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
    headers: &[(&'static str, &str)],
    compression: Option<CompressionEncoding>,
) -> Result<(), tonic::Status> {
    let mut client = LogsServiceClient::connect(url)
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    if let Some(encoding) = compression {
        client = client.send_compressed(encoding);
    }
    let mut request = tonic::Request::new(logs_request());
    for &(key, value) in headers {
        request.metadata_mut().insert(key, value.parse().unwrap());
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
    headers: &[(&'static str, &str)],
    compression: Option<CompressionEncoding>,
    messages: Vec<BatchArrowRecords>,
) -> Result<Vec<BatchStatus>, tonic::Status> {
    arrow_stream(url, ARROW_LOGS, headers, compression, messages).await
}

async fn arrow_stream(
    url: String,
    path: &'static str,
    headers: &[(&'static str, &str)],
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
    for &(key, value) in headers {
        request.metadata_mut().insert(key, value.parse().unwrap());
    }
    let codec = tonic_prost::ProstCodec::<BatchArrowRecords, BatchStatus>::default();
    let mut acks = grpc
        .streaming(
            request,
            tonic::codegen::http::uri::PathAndQuery::from_static(path),
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
    let export = rt.spawn(async move { export_logs(url, &[], None).await });

    wait_until("the callback to start", || {
        server.host.entered.load(Ordering::SeqCst) == 1
    });
    server.stop(10);

    assert!(
        !server.host.in_callback(),
        "otlp_ingest_server_stop returned while the host callback was still running; \
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
                    let unary = export_logs(url.clone(), &[], None);
                    let stream = arrow_logs(
                        url.clone(),
                        &[],
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
    rt.spawn(async move { tx.send(export_logs(url, &[], None).await) });
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
    let _ = rt.block_on(export_logs(url.clone(), &[], None));
    wait_until("the freed server stops accepting", || {
        rt.block_on(export_logs(url.clone(), &[], None)).is_err()
    });
    assert!(!RUNTIME_DROPPED_IN_CALLBACK.load(Ordering::SeqCst));
}

static RUNTIME_DROPPED_IN_CALLBACK: AtomicBool = AtomicBool::new(false);

const REJECTED_HEADERS: &[&[(&str, &str)]] = &[
    &[],
    &[("authorization", "Bearer wrong")],
    &[("authorization", "")],
    &[("x-api-key", "wrong")],
];

const ACCEPTED_HEADERS: &[&[(&str, &str)]] = &[
    &[("authorization", "Bearer s3cret")],
    &[("authorization", "BEARER s3cret")],
    &[("x-api-key", "s3cret")],
    &[("authorization", "invalid"), ("x-api-key", "s3cret")],
];

#[test]
fn grpc_authentication_uses_bearer_or_api_key() {
    let server = Server::start(
        Options {
            token: Some(b"s3cret"),
            ..Options::default()
        },
        Host::default(),
    );
    let rt = runtime();
    for headers in REJECTED_HEADERS {
        let unary = rt
            .block_on(export_logs(server.url(), headers, None))
            .unwrap_err();
        assert_eq!(unary.code(), Code::Unauthenticated, "{headers:?}");
        assert_eq!(unary.message(), "invalid token");
        let stream = rt
            .block_on(arrow_logs(
                server.url(),
                headers,
                None,
                vec![envelope(1, OTAP_LOGS_INITIAL)],
            ))
            .unwrap_err();
        assert_eq!(stream.code(), Code::Unauthenticated, "{headers:?}");
    }
    assert!(server.host.delivered().is_empty());

    for headers in ACCEPTED_HEADERS {
        rt.block_on(export_logs(server.url(), headers, None))
            .unwrap();
        let acks = rt
            .block_on(arrow_logs(
                server.url(),
                headers,
                None,
                vec![envelope(7, OTAP_LOGS_INITIAL)],
            ))
            .unwrap();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].status_code, StatusCode::Ok as i32);
    }
    assert_eq!(server.host.delivered().len(), 2 * ACCEPTED_HEADERS.len());
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
        .block_on(export_logs(server.url(), &[], None))
        .unwrap_err();
    assert_eq!(unary.code(), Code::OutOfRange);
    let stream = rt
        .block_on(arrow_logs(
            server.url(),
            &[],
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
        &[],
        Some(CompressionEncoding::Zstd),
    ))
    .unwrap();
    let acks = rt
        .block_on(arrow_logs(
            server.url(),
            &[],
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
            services: OTLP_INGEST_SERVE_OTLP_GRPC,
            ..Options::default()
        },
        Host::default(),
    );
    rt.block_on(export_logs(otlp_only.url(), &[], None))
        .unwrap();
    let arrow = rt
        .block_on(arrow_logs(otlp_only.url(), &[], None, vec![]))
        .unwrap_err();
    assert_eq!(arrow.code(), Code::Unimplemented);

    let otap_only = Server::start(
        Options {
            services: OTLP_INGEST_SERVE_OTAP_GRPC,
            ..Options::default()
        },
        Host::default(),
    );
    let unary = rt
        .block_on(export_logs(otap_only.url(), &[], None))
        .unwrap_err();
    assert_eq!(unary.code(), Code::Unimplemented);
    rt.block_on(arrow_logs(
        otap_only.url(),
        &[],
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
            &[],
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
    for (message, (id, fixture)) in delivered
        .iter()
        .zip([(10, OTAP_LOGS_INITIAL), (11, OTAP_LOGS_REUSE)])
    {
        assert_eq!(
            message.input_bytes,
            envelope(id, fixture).encoded_len() as u64
        );
        assert_eq!(message.batches.len(), 1);
        assert_eq!(message.batches[0].signal, OtlpSignalType::Logs);
        assert!(message.batches[0].rows > 0);
    }
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
        .block_on(export_logs(server.url(), &[], None))
        .unwrap_err();
    assert_eq!(unary.code(), Code::ResourceExhausted);
    assert_eq!(unary.message(), "ingest buffer full");
    assert_http(
        &server,
        "POST /v1/logs",
        &[("Content-Type", "application/json")],
        LOG_JSON,
        503,
        r#"{"error":"request_failed","message":"ingest buffer full"}"#,
    );
    let acks = rt
        .block_on(arrow_logs(
            server.url(),
            &[],
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
    delivered.pop().unwrap().batches.pop().unwrap().formats
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

const LOG_JSON: &[u8] =
    br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"test"}}]}]}]}"#;
const LOGS_BUFFERED: &str = r#"{"status":"buffered","rows":1,"batches":1}"#;
const UNAUTHORIZED: &str = r#"{"error":"unauthorized","message":"missing or invalid Authorization bearer token or x-api-key"}"#;
const METRICS_PB: &[u8] = include_bytes!("fixtures/otap/metrics-initial.otlp");
const OTAP_METRICS: &[u8] = include_bytes!("fixtures/otap/metrics-initial.bar");
const ARROW_METRICS: &str =
    "/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService/ArrowMetrics";
const METRIC_SIGNALS: [OtlpSignalType; 4] = [
    OtlpSignalType::MetricsGauge,
    OtlpSignalType::MetricsSum,
    OtlpSignalType::MetricsHistogram,
    OtlpSignalType::MetricsExpHistogram,
];

fn http_exchange(
    addr: &str,
    request_line: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> std::io::Result<Vec<u8>> {
    let mut request =
        format!("{request_line} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n").into_bytes();
    if !headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("transfer-encoding"))
    {
        write!(request, "Content-Length: {}\r\n", body.len()).unwrap();
    }
    for (name, value) in headers {
        write!(request, "{name}: {value}\r\n").unwrap();
    }
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(body);
    exchange(addr, &request)
}

fn exchange(addr: &str, request: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut conn = TcpStream::connect(addr)?;
    conn.set_read_timeout(Some(Duration::from_secs(5)))?;
    conn.set_write_timeout(Some(Duration::from_secs(5)))?;
    conn.write_all(request)?;
    let mut response = Vec::new();
    conn.read_to_end(&mut response)?;
    Ok(response)
}

fn http(addr: &str, request_line: &str, headers: &[(&str, &str)], body: &[u8]) -> (u16, String) {
    let response =
        String::from_utf8(http_exchange(addr, request_line, headers, body).unwrap()).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(
        headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("content-type: application/json")),
        "{headers}"
    );
    (
        headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        body.into(),
    )
}

fn assert_http(
    server: &Server,
    request_line: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    status: u16,
    response: &str,
) {
    let (actual_status, actual_body) = http(&server.addr, request_line, headers, body);
    assert_eq!((actual_status, actual_body.as_str()), (status, response));
}

fn assert_shapes(message: &Delivered, input_bytes: usize, signals: &[OtlpSignalType]) {
    assert_eq!(message.input_bytes, input_bytes as u64);
    assert_eq!(
        message
            .batches
            .iter()
            .map(|batch| batch.signal)
            .collect::<Vec<_>>(),
        signals
    );
    assert!(message.batches.iter().all(|batch| batch.rows == 1));
}

#[test]
fn http_logs_support_protobuf_json_and_json_lines() {
    let server = Server::start(Options::default(), Host::default());
    let mut logs = logs_request();
    logs.resource_logs.truncate(1);
    logs.resource_logs[0].scope_logs.truncate(1);
    logs.resource_logs[0].scope_logs[0].log_records.truncate(1);
    let logs_pb = logs.encode_to_vec();
    let jsonl = [LOG_JSON, b"\n", LOG_JSON, b"\n"].concat();
    for content_type in [
        "application/x-protobuf",
        "application/protobuf",
        "application/otlp",
    ] {
        assert_http(
            &server,
            "POST /v1/logs",
            &[("Content-Type", content_type)],
            &logs_pb,
            202,
            LOGS_BUFFERED,
        );
    }
    for content_type in ["APPLICATION/JSON ; charset=utf-8", "application/otlp+json"] {
        assert_http(
            &server,
            "POST /v1/logs",
            &[("Content-Type", content_type)],
            LOG_JSON,
            202,
            LOGS_BUFFERED,
        );
    }
    for content_type in ["application/x-ndjson", "application/jsonl"] {
        assert_http(
            &server,
            "POST /v1/logs",
            &[("Content-Type", content_type)],
            &jsonl,
            202,
            r#"{"status":"buffered","rows":2,"batches":1}"#,
        );
    }
    let delivered = server.host.delivered();
    assert_eq!(delivered.len(), 7);
    for message in &delivered[..3] {
        assert_shapes(message, logs_pb.len(), &[OtlpSignalType::Logs]);
    }
    for message in &delivered[3..5] {
        assert_shapes(message, LOG_JSON.len(), &[OtlpSignalType::Logs]);
    }
    for message in &delivered[5..] {
        assert_eq!(message.input_bytes, jsonl.len() as u64);
        assert_eq!(message.batches.len(), 1);
        assert_eq!(message.batches[0].signal, OtlpSignalType::Logs);
        assert_eq!(message.batches[0].rows, 2);
    }
}

#[test]
fn http_delivers_traces_and_all_metric_shapes() {
    let server = Server::start(Options::default(), Host::default());
    let trace = span_request().encode_to_vec();
    let protobuf = &[("Content-Type", "application/x-protobuf")];
    assert_http(
        &server,
        "POST /v1/traces",
        protobuf,
        &trace,
        202,
        LOGS_BUFFERED,
    );
    assert_http(
        &server,
        "POST /v1/metrics",
        protobuf,
        METRICS_PB,
        202,
        r#"{"status":"buffered","rows":4,"batches":4,"skipped":{"summaries":1,"nan_values":0,"infinity_values":0,"missing_values":0}}"#,
    );
    assert_http(
        &server,
        "POST /v1/metrics",
        protobuf,
        b"",
        202,
        r#"{"status":"buffered","rows":0,"batches":0,"skipped":{"summaries":0,"nan_values":0,"infinity_values":0,"missing_values":0}}"#,
    );
    assert_http(
        &server,
        "POST /v1/logs",
        protobuf,
        b"",
        202,
        r#"{"status":"buffered","rows":0,"batches":0}"#,
    );
    let delivered = server.host.delivered();
    assert_eq!(delivered.len(), 4);
    assert_shapes(&delivered[0], trace.len(), &[OtlpSignalType::Traces]);
    assert_shapes(&delivered[1], METRICS_PB.len(), &METRIC_SIGNALS);
    assert_shapes(&delivered[2], 0, &[]);
    assert_eq!(delivered[3].batches.len(), 1);
    assert_eq!(delivered[3].batches[0].rows, 0);
}

fn gzip(body: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap()
}

fn deflate(body: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body).unwrap();
    encoder.finish().unwrap()
}

#[test]
fn http_checks_encodings_and_reports_decoded_bytes() {
    let server = Server::start(Options::default(), Host::default());
    let (left, right) = LOG_JSON.split_at(LOG_JSON.len() / 2);
    for (encoding, body) in [
        ("GZiP", gzip(LOG_JSON)),
        ("gzip", [gzip(left), gzip(right)].concat()),
        ("deflate", deflate(LOG_JSON)),
        ("IdEnTiTy", LOG_JSON.to_vec()),
        ("", LOG_JSON.to_vec()),
    ] {
        assert_http(
            &server,
            "POST /v1/logs",
            &[
                ("Content-Encoding", encoding),
                ("Content-Type", "application/json"),
            ],
            &body,
            202,
            LOGS_BUFFERED,
        );
    }
    assert_http(
        &server,
        "POST /v1/logs",
        &[("Content-Encoding", "Br")],
        b"",
        415,
        r#"{"error":"request_failed","message":"unsupported content-encoding Br"}"#,
    );
    let mut truncated = deflate(LOG_JSON);
    truncated.pop();
    for (encoding, body) in [("gzip", b"not gzip".as_slice()), ("deflate", &truncated)] {
        let (status, response) = http(
            &server.addr,
            "POST /v1/logs",
            &[
                ("Content-Encoding", encoding),
                ("Content-Type", "application/json"),
            ],
            body,
        );
        assert_eq!(status, 400);
        assert!(
            response.starts_with(r#"{"error":"bad_request","message":"failed to decompress body:"#),
            "{response}"
        );
    }
    let delivered = server.host.delivered();
    assert_eq!(delivered.len(), 5);
    for message in delivered {
        assert_shapes(&message, LOG_JSON.len(), &[OtlpSignalType::Logs]);
    }
}

#[test]
fn http_authentication_precedes_content_checks_and_body_read() {
    let server = Server::start(
        Options {
            token: Some(b"s3cret"),
            ..Options::default()
        },
        Host::default(),
    );
    for headers in REJECTED_HEADERS {
        assert_http(
            &server,
            "POST /v1/logs",
            headers,
            LOG_JSON,
            401,
            UNAUTHORIZED,
        );
    }
    let head = format!(
        "POST /v1/logs HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nExpect: 100-continue\r\nContent-Length: 100\r\n\r\n",
        server.addr
    );
    let response = String::from_utf8(exchange(&server.addr, head.as_bytes()).unwrap()).unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 401"), "{headers}");
    assert_eq!(body, UNAUTHORIZED);
    assert!(server.host.delivered().is_empty());

    for headers in ACCEPTED_HEADERS {
        let mut headers = headers.to_vec();
        headers.push(("Content-Type", "application/json"));
        assert_http(
            &server,
            "POST /v1/logs",
            &headers,
            LOG_JSON,
            202,
            LOGS_BUFFERED,
        );
    }
    assert_eq!(server.host.delivered().len(), ACCEPTED_HEADERS.len());
}

#[test]
fn grpc_authentication_precedes_message_decoding() {
    let server = Server::start(
        Options {
            token: Some(b"s3cret"),
            max_decoding: 1,
            ..Options::default()
        },
        Host::default(),
    );
    let rt = runtime();
    let status = rt
        .block_on(export_logs(server.url(), &[], None))
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
    let status = rt
        .block_on(export_logs(server.url(), ACCEPTED_HEADERS[0], None))
        .unwrap_err();
    assert_eq!(status.code(), Code::OutOfRange);
    assert!(server.host.delivered().is_empty());
}

#[test]
fn start_validates_and_copies_the_auth_token() {
    let host = Box::<Host>::default();
    let mut err = [0; 256];
    for (token, expected) in [
        (b"".as_slice(), "auth_token is empty"),
        (b"\xff".as_slice(), "auth_token is not valid UTF-8"),
    ] {
        let config = configuration(
            &host,
            Options {
                token: Some(token),
                ..Options::default()
            },
        );
        assert!(start("127.0.0.1:0", &config, &mut err).is_null());
        assert_eq!(
            unsafe { std::ffi::CStr::from_ptr(err.as_ptr()) }
                .to_str()
                .unwrap(),
            expected
        );
    }
    let mut token = b"s3cret".to_vec();
    let server = Server::start(
        Options {
            token: Some(&token),
            ..Options::default()
        },
        Host::default(),
    );
    token.fill(b'x');
    assert_http(
        &server,
        "POST /v1/logs",
        &[
            ("x-api-key", "s3cret"),
            ("Content-Type", "application/json"),
        ],
        LOG_JSON,
        202,
        LOGS_BUFFERED,
    );
}

#[test]
fn http_limits_wire_and_decompressed_bodies() {
    let server = Server::start(
        Options {
            max_decoding: 128,
            ..Options::default()
        },
        Host::default(),
    );
    let body = vec![b' '; 4096];
    assert_http(
        &server,
        "POST /v1/logs",
        &[("Content-Type", "application/json")],
        &body,
        413,
        r#"{"error":"request_failed","message":"payload has 4096 bytes; max is 128"}"#,
    );
    let chunked = [b"1000\r\n", body.as_slice(), b"\r\n0\r\n\r\n"].concat();
    assert_http(
        &server,
        "POST /v1/logs",
        &[
            ("Transfer-Encoding", "chunked"),
            ("Content-Type", "application/json"),
        ],
        &chunked,
        413,
        r#"{"error":"request_failed","message":"payload exceeds 128 bytes"}"#,
    );
    for (encoding, compressed) in [("gzip", gzip(&body)), ("deflate", deflate(&body))] {
        assert!(compressed.len() < 128);
        assert_http(
            &server,
            "POST /v1/logs",
            &[
                ("Content-Encoding", encoding),
                ("Content-Type", "application/json"),
            ],
            &compressed,
            413,
            r#"{"error":"request_failed","message":"payload exceeds 128 bytes"}"#,
        );
    }
    assert!(server.host.delivered().is_empty());

    let mut boundary = LOG_JSON.to_vec();
    boundary.resize(128, b' ');
    assert_http(
        &server,
        "POST /v1/logs",
        &[
            ("Content-Encoding", "gzip"),
            ("Content-Type", "application/json"),
        ],
        &gzip(&boundary),
        202,
        LOGS_BUFFERED,
    );
    assert_shapes(&server.host.delivered()[0], 128, &[OtlpSignalType::Logs]);
}

#[test]
fn http_rejects_unsupported_and_unparseable_payloads() {
    let server = Server::start(Options::default(), Host::default());
    assert_http(
        &server,
        "POST /v1/logs",
        &[],
        LOG_JSON,
        415,
        r#"{"error":"request_failed","message":"unsupported content-type <missing>"}"#,
    );
    assert_http(
        &server,
        "POST /v1/logs",
        &[("Content-Type", " Text/Plain ; charset=UTF-8")],
        LOG_JSON,
        415,
        r#"{"error":"request_failed","message":"unsupported content-type text/plain"}"#,
    );
    let (status, response) = http(
        &server.addr,
        "POST /v1/logs",
        &[("Content-Type", "application/json")],
        b"no JSON",
    );
    assert_eq!(status, 400);
    assert!(
        response.starts_with(r#"{"error":"bad_request","message":"failed to transform logs:"#),
        "{response}"
    );
    assert!(server.host.delivered().is_empty());
}

#[test]
fn http_and_grpc_report_host_status_and_detail() {
    for (reply, grpc_code, http_status, response) in [
        (
            OtlpIngestStatus::ResourceExhausted,
            Code::ResourceExhausted,
            503,
            r#"{"error":"request_failed","message":"try \"later\""}"#,
        ),
        (
            OtlpIngestStatus::Invalid,
            Code::InvalidArgument,
            400,
            r#"{"error":"bad_request","message":"try \"later\""}"#,
        ),
        (
            OtlpIngestStatus::Internal,
            Code::Internal,
            500,
            r#"{"error":"internal_error","message":"try \"later\""}"#,
        ),
    ] {
        let server = Server::start(
            Options::default(),
            Host {
                reply: Some(reply),
                detail: b"try \"later\"\0ignored",
                ..Host::default()
            },
        );
        assert_http(
            &server,
            "POST /v1/logs",
            &[("Content-Type", "application/json")],
            LOG_JSON,
            http_status,
            response,
        );
        let rt = runtime();
        let grpc = rt
            .block_on(export_logs(server.url(), &[], None))
            .unwrap_err();
        assert_eq!(grpc.code(), grpc_code);
        assert_eq!(grpc.message(), "try \"later\"");
        let acks = rt
            .block_on(arrow_logs(
                server.url(),
                &[],
                None,
                vec![envelope(7, OTAP_LOGS_INITIAL)],
            ))
            .unwrap();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].status_code, grpc_code as i32);
        assert_eq!(acks[0].status_message, "try \"later\"");
    }
}

#[test]
fn callback_detail_is_bounded_and_lossy() {
    static DETAIL: [u8; 256] = [0xff; 256];
    let server = Server::start(
        Options::default(),
        Host {
            reply: Some(OtlpIngestStatus::Invalid),
            detail: &DETAIL,
            ..Host::default()
        },
    );
    let (status, body) = http(
        &server.addr,
        "POST /v1/logs",
        &[("Content-Type", "application/json")],
        LOG_JSON,
    );
    assert_eq!(status, 400);
    assert_eq!(
        body,
        format!(
            r#"{{"error":"bad_request","message":"{}"}}"#,
            "�".repeat(256)
        )
    );
}

#[test]
fn http_health_and_readiness() {
    for (readiness, status, response) in [
        (None, 200, r#"{"status":"ready"}"#),
        (
            Some((true, b"unused".as_slice())),
            200,
            r#"{"status":"ready"}"#,
        ),
        (
            Some((false, b"seal \"stalled\"".as_slice())),
            503,
            r#"{"status":"degraded","reason":"seal \"stalled\""}"#,
        ),
        (
            Some((false, b"".as_slice())),
            503,
            r#"{"status":"degraded","reason":"not ready"}"#,
        ),
    ] {
        let server = Server::start(
            Options {
                token: Some(b"secret"),
                ..Options::default()
            },
            Host {
                readiness,
                ..Host::default()
            },
        );
        assert_http(&server, "GET /healthz", &[], b"", 200, r#"{"status":"ok"}"#);
        assert_http(&server, "HEAD /healthz", &[], b"", 200, "");
        assert_eq!(server.host.readiness_calls.load(Ordering::SeqCst), 0);
        assert_http(&server, "GET /readyz", &[], b"", status, response);
        assert_eq!(
            server.host.readiness_calls.load(Ordering::SeqCst),
            usize::from(readiness.is_some())
        );
        assert!(server.host.delivered().is_empty());
    }
}

#[test]
fn http_and_grpc_share_one_port_when_enabled() {
    let rt = runtime();
    let http_only = Server::start(
        Options {
            services: OTLP_INGEST_SERVE_OTLP_HTTP,
            ..Options::default()
        },
        Host::default(),
    );
    let status = rt
        .block_on(export_logs(http_only.url(), &[], None))
        .unwrap_err();
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(http_only.host.delivered().is_empty());
    assert_http(
        &http_only,
        "POST /v1/logs",
        &[("Content-Type", "application/json")],
        LOG_JSON,
        202,
        LOGS_BUFFERED,
    );

    let grpc_only = Server::start(
        Options {
            services: OTLP_INGEST_SERVE_OTLP_GRPC,
            ..Options::default()
        },
        Host::default(),
    );
    let response = http_exchange(&grpc_only.addr, "GET /healthz", &[], b"");
    match response {
        Ok(response) => assert!(response.is_empty(), "{response:?}"),
        Err(error) => assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ),
            "{error}"
        ),
    }

    let shared = Server::start(
        Options {
            token: Some(b"s3cret"),
            ..Options::default()
        },
        Host::default(),
    );
    assert_http(
        &shared,
        "POST /v1/logs",
        &[
            ("Content-Type", "application/json"),
            ("x-api-key", "s3cret"),
        ],
        LOG_JSON,
        202,
        LOGS_BUFFERED,
    );
    rt.block_on(export_logs(shared.url(), ACCEPTED_HEADERS[2], None))
        .unwrap();
    assert_eq!(shared.host.delivered().len(), 2);
    for path in [
        "/does-not-exist",
        "/opentelemetry.proto.collector.logs.v1.LogsService/Unknown",
    ] {
        assert_eq!(http(&shared.addr, &format!("GET {path}"), &[], b"").0, 404);
        let status = rt
            .block_on(arrow_stream(shared.url(), path, &[], None, vec![]))
            .unwrap_err();
        assert_eq!(status.code(), Code::Unimplemented);
    }
}

#[test]
fn grpc_unary_and_otap_deliver_one_message_with_all_metric_shapes() {
    let server = Server::start(Options::default(), Host::default());
    let request = ExportMetricsServiceRequest::decode(METRICS_PB).unwrap();
    let input_bytes = request.encoded_len();
    let otap = envelope(13, OTAP_METRICS);
    let otap_bytes = otap.encoded_len();
    runtime().block_on(async {
        let mut client = MetricsServiceClient::connect(server.url()).await.unwrap();
        client.export(request).await.unwrap();
        client
            .export(ExportMetricsServiceRequest::default())
            .await
            .unwrap();
        let acks = arrow_stream(server.url(), ARROW_METRICS, &[], None, vec![otap])
            .await
            .unwrap();
        assert_eq!(acks.len(), 1);
        assert_eq!(
            (acks[0].batch_id, acks[0].status_code),
            (13, StatusCode::Ok as i32)
        );
    });
    let delivered = server.host.delivered();
    assert_eq!(delivered.len(), 3);
    assert_shapes(&delivered[0], input_bytes, &METRIC_SIGNALS);
    assert_shapes(&delivered[1], 0, &[]);
    assert_shapes(&delivered[2], otap_bytes, &METRIC_SIGNALS);
}

#[test]
fn http_profile_refusal_delivers_no_metric_shapes() {
    use opentelemetry_proto::tonic::metrics::v1::metric;
    let server = Server::start(
        Options {
            profile: OtlpArrowProfile::DuckDb,
            ..Options::default()
        },
        Host::default(),
    );
    let mut request = ExportMetricsServiceRequest::decode(METRICS_PB).unwrap();
    let metrics = &mut request.resource_metrics[0].scope_metrics[0].metrics;
    let Some(metric::Data::Histogram(histogram)) = &mut metrics[2].data else {
        panic!("fixture histogram");
    };
    histogram.data_points[0].count = u64::MAX;
    assert_http(
        &server,
        "POST /v1/metrics",
        &[("Content-Type", "application/x-protobuf")],
        &request.encode_to_vec(),
        400,
        r#"{"error":"bad_request","message":"invalid payload"}"#,
    );
    assert!(server.host.delivered().is_empty());
}

#[test]
fn http_stop_waits_for_in_flight_callback_and_rejects_late_traffic() {
    let host = Host::default();
    host.callback_sleep_ms.store(400, Ordering::SeqCst);
    let server = Server::start(Options::default(), host);
    let addr = &server.addr;
    let headers = &[("Content-Type", "application/json")];
    std::thread::scope(|scope| {
        let first = scope.spawn(|| http_exchange(addr, "POST /v1/logs", headers, LOG_JSON));
        wait_until("the HTTP callback to start", || server.host.in_callback());
        let late = scope.spawn(|| {
            wait_until("the listener to stop accepting", || {
                TcpStream::connect(addr).is_err()
            });
            assert!(http_exchange(addr, "POST /v1/logs", headers, LOG_JSON).is_err());
        });
        server.stop(10);
        assert!(!server.host.in_callback());
        let _ = first.join().unwrap();
        late.join().unwrap();
    });
    assert_eq!(server.host.delivered().len(), 1);
}
