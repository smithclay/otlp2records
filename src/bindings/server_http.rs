use std::io::{self, Write};
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use flate2::write::{MultiGzDecoder, ZlibDecoder};
use serde::Serialize;
use tokio_stream::StreamExt;
use tonic::service::Routes;
use tonic::Code;

use super::{invalid, Host, Refusal};
use crate::ffi::OtlpSignalType;
use crate::{transform_logs, transform_metrics, transform_traces, InputFormat, TransformSignal};

pub(super) fn routes(grpc: Router, host: Arc<Host>) -> Routes {
    let http = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route(
            "/v1/logs",
            post(|State(host), request| post_message(host, TransformSignal::Logs, request)),
        )
        .route(
            "/v1/traces",
            post(|State(host), request| post_message(host, TransformSignal::Traces, request)),
        )
        .route(
            "/v1/metrics",
            post(|State(host), request| post_message(host, TransformSignal::Metrics, request)),
        )
        .method_not_allowed_fallback(|| async {
            error(
                StatusCode::METHOD_NOT_ALLOWED,
                "request_failed",
                "method not allowed",
            )
        })
        .with_state(host);
    grpc.layer(middleware::from_fn(grpc_only))
        .merge(http)
        .fallback(|headers: HeaderMap| async move {
            if is_grpc(&headers) {
                tonic::Status::unimplemented("").into_http::<Body>()
            } else {
                not_found()
            }
        })
        .into()
}

fn is_grpc(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"application/grpc"))
}

async fn grpc_only(request: Request, next: Next) -> Response {
    if is_grpc(request.headers()) {
        next.run(request).await
    } else {
        not_found()
    }
}

fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "request_failed", "not found")
}

fn json(status: StatusCode, body: impl Serialize) -> Response {
    let body = serde_json::to_vec(&body).expect("HTTP response is JSON serializable");
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    #[derive(Serialize)]
    struct Error<'a> {
        error: &'a str,
        message: &'a str,
    }
    json(
        status,
        Error {
            error: code,
            message,
        },
    )
}

async fn health() -> Response {
    json(StatusCode::OK, serde_json::json!({"status": "ok"}))
}

async fn ready(State(host): State<Arc<Host>>) -> Response {
    #[derive(Serialize)]
    struct Degraded<'a> {
        status: &'static str,
        reason: &'a str,
    }
    match host.readiness() {
        Ok(()) => json(StatusCode::OK, serde_json::json!({"status": "ready"})),
        Err(reason) => json(
            StatusCode::SERVICE_UNAVAILABLE,
            Degraded {
                status: "degraded",
                reason: &reason,
            },
        ),
    }
}

fn refusal_response(refusal: Refusal) -> Response {
    let (status, code) = match refusal.code {
        Code::ResourceExhausted | Code::Unavailable => {
            (StatusCode::SERVICE_UNAVAILABLE, "request_failed")
        }
        Code::InvalidArgument => (StatusCode::BAD_REQUEST, "bad_request"),
        _ => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
    };
    error(status, code, &refusal.message)
}

async fn post_message(host: Arc<Host>, signal: TransformSignal, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let header_bytes = |key| {
        parts
            .headers
            .get(key)
            .map_or(&[][..], |value| value.as_bytes())
    };
    if !host.authorized(
        header_bytes(header::AUTHORIZATION),
        header_bytes(header::HeaderName::from_static("x-api-key")),
    ) {
        return error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing or invalid Authorization bearer token or x-api-key",
        );
    }
    if host.cancel.is_cancelled() {
        return refusal_response(Refusal::STOPPING);
    }

    let encoding = String::from_utf8_lossy(header_bytes(header::CONTENT_ENCODING));
    let encoding = match encoding.trim().to_ascii_lowercase().as_str() {
        "" | "identity" => Encoding::Identity,
        "gzip" => Encoding::Gzip,
        "deflate" => Encoding::Deflate,
        _ => {
            return error(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "request_failed",
                &format!("unsupported content-encoding {encoding}"),
            )
        }
    };
    let content_type = header_bytes(header::CONTENT_TYPE)
        .split(|&byte| byte == b';')
        .next()
        .unwrap_or_default();
    let content_type = String::from_utf8_lossy(content_type);
    let media_type = content_type.trim();
    let format = InputFormat::from_content_type(Some(media_type));
    if format == InputFormat::Auto {
        let media_type = if media_type.is_empty() {
            "<missing>".into()
        } else {
            media_type.to_ascii_lowercase()
        };
        return error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "request_failed",
            &format!("unsupported content-type {media_type}"),
        );
    }

    let limit = host.max_message_bytes;
    if let Some(size) = parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|&size| size > limit as u64)
    {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request_failed",
            &format!("payload has {size} bytes; max is {limit}"),
        );
    }
    let payload = match read_body(body, encoding, limit).await {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    match transform_and_deliver(&host, signal, format, &payload) {
        Ok(buffered) => json(StatusCode::ACCEPTED, buffered),
        Err(refusal) => refusal_response(refusal),
    }
}

#[derive(Serialize)]
struct Buffered {
    status: &'static str,
    rows: usize,
    batches: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped: Option<Skipped>,
}

#[derive(Serialize)]
struct Skipped {
    summaries: usize,
    nan_values: usize,
    infinity_values: usize,
    missing_values: usize,
}

fn transform_and_deliver(
    host: &Host,
    signal: TransformSignal,
    format: InputFormat,
    payload: &[u8],
) -> Result<Buffered, Refusal> {
    let input_bytes = payload.len() as u64;
    let mut response = Buffered {
        status: "buffered",
        rows: 0,
        batches: 0,
        skipped: None,
    };
    let mut count = |batch: &arrow_array::RecordBatch| {
        response.rows += batch.num_rows();
        response.batches += usize::from(batch.num_rows() != 0);
    };
    match signal {
        TransformSignal::Metrics => {
            let batches = transform_metrics(payload, format).map_err(|e| invalid("metrics", e))?;
            [
                &batches.gauge,
                &batches.sum,
                &batches.histogram,
                &batches.exp_histogram,
            ]
            .into_iter()
            .flatten()
            .for_each(count);
            response.skipped = Some(Skipped {
                summaries: batches.skipped.summaries,
                nan_values: batches.skipped.nan_values,
                infinity_values: batches.skipped.infinity_values,
                missing_values: batches.skipped.missing_values,
            });
            host.deliver_metrics(input_bytes, batches)?;
        }
        TransformSignal::Logs | TransformSignal::Traces => {
            let (signal, batch) = match signal {
                TransformSignal::Logs => (
                    OtlpSignalType::Logs,
                    transform_logs(payload, format).map_err(|e| invalid("logs", e))?,
                ),
                _ => (
                    OtlpSignalType::Traces,
                    transform_traces(payload, format).map_err(|e| invalid("traces", e))?,
                ),
            };
            count(&batch);
            host.deliver_one(signal, input_bytes, batch)?;
        }
    }
    Ok(response)
}

enum Encoding {
    Identity,
    Gzip,
    Deflate,
}

async fn read_body(body: Body, encoding: Encoding, limit: usize) -> Result<Vec<u8>, Response> {
    let output = Payload {
        bytes: Vec::new(),
        limit,
    };
    let output = match encoding {
        Encoding::Identity => read_wire(body, output, limit).await?,
        Encoding::Gzip => read_wire(body, MultiGzDecoder::new(output), limit)
            .await?
            .finish()
            .map_err(|err| decompression_error(err, limit))?,
        Encoding::Deflate => read_wire(body, ZlibDecoder::new(output), limit)
            .await?
            .finish()
            .map_err(|err| decompression_error(err, limit))?,
    };
    Ok(output.bytes)
}

async fn read_wire<W: Write>(body: Body, mut output: W, limit: usize) -> Result<W, Response> {
    let mut wire = body.into_data_stream();
    let mut read = 0;
    while let Some(chunk) = wire.next().await {
        let bytes = chunk.map_err(|err| {
            error(
                StatusCode::BAD_REQUEST,
                "bad_request",
                &format!("failed to read body: {err}"),
            )
        })?;
        if bytes.len() > limit - read {
            return Err(payload_exceeds(limit));
        }
        read += bytes.len();
        output
            .write_all(&bytes)
            .map_err(|err| decompression_error(err, limit))?;
    }
    Ok(output)
}

fn payload_exceeds(limit: usize) -> Response {
    error(
        StatusCode::PAYLOAD_TOO_LARGE,
        "request_failed",
        &format!("payload exceeds {limit} bytes"),
    )
}

fn decompression_error(err: io::Error, limit: usize) -> Response {
    if err.kind() == io::ErrorKind::FileTooLarge {
        payload_exceeds(limit)
    } else {
        error(
            StatusCode::BAD_REQUEST,
            "bad_request",
            &format!("failed to decompress body: {err}"),
        )
    }
}

/// The decoded body, refusing writes past the size limit.
struct Payload {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for Payload {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::ErrorKind::FileTooLarge.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
