//! Vendored tonic server stub for the canonical OTAP `ArrowLogsService`.
//!
//! Adapted (logs only) from the code `tonic` generates for
//! `opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`
//! (open-telemetry/otel-arrow, commit `f8cd17f0…`). It is hand-vendored — like
//! the message types in [`super::wire`] — so the crate needs no `protoc` /
//! `tonic-build` build dependency. The request/response messages are this
//! crate's own [`BatchArrowRecords`]/[`BatchStatus`] (both `prost::Message`), so
//! there are no duplicate generated types.
//!
//! Only the server half of the logs service is vendored; the client and the
//! traces/metrics services are intentionally omitted (added when those signals
//! gain streaming support).
#![allow(clippy::wildcard_imports)]

use super::wire::{BatchArrowRecords, BatchStatus};
use tonic::codegen::*;

/// Generated trait containing the gRPC method to implement for
/// `ArrowLogsServiceServer`.
#[async_trait]
pub trait ArrowLogsService: std::marker::Send + std::marker::Sync + 'static {
    /// Server-streaming response type for the `ArrowLogs` method.
    type ArrowLogsStream: tonic::codegen::tokio_stream::Stream<Item = std::result::Result<BatchStatus, tonic::Status>>
        + std::marker::Send
        + 'static;
    async fn arrow_logs(
        &self,
        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
    ) -> std::result::Result<tonic::Response<Self::ArrowLogsStream>, tonic::Status>;
}

/// gRPC server for the canonical OTAP `ArrowLogsService`.
#[derive(Debug)]
pub struct ArrowLogsServiceServer<T> {
    inner: Arc<T>,
    accept_compression_encodings: EnabledCompressionEncodings,
    send_compression_encodings: EnabledCompressionEncodings,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl<T> ArrowLogsServiceServer<T> {
    pub fn new(inner: T) -> Self {
        Self::from_arc(Arc::new(inner))
    }

    pub fn from_arc(inner: Arc<T>) -> Self {
        Self {
            inner,
            accept_compression_encodings: Default::default(),
            send_compression_encodings: Default::default(),
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }

    /// Enable decompressing requests with the given encoding.
    #[must_use]
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.accept_compression_encodings.enable(encoding);
        self
    }

    /// Compress responses with the given encoding, if the client supports it.
    #[must_use]
    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.send_compression_encodings.enable(encoding);
        self
    }

    /// Limit the maximum size of a decoded message (default 4 MiB).
    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.max_decoding_message_size = Some(limit);
        self
    }

    /// Limit the maximum size of an encoded message (default `usize::MAX`).
    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.max_encoding_message_size = Some(limit);
        self
    }
}

impl<T, B> tonic::codegen::Service<http::Request<B>> for ArrowLogsServiceServer<T>
where
    T: ArrowLogsService,
    B: Body + std::marker::Send + 'static,
    B::Error: Into<StdError> + std::marker::Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.uri().path() {
            "/opentelemetry.proto.experimental.arrow.v1.ArrowLogsService/ArrowLogs" => {
                #[allow(non_camel_case_types)]
                struct ArrowLogsSvc<T: ArrowLogsService>(pub Arc<T>);
                impl<T: ArrowLogsService> tonic::server::StreamingService<BatchArrowRecords> for ArrowLogsSvc<T> {
                    type Response = BatchStatus;
                    type ResponseStream = T::ArrowLogsStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut = async move {
                            <T as ArrowLogsService>::arrow_logs(&inner, request).await
                        };
                        Box::pin(fut)
                    }
                }
                let accept_compression_encodings = self.accept_compression_encodings;
                let send_compression_encodings = self.send_compression_encodings;
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let method = ArrowLogsSvc(inner);
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec)
                        .apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        )
                        .apply_max_message_size_config(
                            max_decoding_message_size,
                            max_encoding_message_size,
                        );
                    let res = grpc.streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            _ => Box::pin(async move {
                let mut response = http::Response::new(tonic::body::Body::default());
                let headers = response.headers_mut();
                headers.insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                headers.insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            }),
        }
    }
}

impl<T> Clone for ArrowLogsServiceServer<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self {
            inner,
            accept_compression_encodings: self.accept_compression_encodings,
            send_compression_encodings: self.send_compression_encodings,
            max_decoding_message_size: self.max_decoding_message_size,
            max_encoding_message_size: self.max_encoding_message_size,
        }
    }
}

/// Canonical gRPC service name.
pub const SERVICE_NAME: &str = "opentelemetry.proto.experimental.arrow.v1.ArrowLogsService";

impl<T> tonic::server::NamedService for ArrowLogsServiceServer<T> {
    const NAME: &'static str = SERVICE_NAME;
}
