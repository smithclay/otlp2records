//! Vendored tonic server stubs for the canonical OTAP Arrow services.
//!
//! Adapted from the code `tonic` generates for
//! `opentelemetry/proto/experimental/arrow/v1/arrow_service.proto`
//! (open-telemetry/otel-arrow, commit `f8cd17f0…`). The three signal services
//! (`ArrowLogsService`/`ArrowTracesService`/`ArrowMetricsService`) are identical
//! apart from their method name and route, so they are generated from one macro
//! rather than copied three times. They are hand-vendored — like the message
//! types in [`super::wire`] — so the crate needs no `protoc` / `tonic-build`
//! build dependency, and the request/response messages are this crate's own
//! [`BatchArrowRecords`]/[`BatchStatus`] (both `prost::Message`), so there are no
//! duplicate generated types.
//!
//! Only the server halves are vendored; the clients are intentionally omitted.
#![allow(clippy::wildcard_imports)]

use super::wire::{BatchArrowRecords, BatchStatus};
use tonic::codegen::*;

/// Generate the server half of one canonical OTAP Arrow service
/// (`rpc <method>(stream BatchArrowRecords) returns (stream BatchStatus)`).
macro_rules! define_arrow_service {
    ($trait:ident, $server:ident, $stream:ident, $method:ident, $svc:ident, $path:literal, $name:literal) => {
        /// Generated trait containing the gRPC method to implement.
        #[async_trait]
        pub trait $trait: std::marker::Send + std::marker::Sync + 'static {
            /// Server-streaming response type for the RPC.
            type $stream: tonic::codegen::tokio_stream::Stream<Item = std::result::Result<BatchStatus, tonic::Status>>
                + std::marker::Send
                + 'static;
            async fn $method(
                &self,
                request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
            ) -> std::result::Result<tonic::Response<Self::$stream>, tonic::Status>;
        }

        #[derive(Debug)]
        pub struct $server<T> {
            inner: Arc<T>,
            accept_compression_encodings: EnabledCompressionEncodings,
            send_compression_encodings: EnabledCompressionEncodings,
            max_decoding_message_size: Option<usize>,
            max_encoding_message_size: Option<usize>,
        }

        impl<T> $server<T> {
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

        impl<T, B> tonic::codegen::Service<http::Request<B>> for $server<T>
        where
            T: $trait,
            B: Body + std::marker::Send + 'static,
            B::Error: Into<StdError> + std::marker::Send + 'static,
        {
            type Response = http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = BoxFuture<Self::Response, Self::Error>;

            fn poll_ready(
                &mut self,
                _cx: &mut Context<'_>,
            ) -> Poll<std::result::Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: http::Request<B>) -> Self::Future {
                match req.uri().path() {
                    $path => {
                        #[allow(non_camel_case_types)]
                        struct $svc<T: $trait>(pub Arc<T>);
                        impl<T: $trait> tonic::server::StreamingService<BatchArrowRecords> for $svc<T> {
                            type Response = BatchStatus;
                            type ResponseStream = T::$stream;
                            type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                            fn call(
                                &mut self,
                                request: tonic::Request<tonic::Streaming<BatchArrowRecords>>,
                            ) -> Self::Future {
                                let inner = Arc::clone(&self.0);
                                let fut = async move { <T as $trait>::$method(&inner, request).await };
                                Box::pin(fut)
                            }
                        }
                        let accept_compression_encodings = self.accept_compression_encodings;
                        let send_compression_encodings = self.send_compression_encodings;
                        let max_decoding_message_size = self.max_decoding_message_size;
                        let max_encoding_message_size = self.max_encoding_message_size;
                        let inner = self.inner.clone();
                        let fut = async move {
                            let method = $svc(inner);
                            let codec = tonic_prost::ProstCodec::default();
                            let mut grpc = tonic::server::Grpc::new(codec)
                                .apply_compression_config(accept_compression_encodings, send_compression_encodings)
                                .apply_max_message_size_config(max_decoding_message_size, max_encoding_message_size);
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
                        headers.insert(http::header::CONTENT_TYPE, tonic::metadata::GRPC_CONTENT_TYPE);
                        Ok(response)
                    }),
                }
            }
        }

        impl<T> Clone for $server<T> {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                    accept_compression_encodings: self.accept_compression_encodings,
                    send_compression_encodings: self.send_compression_encodings,
                    max_decoding_message_size: self.max_decoding_message_size,
                    max_encoding_message_size: self.max_encoding_message_size,
                }
            }
        }

        impl<T> tonic::server::NamedService for $server<T> {
            const NAME: &'static str = $name;
        }
    };
}

define_arrow_service!(
    ArrowLogsService,
    ArrowLogsServiceServer,
    ArrowLogsStream,
    arrow_logs,
    ArrowLogsSvc,
    "/opentelemetry.proto.experimental.arrow.v1.ArrowLogsService/ArrowLogs",
    "opentelemetry.proto.experimental.arrow.v1.ArrowLogsService"
);
define_arrow_service!(
    ArrowTracesService,
    ArrowTracesServiceServer,
    ArrowTracesStream,
    arrow_traces,
    ArrowTracesSvc,
    "/opentelemetry.proto.experimental.arrow.v1.ArrowTracesService/ArrowTraces",
    "opentelemetry.proto.experimental.arrow.v1.ArrowTracesService"
);
define_arrow_service!(
    ArrowMetricsService,
    ArrowMetricsServiceServer,
    ArrowMetricsStream,
    arrow_metrics,
    ArrowMetricsSvc,
    "/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService/ArrowMetrics",
    "opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService"
);
