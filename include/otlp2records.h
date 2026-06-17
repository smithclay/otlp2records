/**
 * @file otlp2records.h
 * @brief C FFI bindings for otlp2records - Transform OTLP telemetry to Arrow
 *
 * This header provides C-compatible bindings for the otlp2records Rust library.
 * It uses the Arrow C Data Interface for passing Arrow data across the FFI boundary.
 */

#ifndef OTLP2RECORDS_H
#define OTLP2RECORDS_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Arrow C Data Interface
 * https://arrow.apache.org/docs/format/CDataInterface.html
 * ============================================================================ */

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

struct ArrowSchema {
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;
    void (*release)(struct ArrowSchema*);
    void* private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;
    void (*release)(struct ArrowArray*);
    void* private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

/* ============================================================================
 * otlp2records Types
 * ============================================================================ */

/**
 * @brief OTLP signal types supported by the parser.
 */
typedef enum OtlpSignalType {
    /** Log records */
    OTLP_SIGNAL_LOGS = 0,
    /** Trace spans */
    OTLP_SIGNAL_TRACES = 1,
    /** Gauge metrics */
    OTLP_SIGNAL_METRICS_GAUGE = 2,
    /** Sum/counter metrics */
    OTLP_SIGNAL_METRICS_SUM = 3,
    /** Histogram metrics */
    OTLP_SIGNAL_METRICS_HISTOGRAM = 4,
    /** Exponential histogram metrics */
    OTLP_SIGNAL_METRICS_EXP_HISTOGRAM = 5,
} OtlpSignalType;

/**
 * @brief Input format for OTLP data.
 */
typedef enum OtlpInputFormat {
    /** Auto-detect format from content */
    OTLP_FORMAT_AUTO = 0,
    /** Binary protobuf format */
    OTLP_FORMAT_PROTOBUF = 1,
    /** JSON format */
    OTLP_FORMAT_JSON = 2,
    /** Newline-delimited JSON */
    OTLP_FORMAT_JSONL = 3,
} OtlpInputFormat;

/**
 * @brief Status codes returned by FFI functions.
 */
typedef enum OtlpStatus {
    /** Success */
    OTLP_OK = 0,
    /** Invalid argument (null pointer, invalid enum value, etc.) */
    OTLP_ERROR_INVALID_ARGUMENT = 1,
    /** Parse failed (invalid OTLP data) */
    OTLP_ERROR_PARSE_FAILED = 2,
    /** Internal error (panic caught, unexpected state) */
    OTLP_ERROR_INTERNAL = 3,
} OtlpStatus;

/**
 * @brief One optional Arrow batch returned by otlp_transform_metrics_all().
 *
 * If present is 0, array/schema are not initialized and must not be released.
 * If present is non-zero, caller must release both array and schema.
 */
typedef struct OtlpArrowBatch {
    struct ArrowArray array;
    struct ArrowSchema schema;
    int present;
} OtlpArrowBatch;

/**
 * @brief Output batches for all normalized metric shapes.
 */
typedef struct OtlpMetricsArrowBatches {
    OtlpArrowBatch gauge;
    OtlpArrowBatch sum;
    OtlpArrowBatch histogram;
    OtlpArrowBatch exp_histogram;
    uint64_t skipped_summaries;
    uint64_t skipped_nan_values;
    uint64_t skipped_infinity_values;
    uint64_t skipped_missing_values;
} OtlpMetricsArrowBatches;

/* ============================================================================
 * Schema Access
 * ============================================================================ */

/**
 * @brief Get the Arrow schema for a signal type.
 *
 * @param signal_type The OTLP signal type
 * @param out_schema Output: ArrowSchema to populate
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller must call out_schema->release() when done.
 */
OtlpStatus otlp_get_schema(
    OtlpSignalType signal_type,
    struct ArrowSchema* out_schema
);

/* ============================================================================
 * One-Shot Convenience API
 * ============================================================================ */

/**
 * @brief Transform OTLP bytes to Arrow in one call (non-streaming).
 *
 * @param signal_type The OTLP signal type
 * @param format Input format (OTLP_FORMAT_AUTO for auto-detection)
 * @param data Input bytes
 * @param len Length of input bytes
 * @param out_array Output: ArrowArray with the batch data
 * @param out_schema Output: ArrowSchema for the batch
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller must call out_array->release() and out_schema->release().
 * @note Array and schema are independent (can release in any order).
 */
OtlpStatus otlp_transform(
    OtlpSignalType signal_type,
    OtlpInputFormat format,
    const uint8_t* data,
    size_t len,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema
);

/**
 * @brief Transform OTLP metric bytes to all metric Arrow batches in one parse.
 *
 * @param format Input format (OTLP_FORMAT_AUTO for auto-detection)
 * @param data Input bytes
 * @param len Length of input bytes
 * @param out_batches Output: optional batches for gauge/sum/histogram/exp_histogram
 *                     plus skipped metric counters
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller should zero-initialize out_batches before calling.
 * @note For each output with present != 0, caller must release array and schema.
 * @note Outputs with present == 0 must not be released.
 */
OtlpStatus otlp_transform_metrics_all(
    OtlpInputFormat format,
    const uint8_t* data,
    size_t len,
    OtlpMetricsArrowBatches* out_batches
);

/* ============================================================================
 * Native OTAP Streaming API
 *
 * Decode canonical OTAP (BatchArrowRecords) envelopes into the same normalized
 * Arrow batches as the one-shot OTLP API. OTAP is stateful: later messages may
 * omit Arrow schemas/dictionaries and reuse those from earlier messages on the
 * same decoder, so a decoder must persist across calls. Create one decoder per
 * OTAP stream, feed it messages with the decode functions, then free it.
 *
 * OTAP envelopes are protobuf and are NOT auto-distinguishable from OTLP
 * protobuf; callers must select this API explicitly (it is not part of
 * OtlpInputFormat auto-detection).
 *
 * Compression: this API decodes whatever Arrow IPC compression the build links.
 * Uncompressed and LZ4 are always available. The upstream OTAP Producer
 * defaults to Zstandard; decoding that output requires building this crate with
 * --features ffi,otap-zstd (native only; the Zstandard backend cannot target
 * wasm32).
 *
 * Poisoning: on any non-OK return the decoder's stream state may be partially
 * advanced and is no longer trustworthy. Free the decoder and start a new one
 * rather than feeding it more messages; do not attempt mid-stream recovery.
 * ============================================================================ */

/** Opaque stateful OTAP decoder. One per OTAP stream; not thread-safe. */
typedef struct OtlpOtapDecoder OtlpOtapDecoder;

/**
 * @brief Create a new stateful OTAP decoder.
 *
 * @return Non-null handle owned by the caller; release with
 *         otlp_otap_decoder_free().
 */
OtlpOtapDecoder* otlp_otap_decoder_new(void);

/**
 * @brief Free an OTAP decoder created by otlp_otap_decoder_new().
 *
 * @param decoder Handle from otlp_otap_decoder_new(), or NULL.
 *
 * @note Passing NULL is a safe no-op. Must not be called twice on the same
 *       handle.
 */
void otlp_otap_decoder_free(OtlpOtapDecoder* decoder);

/**
 * @brief Decode one OTAP message into a normalized logs Arrow batch.
 *
 * @param decoder Handle from otlp_otap_decoder_new()
 * @param data Input bytes (one BatchArrowRecords envelope)
 * @param len Length of input bytes
 * @param out_array Output: ArrowArray with the batch data
 * @param out_schema Output: ArrowSchema for the batch
 * @return OTLP_OK on success, error code otherwise
 *
 * @note On OTLP_OK the caller must call out_array->release() and
 *       out_schema->release(). On any non-OK status nothing is written and the
 *       stream should be discarded (see Poisoning note above).
 */
OtlpStatus otlp_otap_decode_logs(
    OtlpOtapDecoder* decoder,
    const uint8_t* data,
    size_t len,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema
);

/**
 * @brief Decode one OTAP message into a normalized traces Arrow batch.
 *
 * Identical contract to otlp_otap_decode_logs().
 */
OtlpStatus otlp_otap_decode_traces(
    OtlpOtapDecoder* decoder,
    const uint8_t* data,
    size_t len,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema
);

/**
 * @brief Decode one OTAP message into the normalized metric Arrow batches.
 *
 * @param decoder Handle from otlp_otap_decoder_new()
 * @param data Input bytes (one BatchArrowRecords envelope)
 * @param len Length of input bytes
 * @param out_batches Output: optional batches for gauge/sum/histogram/
 *                     exp_histogram plus skipped metric counters
 * @return OTLP_OK on success, error code otherwise
 *
 * @note Caller should zero-initialize out_batches before calling.
 * @note For each output with present != 0, caller must release array and schema.
 * @note Outputs with present == 0 must not be released.
 * @note On any non-OK status no batch is present and the stream should be
 *       discarded (see Poisoning note above).
 */
OtlpStatus otlp_otap_decode_metrics(
    OtlpOtapDecoder* decoder,
    const uint8_t* data,
    size_t len,
    OtlpMetricsArrowBatches* out_batches
);

/* ============================================================================
 * Native gRPC Ingest Server
 *
 * Embeds a tonic (Rust) gRPC server on its own runtime threads and pushes each
 * decoded Arrow batch back through a host callback. This is the live-ingest
 * counterpart to otlp_transform()/the OTAP decoder: rather than the host
 * feeding bytes in and pulling Arrow out, the host starts a server and Arrow
 * batches arrive as telemetry is received over the wire.
 *
 * Serves all six signals over two service families on one listener: standard
 * OTLP/gRPC unary Export ({Logs,Trace,Metrics}Service) and canonical OTAP/Arrow
 * bidirectional streaming (Arrow{Logs,Traces,Metrics}Service). The callback ABI
 * carries stream_id/batch_id so the unary and streaming paths share one
 * signature; unary requests use 0 for both.
 *
 * Native only: this API is absent from wasm builds (no runtime/sockets).
 * ============================================================================ */

/**
 * @brief Outcome the host's batch callback reports to the server.
 *
 * Mapped to a gRPC status (and, for streaming, an OTAP BatchStatus).
 */
typedef enum OtlpIngestStatus {
    /** Buffered successfully -> gRPC OK */
    OTLP_INGEST_OK = 0,
    /** Ingest buffer full (backpressure) -> gRPC RESOURCE_EXHAUSTED */
    OTLP_INGEST_RESOURCE_EXHAUSTED = 1,
    /** Malformed/unconvertible payload -> gRPC INVALID_ARGUMENT */
    OTLP_INGEST_INVALID = 2,
    /** Host-side internal error -> gRPC INTERNAL */
    OTLP_INGEST_INTERNAL = 3,
    /** Rejected by auth -> gRPC UNAUTHENTICATED */
    OTLP_INGEST_UNAUTHENTICATED = 4,
} OtlpIngestStatus;

/** Opaque running gRPC server. */
typedef struct OtlpGrpcServer OtlpGrpcServer;

/**
 * @brief Per-batch callback: one decoded Arrow batch destined for a signal.
 *
 * Invoked on a server worker thread. stream_id/batch_id are 0 for unary
 * requests; for streaming they identify the source stream and message.
 * input_bytes is the wire size of the source payload, charged against the
 * host's admission/backpressure budget. The array/schema are borrowed for the
 * duration of the call only and are released by the server after it returns --
 * the callee MUST copy out only the data they reference and MUST NOT call their
 * release callbacks. Do NOT copy the ArrowArray/ArrowSchema structs themselves
 * (e.g. via memcpy) and release the copy later: the server releases the
 * originals on return, so a release on a struct copy is a double-free.
 * The callee must not throw across this boundary; translate any error into an
 * OtlpIngestStatus.
 *
 * @return OTLP_INGEST_OK on success, or an error mapped to a gRPC status.
 */
typedef OtlpIngestStatus (*OtlpBatchCallback)(
    void* user_data,
    OtlpSignalType signal_type,
    uint64_t stream_id,
    int64_t batch_id,
    uint64_t input_bytes,
    struct ArrowArray* array,
    struct ArrowSchema* schema
);

/**
 * @brief Optional auth callback: validate the raw `authorization` metadata.
 *
 * @param user_data Opaque host pointer passed to otlp_grpc_server_start()
 * @param metadata_token The `authorization` metadata value (may be empty)
 * @param len Length of metadata_token in bytes
 * @return Non-zero to accept the request, 0 to reject (UNAUTHENTICATED)
 *
 * @note Pass NULL for this callback to accept every request.
 */
typedef int (*OtlpAuthCallback)(
    void* user_data,
    const char* metadata_token,
    size_t len
);

/**
 * @brief Start a gRPC ingest server bound to "host:port".
 *
 * @param bind_addr Bind address bytes, "host:port" (e.g. "127.0.0.1:4317")
 * @param bind_addr_len Length of bind_addr in bytes
 * @param on_batch Per-batch callback (must be non-NULL)
 * @param on_auth Auth callback, or NULL to disable auth
 * @param user_data Opaque pointer passed verbatim to the callbacks
 * @param max_decoding_message_bytes Cap on a single received gRPC message (one
 *        OTLP Export request or one OTAP BatchArrowRecords), applied to every
 *        service. Pass 0 for tonic's 4 MiB default; raise it when producers send
 *        batches larger than 4 MiB.
 * @param err_buf Optional buffer for an error message on failure (may be NULL)
 * @param err_buf_len Size of err_buf in bytes
 * @return A non-NULL server handle on success; NULL on failure (with err_buf
 *         filled, truncated and null-terminated, when provided).
 *
 * @note The listener is bound synchronously, so address-in-use is reported here.
 * @note user_data and the callbacks must remain valid until
 *       otlp_grpc_server_stop() returns.
 */
OtlpGrpcServer* otlp_grpc_server_start(
    const char* bind_addr,
    size_t bind_addr_len,
    OtlpBatchCallback on_batch,
    OtlpAuthCallback on_auth,
    void* user_data,
    uint64_t max_decoding_message_bytes,
    char* err_buf,
    size_t err_buf_len
);

/**
 * @brief Stop the server: graceful shutdown, drain in-flight work up to the
 *        deadline, then join the runtime threads. Idempotent.
 *
 * @param server Handle from otlp_grpc_server_start(), or NULL (a no-op)
 * @param drain_deadline_ms Max time to wait for in-flight requests to drain
 *
 * @note Must NOT be called from within a batch/auth callback.
 */
void otlp_grpc_server_stop(OtlpGrpcServer* server, uint64_t drain_deadline_ms);

/**
 * @brief Free a server handle. Call otlp_grpc_server_stop() first.
 *
 * @param server Handle from otlp_grpc_server_start(), or NULL (a no-op)
 */
void otlp_grpc_server_free(OtlpGrpcServer* server);

/**
 * @brief Get a static message for a status code.
 *
 * @param status Status code
 * @return Static string describing the status (never NULL)
 */
const char* otlp_status_message(OtlpStatus status);

#ifdef __cplusplus
}
#endif

#endif /* OTLP2RECORDS_H */
