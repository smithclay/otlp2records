/**
 * @file otlp2records.h
 * @brief C FFI bindings for otlp2records - Transform OTLP telemetry to Arrow
 *
 * This header provides C-compatible bindings for the otlp2records Rust library.
 * It uses the Arrow C Data Interface for passing Arrow data across the FFI boundary.
 */

#ifndef OTLP2RECORDS_H
#define OTLP2RECORDS_H

#include <stdbool.h>
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
 * @brief Arrow type profile for the *_with_profile entry points.
 *
 * OTLP_ARROW_PROFILE_CANONICAL is the canonical schema every profile-less
 * entry point returns. OTLP_ARROW_PROFILE_DUCKDB re-types the same batch so
 * DuckDB's stock Arrow importer yields the duckdb-otlp column types:
 *
 *   FixedSizeBinary(N) ("w:N")    -> Utf8 ("u"), lowercase hex, 2N chars
 *   Duration ("tD*")              -> Int64 ("l"), raw value
 *   UInt8/16/32 ("C"/"S"/"I")     -> Int32 ("i")
 *   UInt64 ("L")                  -> Int64 ("l")
 *   List(T)                       -> List(T mapped as above)
 *
 * Everything else (including Timestamp(ns) "tsn:") is unchanged, and nulls
 * stay null. A UInt32/UInt64 value above the signed maximum is an error, not
 * a wrap: the transform/decode returns OTLP_ERROR_PARSE_FAILED and the ingest
 * server answers INVALID_ARGUMENT without invoking the batch callback.
 */
typedef enum OtlpArrowProfile {
    /** Canonical schema (the default everywhere) */
    OTLP_ARROW_PROFILE_CANONICAL = 0,
    /** Types DuckDB's stock Arrow importer maps to the duckdb-otlp columns */
    OTLP_ARROW_PROFILE_DUCKDB = 1,
} OtlpArrowProfile;

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

/**
 * @brief Rows whose timestamp column is within [min, max] (inclusive, ns).
 *
 * column indexes the signal's schema (for metrics, the shape's) and must be a
 * Timestamp(ns) column. A NULL timestamp is outside every range.
 */
typedef struct OtlpTimeRange {
    uint32_t column;
    int64_t min;
    int64_t max;
} OtlpTimeRange;

/**
 * @brief Decides whether a resource's rows are kept, from its service_name.
 *
 * service_name is the value the transform emits (len UTF-8 bytes, not
 * NUL-terminated), so a resource without service.name is asked about
 * "unknown". Called once per resource, on the calling thread, during the
 * *_with_options call. It must not unwind.
 */
typedef bool (*OtlpServiceFilter)(void* user_data, const uint8_t* service_name, size_t len);

/**
 * @brief What a *_with_options call builds: some columns of some rows.
 *
 * A reader that needs a few columns of a few rows says so, and the transform
 * skips the work for the rest: the batch holds exactly the requested columns,
 * in the requested order, and only the rows that pass the filters. Rows of a
 * resource keep_service rejects, or outside a time range, are dropped before
 * any of their columns are built.
 *
 * A zero-initialized struct builds every column of every row.
 */
typedef struct OtlpReadOptions {
    /** Columns to return, in order, by index in the signal's schema. NULL: every column, in schema order.
     *  A non-NULL pointer with columns_len 0 returns no columns, only the row count. */
    const uint32_t* columns;
    size_t columns_len;
    /** Keep only the rows of resources this accepts. NULL: every resource. */
    OtlpServiceFilter keep_service;
    void* keep_service_data;
    /** Keep only rows inside every range. NULL or 0: no ranges. */
    const OtlpTimeRange* time_ranges;
    size_t time_ranges_len;
} OtlpReadOptions;

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

/**
 * @brief Get the Arrow schema for a signal type under an output profile.
 *
 * Identical to otlp_get_schema() for OTLP_ARROW_PROFILE_CANONICAL. The schema
 * matches the batches the same profile produces.
 */
OtlpStatus otlp_get_schema_with_profile(
    OtlpSignalType signal_type,
    OtlpArrowProfile profile,
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
 * @brief otlp_transform() with an output profile.
 *
 * Identical to otlp_transform() for OTLP_ARROW_PROFILE_CANONICAL. Returns
 * OTLP_ERROR_PARSE_FAILED (nothing written) if a value cannot be represented
 * in the profile.
 */
OtlpStatus otlp_transform_with_profile(
    OtlpSignalType signal_type,
    OtlpInputFormat format,
    OtlpArrowProfile profile,
    const uint8_t* data,
    size_t len,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema
);

/**
 * @brief otlp_transform_with_profile() narrowed to the columns and rows a
 *        reader needs.
 *
 * Unlike otlp_transform(), every signal type is accepted: a metric signal type
 * builds only that metric shape (the others are not built at all), and returns
 * an empty batch (length 0) when the input holds none of it.
 *
 * @param options NULL for every column of every row
 * @return OTLP_ERROR_INVALID_ARGUMENT if options does not fit the signal's
 *         schema (a column past it, a range on a non-timestamp column);
 *         otherwise as otlp_transform_with_profile()
 *
 * @note Caller must call out_array->release() and out_schema->release().
 */
OtlpStatus otlp_transform_with_options(
    OtlpSignalType signal_type,
    OtlpInputFormat format,
    OtlpArrowProfile profile,
    const OtlpReadOptions* options,
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
 * @note out_batches is fully overwritten, so it need not be initialized. On
 *       any non-OK status every present is 0 and every counter is 0.
 * @note For each output with present != 0, caller must release array and schema.
 * @note Outputs with present == 0 must not be released.
 */
OtlpStatus otlp_transform_metrics_all(
    OtlpInputFormat format,
    const uint8_t* data,
    size_t len,
    OtlpMetricsArrowBatches* out_batches
);

/**
 * @brief otlp_transform_metrics_all() with an output profile.
 *
 * Identical to otlp_transform_metrics_all() for OTLP_ARROW_PROFILE_CANONICAL.
 * Returns OTLP_ERROR_PARSE_FAILED with no batch present if a value cannot be
 * represented in the profile.
 */
OtlpStatus otlp_transform_metrics_all_with_profile(
    OtlpInputFormat format,
    OtlpArrowProfile profile,
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
 * Poisoning: any non-OK return poisons the decoder, because the failed message
 * may have advanced some of its stream state. Every later decode on it returns
 * OTLP_ERROR_PARSE_FAILED; free it and start a new decoder for a new stream.
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
 * @brief Create a new stateful OTAP decoder whose output uses a profile.
 *
 * Every batch the decoder returns (logs, traces, every metric shape) is
 * re-typed for profile; identical to otlp_otap_decoder_new() for
 * OTLP_ARROW_PROFILE_CANONICAL. A value the profile cannot represent fails
 * the decode with OTLP_ERROR_PARSE_FAILED (see the Poisoning note above).
 *
 * @return Non-null handle owned by the caller; release with
 *         otlp_otap_decoder_free().
 */
OtlpOtapDecoder* otlp_otap_decoder_new_with_profile(OtlpArrowProfile profile);

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
 *       decoder is poisoned (see the Poisoning note above).
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
 * @note out_batches is fully overwritten, so it need not be initialized. On
 *       any non-OK status every present is 0 and every counter is 0.
 * @note For each output with present != 0, caller must release array and schema.
 * @note Outputs with present == 0 must not be released.
 */
OtlpStatus otlp_otap_decode_metrics(
    OtlpOtapDecoder* decoder,
    const uint8_t* data,
    size_t len,
    OtlpMetricsArrowBatches* out_batches
);

/**
 * @brief Decode one OTAP message of one signal (for metrics, one shape),
 *        narrowed to the columns and rows a reader needs.
 *
 * The whole message is decoded either way, so the decoder's stream state
 * stays complete for later messages. Options, empty metric shapes and
 * statuses are as for otlp_transform_with_options().
 *
 * @note Caller must call out_array->release() and out_schema->release().
 */
OtlpStatus otlp_otap_decode_with_options(
    OtlpOtapDecoder* decoder,
    OtlpSignalType signal_type,
    const OtlpReadOptions* options,
    const uint8_t* data,
    size_t len,
    struct ArrowArray* out_array,
    struct ArrowSchema* out_schema
);

/* ============================================================================
 * Native Ingest Server (Cargo feature "server")
 *
 * One listener and runtime for OTLP/gRPC, OTAP/gRPC and OTLP/HTTP.
 * ============================================================================ */

typedef enum OtlpIngestStatus {
    OTLP_INGEST_OK = 0,                 /**< Buffered */
    OTLP_INGEST_RESOURCE_EXHAUSTED = 1, /**< Backpressure */
    OTLP_INGEST_INVALID = 2,            /**< Payload the host cannot accept */
    OTLP_INGEST_INTERNAL = 3,           /**< Host-side failure */
} OtlpIngestStatus;

/** OR these service bits together. Zero serves all three on one port. */
#define OTLP_INGEST_SERVE_OTLP_GRPC 1u /**< {Logs,Trace,Metrics}Service unary Export */
#define OTLP_INGEST_SERVE_OTAP_GRPC 2u /**< Arrow{Logs,Traces,Metrics}Service streams */
#define OTLP_INGEST_SERVE_OTLP_HTTP 4u /**< POST /v1/{logs,traces,metrics}, GET /healthz, /readyz */

/** One decoded Arrow batch, borrowed for the message callback. */
typedef struct OtlpIngestBatch {
    OtlpSignalType signal_type;
    struct ArrowArray* array;
    struct ArrowSchema* schema;
} OtlpIngestBatch;

/**
 * Called once per decoded OTLP Export, OTAP BatchArrowRecords, or HTTP POST.
 * Logs and traces each pass one batch. Metrics pass every present shape in
 * gauge, sum, histogram, exponential histogram order. If no shapes are
 * present, batch_count is zero and batches is NULL.
 *
 * input_bytes is the encoded gRPC message size or decompressed HTTP body size.
 * The server releases the Arrow arrays and schemas after the callback returns.
 * Copy any data needed later. The host must never release these structures or
 * release copies of them. Callbacks can run concurrently on server worker
 * threads. Catch host exceptions before returning through the C ABI.
 *
 * On a refusal, the host may write NUL-terminated UTF-8 into the zero-filled
 * detail buffer. The server uses it as the gRPC status message, OTAP
 * status_message, or HTTP error message. Empty detail uses a default message.
 */
typedef OtlpIngestStatus (*OtlpIngestCallback)(
    void* user_data,
    uint64_t input_bytes,
    const OtlpIngestBatch* batches,
    size_t batch_count,
    char* detail,
    size_t detail_len
);

/** Non-zero means ready. Zero may include a NUL-terminated UTF-8 reason. */
typedef int (*OtlpReadyCallback)(void* user_data, char* reason, size_t reason_len);

typedef struct OtlpIngestServerConfig {
    void* user_data;
    OtlpIngestCallback on_message;  /**< Required */
    OtlpReadyCallback on_ready;     /**< NULL means always ready until shutdown */
    const char* auth_token;         /**< NULL disables authentication */
    size_t auth_token_len;
    uint32_t services;              /**< OTLP_INGEST_SERVE_* bits, zero enables all */
    OtlpArrowProfile profile;
    uint64_t max_message_bytes;     /**< Zero uses 4 MiB */
} OtlpIngestServerConfig;

typedef struct OtlpIngestServer OtlpIngestServer;

/**
 * Start the server on a UTF-8 "host:port" address. The socket is bound before
 * the call returns. On failure, returns NULL and writes a truncated,
 * NUL-terminated message when err_buf and its size are provided.
 *
 * If provided, the token must be non-empty UTF-8 and is copied at start.
 * Ingest requests accept either Authorization: Bearer <token> (the scheme is
 * case-insensitive) or x-api-key: <token>. Authentication precedes body decoding
 * and failures return gRPC UNAUTHENTICATED or HTTP 401.
 *
 * The profile applies to every batch. If it cannot represent a batch, gRPC
 * returns INVALID_ARGUMENT or HTTP returns 400 without invoking on_message.
 * The message limit applies per gRPC message and to both the received and
 * decompressed HTTP body. The callbacks and user_data must remain valid until
 * otlp_ingest_server_stop() returns.
 *
 * OTLP/HTTP POST accepts protobuf, JSON, and newline-delimited JSON, with
 * identity, gzip, or zlib-wrapped deflate encoding. Responses use JSON.
 * Success returns 202 and {"status":"buffered","rows":<r>,"batches":<b>},
 * where batches counts batches with rows. Metrics add a "skipped" object with
 * summaries, nan_values, infinity_values and missing_values.
 *
 * Errors have "error" and "message" fields. Unsupported media returns 415,
 * oversized bodies 413, and malformed payloads 400. Host statuses map to
 * gRPC RESOURCE_EXHAUSTED, INVALID_ARGUMENT or INTERNAL and to HTTP 503,
 * 400 or 500 respectively. Missing HTTP routes return 404.
 *
 * Health checks are unauthenticated. GET /healthz returns 200 and
 * {"status":"ok"}. GET /readyz returns 200 and {"status":"ready"}, or
 * 503 and {"status":"degraded","reason":"<reason>"}. The default degraded
 * reason is "not ready", or "server is stopping" during shutdown.
 */
OtlpIngestServer* otlp_ingest_server_start(
    const char* bind_addr, size_t bind_addr_len,
    const OtlpIngestServerConfig* config,
    char* err_buf, size_t err_buf_len
);

/**
 * Stop accepting connections and drain in-flight requests for up to
 * drain_deadline_ms. Then close callback admission and wait for any running
 * callbacks, even beyond that deadline. After this returns, user_data may be
 * freed. Shutdown refusals are gRPC UNAVAILABLE or HTTP 503.
 *
 * A NULL server is a no-op. Repeated stops are safe. When called inside
 * a callback, stop returns immediately and the server keeps running.
 */
void otlp_ingest_server_stop(OtlpIngestServer* server, uint64_t drain_deadline_ms);

/**
 * Free a server handle, stopping it first if it is still running. When called
 * inside a callback, free returns immediately and shutdown completes after
 * the callback returns. A NULL server is a no-op.
 */
void otlp_ingest_server_free(OtlpIngestServer* server);

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
