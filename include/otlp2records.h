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
