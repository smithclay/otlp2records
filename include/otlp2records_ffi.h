#ifndef OTLP2RECORDS_FFI_H
#define OTLP2RECORDS_FFI_H

#include "stdint.h"
#include "stddef.h"

#ifdef __cplusplus
extern "C" {
#endif


/*
 Input format for OTLP data.

 C names: OTLP_FORMAT_AUTO, OTLP_FORMAT_PROTOBUF, etc.
 */
typedef enum OtlpInputFormat {
  /*
   Auto-detect format from content (C: OTLP_FORMAT_AUTO)
   */
  Auto = 0,
  /*
   Binary protobuf format (C: OTLP_FORMAT_PROTOBUF)
   */
  Protobuf = 1,
  /*
   JSON format (C: OTLP_FORMAT_JSON)
   */
  Json = 2,
  /*
   Newline-delimited JSON (C: OTLP_FORMAT_JSONL)
   */
  Jsonl = 3,
} OtlpInputFormat;

/*
 OTLP signal types supported by the parser.

 C names: OTLP_SIGNAL_LOGS, OTLP_SIGNAL_TRACES, etc.
 */
typedef enum OtlpSignalType {
  /*
   Log records (C: OTLP_SIGNAL_LOGS)
   */
  Logs = 0,
  /*
   Trace spans (C: OTLP_SIGNAL_TRACES)
   */
  Traces = 1,
  /*
   Gauge metrics (C: OTLP_SIGNAL_METRICS_GAUGE)
   */
  MetricsGauge = 2,
  /*
   Sum/counter metrics (C: OTLP_SIGNAL_METRICS_SUM)
   */
  MetricsSum = 3,
  /*
   Histogram metrics (C: OTLP_SIGNAL_METRICS_HISTOGRAM)
   */
  MetricsHistogram = 4,
  /*
   Exponential histogram metrics (C: OTLP_SIGNAL_METRICS_EXP_HISTOGRAM)
   */
  MetricsExpHistogram = 5,
} OtlpSignalType;

/*
 Status codes returned by FFI functions.

 C names: OTLP_OK, OTLP_ERROR_INVALID_ARGUMENT, etc.
 */
typedef enum OtlpStatus {
  /*
   Success (C: OTLP_OK)
   */
  Ok = 0,
  /*
   Invalid argument (C: OTLP_ERROR_INVALID_ARGUMENT)
   */
  InvalidArgument = 1,
  /*
   Parse failed (C: OTLP_ERROR_PARSE_FAILED)
   */
  ParseFailed = 2,
  /*
   Invalid format (C: OTLP_ERROR_INVALID_FORMAT)
   */
  InvalidFormat = 3,
  /*
   Out of memory (C: OTLP_ERROR_OUT_OF_MEMORY)
   */
  OutOfMemory = 4,
  /*
   Internal error (C: OTLP_ERROR_INTERNAL)
   */
  Internal = 5,
} OtlpStatus;

/*
 Opaque parser handle for streaming OTLP data.

 This handle maintains state for parsing OTLP data and producing Arrow batches.
 It is NOT thread-safe - use one handle per thread.
 */
typedef struct OtlpParserHandle OtlpParserHandle;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/*
 Create a new streaming parser for the given signal type.

 # Safety

 - `out_handle` must be a valid, non-null pointer to a pointer
 - On success, caller owns the handle and must call `otlp_parser_destroy()`

 # Returns

 `OTLP_OK` on success, error code otherwise.
 */
enum OtlpStatus otlp_parser_create(enum OtlpSignalType signal_type,
                                   enum OtlpInputFormat format,
                                   struct OtlpParserHandle **out_handle);

/*
 Destroy a parser handle and release all resources.

 # Safety

 - `handle` may be null (no-op in that case)
 - After this call, the handle pointer is invalid
 */
void otlp_parser_destroy(struct OtlpParserHandle *handle);

/*
 Push input bytes to the parser.

 # Safety

 - `handle` must be a valid parser handle
 - `data` must be valid for `len` bytes (or null if len is 0)
 - Caller retains ownership of `data` buffer

 # Arguments

 - `handle`: Parser handle
 - `data`: Pointer to input bytes (JSON or protobuf)
 - `len`: Length of input bytes
 - `is_final`: Non-zero if this is the last chunk (triggers parsing)

 # Returns

 `OTLP_OK` on success, error code otherwise.
 */
enum OtlpStatus otlp_parser_push(struct OtlpParserHandle *handle,
                                 const uint8_t *data,
                                 uintptr_t len,
                                 int is_final);

/*
 Export available batches as an ArrowArrayStream.

 # Safety

 - `handle` must be a valid parser handle
 - `out_stream` must be a valid pointer to FFI_ArrowArrayStream
 - Caller must call `out_stream->release()` when done
 - Stream is valid until next `push()` or `destroy()` on handle

 # Returns

 `OTLP_OK` on success, error code otherwise.
 Stream may yield 0 batches if no data was parsed.
 */
enum OtlpStatus otlp_parser_drain(struct OtlpParserHandle *handle,
                                  FFI_ArrowArrayStream *out_stream);

/*
 Get the Arrow schema for a signal type.

 # Safety

 - `out_schema` must be a valid pointer to FFI_ArrowSchema
 - Caller must call `out_schema->release()` when done

 # Returns

 `OTLP_OK` on success, error code otherwise.
 */
enum OtlpStatus otlp_get_schema(enum OtlpSignalType signal_type, FFI_ArrowSchema *out_schema);

/*
 Transform OTLP bytes to Arrow in one call (non-streaming).

 # Safety

 - `data` must be valid for `len` bytes
 - `out_array` and `out_schema` must be valid pointers
 - Caller must call `release()` on both out_array and out_schema

 # Returns

 `OTLP_OK` on success, error code otherwise.
 */
enum OtlpStatus otlp_transform(enum OtlpSignalType signal_type,
                               enum OtlpInputFormat format,
                               const uint8_t *data,
                               uintptr_t len,
                               FFI_ArrowArray *out_array,
                               FFI_ArrowSchema *out_schema);

/*
 Get the last error message for a parser handle.

 # Safety

 - `handle` may be null (returns null in that case)
 - Returned string is valid until next FFI call on same handle

 # Returns

 Error message string, or null if no error.
 */
const char *otlp_parser_last_error(const struct OtlpParserHandle *handle);

/*
 Get a static message for a status code.

 # Returns

 Static string describing the status (never null).
 */
const char *otlp_status_message(enum OtlpStatus status);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

#endif  /* OTLP2RECORDS_FFI_H */


#ifdef __cplusplus
}
#endif
