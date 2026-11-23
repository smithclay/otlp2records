#include "otlp2records.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(cond)                                                              \
    do {                                                                         \
        if (!(cond)) {                                                           \
            fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
            failures++;                                                          \
        }                                                                        \
    } while (0)

typedef struct {
    uint8_t* data;
    size_t len;
} Bytes;

static Bytes read_file(const char* root, const char* relative) {
    char path[4096];
    snprintf(path, sizeof path, "%s/%s", root, relative);
    FILE* f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "cannot open %s\n", path);
        exit(2);
    }
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    Bytes bytes = {malloc((size_t)len), (size_t)len};
    if (fread(bytes.data, 1, bytes.len, f) != bytes.len) {
        fprintf(stderr, "short read %s\n", path);
        exit(2);
    }
    fclose(f);
    return bytes;
}

static const char* child_format(const struct ArrowSchema* schema, const char* name) {
    for (int64_t i = 0; i < schema->n_children; i++) {
        if (strcmp(schema->children[i]->name, name) == 0) {
            return schema->children[i]->format;
        }
    }
    return NULL;
}

static void release_batches(OtlpMetricsArrowBatches* out, int* present_count) {
    OtlpArrowBatch* shapes[4] = {&out->gauge, &out->sum, &out->histogram, &out->exp_histogram};
    *present_count = 0;
    for (int i = 0; i < 4; i++) {
        if (shapes[i]->present) {
            (*present_count)++;
            CHECK(shapes[i]->array.length > 0);
            CHECK(shapes[i]->schema.n_children > 0);
            shapes[i]->array.release(&shapes[i]->array);
            shapes[i]->schema.release(&shapes[i]->schema);
        }
    }
}

static void test_status_messages(void) {
    CHECK(strcmp(otlp_status_message(OTLP_OK), "Success") == 0);
    CHECK(strcmp(otlp_status_message(OTLP_ERROR_INVALID_ARGUMENT), "Invalid argument") == 0);
    CHECK(strcmp(otlp_status_message(OTLP_ERROR_PARSE_FAILED), "Parse failed") == 0);
    CHECK(strcmp(otlp_status_message(OTLP_ERROR_INTERNAL), "Internal error") == 0);
}

static void test_schemas(void) {
    for (int signal = OTLP_SIGNAL_LOGS; signal <= OTLP_SIGNAL_METRICS_EXP_HISTOGRAM; signal++) {
        struct ArrowSchema schema;
        CHECK(otlp_get_schema((OtlpSignalType)signal, &schema) == OTLP_OK);
        CHECK(schema.n_children > 0);
        schema.release(&schema);
    }
    struct ArrowSchema canonical, duckdb;
    CHECK(otlp_get_schema_with_profile(OTLP_SIGNAL_TRACES, OTLP_ARROW_PROFILE_CANONICAL, &canonical) == OTLP_OK);
    CHECK(otlp_get_schema_with_profile(OTLP_SIGNAL_TRACES, OTLP_ARROW_PROFILE_DUCKDB, &duckdb) == OTLP_OK);
    CHECK(strcmp(child_format(&canonical, "trace_id"), "w:16") == 0);
    CHECK(strcmp(child_format(&duckdb, "trace_id"), "u") == 0);
    canonical.release(&canonical);
    duckdb.release(&duckdb);
    CHECK(otlp_get_schema(OTLP_SIGNAL_LOGS, NULL) == OTLP_ERROR_INVALID_ARGUMENT);
}

static void test_one_shot(const char* root) {
    Bytes logs = read_file(root, "testdata/logs_large.pb");
    struct ArrowArray array;
    struct ArrowSchema schema;
    CHECK(otlp_transform(OTLP_SIGNAL_LOGS, OTLP_FORMAT_PROTOBUF, logs.data, logs.len, &array, &schema) == OTLP_OK);
    CHECK(array.length > 0);
    CHECK(array.n_children == schema.n_children);
    array.release(&array);
    schema.release(&schema);

    Bytes traces = read_file(root, "testdata/traces_large.pb");
    CHECK(otlp_transform_with_profile(OTLP_SIGNAL_TRACES, OTLP_FORMAT_AUTO, OTLP_ARROW_PROFILE_DUCKDB,
                                      traces.data, traces.len, &array, &schema) == OTLP_OK);
    CHECK(array.length > 0);
    CHECK(strcmp(child_format(&schema, "span_id"), "u") == 0);
    array.release(&array);
    schema.release(&schema);

    CHECK(otlp_transform(OTLP_SIGNAL_METRICS_GAUGE, OTLP_FORMAT_PROTOBUF, logs.data, logs.len, &array, &schema) ==
          OTLP_ERROR_INVALID_ARGUMENT);
    static const uint8_t garbage[] = {0xff, 0xff, 0xff, 0xff, 0x0f};
    CHECK(otlp_transform(OTLP_SIGNAL_LOGS, OTLP_FORMAT_PROTOBUF, garbage, sizeof garbage, &array, &schema) ==
          OTLP_ERROR_PARSE_FAILED);
    CHECK(otlp_transform(OTLP_SIGNAL_LOGS, OTLP_FORMAT_PROTOBUF, NULL, 0, &array, &schema) ==
          OTLP_ERROR_INVALID_ARGUMENT);
    free(logs.data);
    free(traces.data);
}

static void test_metrics_all(const char* root) {
    OtlpMetricsArrowBatches out;
    int present = 0;

    Bytes mixed = read_file(root, "testdata/metrics_mixed.pb");
    memset(&out, 0xab, sizeof out);
    CHECK(otlp_transform_metrics_all(OTLP_FORMAT_PROTOBUF, mixed.data, mixed.len, &out) == OTLP_OK);
    release_batches(&out, &present);
    CHECK(present >= 2);

    memset(&out, 0xab, sizeof out);
    CHECK(otlp_transform_metrics_all_with_profile(OTLP_FORMAT_PROTOBUF, OTLP_ARROW_PROFILE_DUCKDB, mixed.data,
                                                  mixed.len, &out) == OTLP_OK);
    if (out.gauge.present) {
        CHECK(strcmp(child_format(&out.gauge.schema, "flags"), "i") == 0);
    }
    release_batches(&out, &present);
    CHECK(present >= 2);

    Bytes summary = read_file(root, "testdata/metrics_summary.pb");
    memset(&out, 0xab, sizeof out);
    CHECK(otlp_transform_metrics_all(OTLP_FORMAT_PROTOBUF, summary.data, summary.len, &out) == OTLP_OK);
    CHECK(out.skipped_summaries > 0);
    CHECK(out.skipped_nan_values == 0);
    release_batches(&out, &present);
    CHECK(present == 0);

    static const uint8_t garbage[] = {0xff, 0xff, 0xff, 0xff, 0x0f};
    memset(&out, 0xab, sizeof out);
    CHECK(otlp_transform_metrics_all(OTLP_FORMAT_PROTOBUF, garbage, sizeof garbage, &out) ==
          OTLP_ERROR_PARSE_FAILED);
    CHECK(out.gauge.present == 0 && out.sum.present == 0 && out.histogram.present == 0 &&
          out.exp_histogram.present == 0);
    CHECK(out.skipped_summaries == 0);
    free(mixed.data);
    free(summary.data);
}

static void test_otap(const char* root) {
    Bytes initial = read_file(root, "tests/fixtures/otap/logs-initial.bar");
    Bytes reuse = read_file(root, "tests/fixtures/otap/logs-reuse.bar");
    struct ArrowArray array;
    struct ArrowSchema schema;

    OtlpOtapDecoder* decoder = otlp_otap_decoder_new();
    CHECK(decoder != NULL);
    CHECK(otlp_otap_decode_logs(decoder, initial.data, initial.len, &array, &schema) == OTLP_OK);
    CHECK(array.length > 0);
    array.release(&array);
    schema.release(&schema);
    CHECK(otlp_otap_decode_logs(decoder, reuse.data, reuse.len, &array, &schema) == OTLP_OK);
    array.release(&array);
    schema.release(&schema);
    otlp_otap_decoder_free(decoder);

    decoder = otlp_otap_decoder_new_with_profile(OTLP_ARROW_PROFILE_DUCKDB);
    CHECK(otlp_otap_decode_logs(decoder, reuse.data, reuse.len, &array, &schema) == OTLP_ERROR_PARSE_FAILED);
    CHECK(otlp_otap_decode_logs(decoder, initial.data, initial.len, &array, &schema) == OTLP_ERROR_PARSE_FAILED);
    otlp_otap_decoder_free(decoder);
    otlp_otap_decoder_free(NULL);

    Bytes metrics = read_file(root, "tests/fixtures/otap/metrics-initial.bar");
    OtlpMetricsArrowBatches out;
    int present = 0;
    decoder = otlp_otap_decoder_new();
    memset(&out, 0xab, sizeof out);
    CHECK(otlp_otap_decode_metrics(decoder, metrics.data, metrics.len, &out) == OTLP_OK);
    release_batches(&out, &present);
    CHECK(present > 0);
    otlp_otap_decoder_free(decoder);

    free(initial.data);
    free(reuse.data);
    free(metrics.data);
}

static OtlpIngestStatus never_called(void* user_data, OtlpSignalType signal, uint64_t stream_id, int64_t batch_id,
                                     uint64_t input_bytes, struct ArrowArray* array, struct ArrowSchema* schema) {
    (void)user_data, (void)signal, (void)stream_id, (void)batch_id, (void)input_bytes, (void)array, (void)schema;
    return OTLP_INGEST_INTERNAL;
}

static void test_grpc_lifecycle(void) {
    char err[256];
    const char* bad = "256.0.0.1:1";
    memset(err, 0, sizeof err);
    CHECK(otlp_grpc_server_start(bad, strlen(bad), NULL, NULL, NULL, 0, 0, err, sizeof err) == NULL);
    CHECK(strcmp(err, "on_batch callback is null") == 0);

    memset(err, 0, sizeof err);
    CHECK(otlp_grpc_server_start(bad, strlen(bad), never_called, NULL, NULL, OTLP_GRPC_SERVICE_OTLP_UNARY, 0, err,
                                 sizeof err) == NULL);
    CHECK(strncmp(err, "failed to bind", strlen("failed to bind")) == 0);

    memset(err, 0, sizeof err);
    CHECK(otlp_grpc_server_start_with_profile(bad, strlen(bad), never_called, NULL, NULL, 0, 1 << 20,
                                              OTLP_ARROW_PROFILE_DUCKDB, err, 4) == NULL);
    CHECK(strcmp(err, "fai") == 0);

    const char* any = "127.0.0.1:0";
    OtlpGrpcServer* server = otlp_grpc_server_start_with_profile(
        any, strlen(any), never_called, NULL, NULL, OTLP_GRPC_SERVICE_OTAP_ARROW, 1 << 20, OTLP_ARROW_PROFILE_DUCKDB,
        err, sizeof err);
    CHECK(server != NULL);
    otlp_grpc_server_stop(server, 100);
    otlp_grpc_server_stop(server, 100);
    otlp_grpc_server_free(server);
    otlp_grpc_server_stop(NULL, 0);
    otlp_grpc_server_free(NULL);

    server = otlp_grpc_server_start(any, strlen(any), never_called, NULL, NULL, 0, 0, err, sizeof err);
    CHECK(server != NULL);
    otlp_grpc_server_free(server);
}

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <repository root>\n", argv[0]);
        return 2;
    }
    test_status_messages();
    test_schemas();
    test_one_shot(argv[1]);
    test_metrics_all(argv[1]);
    test_otap(argv[1]);
    test_grpc_lifecycle();
    if (failures) {
        fprintf(stderr, "%d C ABI check(s) failed\n", failures);
        return 1;
    }
    printf("C ABI: all checks passed\n");
    return 0;
}
