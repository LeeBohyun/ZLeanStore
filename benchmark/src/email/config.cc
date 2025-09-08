#include "benchmark/email/config.h"

DEFINE_bool(email_index_evaluate_deduplication, false,
            "Whether to check for BLOB deduplication during prefix indexing, "
            "i.e. FLAGS_blob_indexing_variant == 2");
DEFINE_bool(
    email_clear_cache_before_expr, true,
    "Whether to clear the cache, i.e. page cache in OS or buffer pool in DB, "
    "before running the experiment");
DEFINE_string(
    email_workload_config_path,
    "/home/lbh/leanstore-vmcache-main/benchmark/src/email/summary.csv",
    "The CSV file which contains email workload characteristics");
DEFINE_string(
    email_data_path,
    "/home/lbh/leanstore-vmcache-main/benchmark/src/email/shuf-email.csv",
    "The file which contains all non-empty articles of enemail in json format");

DEFINE_bool(email_benchmark_fstat, false,
            "Whether to use normal key-value benchmark or to use fstat() scan");
DEFINE_bool(email_random_payload, false,
            "Whether to use random payload for email");
DEFINE_uint32(email_record_count, 1000000, "Number of initial records");
DEFINE_double(email_zipf_theta, 0, "The zipfian dist's theta parameter");
DEFINE_uint32(email_read_ratio, 60, "Read ratio");
DEFINE_uint64(email_exec_seconds, 3600, "Execution time");
DEFINE_uint64(email_payload_size, 60,
              "Size of key-value payload. Only support 120 bytes (default), "
              "4KB, 100KB, and 10MB");
DEFINE_uint64(email_payload_size_align, 1024,
              "All generated payload sizes align to this number");
DEFINE_uint64(
    email_max_payload_size, 10485760,
    "Maximum size of payload in case FLAGS_email_random_payload==true");
