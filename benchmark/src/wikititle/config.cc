#include "benchmark/wikititle/config.h"

DEFINE_bool(wikititle_index_evaluate_deduplication, false,
            "Whether to check for BLOB deduplication during prefix indexing, "
            "i.e. FLAGS_blob_indexing_variant == 2");
DEFINE_bool(
    wikititle_clear_cache_before_expr, true,
    "Whether to clear the cache, i.e. page cache in OS or buffer pool in DB, "
    "before running the experiment");
DEFINE_string(
    wikititle_workload_config_path,
    "/home/lbh/leanstore-vmcache-main/benchmark/src/wikititle/summary.csv",
    "The CSV file which contains wikititle workload characteristics");
DEFINE_string(
    wikititle_data_path,
    "/home/lbh/leanstore-vmcache-main/benchmark/src/wikititle/wikititles.csv",
    "The file which contains all non-empty articles of enwikititle in json "
    "format");

DEFINE_bool(wikititle_benchmark_fstat, false,
            "Whether to use normal key-value benchmark or to use fstat() scan");
DEFINE_bool(wikititle_random_payload, false,
            "Whether to use random payload for wikititle");
DEFINE_uint32(wikititle_record_count, 10000000, "Number of initial records");
DEFINE_double(wikititle_zipf_theta, 0, "The zipfian dist's theta parameter");
DEFINE_uint32(wikititle_read_ratio, 100, "Read ratio");
DEFINE_uint64(wikititle_exec_seconds, 3600, "Execution time");
DEFINE_uint64(wikititle_payload_size, 64,
              "Size of key-value payload. Only support 120 bytes (default), "
              "4KB, 100KB, and 10MB");
DEFINE_uint64(wikititle_payload_size_align, 1024,
              "All generated payload sizes align to this number");
DEFINE_uint64(
    wikititle_max_payload_size, 10485760,
    "Maximum size of payload in case FLAGS_wikititle_random_payload==true");
