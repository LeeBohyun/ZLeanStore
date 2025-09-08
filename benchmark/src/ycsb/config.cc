#include "benchmark/ycsb/config.h"

DEFINE_bool(ycsb_benchmark_fstat, false,
            "Whether to use normal key-value benchmark or to use fstat() scan");
DEFINE_bool(ycsb_random_payload, false,
            "Whether to use random payload for YCSB");
DEFINE_uint64(ycsb_record_count, 18000000,  // 50GB
              "Number of initial records"); // 1 050 000 000 // 100000000,: 28GB
DEFINE_uint64(ycsb_dataset_size_gb, 10, "initial dataset size");
DEFINE_double(ycsb_zipf_theta, 0.6, "The zipfian dist's theta parameter");
DEFINE_uint32(ycsb_read_ratio, 20, "Read ratio in percentage");
DEFINE_uint64(ycsb_exec_seconds, 10800, "Execution time");
DEFINE_uint64(ycsb_payload_size, 120,
              "Size of key-value payload. Only support 120 bytes (default), "
              "4KB, 100KB, and 10MB");
DEFINE_uint64(ycsb_payload_size_align, 1024,
              "All generated payload sizes align to this number");
DEFINE_uint64(
    ycsb_max_payload_size, 10485760,
    "Maximum size of payload in case FLAGS_ycsb_random_payload==true");
