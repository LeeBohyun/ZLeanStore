#pragma once

#include "gflags/gflags.h"

DECLARE_bool(url_index_evaluate_deduplication);
DECLARE_bool(url_clear_cache_before_expr);
DECLARE_string(url_workload_config_path);
DECLARE_string(url_data_path);
DECLARE_uint64(url_exec_seconds);
DECLARE_bool(url_benchmark_fstat);
DECLARE_bool(url_random_payload);
DECLARE_uint32(url_record_count);
DECLARE_double(url_zipf_theta);
DECLARE_uint32(url_read_ratio);
DECLARE_uint64(url_exec_seconds);
DECLARE_uint64(url_payload_size);
DECLARE_uint64(url_payload_size_align);
DECLARE_uint64(url_max_payload_size);