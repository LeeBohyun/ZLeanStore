#pragma once

#include "gflags/gflags.h"

DECLARE_bool(wikititle_index_evaluate_deduplication);
DECLARE_bool(wikititle_clear_cache_before_expr);
DECLARE_string(wikititle_workload_config_path);
DECLARE_string(wikititle_data_path);
DECLARE_uint64(wikititle_exec_seconds);
DECLARE_bool(wikititle_benchmark_fstat);
DECLARE_bool(wikititle_random_payload);
DECLARE_uint32(wikititle_record_count);
DECLARE_double(wikititle_zipf_theta);
DECLARE_uint32(wikititle_read_ratio);
DECLARE_uint64(wikititle_exec_seconds);
DECLARE_uint64(wikititle_payload_size);
DECLARE_uint64(wikititle_payload_size_align);
DECLARE_uint64(wikititle_max_payload_size);