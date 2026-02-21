#pragma once

#include "gflags/gflags.h"

DECLARE_bool(email_index_evaluate_deduplication);
DECLARE_bool(email_clear_cache_before_expr);
DECLARE_string(email_workload_config_path);
DECLARE_string(email_data_path);
DECLARE_uint64(email_exec_seconds);
DECLARE_bool(email_benchmark_fstat);
DECLARE_bool(email_random_payload);
DECLARE_uint32(email_record_count);
DECLARE_double(email_zipf_theta);
DECLARE_uint32(email_read_ratio);
DECLARE_uint64(email_exec_seconds);
DECLARE_uint64(email_payload_size);
DECLARE_uint64(email_payload_size_align);
DECLARE_uint64(email_max_payload_size);