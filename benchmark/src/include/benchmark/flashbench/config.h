#pragma once

#include "gflags/gflags.h"

/* SSD settings */
DECLARE_uint32(op_pct);

/* benchmark settings */
DECLARE_uint64(exec_seconds);
DECLARE_uint32(exec_writes_pct);
DECLARE_uint64(max_dataset_size_gb);
/* workload settings */
DECLARE_string(workload_pattern);
DECLARE_string(zone_pattern);
DECLARE_bool(shuffle_space_manager);
DECLARE_bool(growing_workload);

DECLARE_double(zipf_factor);
DECLARE_uint32(read_ratio);
DECLARE_uint64(payload_size);
DECLARE_uint64(payload_size_align);
DECLARE_uint64(initial_dataset_size_gb);
