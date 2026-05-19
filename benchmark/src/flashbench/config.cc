#include "benchmark/flashbench/config.h"

/* SSD settings */
DEFINE_uint32(op_pct, 7, "Overprovisioning space ratio in percentage (%)");

/* workload settings */
DEFINE_uint64(exec_seconds, 10800, "Execution time");
DEFINE_uint32(exec_writes_pct, 1000,
              "Execute benchmark until n(% ) of the SSD capacity is written");
DEFINE_uint64(max_dataset_size_gb, 200, "maximum dataset size in GB");

/* workload settings */
DEFINE_string(workload_pattern, "zipf", "uniform zipf zone sequential..");
DEFINE_string(
    zone_pattern, "",
    "define workload pattern per zone if workload pattern is zone"); // e.g.,
                                                                     // s0.5
                                                                     // f0.5
                                                                     // uniform
                                                                     // s0.5
                                                                     // f0.5
                                                                     // uniform
DEFINE_string(space_pct_access_freq_pct_access_type, "s50f90rand,s50f10rand",
              "space pct total should be 100"
              "access frequency total should be 100"
              "access type should be either rand, seq, or mixed");
DEFINE_bool(shuffle_space_manager, false,
            "shuffle pids from same region to nontiguous LBA range");
DEFINE_bool(growing_workload, false, "false if dataset size doesn't grow");
DEFINE_double(zipf_factor, 0.6,
              "The zipfian dist's theta parameter (for access skew)");
DEFINE_uint32(read_ratio, 0, "read ratio of the workload in percentage");
DEFINE_uint64(
    payload_size, 1200,
    "Size of key-value payload. Only support 120 bytes or 4KB (default)");
DEFINE_uint64(payload_size_align, 1024,
              "All generated payload sizes align to this number");
DEFINE_uint64(initial_dataset_size_gb, 5, "Initial dataset size");
