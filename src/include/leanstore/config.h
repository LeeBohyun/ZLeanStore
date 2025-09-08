#pragma once

#include "gflags/gflags.h"
#include "share_headers/config.h"

DECLARE_string(exmap_path);
DECLARE_string(wal_path);
// -------------------------------------------------------------------------------------
DECLARE_uint32(worker_count);
DECLARE_uint32(page_provider_thread);
DECLARE_bool(worker_pin_thread);
DECLARE_bool(use_exmap);
// -------------------------------------------------------------------------------------
DECLARE_uint64(bm_virtual_gb);
DECLARE_double(bm_physical_gb);
DECLARE_uint64(bm_wl_alias_mb);
DECLARE_uint64(bm_evict_batch_size);
DECLARE_uint32(bm_aio_qd);
DECLARE_bool(bm_enable_fair_eviction);

// -------------------------------------------------------------------------------------
DECLARE_bool(fp_enable);
// -------------------------------------------------------------------------------------
DECLARE_bool(wal_enable);
DECLARE_bool(wal_enable_rfa);
DECLARE_bool(wal_debug);
DECLARE_bool(wal_fsync);
DECLARE_uint32(wal_max_qd);
DECLARE_uint64(max_wal_capacity_gb);
// -------------------------------------------------------------------------------------
DECLARE_bool(txn_debug);
// -----------------------------------------------------------------------------------
DECLARE_bool(enable_blob);
DECLARE_bool(blob_tail_extent);
DECLARE_bool(blob_normal_buffer_pool);
DECLARE_uint32(blob_log_segment_size);
DECLARE_int32(blob_logging_variant);
DECLARE_int32(blob_indexing_variant);
DECLARE_uint32(zstd_compress_level);
DECLARE_uint32(lz4_compress_acceleration);
// -----------------------------------------------------------------------------------
DECLARE_bool(enable_checkpoint);
DECLARE_uint32(checkpointer_cnt);
DECLARE_bool(enable_doublewrite);
// -----------------------------------------------------------------------------------
DECLARE_uint64(max_ssd_capacity_gb);
DECLARE_bool(use_out_of_place_write);
DECLARE_uint32(block_size_mb);
DECLARE_uint64(max_open_block_cnt);
DECLARE_uint32(garbage_collector_cnt);
DECLARE_uint32(gc_victim_block_selection_algorithm);
DECLARE_bool(use_compression);
DECLARE_uint32(compression_algorithm);
DECLARE_bool(use_binpacking);
DECLARE_uint32(read_size);
DECLARE_bool(batch_writes);
DECLARE_uint32(write_buffer_partition_cnt);
DECLARE_bool(use_edt);
DECLARE_bool(invalidation_history_based_empty_block_selection);
// -----------------------------------------------------------------------------------
DECLARE_bool(use_trim);
DECLARE_string(user_pwd);
DECLARE_bool(use_ZNS);
DECLARE_bool(use_FDP);
DECLARE_bool(use_SSDWA1_pattern);
DECLARE_string(trace_file);
DECLARE_bool(measure_waf);
DECLARE_double(SSD_OP);
DECLARE_bool(simulator_mode);
DECLARE_string(simulator_SSD_gc);
DECLARE_uint32(simulator_SSD_gc_unit_mb);