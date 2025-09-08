#pragma once

#include "common/constants.h"
#include "common/typedefs.h"

#include <atomic>

namespace leanstore::statistics {

extern std::atomic<u64> total_txn_completed;
extern std::atomic<u64> txn_processed;
extern std::atomic<u64> precommited_txn_processed[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> precommited_rfa_txn_processed[MAX_NUMBER_OF_WORKER];
extern std::atomic<u64> flash_writes;

namespace blob {
extern std::atomic<u64> blob_logging_io;
} // namespace blob

namespace storage {
extern std::atomic<u64> free_size;
}

namespace buffer {
extern std::atomic<u64> read_cnt;
extern std::atomic<u64> write_cnt;
extern std::atomic<u64> cum_total_writes_cnt;
extern std::atomic<u64> alloc_cnt;
extern std::atomic<u64> dealloc_cnt;
extern std::atomic<u64> buffer_hit_cnt;
extern std::atomic<u64> buffer_miss_cnt;
extern std::atomic<u64> gc_read_cnt;
extern std::atomic<u64> edt_gap;
extern std::atomic<u64> edt_cnt;
} // namespace buffer

namespace storage::space {
extern std::atomic<u64> total_reads;
extern std::atomic<u64> total_reads_cnt;              // == total written pages
extern std::atomic<u64> total_size_after_compression; //
extern std::atomic<u64> total_writes_cnt;             // == total written pages
extern std::atomic<u64> total_writes; // == evict_writes + checkpint_writes +
                                      // doublewrite_writes + gc_writes
extern std::atomic<u64> cum_total_writes;
extern std::atomic<u64> evict_writes;
extern std::atomic<u64> checkpoint_writes;
extern std::atomic<u64> doublewrite_writes;
// gc statistics
extern std::atomic<u64> gc_reads;
extern std::atomic<u64> gc_writes;
extern std::atomic<u64> gced_page_cnt;
extern std::atomic<u64> valid_page_cnt;
} // namespace storage::space

namespace recovery {
extern std::atomic<u64> gct_phase_1_ms;
extern std::atomic<u64> gct_phase_2_ms;
extern std::atomic<u64> gct_phase_3_ms;
extern std::atomic<u64> gct_write_bytes;
extern std::atomic<u64> log_write_bytes;
extern std::atomic<u64> cum_log_write_bytes;
} // namespace recovery

} // namespace leanstore::statistics
