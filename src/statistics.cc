#include "leanstore/statistics.h"

namespace leanstore::statistics {

std::atomic<u64> total_txn_completed = 0;
std::atomic<u64> txn_processed = 0;
std::atomic<u64> precommited_txn_processed[MAX_NUMBER_OF_WORKER] = {0};
std::atomic<u64> precommited_rfa_txn_processed[MAX_NUMBER_OF_WORKER] = {0};
std::atomic<u64> flash_writes = 0;

namespace buffer {
std::atomic<u64> read_cnt = 0;
std::atomic<u64> write_cnt = 0;
std::atomic<u64> cum_total_writes_cnt = 0;
std::atomic<u64> alloc_cnt = 0;
std::atomic<u64> dealloc_cnt = 0;
std::atomic<u64> buffer_hit_cnt = 0;
std::atomic<u64> buffer_miss_cnt = 0;
std::atomic<u64> gc_read_cnt = 0;
std::atomic<u64> edt_gap = 0;
std::atomic<u64> edt_cnt = 0;
} // namespace buffer

namespace blob {
std::atomic<u64> blob_logging_io = 0;
} // namespace blob

namespace storage {
std::atomic<u64> free_size = 0;
}

namespace storage::space {
std::atomic<u64> total_reads = 0;
std::atomic<u64> total_reads_cnt = 0;
std::atomic<u64> total_size_after_compression = 0; //
std::atomic<u64> total_writes_cnt = 0;             // == total written pages
std::atomic<u64> total_writes =
    0; // == evict_writes + checkpint_writes + doublewrite_writes + gc_writes
std::atomic<u64> cum_total_writes = 0;
std::atomic<u64> evict_writes = 0;
std::atomic<u64> checkpoint_writes = 0;
std::atomic<u64> doublewrite_writes = 0;
std::atomic<u64> gc_writes = 0;
std::atomic<u64> gc_reads = 0;
// gc statistics
std::atomic<u64> gced_page_cnt = 0;  // Number of GCed pages
std::atomic<u64> valid_page_cnt = 0; // Number of GCed pages
} // namespace storage::space

namespace recovery {
std::atomic<u64> gct_phase_1_ms = 0;
std::atomic<u64> gct_phase_2_ms = 0;
std::atomic<u64> gct_phase_3_ms = 0;
std::atomic<u64> gct_write_bytes = 0;
std::atomic<u64> log_write_bytes = 0;
std::atomic<u64> cum_log_write_bytes = 0;
} // namespace recovery

} // namespace leanstore::statistics
