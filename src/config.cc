#include "common/constants.h"
#include "gflags/gflags.h"

// -----------------------------------------------------------------------------------
/* System in general */
DEFINE_string(db_path, "/dev/nvme2n1", "Default atabase path");
DEFINE_uint64(max_ssd_capacity_gb, 512, "Maximum SSD fill capacity");
DEFINE_string(
    wal_path, "/dev/nvme1n1",
    "Default WAL path is same as the database path"); // empty means disable wal
                                                      // writes
DEFINE_uint32(worker_count, 8, "The number of workers");
DEFINE_uint32(page_provider_thread, 0, "Number of page provider threads");
DEFINE_bool(worker_pin_thread, false, "Pin worker to a specific thread");
DEFINE_bool(use_exmap, false, "true: use exmap interface");
DEFINE_string(exmap_path, "/dev/exmap0", "Default Exmap path");
// -----------------------------------------------------------------------
/* Buffer manager */
DEFINE_uint64(bm_virtual_gb, 64,
              "Size of virtual memory in GB"); // kioxia max 3560 // should be
                                               // less than SSD capacity
DEFINE_double(bm_physical_gb, 4.0,
              "Size of physical memory in GB"); // buffer pool size
DEFINE_uint64(bm_wl_alias_mb, 1024,
              "Size of worker-local mmap() for aliasing in MB");
DEFINE_uint64(bm_evict_batch_size, 128,
              "Expected number of pages to be evicted during each eviction");
DEFINE_bool(bm_enable_fair_eviction, true,
            "Whether to use fair extent eviction policy or not"
            "Fair eviction policy: Large extents are more likely to be evicted "
            "than small extents/pages");
// -----------------------------------------------------------------------------------
/* Free Page manager */
DEFINE_bool(fp_enable, false, "Whether to enable free page manager or not");
// -----------------------------------------------------------------------------------
/* Write-ahead logging and checkpointing */
DEFINE_bool(wal_enable, true, "Whether to enable WAL logging or not");
DEFINE_bool(wal_enable_rfa, true,
            "Whether to enable Remote-Flush-Avoidance or not");
DEFINE_bool(wal_debug, false, "Enable debugging for WAL ops");
DEFINE_bool(wal_fsync, false, "Force FSync for WAL");
DEFINE_uint32(wal_max_qd, 64, "Queue depth of WAL's aio backend");
DEFINE_uint64(max_wal_capacity_gb, 10, "WAL log capacity");
DEFINE_bool(enable_checkpoint, true,
            "true: use checkpoint to truncate wal log file");
DEFINE_uint32(
    checkpointer_cnt, 1,
    "number of checkpoint threads"); // supports only one thread for now
// -----------------------------------------------------------------------------------
/* Double write buffering to gurantee recoverability upon- partial page write */
DEFINE_bool(enable_doublewrite, true,
            "true: ensure recovery upon partial page write");
// -----------------------------------------------------------------------------------
/* Transaction */
DEFINE_bool(txn_debug, false, "Enable debugging for transaction ops");
DEFINE_string(txn_default_isolation_level, "ru",
              "The serializable mode used in LeanStore"
              "(ru: READ_UNCOMMITTED, rc: READ_COMMITTED, si: "
              "SNAPSHOT_ISOLATION, ser: SERIALIZABLE)");
// -----------------------------------------------------------------------------------
/* BLOB */
DEFINE_bool(enable_blob, false, "Whethser to enable blob or not");
DEFINE_bool(blob_tail_extent, false, "Whether to enable Tail Extent or not");
DEFINE_bool(
    blob_normal_buffer_pool, false,
    "Extra overheads to emulate normal buffer pool"
    "1. *IMPORTANT* PageAliasGuard(): malloc() and memcpy() all the extents"
    "2. *IMPORTANT* Extra hashtable lookup on Buffer's ToPtr & Read op"
    "3. *IMPORTANT* Require chunked processing on large object operations"
    "4. GroupCommit::PrepareLargePageWrite: Write on 4KB granularity instead "
    "of extent granularity");

DEFINE_uint32(blob_log_segment_size, 1 * leanstore::KB,
              "If blob_logging_variant = -1, i.e. BLOB is written to WAL, the "
              "blob can be bigger than WAL buffer size."
              "In this case, LeanStore splits the BLOB into multiple segments, "
              "each is very small."
              "This flag is used to determine the size of those segments");
DEFINE_int32(blob_logging_variant, -1,
             "Which strategy to be used for writing blob to consecutive pages"
             "-1. BLOB is copied to buffer as normal DB pages, and also to WAL "
             "as in commercial DBMSes"
             "0. All pages are evicted after the transaction is committed"
             "1. BLOB is copied to those pages in buffer, and these pages are "
             "marked as UNLOCKED after commits"
             "2. BLOB is copie:/blockd to those pages in buffer, and these "
             "pages are marked as MARKED after commits");
DEFINE_int32(blob_indexing_variant, 0,
             "Which strategy to be used for indexing blob"
             "0. Disable BLOB indexing"
             "1. Use Blob State for indexing"
             "2. 1KB Prefix of BLOB is used for indexing");
/* Compression config for blob */
DEFINE_uint32(zstd_compress_level, 1,
              "The compression level of ZSTD used in LeanStore");
DEFINE_uint32(lz4_compress_acceleration, 1,
              "The acceleration value for LZ4_compress_fast call");
// -----------------------------------------------------------------------------------
/* Storage Layer */
/* Out-of-place write */
DEFINE_bool(use_out_of_place_write, false,
            "true: store pages out-of-place" // gc enabled automatically
            "false: store pages in place");
/* Block (i.e., GC unit) size */
DEFINE_uint32(block_size_mb, 4096, "Size of a single block");
/* The maximum number of blocks that can serve write requests  */
DEFINE_uint64(max_open_block_cnt, 0,
              "If it is set to 0, then it doesn't have a limit");
/* Garbage collection */
DEFINE_uint32(
    garbage_collector_cnt, 0, // used when allowing background gc
    "number of garbage collector threads when using out-of-place write");
DEFINE_uint32(
    gc_victim_block_selection_algorithm, 0,
    "0: Greedy Algorithm: selects the block with the most invalidated page"
    "1: EDT-based Algorithm: selects the block with the smallest death time"
    "2: Totally Random: selects the victim block randomly"
    "3: KGreedy (default: greedy-8)"
    "4: 2R-Greedy");
/* Use compression */
DEFINE_bool(use_compression, false,
            "true: use LZ4 compression upon out-of-place write"
            "false: don't compress pages upon out-of-place write");
DEFINE_uint32(compression_algorithm, 0,
              "0: LZ4 algorithm"
              "1: ZSTD algorithm"
              "2: Zlib algorithm");
DEFINE_bool(use_binpacking, false,
            "true: perform binpacking when compression is enabled"
            "false: don't perform binpacking when compression is enabled");
DEFINE_uint32(read_size, 4096, "read size, either 4KiB or 512 byte read");
DEFINE_uint32(write_buffer_partition_cnt, 1,
              "Number of blocks to place written pages per buffered writes");
/* Death time estimation */
DEFINE_bool(use_edt, false, "true: use edt algorithm to estimate death time");
/* Data placement -> to achieve uniform distribution */
DEFINE_bool(invalidation_history_based_empty_block_selection, false,
            "when selecting block to write, use block invalidation history to "
            "even out the writes to the all the blocks");
// -----------------------------------------------------------------------------------
/* IO backend configuration */
DEFINE_uint32(
    bm_aio_qd, 64,
    "Maximum number of concurrent I/O processed by backend's io_uring");
// -----------------------------------------------------------------------------------
/* SSD interface */
DEFINE_string(user_pwd, "", "User pwd for OCP command and trim command");
DEFINE_bool(use_trim, false, "use trim command");
DEFINE_bool(
    measure_waf, true,
    "true: measure device-level write amplification based on OCP results");
DEFINE_bool(use_ZNS, false, "true: use ZNS SSD as a storage device");
DEFINE_bool(use_FDP, false, "true: use FDP SSD as a storage device");
DEFINE_bool(use_SSDWA1_pattern, false, "true: use SSD1WA 1 magic pattern");
DEFINE_double(
    SSD_OP, 0.125,
    "0.1: assuming SSD has 12.5pct of the overprovisioning space internally");
DEFINE_bool(batch_writes, false, "batch writes into a single writes");
// -----------------------------------------------------------------------------------
/* log page traces */
DEFINE_string(trace_file, "", "trace file directory");
DEFINE_bool(simulator_mode, false, "SSD simulator mode");
DEFINE_string(simulator_SSD_gc, "greedy", "GC algorithm of SSD simulator");
DEFINE_uint32(simulator_SSD_gc_unit_mb, 4096,
              "GC granularity in SSD simulator");
