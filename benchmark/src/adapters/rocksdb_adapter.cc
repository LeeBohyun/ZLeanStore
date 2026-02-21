#include "benchmark/adapters/rocksdb_adapter.h"
#include "benchmark/ycsb/schema.h"

#include "common/typedefs.h"
#include "fmt/core.h"
#include "gflags/gflags.h"
#include "leanstore/config.h"
#include "share_headers/config.h"
#include "share_headers/logger.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include <cassert>
#include <rocksdb/perf_context.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <string>

#include <chrono>
#include <cstdio>
#include <inttypes.h>

#include <cassert>
#include <cstring>
#include <memory>
#include <string>

// Forward declaration: implemented elsewhere (used by WiredTiger already)
bool GetFSUsage(const std::string &path, double &used_gb, double &cap_gb,
                double &used_pct);

// ---------------------------------------------------------------------------
// Constructor: adapter uses externally-owned DB instance
// ---------------------------------------------------------------------------
template <class RecordBase>
RocksDBAdapter<RecordBase>::RocksDBAdapter(rocksdb::DB *db) : db_(db) {
  assert(db_ != nullptr);
}

// ---------------------------------------------------------------------------
// OpenRocksDB(): configure using FLAGS_*
// ---------------------------------------------------------------------------
rocksdb::DB *OpenRocksDB() {
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = false;

  // WAL directory from flag
  if (!FLAGS_wal_path.empty()) {
    opts.wal_dir = FLAGS_wal_path;
  }

  // Basic LSM / memtable tuning (you can replace with your FLAGS_* later)
  opts.write_buffer_size = 1342177280ULL; // 24 MiB
  opts.max_write_buffer_number = 1;
  opts.min_write_buffer_number_to_merge = 1;
  // opts.target_file_size_base = 8ULL* 1024 * 1024;
  opts.use_direct_io_for_flush_and_compaction = true;
  opts.use_direct_reads = true;
  opts.compaction_style = rocksdb::kCompactionStyleLevel;
  opts.max_background_compactions = 32;
  opts.target_file_size_multiplier = 1;

  // Table options (Bloom, block cache, etc.)
  rocksdb::BlockBasedTableOptions table_opts;
  table_opts.block_cache = rocksdb::NewLRUCache(2048ULL * 1024 * 1024); // 1 GiB
  opts.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_opts));

  // Statistics (optional but useful)
  opts.statistics = rocksdb::CreateDBStatistics();

  // Open DB at FLAGS_db_dir
  rocksdb::DB *raw_db = nullptr;
  auto s = rocksdb::DB::Open(opts, FLAGS_db_dir, &raw_db);
  if (!s.ok()) {
    std::fprintf(stderr, "Failed to open RocksDB at %s: %s\n",
                 FLAGS_db_dir.c_str(), s.ToString().c_str());
    std::exit(1);
  }
  return raw_db;
}

// ---------------------------------------------------------------------------
// Internal scan helper
// ---------------------------------------------------------------------------
template <class RecordBase>
void RocksDBAdapter<RecordBase>::ScanImpl(
    const KeyType &r_key, const FoundRecordFunc &found_record_cb,
    bool scan_ascending) {

  // Fold key like LeanStoreAdapter does
  u8 kbuf[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(kbuf, r_key);

  rocksdb::ReadOptions ro;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));

  rocksdb::Slice seek_key(reinterpret_cast<const char *>(kbuf), len);

  if (scan_ascending) {
    for (it->Seek(seek_key); it->Valid(); it->Next()) {
      auto ks = it->key();
      auto vs = it->value();

      typename RecordBase::Key typed_key;
      RecordBase::UnfoldKey(reinterpret_cast<const u8 *>(ks.data()), typed_key);

      const RecordBase &rec = *reinterpret_cast<const RecordBase *>(vs.data());

      if (!found_record_cb(typed_key, rec)) {
        break;
      }
    }
  } else {
    it->SeekForPrev(seek_key);
    for (; it->Valid(); it->Prev()) {
      auto ks = it->key();
      auto vs = it->value();

      typename RecordBase::Key typed_key;
      RecordBase::UnfoldKey(reinterpret_cast<const u8 *>(ks.data()), typed_key);

      const RecordBase &rec = *reinterpret_cast<const RecordBase *>(vs.data());

      if (!found_record_cb(typed_key, rec)) {
        break;
      }
    }
  }
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::Scan(const KeyType &key,
                                      const FoundRecordFunc &cb) {
  ScanImpl(key, cb, true);
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::ScanDesc(const KeyType &key,
                                          const FoundRecordFunc &cb) {
  ScanImpl(key, cb, false);
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::Insert(const KeyType &key,
                                        const RecordBase &record) {
  u8 kbuf[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(kbuf, key);

  rocksdb::Slice key_slice(reinterpret_cast<const char *>(kbuf), len);
  rocksdb::Slice value_slice(reinterpret_cast<const char *>(&record),
                             sizeof(RecordBase));

  auto s = db_->Put(rocksdb::WriteOptions(), key_slice, value_slice);
  assert(s.ok());
}

template <class RecordBase>
auto RocksDBAdapter<RecordBase>::LookUp(const KeyType &key,
                                        const AccessRecordFunc &fn) -> bool {
  u8 kbuf[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(kbuf, key);

  std::string out;
  auto s =
      db_->Get(rocksdb::ReadOptions(),
               rocksdb::Slice(reinterpret_cast<const char *>(kbuf), len), &out);
  if (!s.ok()) {
    if (s.IsNotFound())
      return false;
    return false;
  }

  RecordBase rec{};
  std::memcpy(&rec, out.data(), std::min(out.size(), sizeof(RecordBase)));
  fn(rec);
  return true;
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::Update(const KeyType &key,
                                        const RecordBase &record) {
  // Overwrite in RocksDB
  Insert(key, record);
}

template <class RecordBase>
void RocksDBAdapter<RecordBase>::UpdateInPlace(const KeyType &key,
                                               const ModifyRecordFunc &fn) {
  u8 kbuf[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(kbuf, key);

  std::string out;
  auto status =
      db_->Get(rocksdb::ReadOptions(),
               rocksdb::Slice(reinterpret_cast<const char *>(kbuf), len), &out);
  if (!status.ok()) {
    if (status.IsNotFound())
      return;
    return;
  }

  RecordBase rec{};
  std::memcpy(&rec, out.data(), std::min(out.size(), sizeof(RecordBase)));

  fn(rec);

  auto s = db_->Put(
      rocksdb::WriteOptions(),
      rocksdb::Slice(reinterpret_cast<const char *>(kbuf), len),
      rocksdb::Slice(reinterpret_cast<const char *>(&rec), sizeof(rec)));
  assert(s.ok());
}

template <class RecordBase>
auto RocksDBAdapter<RecordBase>::Erase(const KeyType &key) -> bool {
  u8 kbuf[RecordBase::MaxFoldLength()];
  auto len = RecordBase::FoldKey(kbuf, key);
  auto s =
      db_->Delete(rocksdb::WriteOptions(),
                  rocksdb::Slice(reinterpret_cast<const char *>(kbuf), len));
  return s.ok();
}

template <class RecordBase> auto RocksDBAdapter<RecordBase>::Count() -> u64 {
  rocksdb::ReadOptions ro;
  std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(ro));
  u64 cnt = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ++cnt;
  }
  return cnt;
}

template <class RecordBase>
auto RocksDBAdapter<RecordBase>::RelationSize() -> float {
  std::string v;
  if (db_->GetProperty("rocksdb.total-sst-files-size", &v)) {
    long long bytes = std::stoll(v);
    return static_cast<float>(bytes) / (1024.0f * 1024.0f); // MB
  }
  return 0.0f;
}

// Small helper to read a uint64 RocksDB property safely.
static uint64_t GetUInt64Property(rocksdb::DB *db, const std::string &name) {
  uint64_t v = 0;
  if (!db)
    return 0;
  if (db->GetIntProperty(name, &v)) {
    return v;
  }
  return 0;
}

// Helper to read a string property and parse it as uint64 if needed.
static uint64_t GetUInt64PropertyStr(rocksdb::DB *db, const std::string &name) {
  std::string s;
  if (!db)
    return 0;
  if (!db->GetProperty(name, &s))
    return 0;
  try {
    return static_cast<uint64_t>(std::stoull(s));
  } catch (...) {
    return 0;
  }
}

template <class RecordBase>
std::thread RocksDBAdapter<RecordBase>::StartProfilingThread(
    const std::string &system_name, std::atomic<bool> &is_loading,
    std::atomic<bool> &keep_running, std::atomic<uint64_t> &completed_txn) {

  return std::thread([this, system_name, &is_loading, &keep_running,
                      &completed_txn]() {
    try {
      uint64_t tick = 0;

      // We rely on RocksDB's statistics object (enabled in OpenRocksDB())
      auto *stats = db_->GetOptions().statistics.get();
      if (!stats) {
        std::fprintf(stderr,
                     "[RocksDB profiler] statistics not enabled; "
                     "no flush/compaction/bytes/WAL counters available.\n");
        return;
      }

      // Initial samples for per-second deltas
      uint64_t last_flush_bytes =
          stats->getTickerCount(rocksdb::FLUSH_WRITE_BYTES);
      uint64_t last_compact_bytes =
          stats->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
      uint64_t last_bytes_written =
          stats->getTickerCount(rocksdb::BYTES_WRITTEN);
      uint64_t last_wal_file_bytes =
          stats->getTickerCount(rocksdb::WAL_FILE_BYTES);

      // CSV header (added wal_bytes_s and wal_bytes_total)
      std::printf("system,ts,tx,"
                  "sst_GB,wal_GB,live_data_GB,"
                  "mem_active_MB,mem_all_MB,"
                  "flush_bytes_s,compact_bytes_s,bytes_written_s,wal_bytes_s,"
                  "write_amp_est,"
                  "flush_bytes_total,compact_bytes_total,wal_bytes_total,"
                  "running_flushes,running_compactions,"
                  "fs_used_GB,fs_cap_GB,fs_used_pct\n");

      uint64_t flush_bytes_total = 0;
      uint64_t compact_bytes_total = 0;
      uint64_t wal_file_bytes_total = 0;

      while (is_loading.load(std::memory_order_relaxed) ||
             keep_running.load(std::memory_order_relaxed)) {

        std::this_thread::sleep_for(std::chrono::seconds(1));

        // tx/sec
        auto tx = completed_txn.exchange(0, std::memory_order_relaxed);

        // ---- Size-related RocksDB properties ----
        uint64_t sst_bytes =
            GetUInt64PropertyStr(db_, "rocksdb.total-sst-files-size");
        double sst_gb = sst_bytes / (1024.0 * 1024.0 * 1024.0);

        uint64_t wal_bytes_prop =
            GetUInt64PropertyStr(db_, "rocksdb.total-log-size");
        double wal_gb = wal_bytes_prop / (1024.0 * 1024.0 * 1024.0);

        uint64_t live_bytes =
            GetUInt64PropertyStr(db_, "rocksdb.estimate-live-data-size");
        double live_gb = live_bytes / (1024.0 * 1024.0 * 1024.0);

        // Memtable sizes
        uint64_t mem_active_bytes =
            GetUInt64PropertyStr(db_, "rocksdb.cur-size-active-mem-table");
        uint64_t mem_all_bytes =
            GetUInt64PropertyStr(db_, "rocksdb.size-all-mem-tables");
        double mem_active_mb = mem_active_bytes / (1024.0 * 1024.0);
        double mem_all_mb = mem_all_bytes / (1024.0 * 1024.0);

        // ---- Flush / compaction / WAL / logical bytes, write amplification
        // ----
        uint64_t cur_flush_bytes =
            stats->getTickerCount(rocksdb::FLUSH_WRITE_BYTES);
        uint64_t cur_compact_bytes =
            stats->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
        uint64_t cur_bytes_written =
            stats->getTickerCount(rocksdb::BYTES_WRITTEN);
        uint64_t cur_wal_file_bytes =
            stats->getTickerCount(rocksdb::WAL_FILE_BYTES);

        std::fprintf(stderr,
                     "[DBG] flush_bytes=%" PRIu64 " compact_bytes=%" PRIu64
                     " bytes_written=%" PRIu64 " wal_file_bytes=%" PRIu64 "\n",
                     cur_flush_bytes, cur_compact_bytes, cur_bytes_written,
                     cur_wal_file_bytes);

        uint64_t d_flush_bytes = cur_flush_bytes - last_flush_bytes;
        uint64_t d_compact_bytes = cur_compact_bytes - last_compact_bytes;
        uint64_t d_bytes_written = cur_bytes_written - last_bytes_written;
        uint64_t d_wal_file_bytes = cur_wal_file_bytes - last_wal_file_bytes;

        last_flush_bytes = cur_flush_bytes;
        last_compact_bytes = cur_compact_bytes;
        last_bytes_written = cur_bytes_written;
        last_wal_file_bytes = cur_wal_file_bytes;

        flush_bytes_total += d_flush_bytes;
        compact_bytes_total += d_compact_bytes;
        wal_file_bytes_total += d_wal_file_bytes;

        double write_amp_est = 0.0;
        uint64_t physical_bytes = d_flush_bytes + d_compact_bytes;
        if (d_bytes_written > 0 && physical_bytes > 0) {
          write_amp_est = static_cast<double>(physical_bytes) /
                          static_cast<double>(d_bytes_written);
        }

        // ---- Background activity ----
        uint64_t running_flushes =
            GetUInt64Property(db_, "rocksdb.num-running-flushes");
        uint64_t running_compactions =
            GetUInt64Property(db_, "rocksdb.num-running-compactions");

        // ---- Filesystem usage ----
        double fs_used_gb = 0.0, fs_cap_gb = 0.0, fs_used_pct = 0.0;
        if (!FLAGS_db_dir.empty()) {
          GetFSUsage(FLAGS_db_dir, fs_used_gb, fs_cap_gb, fs_used_pct);
        }

        // ---- Print CSV line ----
        std::printf("%s,%" PRIu64 ",%lu,"
                    "%.4f,%.4f,%.4f,"
                    "%.2f,%.2f,"
                    "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%.4f,"
                    "%" PRIu64 ",%" PRIu64 ",%" PRIu64 ","
                    "%" PRIu64 ",%" PRIu64 ","
                    "%.4f,%.4f,%.2f\n",
                    system_name.c_str(), tick++, static_cast<unsigned long>(tx),
                    sst_gb, wal_gb, live_gb, mem_active_mb, mem_all_mb,
                    d_flush_bytes, d_compact_bytes, d_bytes_written,
                    d_wal_file_bytes, write_amp_est, flush_bytes_total,
                    compact_bytes_total, wal_file_bytes_total, running_flushes,
                    running_compactions, fs_used_gb, fs_cap_gb, fs_used_pct);

        std::fflush(stdout);
      }

      std::printf("Halt Profiling thread (RocksDB)\n");
    } catch (...) {
      std::printf("RocksDB profiling thread crashed\n");
    }
  });
}

// ---------------------------------------------------------------------------
// Explicit instantiations
// ---------------------------------------------------------------------------

// For YCSB fixed-size payload relations
template class RocksDBAdapter<ycsb::Relation<BytesPayload<120>, 0>>;
template class RocksDBAdapter<ycsb::Relation<BytesPayload<4096>, 0>>;
template class RocksDBAdapter<ycsb::Relation<BytesPayload<102400>, 0>>;
template class RocksDBAdapter<ycsb::Relation<BytesPayload<10485760>, 0>>;

// If you actually use BlobStateRelation with RocksDB, also instantiate:
template class RocksDBAdapter<ycsb::BlobStateRelation>;
