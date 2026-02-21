#include "benchmark/adapters/wiredtiger_adapter.h"
#include "benchmark/tpcc/schema.h"
#include "benchmark/utils/test_utils.h"
#include "benchmark/ycsb/schema.h"

#include <cassert>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <thread>

uint64_t GetConnStat(WT_SESSION *session, int stat_key) {
  WT_CURSOR *cursor = nullptr;
  int ret = session->open_cursor(
      session,
      "statistics:", // connection-level stats
      nullptr,
      "statistics=(fast)", // or "all" if you want everything
      &cursor);
  if (ret != 0)
    return 0;

  const char *desc, *pvalue;
  int64_t v = 0;
  cursor->set_key(cursor, stat_key);
  ret = cursor->search(cursor);
  if (ret == 0) {
    ret = cursor->get_value(cursor, &desc, &pvalue, &v);
  }
  cursor->close(cursor);
  if (ret != 0)
    return 0;
  return static_cast<uint64_t>(v);
}

uint64_t GetTableStat(WT_SESSION *session, const std::string &uri,
                      int stat_key) {
  WT_CURSOR *cursor = nullptr;
  int ret = session->open_cursor(session, ("statistics:" + uri).c_str(),
                                 nullptr, "statistics=(fast)", &cursor);
  if (ret != 0)
    return 0;

  const char *desc, *pvalue;
  int64_t v = 0;
  cursor->set_key(cursor, stat_key);
  ret = cursor->search(cursor);
  if (ret == 0) {
    ret = cursor->get_value(cursor, &desc, &pvalue, &v);
  }
  cursor->close(cursor);
  if (ret != 0)
    return 0;
  return static_cast<uint64_t>(v);
}

// Filesystem usage for the filesystem where FLAGS_db_path lives
bool GetFSUsage(const std::string &path, double &used_gb, double &capacity_gb,
                double &used_pct) {
  namespace fs = std::filesystem;
  std::error_code ec;
  auto sp = fs::space(path, ec);
  if (ec)
    return false;

  double cap = static_cast<double>(sp.capacity);
  double free = static_cast<double>(sp.free);
  double used = cap - free;

  capacity_gb = cap / (1024.0 * 1024.0 * 1024.0);
  used_gb = used / (1024.0 * 1024.0 * 1024.0);
  used_pct = (cap > 0.0) ? (used * 100.0 / cap) : 0.0;
  return true;
}

WiredTigerDB::WiredTigerDB(bool load) {
  const std::uint64_t cache_gb = FLAGS_bm_physical_gb;
  const std::uint64_t cache_bytes = cache_gb * 1024ull * 1024ull * 1024ull;
  std::string wal_dir = FLAGS_wal_path.c_str();

  std::string config_string = "create,"
                              "direct_io=[data,log,checkpoint],"
                              "cache_size=" +
                              std::to_string(cache_bytes) +
                              "," // <-- BYTES, no suffix
                              "log=(enabled=true,path=\"" +
                              wal_dir +
                              "\"),"
                              "statistics=(all),"
                              "statistics_log=(wait=1),"
                              "session_max=2000,"
                              "eviction=(threads_max=4)";

  if (load) {
    std::string cmd = "rm -rf " + FLAGS_db_dir + " && mkdir -p " + FLAGS_db_dir;
    int ret0 = system(cmd.c_str());
    ensure(ret0 == 0);
  } else {
    std::string cmd = "mkdir -p " + FLAGS_db_dir;
    int ret0 = system(cmd.c_str());
    ensure(ret0 == 0);
  }

  int ret = wiredtiger_open(FLAGS_db_dir.c_str(), nullptr,
                            config_string.c_str(), &conn);
  WTErrorCheck(ret);
}

WiredTigerDB::~WiredTigerDB() { conn->close(conn, nullptr); }

void WiredTigerDB::PrepareThread() {
  // Just open a default session; isolation handled per-transaction
  int ret = conn->open_session(conn, nullptr, nullptr, &session);
  WTErrorCheck(ret);
}

void WiredTigerDB::StartTransaction(bool /*si*/) {
  const auto &lvl = FLAGS_txn_default_isolation_level;
  const char *cfg = nullptr;

  if (lvl == "si") {
    cfg = "isolation=snapshot";
  } else if (lvl == "rc") {
    cfg = "isolation=read-committed";
  } else {
    // default: read-uncommitted
    cfg = "isolation=read-uncommitted";
  }

  int ret = session->begin_transaction(session, cfg);
  WTErrorCheck(ret);
}

void WiredTigerDB::CommitTransaction() {
  session->commit_transaction(session, nullptr);
}

void WiredTigerDB::CloseSession() { session->close(session, nullptr); }

// -------------------------------------------------------------------------------------
template <class RecordBase>
WiredTigerAdapter<RecordBase>::WiredTigerAdapter(WiredTigerDB &map)
    : map_(map) {
  relation_name_ = static_cast<std::string>(
      "table:tree_" + std::to_string(RecordBase::TYPE_ID));
  int ret = map_.session->create(map_.session, relation_name_.c_str(),
                                 "key_format=S,value_format=S,"
                                 "memory_page_max=10M,");
  WTErrorCheck(ret);
}

template <class RecordBase>
WiredTigerAdapter<RecordBase>::~WiredTigerAdapter() = default;

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::Scan(
    const typename RecordBase::Key &key,
    const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, key);
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret =
        map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr,
                                  "raw", &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  // -------------------------------------------------------------------------------------
  WT_ITEM key_item;
  WT_ITEM payload_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;
  // -------------------------------------------------------------------------------------
  cursor->set_key(cursor, &key_item);
  int exact;
  ret = cursor->search_near(cursor, &exact);
  if (exact < 0) {
    ret = cursor->next(cursor);
  }

  while (ret == 0) {
    cursor->get_key(cursor, &key_item);
    cursor->get_value(cursor, &payload_item);
    typename RecordBase::Key s_key;
    RecordBase::UnfoldKey(reinterpret_cast<const uint8_t *>(key_item.data),
                          s_key);
    const RecordBase &s_value =
        *reinterpret_cast<const RecordBase *>(payload_item.data);
    if (!found_record_cb(s_key, s_value)) {
      break;
    }
    ret = cursor->next(cursor);
  }
}

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::ScanDesc(
    const typename RecordBase::Key &key,
    const typename Adapter<RecordBase>::FoundRecordFunc &found_record_cb) {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, key);
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret =
        map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr,
                                  "raw", &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  // -------------------------------------------------------------------------------------
  WT_ITEM key_item;
  WT_ITEM payload_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;
  // -------------------------------------------------------------------------------------
  cursor->set_key(cursor, &key_item);
  int exact;
  ret = cursor->search_near(cursor, &exact);
  if (exact > 0) {
    ret = cursor->prev(cursor);
  }
  while (ret == 0) {
    cursor->get_key(cursor, &key_item);
    cursor->get_value(cursor, &payload_item);
    typename RecordBase::Key s_key;
    RecordBase::UnfoldKey(reinterpret_cast<const uint8_t *>(key_item.data),
                          s_key);
    const auto &s_value =
        *reinterpret_cast<const RecordBase *>(payload_item.data);
    if (!found_record_cb(s_key, s_value)) {
      break;
    }
    ret = cursor->prev(cursor);
  }
}

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::Insert(
    const typename RecordBase::Key &r_key, const RecordBase &record) {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, r_key);
  int ret;

  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret =
        map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr,
                                  "raw", &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];

  WT_ITEM key_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;

  WT_ITEM payload_item;
  payload_item.data = &record;
  payload_item.size = sizeof(record);

  cursor->set_key(cursor, &key_item);
  cursor->set_value(cursor, &payload_item);
  ret = cursor->insert(cursor);

  // *** No rollback_transaction here – we are not using explicit WT txns ***
  WTErrorCheck(ret);
}

template <class RecordBase>
auto WiredTigerAdapter<RecordBase>::Erase(const typename RecordBase::Key &r_key)
    -> bool {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, r_key);
  int ret;

  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret =
        map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr,
                                  "raw", &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];

  WT_ITEM key_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;

  cursor->set_key(cursor, &key_item);
  ret = cursor->search(cursor);

  if (ret == WT_NOTFOUND) {
    // Key wasn’t there – not an error, just “nothing erased”.
    return false;
  }
  if (ret != 0) {
    // Anything else from search is a real error.
    WTErrorCheck(ret);
  }

  ret = cursor->remove(cursor);

  if (ret == 0) {
    return true;
  }

  if (ret == WT_NOTFOUND) {
    // Somebody raced and deleted it first – fine.
    return false;
  }

  if (ret == WT_ROLLBACK) {
    // Conflict between concurrent ops. For this benchmark code,
    // just report "erase failed" and move on instead of trapping.
    return false;
  }

  // Any other error is real.
  WTErrorCheck(ret);
  return false; // not reached, but keeps compiler happy
}

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::Update(
    const typename RecordBase::Key &r_key, const RecordBase &record) {
  // Reuse the in-place update path; just overwrite the whole record.
  UpdateInPlace(r_key, [&](RecordBase &dst) { dst = record; });
}

template <class RecordBase>
auto WiredTigerAdapter<RecordBase>::LookUp(
    const typename RecordBase::Key &r_key,
    const typename Adapter<RecordBase>::AccessRecordFunc &fn) -> bool {
  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, r_key);
  int ret;

  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret =
        map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr,
                                  "raw", &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];

  WT_ITEM key_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;
  WT_ITEM payload_item;

  cursor->set_key(cursor, &key_item);
  ret = cursor->search(cursor);
  if (ret == WT_NOTFOUND) {
    return false;
  }
  WTErrorCheck(ret);

  ret = cursor->get_value(cursor, &payload_item);
  WTErrorCheck(ret);

  auto &record = *reinterpret_cast<const RecordBase *>(payload_item.data);
  fn(record);
  cursor->reset(cursor);
  return true;
}

template <class RecordBase>
void WiredTigerAdapter<RecordBase>::UpdateInPlace(
    const typename RecordBase::Key &r_key,
    const typename Adapter<RecordBase>::ModifyRecordFunc &fn) {

  uint8_t folded_key[RecordBase::MaxFoldLength()];
  const auto folded_key_len = RecordBase::FoldKey(folded_key, r_key);
  int ret;

  // Ensure we have a cursor for this relation
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret =
        map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr,
                                  "raw", &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];

  WT_ITEM key_item;
  key_item.data = folded_key;
  key_item.size = folded_key_len;

  // We will retry a couple of times if we hit WT_ROLLBACK
  constexpr int MAX_RETRIES = 5;

  for (int attempt = 0; attempt < MAX_RETRIES; ++attempt) {
    cursor->set_key(cursor, &key_item);

    ret = cursor->search(cursor);
    if (ret == WT_NOTFOUND) {
      // Record disappeared, nothing to update
      return;
    }
    WTErrorCheck(ret);

    WT_ITEM payload_item;
    ret = cursor->get_value(cursor, &payload_item);
    WTErrorCheck(ret);

    // Copy current value into a local object
    RecordBase record =
        *reinterpret_cast<const RecordBase *>(payload_item.data);

    // Apply user-provided modification
    fn(record);

    // Write back the modified record
    WT_ITEM new_payload;
    new_payload.data = &record;
    new_payload.size = sizeof(record);

    cursor->set_value(cursor, &new_payload);
    ret = cursor->update(cursor);

    if (ret == 0) {
      // success
      return;
    }
    if (ret == WT_ROLLBACK) {
      // concurrency conflict: let WT roll back and retry
      std::this_thread::yield();
      continue;
    }

    // other errors are fatal
    WTErrorCheck(ret);
  }

  // If we get here, we kept getting WT_ROLLBACK; you can either ignore
  // silently or log a warning.
  // fprintf(stderr, "WiredTigerAdapter::UpdateInPlace: too many
  // WT_ROLLBACKs\n");
}

template <class RecordBase>
auto WiredTigerAdapter<RecordBase>::Count() -> uint64_t {
  int ret;
  // -------------------------------------------------------------------------------------
  if (map_.cursors[RecordBase::TYPE_ID] == nullptr) {
    ret =
        map_.session->open_cursor(map_.session, relation_name_.c_str(), nullptr,
                                  "raw", &map_.cursors[RecordBase::TYPE_ID]);
    WTErrorCheck(ret);
  }
  auto cursor = map_.cursors[RecordBase::TYPE_ID];
  cursor->reset(cursor);
  ret = cursor->next(cursor);
  // -------------------------------------------------------------------------------------
  uint64_t count = 0;
  while (ret == 0) {
    count++;
    ret = cursor->next(cursor);
  }
  return count;
}

// static double OSFileSizeGB(const std::string &path) {
//   namespace fs = std::filesystem;

//   std::uintmax_t total_bytes = 0;
//   std::error_code ec;

//   if (!fs::exists(path, ec)) {
//     return 0.0;
//   }

//   for (auto const &entry : fs::recursive_directory_iterator(path, ec)) {
//     if (entry.is_regular_file()) {
//       total_bytes += entry.file_size(ec);
//     }
//   }

//   return static_cast<double>(total_bytes) / (1024.0 * 1024.0 * 1024.0);
// }

std::thread WiredTigerDB::StartProfilingThread(
    const std::string &system_name, std::atomic<bool> &is_loading,
    std::atomic<bool> &keep_running, std::atomic<uint64_t> &completed_txn) {

  return std::thread([this, system_name, &is_loading, &keep_running,
                      &completed_txn]() {
    try {
      // Dedicated session for statistics & fs monitoring
      WT_SESSION *stat_session = nullptr;
      int ret = conn->open_session(conn, nullptr, nullptr, &stat_session);
      WTErrorCheck(ret);

      uint64_t tick = 0;

      // -------------------------------------------------------------------
      // Initial samples for per-second deltas
      // -------------------------------------------------------------------
      uint64_t last_block_bytes_write =
          GetConnStat(stat_session, WT_STAT_CONN_BLOCK_BYTE_WRITE);
      uint64_t last_block_writes =
          GetConnStat(stat_session, WT_STAT_CONN_BLOCK_WRITE);
      uint64_t last_cache_bytes_write =
          GetConnStat(stat_session, WT_STAT_CONN_CACHE_BYTES_WRITE);
      uint64_t last_cache_bytes_read =
          GetConnStat(stat_session, WT_STAT_CONN_CACHE_BYTES_READ);

      // NEW: log bytes, for triggering checkpoints every 2GB
      uint64_t last_log_bytes_write =
          GetConnStat(stat_session, WT_STAT_CONN_LOG_BYTES_WRITTEN);
      uint64_t log_bytes_accum = 0;
      uint64_t TWO_GB = 40ULL * 1024ULL * 1024ULL;

      if (is_loading.load()) {
        TWO_GB = 40ULL * 1024ULL * 1024ULL * 1024ULL; // * 1024ULL * 1024ULL
      } else {
        ensure(keep_running.load());
        TWO_GB =
            1419ULL * 1024ULL * 1024ULL; // * 1024ULL * 1024ULL // 262,144,000
      }
      // Eviction stats
      uint64_t last_evict_clean =
          GetConnStat(stat_session, WT_STAT_CONN_CACHE_EVICTION_CLEAN);
      uint64_t last_evict_dirty =
          GetConnStat(stat_session, WT_STAT_CONN_CACHE_EVICTION_DIRTY);

      // -------------------------------------------------------------------
      // CSV header (unchanged – add fields if you want log stats visible)
      // -------------------------------------------------------------------
      std::printf("system,ts,tx,"
                  "block_bytes_write_s,block_writes_s,ckpt_count_s,"
                  "cache_bytes_write_s,cache_bytes_read_s,"
                  "evict_clean_pages_s,evict_dirty_pages_s,"
                  "cache_inuse_MB,cache_max_MB,"
                  "tree0_ckpt_MB,tree0_file_MB,tree0_frag_pct,tree0_comp_ratio,"
                  "fs_used_GB,fs_cap_GB,fs_used_pct,"
                  "compact_active,compact_bytes_total,compact_writes_total,"
                  "compact_evict_clean_total,compact_evict_dirty_total\n");

      // Compaction I/O accumulators you already had
      uint64_t compact_bytes_total = 0;
      uint64_t compact_writes_total = 0;
      uint64_t compact_evict_clean_total = 0;
      uint64_t compact_evict_dirty_total = 0;

      while (is_loading.load(std::memory_order_relaxed) ||
             keep_running.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // tx/sec
        auto tx = completed_txn.exchange(0, std::memory_order_relaxed);
        total_txn_completed += tx;

        // ----- Connection-level stats (overall writes, cache, etc.) -----
        uint64_t cur_block_bytes_write =
            GetConnStat(stat_session, WT_STAT_CONN_BLOCK_BYTE_WRITE);
        uint64_t cur_block_writes =
            GetConnStat(stat_session, WT_STAT_CONN_BLOCK_WRITE);
        uint64_t cur_cache_bytes_write =
            GetConnStat(stat_session, WT_STAT_CONN_CACHE_BYTES_WRITE);
        uint64_t cur_cache_bytes_read =
            GetConnStat(stat_session, WT_STAT_CONN_CACHE_BYTES_READ);

        uint64_t cache_inuse_bytes =
            GetConnStat(stat_session, WT_STAT_CONN_CACHE_BYTES_INUSE);
        uint64_t cache_max_bytes =
            GetConnStat(stat_session, WT_STAT_CONN_CACHE_BYTES_MAX);

        // Deltas per second (overall)
        uint64_t d_block_bytes_write =
            cur_block_bytes_write - last_block_bytes_write;
        uint64_t d_block_writes = cur_block_writes - last_block_writes;
        uint64_t d_cache_bytes_write =
            cur_cache_bytes_write - last_cache_bytes_write;
        uint64_t d_cache_bytes_read =
            cur_cache_bytes_read - last_cache_bytes_read;

        last_block_bytes_write = cur_block_bytes_write;
        last_block_writes = cur_block_writes;
        last_cache_bytes_write = cur_cache_bytes_write;
        last_cache_bytes_read = cur_cache_bytes_read;

        double cache_inuse_MB = cache_inuse_bytes / (1024.0 * 1024.0);
        double cache_max_MB = cache_max_bytes / (1024.0 * 1024.0);

        // ----- NEW: Log bytes written, accumulate for cp trigger -----
        uint64_t cur_log_bytes_write =
            GetConnStat(stat_session, WT_STAT_CONN_LOG_BYTES_WRITTEN);
        uint64_t d_log_bytes_write = cur_log_bytes_write - last_log_bytes_write;
        last_log_bytes_write = cur_log_bytes_write;

        log_bytes_accum += d_log_bytes_write;

        // Every accumulated 2GB of log writes, request a checkpoint
        if (log_bytes_accum >= TWO_GB &&
            !ckpt_running.load(std::memory_order_relaxed)) {

          // Reset accumulator *before* or after request; here we reset before
          log_bytes_accum = 0;

          bool already = ckpt_request.exchange(true, std::memory_order_relaxed);
          if (!already) {
            std::fprintf(stderr,
                         "[INFO] WiredTiger profiling: requesting checkpoint "
                         "after %" PRIu64 " log bytes\n",
                         TWO_GB);
          }
        }

        // ----- Eviction (GC-ish in cache) -----
        uint64_t cur_evict_clean =
            GetConnStat(stat_session, WT_STAT_CONN_CACHE_EVICTION_CLEAN);
        uint64_t cur_evict_dirty =
            GetConnStat(stat_session, WT_STAT_CONN_CACHE_EVICTION_DIRTY);

        uint64_t d_evict_clean = cur_evict_clean - last_evict_clean;
        uint64_t d_evict_dirty = cur_evict_dirty - last_evict_dirty;

        last_evict_clean = cur_evict_clean;
        last_evict_dirty = cur_evict_dirty;

        // ----- Track I/O while compaction is active -----
        if (compact_running.load(std::memory_order_relaxed)) {
          compact_bytes_total += d_block_bytes_write;
          compact_writes_total += d_block_writes;
          compact_evict_clean_total += d_evict_clean;
          compact_evict_dirty_total += d_evict_dirty;
        }

        // ----- Per-table stats (example: YCSB tree_0) -----
        const std::string tree_uri = "table:tree_0";

        uint64_t ckpt_size = GetTableStat(stat_session, tree_uri,
                                          WT_STAT_DSRC_BLOCK_CHECKPOINT_SIZE);
        uint64_t file_size =
            GetTableStat(stat_session, tree_uri, WT_STAT_DSRC_BLOCK_SIZE);

        double tree_ckpt_MB = ckpt_size / (1024.0 * 1024.0);
        double tree_file_MB = file_size / (1024.0 * 1024.0);

        double frag_pct = 0.0;
        double comp_ratio = 0.0;
        if (file_size > 0) {
          frag_pct = 100.0 * (static_cast<double>(file_size - ckpt_size) /
                              static_cast<double>(file_size));
          comp_ratio = (ckpt_size > 0) ? static_cast<double>(ckpt_size) /
                                             static_cast<double>(file_size)
                                       : 0.0;
        }

        // ----- Filesystem usage & compaction trigger (unchanged) -----
        double fs_used_gb = 0.0, fs_cap_gb = 0.0, fs_used_pct = 0.0;
        double threshold = 87.5; // your current threshold in percent ?

        if (GetFSUsage(FLAGS_db_dir, fs_used_gb, fs_cap_gb, fs_used_pct)) {
          if (fs_used_pct >= threshold &&
              !compact_running.load(std::memory_order_relaxed)) {

            bool req =
                compact_request.exchange(true, std::memory_order_relaxed);
            if (!req) {
              std::printf("[INFO] WiredTiger profiling: requesting compaction "
                          "(fs_used_pct=%.2f)\n",
                          fs_used_pct);
            }
          }
        }

        // ----- Print CSV line (keep your current format) -----
        std::printf(
            "%s,%" PRIu64 ",%lu,"
            "%" PRIu64 ",%" PRIu64
            ",0," // block_bytes_write_s, block_writes_s, ckpt_count_s dummy
            "%" PRIu64 ",%" PRIu64 ","
            "%" PRIu64 ",%" PRIu64 ","
            "%.2f,%.2f,"
            "%.2f,%.2f,%.2f,%.4f,"
            "%.4f,%.4f,%.2f,"
            "%d,%" PRIu64 ",%" PRIu64 ","
            "%" PRIu64 ",%" PRIu64 "\n",
            system_name.c_str(), tick++, static_cast<unsigned long>(tx),
            d_block_bytes_write, d_block_writes, d_cache_bytes_write,
            d_cache_bytes_read, d_evict_clean, d_evict_dirty, cache_inuse_MB,
            cache_max_MB, tree_ckpt_MB, tree_file_MB, frag_pct, comp_ratio,
            fs_used_gb, fs_cap_gb, fs_used_pct,
            compact_running.load(std::memory_order_relaxed) ? 1 : 0,
            compact_bytes_total, compact_writes_total,
            compact_evict_clean_total, compact_evict_dirty_total);

        std::fflush(stdout);
      }

      stat_session->close(stat_session, nullptr);
      std::printf("Halt Profiling thread (WiredTiger)\n");
    } catch (...) {
      std::printf("WiredTiger profiling thread crashed\n");
    }
  });
}

std::thread
WiredTigerDB::StartCompactionThread(std::atomic<bool> &keep_running) {
  return std::thread([this, &keep_running]() {
    try {
      WT_SESSION *comp_sess = nullptr;
      int ret = conn->open_session(conn, nullptr, nullptr, &comp_sess);
      if (ret != 0) {
        std::fprintf(stderr,
                     "[WARN] compaction thread: open_session failed: %s\n",
                     wiredtiger_strerror(ret));
        return;
      }

      const char *uri = "table:tree_0"; // your YCSB table
      const char *cfg =
          "timeout=600,free_space_target=1048576"; // or tweak target

      while (keep_running.load(std::memory_order_relaxed)) {
        if (compact_request.exchange(false, std::memory_order_relaxed)) {
          compact_running.store(true, std::memory_order_relaxed);
          std::fprintf(
              stderr,
              "[INFO] WiredTiger compaction thread: starting compact on %s\n",
              uri);

          int cret = comp_sess->compact(comp_sess, uri, cfg);

          if (cret == 0) {
            std::fprintf(
                stderr,
                "[INFO] WiredTiger compaction thread: compact(%s) done\n", uri);
          } else if (cret == WT_ROLLBACK) {
            std::fprintf(stderr,
                         "[WARN] WiredTiger compaction thread: compact(%s) "
                         "busy/rollback: %s\n",
                         uri, wiredtiger_strerror(cret));
            // You can re-request compaction if you like:
            // compact_request.store(true, std::memory_order_relaxed);
          } else {
            std::fprintf(
                stderr,
                "[WARN] WiredTiger compaction thread: compact(%s) failed: %s\n",
                uri, wiredtiger_strerror(cret));
          }

          compact_running.store(false, std::memory_order_relaxed);
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
      }

      comp_sess->close(comp_sess, nullptr);
      std::fprintf(stderr, "[INFO] WiredTiger compaction thread exiting\n");
    } catch (...) {
      std::fprintf(stderr, "[WARN] WiredTiger compaction thread crashed\n");
    }
  });
}

std::thread
WiredTigerDB::StartCheckpointThread(std::atomic<bool> &keep_running) {
  return std::thread([this, &keep_running]() {
    try {
      WT_SESSION *ckpt_sess = nullptr;
      int ret = conn->open_session(conn, nullptr, nullptr, &ckpt_sess);
      if (ret != 0) {
        std::fprintf(stderr,
                     "[WARN] checkpoint thread: open_session failed: %s\n",
                     wiredtiger_strerror(ret));
        return;
      }

      // You can tweak the config string; nullptr means default checkpoint.
      // "force=1" tends to ignore some heuristics.
      const char *cfg = "force=1";

      while (keep_running.load(std::memory_order_relaxed)) {
        // Only act when profiling thread has requested a checkpoint
        if (ckpt_request.exchange(false, std::memory_order_relaxed)) {
          ckpt_running.store(true, std::memory_order_relaxed);

          std::fprintf(
              stderr,
              "[INFO] WiredTiger checkpoint thread: starting checkpoint\n");

          int cret = ckpt_sess->checkpoint(ckpt_sess, cfg);

          if (cret == 0) {
            std::fprintf(
                stderr,
                "[INFO] WiredTiger checkpoint thread: checkpoint done\n");
          } else if (cret == WT_ROLLBACK) {
            std::fprintf(stderr,
                         "[WARN] WiredTiger checkpoint thread: checkpoint "
                         "busy/rollback: %s\n",
                         wiredtiger_strerror(cret));
            // optional: re-request later
            // ckpt_request.store(true, std::memory_order_relaxed);
          } else {
            std::fprintf(
                stderr,
                "[WARN] WiredTiger checkpoint thread: checkpoint failed: %s\n",
                wiredtiger_strerror(cret));
          }

          ckpt_running.store(false, std::memory_order_relaxed);
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
      }

      ckpt_sess->close(ckpt_sess, nullptr);
      std::fprintf(stderr, "[INFO] WiredTiger checkpoint thread exiting\n");
    } catch (...) {
      std::fprintf(stderr, "[WARN] WiredTiger checkpoint thread crashed\n");
    }
  });
}

// For testing purpose
template class WiredTigerAdapter<benchmark::RelationTest>;

// For TPC-C
template class WiredTigerAdapter<tpcc::WarehouseType>;
template class WiredTigerAdapter<tpcc::DistrictType>;
template class WiredTigerAdapter<tpcc::CustomerType>;
template class WiredTigerAdapter<tpcc::CustomerWDCType>;
template class WiredTigerAdapter<tpcc::HistoryType>;
template class WiredTigerAdapter<tpcc::NewOrderType>;
template class WiredTigerAdapter<tpcc::OrderType>;
template class WiredTigerAdapter<tpcc::OrderWDCType>;
template class WiredTigerAdapter<tpcc::OrderLineType>;
template class WiredTigerAdapter<tpcc::ItemType>;
template class WiredTigerAdapter<tpcc::StockType>;

// For YCSB
template class WiredTigerAdapter<ycsb::Relation<BytesPayload<120>, 0>>;
template class WiredTigerAdapter<ycsb::Relation<BytesPayload<4096>, 0>>;
template class WiredTigerAdapter<ycsb::Relation<BytesPayload<102400>, 0>>;
template class WiredTigerAdapter<ycsb::Relation<BytesPayload<10485760>, 0>>;
