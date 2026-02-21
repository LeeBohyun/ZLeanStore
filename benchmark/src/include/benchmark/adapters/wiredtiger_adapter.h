#pragma once

#include "benchmark/adapters/adapter.h"
#include <atomic>
#include <thread>

#include "share_headers/config.h"
#include "wiredtiger.h"

#define WTErrorCheck(p)                                                        \
  do {                                                                         \
    int __wt_ret = (p);                                                        \
    if (__wt_ret) {                                                            \
      std::cerr << "[WT ERROR] " << __FILE__ << ":" << __LINE__                \
                << " ret=" << __wt_ret << " ("                                 \
                << wiredtiger_strerror(__wt_ret) << ")" << std::endl;          \
      raise(SIGTRAP);                                                          \
    }                                                                          \
  } while (0)

struct WiredTigerDB {
  WT_CONNECTION *conn;
  inline static thread_local WT_SESSION *session = nullptr;
  inline static thread_local WT_CURSOR *cursors[20] = {nullptr};
  std::atomic<uint64_t> total_txn_completed{0};
  std::atomic<bool> compact_request{false};
  std::atomic<bool> compact_running{false};
  // for manual cp
  std::atomic<uint64_t> cumulative_log_bytes{0};
  std::atomic<bool> ckpt_request{false};
  std::atomic<bool> ckpt_running{false};

  WiredTigerDB(bool load);
  ~WiredTigerDB();

  void PrepareThread();
  void StartTransaction(bool si = true);
  void CommitTransaction();
  void CloseSession();

  std::thread StartProfilingThread(const std::string &system_name,
                                   std::atomic<bool> &is_loading,
                                   std::atomic<bool> &keep_running,
                                   std::atomic<uint64_t> &completed_txn);
  std::thread StartCompactionThread(std::atomic<bool> &keep_running);
  std::thread StartCheckpointThread(std::atomic<bool> &keep_running);
};

template <class RecordBase> class WiredTigerAdapter : Adapter<RecordBase> {
private:
  WiredTigerDB &map_;
  std::string relation_name_;

public:
  explicit WiredTigerAdapter(WiredTigerDB &map);
  ~WiredTigerAdapter() override;

  // -------------------------------------------------------------------------------------
  void Scan(const typename RecordBase::Key &key,
            const typename Adapter<RecordBase>::FoundRecordFunc
                &found_record_cb) override;
  void ScanDesc(const typename RecordBase::Key &key,
                const typename Adapter<RecordBase>::FoundRecordFunc
                    &found_record_cb) override;
  void Insert(const typename RecordBase::Key &r_key,
              const RecordBase &record) override;
  void Update(const typename RecordBase::Key &r_key,
              const RecordBase &record) override;
  auto LookUp(const typename RecordBase::Key &r_key,
              const typename Adapter<RecordBase>::AccessRecordFunc &fn)
      -> bool override;
  void UpdateInPlace(
      const typename RecordBase::Key &r_key,
      const typename Adapter<RecordBase>::ModifyRecordFunc &fn) override;
  auto Erase(const typename RecordBase::Key &r_key) -> bool override;

  // -------------------------------------------------------------------------------------
  auto Count() -> uint64_t override;
};