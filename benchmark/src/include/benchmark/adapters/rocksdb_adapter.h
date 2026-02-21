#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <span>
#include <thread>

#include "benchmark/adapters/adapter.h" // <-- make sure this is the correct path
#include "common/typedefs.h"
#include "share_headers/config.h"

#include <rocksdb/db.h>

// Free function to open RocksDB with FLAGS_*
rocksdb::DB *OpenRocksDB();

template <class RecordBase> class RocksDBAdapter : public Adapter<RecordBase> {
public:
  using KeyType = typename RecordBase::Key;
  using FoundRecordFunc = typename Adapter<RecordBase>::FoundRecordFunc;
  using AccessRecordFunc = typename Adapter<RecordBase>::AccessRecordFunc;
  using ModifyRecordFunc = typename Adapter<RecordBase>::ModifyRecordFunc;

  // Non-owning: main() creates rocksdb::DB and passes pointer in.
  explicit RocksDBAdapter(rocksdb::DB *db);

  // Basic operations
  void Scan(const KeyType &key,
            const FoundRecordFunc &found_record_cb) override;

  void ScanDesc(const KeyType &key,
                const FoundRecordFunc &found_record_cb) override;

  void Insert(const KeyType &r_key, const RecordBase &record) override;

  void Update(const KeyType &r_key, const RecordBase &record) override;

  auto LookUp(const KeyType &r_key,
              const AccessRecordFunc &fn) -> bool override;

  void UpdateInPlace(const KeyType &r_key, const ModifyRecordFunc &fn) override;

  auto Erase(const KeyType &r_key) -> bool override;

  auto Count() -> u64 override;
  auto RelationSize() -> float;

  // *** Make this public ***
  std::thread StartProfilingThread(const std::string &system_name,
                                   std::atomic<bool> &is_loading,
                                   std::atomic<bool> &keep_running,
                                   std::atomic<uint64_t> &completed_txn);

private:
  void ScanImpl(const KeyType &r_key, const FoundRecordFunc &found_record_cb,
                bool scan_ascending);

  rocksdb::DB *db_; // non-owning
  std::string db_path_;
};