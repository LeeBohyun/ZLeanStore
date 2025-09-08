#pragma once

#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "common/worker_pool.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/external_schema.h"
#include "leanstore/statistics.h"
#include "recovery/group_commit.h"
#include "recovery/log_manager.h"
#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "storage/free_page_manager.h"
#include "storage/space/device.h"
#include "storage/space/garbage_collector.h"
#include "transaction/transaction_manager.h"
#include <array>
#include <string>
#include <thread>
#include <typeindex>
#include <typeinfo>

namespace leanstore {

using Transaction = transaction::Transaction;
using BlobState = storage::blob::BlobState;
using Checkpointer = recovery::Checkpointer;

class LeanStore {
public:
  // Database is z (Must be on TOP)
  std::atomic<bool> is_running = true;
  std::atomic<bool> is_loading = false;

  // Buffer Manager, Log Manager, Transaction Manager, Group-Commit executor,
  // Checkpointer, Garbage Collector, and Space Manager
  std::unique_ptr<storage::space::NVMeDevice> device;
  std::unique_ptr<storage::space::SpaceManager> sm;
  std::unique_ptr<storage::FreePageManager> fp_manager;
  std::unique_ptr<buffer::BufferManager> buffer_pool;
  std::unique_ptr<recovery::LogManager> log_manager;
  std::unique_ptr<transaction::TransactionManager> transaction_manager;
  std::unique_ptr<storage::blob::BlobManager> blob_manager;
  std::unique_ptr<recovery::GroupCommitExecutor> gct;
  std::vector<storage::space::GarbageCollector> gc;
  std::vector<recovery::Checkpointer> cp;

  // Worker pool (excluding Group commit thread & Profiling thread)
  // thread id: [0 ~ FLAGS_worker_count)
  WorkerPool worker_pool;
  // thead id: [FLAGS_worker_count ~
  // FLAGS_worker_count+FLAGS_checkpointer_count)
  std::thread checkpointer;
  u32 shard_cnt;
  // thead id: [FLAGS_worker_count+FLAGS_checkpointer_count ~
  // FLAGS_worker_count+FLAGS_checkpointer_count+FLAGS_garbage_collector_count)
  std::thread garbage_collector;
  // thead id: FLAGS_worker_count+FLAGS_checkpointer_count ~
  // FLAGS_worker_count+FLAGS_checkpointer_count+FLAGS_garbage_collector_count
  std::thread group_committer;

  std::thread stat_collector;

  // Stupid Catalog
  std::unordered_map<std::type_index, std::unique_ptr<KVInterface>> indexes;

  LeanStore();
  ~LeanStore();
  void Shutdown();

  // Catalog operations
  void RegisterTable(const std::type_index &relation);
  auto RetrieveIndex(const std::type_index &relation) -> KVInterface *;

  // Convenient txn helpers
  void StartTransaction(bool read_only = false,
                        Transaction::Mode tx_mode = Transaction::Mode::OLTP,
                        const std::string &tx_isolation_level =
                            FLAGS_txn_default_isolation_level);
  void CommitTransaction();
  void AbortTransaction();

  // Utilities for benchmarking
  auto AllocatedPIDsCnt() -> u64;
  auto AllocatedSize() -> float;
  auto DBSize() -> float;
  auto WALSize() -> float;
  void DropCache();
  void EndDataLoad();

  // Background thread management
  void StartGroupCommitThread();
  void StartCheckpointThread();
  void StartGarbageCollectorThread();
  void StartProfilingThread();

  std::string ExecCommand(const std::string &cmd);
  size_t GetFlashWrite();
  int blockfd_;

  // profile stats
  u32 GetVersion();
  size_t first_flash_write = 0;
  size_t prev_flash_write = 0;
  size_t prev_total_write = 0;
  float prev_cum_waf = 0.0;
  size_t first_log_write_offset = 0;
  size_t cum_total_wal_write = 0;
  size_t cum_total_write = 0;
  size_t cum_total_flash_write = 0;
  size_t first_cum_write_bytes = 0;
  size_t cum_tps = 0;

  // Blob utilities
  auto CreateNewBlob(std::span<const u8> blob_payload, BlobState *prev_blob,
                     bool likely_grow) -> std::span<const u8>;
  void LoadBlob(const BlobState *blob_t,
                const storage::blob::BlobCallbackFunc &read_cb,
                bool partial_load = true);
  void RemoveBlob(BlobState *blob_t);

  // Comparison utilities
  auto RetrieveComparisonFunc(ComparisonOperator cmp_op) -> ComparisonLambda;
};

} // namespace leanstore