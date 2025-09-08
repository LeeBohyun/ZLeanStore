#pragma once

#include "buffer/buffer_manager.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "recovery/log_worker.h"
#include "storage/space/backend/io_backend.h"

#include "liburing.h"
#include "gtest/gtest_prod.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <unordered_set>

namespace leanstore::recovery {

class LogManager;

class GroupCommitExecutor {
public:
  GroupCommitExecutor(buffer::BufferManager *buffer, LogManager *log_manager,
                      std::atomic<bool> &bg_threads_keep_running, u32 shard_cnt,
                      int wal_fd, u64 wal_start_offset, u64 wal_end_offset,
                      u32 sector_size);
  ~GroupCommitExecutor() = default;

  /* Main exposed API */
  void StartExecution();

  /* Utilities APIs */
  void PrepareWrite(u8 *src, size_t size, size_t offset);
  void InitializeRound();
  void CompleteRound();

  /* Distributed logging 3 phases */
  void PhaseOne();
  void PhaseTwo();
  void PhaseThree();
  void PhaseFour();
  void PhaseFive();
  void TrimWAL();
  void SendBlkdiscardReq(u64 start_offset, u64 size);
  std::string ExecCommand(const std::string &cmd);
  void AddCheckpointTarget(logid_t min_all_workers_gsn_);
  size_t GetWalFileOffset();
  size_t GetCheckpointOffset();
  size_t UpdateCheckpointOffset(u64 offset);
  void UpdateCheckpointOffsetUntilCurGSN(u32 shard_idx, logid_t target_gsn);
  void UpdateCheckpointOffsetUntilShard(u32 shard_idx);
  void FlushBuffer();

  std::atomic<u32> checkpoint_idx = 0;
  std::vector<std::tuple<size_t, logid_t>>
      cp_target_{}; // target cp offset and its gsn
  std::mutex cp_lock;
  u32 total_logger_cnt = 0;
  u32 shard_cnt_ = 0;
  size_t shard_wal_size_;
  u32 last_target_shard_idx = 0;

private:
  friend class leanstore::LeanStore;
  FRIEND_TEST(TestGroupCommit, BasicOperation);
  FRIEND_TEST(TestGroupCommitBlob, BlobSupportVariant);
  using timepoint_t = decltype(std::chrono::high_resolution_clock::now());

  /* Only run one round */
  void ExecuteOneRound();

  /* Large Page async utilities */
  void PrepareLargePageWrite(transaction::Transaction &txn);
  void CompleteTxnBlobs(transaction::Transaction &txn);

  /* IO backend for log writes*/
  storage::space::backend::IOBackend io_backend_;
  void HotUringSubmit();

  /* Env */
  buffer::BufferManager *buffer_;
  std::atomic<bool> *keep_running_;
  bool has_blob_;

  /* IO interfaces */
  int wal_fd_;
  size_t w_offset_;
  size_t w_start_offset_;
  size_t w_end_offset_;
  size_t checkpoint_offset_;
  size_t last_block_offset_;
  bool checkpoint_added_;
  u32 sector_size_;

  /* wal group commit write buffer */
  u8 *write_buffer_;         // Pointer to the write buffer
  size_t write_buffer_size_; // Maximum size of the buffer
  size_t
      buffer_offset_; // Current offset in the buffer (how much data is in it)
  size_t w_cur_offset_;

  std::vector<std::tuple<pageid_t, u64, pageid_t, u64>>
      lp_req_{}; // Vector of [dirty large page, extent]

  /* io_uring properties */
  struct io_uring ring_;
  u32 submitted_io_cnt_{0};

  /* GSN & timestamp management*/
  logid_t min_all_workers_gsn_; // For Remote Flush Avoidance
  logid_t max_all_workers_gsn_; // Sync all workers to this point
  timestamp_t min_hardened_commit_ts_;

  /* Log workers management */

  LogWorker *all_loggers_;
  u32 total_logger_cnt_;
  std::unordered_set<pageid_t> already_prep_; // List of prepared extents
  std::unordered_set<pageid_t> completed_lp_; // List of completed large pages
                                              // (not extents) committed to disk
  std::vector<size_t> ready_to_commit_cut_;
  std::vector<size_t> ready_to_commit_rfa_cut_;
  std::vector<LogWorker::WorkerConsistentState> worker_states_;

  /* Statistics */
  timepoint_t phase_1_begin_;
  timepoint_t phase_1_end_;
  timepoint_t phase_2_begin_;
  timepoint_t phase_2_end_;
  timepoint_t phase_3_begin_;
  timepoint_t phase_3_end_;
  timepoint_t phase_4_begin_;
  timepoint_t phase_4_end_;
  timepoint_t phase_5_begin_;
  timepoint_t phase_5_end_;
  u64 round_{0};
  u64 completed_txn_;
};

class Checkpointer {
public:
  Checkpointer(buffer::BufferManager *buffer,
               GroupCommitExecutor *group_committer,
               std::atomic<bool> &keep_running, u32 shard_cnt);
  ~Checkpointer() = default;

  // Public APIs
  bool RunCheckpointThread(u32 shard_idx, bool is_load);
  void UpdateShardCheckpointGSN(u32 shard_idx, logid_t gsn);
  void UpdateCheckpointOffsetUntilShard(u32 shard_idx);

private:
  buffer::BufferManager *buffer_;
  GroupCommitExecutor *group_committer_;
  std::atomic<bool> *keep_running_;
  std::vector<logid_t> shard_checkpointed_gsn_;
  u32 last_cp_shard_idx;
  logid_t min_cp_gsn_ = UINT64_MAX;

  u32 shard_cnt;
  // std::thread cp_thread_;
  // wid_t cp_thread_id_;
};

} // namespace leanstore::recovery
