#include "recovery/group_commit.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include <fstream>

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <sys/stat.h>
#include <tuple>

namespace leanstore::recovery {

GroupCommitExecutor::GroupCommitExecutor(
    buffer::BufferManager *buffer, LogManager *log_manager,
    std::atomic<bool> &bg_threads_keep_running, u32 shard_cnt, int wal_fd,
    u64 wal_start_offset, u64 wal_end_offset, u32 sector_size)
    : buffer_(buffer), keep_running_(&bg_threads_keep_running), wal_fd_(wal_fd),
      w_offset_(wal_start_offset), w_end_offset_(wal_end_offset),
      all_loggers_(log_manager->logger_),
      total_logger_cnt_(log_manager->total_logger_cnt),
      ready_to_commit_cut_(total_logger_cnt_),
      ready_to_commit_rfa_cut_(total_logger_cnt_),
      worker_states_(total_logger_cnt_), shard_cnt_(shard_cnt),
      sector_size_(sector_size),
      io_backend_(buffer->blockfd_, buffer->sm_, buffer->PID2Offset_table_,
                  buffer->sspace_) {
  pthread_setname_np(pthread_self(), "group_committer");
  w_start_offset_ = w_offset_;
  checkpoint_offset_ = w_offset_;
  checkpoint_added_ = false;
  // gct write buffer
  if (FLAGS_use_out_of_place_write) {
    if (FLAGS_block_size_mb * MB >= 1 * GB) {
      write_buffer_size_ = 128 * MB;
    } else {
      write_buffer_size_ = FLAGS_block_size_mb * MB;
    }
    if (FLAGS_use_ZNS) {
      // because we are writing to the device via nvme passthru, max io size is
      // 128KiB
      write_buffer_size_ = 128 * KB;
    }
  } else {
    write_buffer_size_ = (FLAGS_worker_count +
                          FLAGS_enable_checkpoint * FLAGS_checkpointer_cnt) *
                         MB;
  }
  write_buffer_ = new u8[write_buffer_size_];
  buffer_offset_ = 0;
  w_cur_offset_ = w_offset_;

  if (FLAGS_enable_checkpoint) {
    // Ensure(FLAGS_max_wal_capacity_gb >= FLAGS_bm_physical_gb);
    cp_target_.resize(shard_cnt_);
    shard_wal_size_ = (w_end_offset_ - w_start_offset_) / shard_cnt;
  }
  fprintf(stderr, "WAL size (GB) : %lu\n",
          (w_end_offset_ - w_start_offset_) / GB);
}

/**
 * @brief Reset GroupCommitExecutor's state every round
 */
void GroupCommitExecutor::InitializeRound() {
  round_++;
  lp_req_.clear();
  already_prep_.clear();
  has_blob_ = false;
  submitted_io_cnt_ = 0;
  completed_txn_ = 0;
  min_all_workers_gsn_ = std::numeric_limits<logid_t>::max();
  max_all_workers_gsn_ = 0;
  min_hardened_commit_ts_ = std::numeric_limits<timestamp_t>::max();
}

void GroupCommitExecutor::CompleteRound() {
  // Update RFA env
  assert(LogManager::global_min_gsn_flushed.load() <= min_all_workers_gsn_);
  LogManager::global_min_gsn_flushed.store(min_all_workers_gsn_,
                                           std::memory_order_release);
  LogManager::global_sync_to_this_gsn.store(max_all_workers_gsn_,
                                            std::memory_order_release);

  // Update statistics
  if (start_profiling) {
    statistics::txn_processed += completed_txn_;
    statistics::recovery::gct_phase_1_ms +=
        std::chrono::duration_cast<std::chrono::microseconds>(phase_1_end_ -
                                                              phase_1_begin_)
            .count();
    statistics::recovery::gct_phase_2_ms +=
        (std::chrono::duration_cast<std::chrono::microseconds>(phase_2_end_ -
                                                               phase_2_begin_)
             .count());
    statistics::recovery::gct_phase_3_ms +=
        (std::chrono::duration_cast<std::chrono::microseconds>(phase_3_end_ -
                                                               phase_3_begin_)
             .count());
  }
}

void GroupCommitExecutor::PrepareWrite(u8 *src, size_t size, size_t offset) {
  // Keep track of how much data has been written from the source buffer
  size_t bytes_written = 0;

  while (bytes_written < size) {
    // Calculate the remaining buffer space
    size_t remaining_buffer_space =
        write_buffer_size_ - UpAlign(buffer_offset_);

    // Calculate the amount of data that can be written in this iteration
    size_t chunk_size = std::min(size - bytes_written, remaining_buffer_space);

    assert(src != nullptr);
    assert(write_buffer_ != nullptr);
    assert(buffer_offset_ <= write_buffer_size_);

    // Copy data to the buffer if it fits
    if (chunk_size > 0) {
      std::memcpy(write_buffer_ + buffer_offset_, src + bytes_written,
                  chunk_size);
      buffer_offset_ += chunk_size; // Update the buffer offset
      bytes_written += chunk_size;  // Update the amount of data written
    }

    // If the buffer is full, flush it
    if (UpAlign(buffer_offset_) == write_buffer_size_ && buffer_offset_ > 0) {
      FlushBuffer();
    }
  }
}

void GroupCommitExecutor::FlushBuffer() {
  size_t aligned_size = UpAlign(buffer_offset_); // Align the buffer size

  if (aligned_size == 0) {
    return; // No data to flush
  }

  // Handle wrap-around case
  if (w_cur_offset_ + aligned_size > w_end_offset_) {
    size_t first_part_size = w_end_offset_ - w_cur_offset_;
    size_t second_part_size = aligned_size - first_part_size;
    if (!FLAGS_wal_path.empty()) {
      io_backend_.WriteWAL(w_cur_offset_, first_part_size, write_buffer_, true);
      io_backend_.WriteWAL(w_start_offset_, second_part_size,
                           write_buffer_ + first_part_size, true);
      statistics::recovery::log_write_bytes += first_part_size;
      statistics::recovery::log_write_bytes += second_part_size;
      statistics::recovery::cum_log_write_bytes +=
          first_part_size + second_part_size;
    }
    w_cur_offset_ = w_start_offset_ + second_part_size;
    // submit_write(write_buffer_, first_part_size, w_cur_offset_);
    // submit_write(write_buffer_ + first_part_size, second_part_size,
    // w_start_offset_);

  } else {
    if (!FLAGS_wal_path.empty()) {
      io_backend_.WriteWAL(w_cur_offset_, aligned_size, write_buffer_, true);
      statistics::recovery::log_write_bytes += aligned_size;
      statistics::recovery::cum_log_write_bytes += aligned_size;
    }
    w_cur_offset_ += aligned_size;
  }

  // Trace logging for WAL writes
  if (!FLAGS_trace_file.empty() && !FLAGS_wal_path.empty()) {
    std::ofstream trace_file(FLAGS_trace_file, std::ios_base::app);
    if (!trace_file.is_open()) {
      throw std::runtime_error("Could not open trace file");
    }
    for (size_t i = 0; i < aligned_size; i += PAGE_SIZE) {
      trace_file << "W,WAL," << (w_cur_offset_ - aligned_size + i) << "\n";
    }
  }
  buffer_offset_ = 0; // Reset buffer
}

void GroupCommitExecutor::ExecuteOneRound() {
  assert(keep_running_->load());

  InitializeRound();
  phase_1_begin_ = std::chrono::high_resolution_clock::now();
  PhaseOne();
  phase_2_begin_ = phase_1_end_ = std::chrono::high_resolution_clock::now();
  PhaseTwo();
  phase_2_end_ = phase_3_begin_ = std::chrono::high_resolution_clock::now();
  if (FLAGS_enable_blob) {
    PhaseThree();
    phase_3_end_ = phase_4_begin_ = std::chrono::high_resolution_clock::now();
    PhaseFour();
    phase_4_end_ = phase_5_begin_ = std::chrono::high_resolution_clock::now();
  }
  PhaseFive();
  phase_5_end_ = std::chrono::high_resolution_clock::now();

  CompleteRound();
}

void GroupCommitExecutor::StartExecution() {
  while (keep_running_->load()) {
    try {
      ExecuteOneRound();
    } catch (...) {
      /**
       * @brief The Group-commit thread is detached,
       *  which means LeanStore may be stopped & deallocated while the Group
       * commit is still running. Therefore, the group commit may cause SEGFAULT
       * while LeanStore is deallocating, and this try-catch block is to silent
       * that run-time problem
       */
      return;
    }
  }
}

// -------------------------------------------------------------------------------------
/**
 * @brief 1st Phase:
 * - Retrieve state of all workers for RFA operations
 * - Write WAL logs to WAL file using AsyncIO
 * - Write Blob to main DB file
 */
void GroupCommitExecutor::PhaseOne() {
  for (size_t w_i = 0; w_i < total_logger_cnt_; w_i++) {
    auto &logger = all_loggers_[w_i];
    {
      /**
       * Retrieve a consistent copy of all loggers' state for RFA operations
       */
      {
        std::unique_lock<std::mutex> g(logger.precommitted_queue_mutex);
        ready_to_commit_cut_[w_i] = logger.precommitted_queue.size();
        ready_to_commit_rfa_cut_[w_i] = logger.precommitted_queue_rfa.size();
      }

      worker_states_[w_i].Clone(logger.w_state);
      min_all_workers_gsn_ =
          std::min<logid_t>(min_all_workers_gsn_, worker_states_[w_i].last_gsn);
      max_all_workers_gsn_ =
          std::max<logid_t>(max_all_workers_gsn_, worker_states_[w_i].last_gsn);
      min_hardened_commit_ts_ =
          std::min<timestamp_t>(min_hardened_commit_ts_,
                                worker_states_[w_i].precommitted_tx_commit_ts);
    }
    // LOG_INFO("wi: %lu local_min_gsn :%lu min_all_gsn: %lu global gsn: %lu",
    //  w_i, recovery::LogManager::local_cur_gsn[w_i], min_all_workers_gsn_,
    //  LogManager::global_min_gsn_flushed.load());
    if (FLAGS_checkpointer_cnt) {
      // Only proceed if there's a flushed GSN
      // LOG_INFO("global gsn: %lu min_all_gsn: %lu wid gns: %lu",
      // LogManager::global_min_gsn_flushed.load(), min_all_workers_gsn_,
      // worker_states_[w_i].last_gsn);

      if (LogManager::global_min_gsn_flushed.load() > 0) {

        // Check if the WAL offset has incremented by shard_wal_size_
        if ((w_offset_ - w_start_offset_) / shard_wal_size_ >
                last_target_shard_idx &&
            w_offset_ != w_end_offset_) {
          // Add a checkpoint target with the global minimum GSN flushed
          AddCheckpointTarget(LogManager::global_min_gsn_flushed.load());
          last_target_shard_idx++;

        } else {
          // Handle case when no checkpoint is added based on shard WAL size
          if (w_i == 0 && FLAGS_use_out_of_place_write) {
            // Check if buffer reached max dirty pages, add a checkpoint
            //  if (buffer_->ReachedMaxDirtyPages()) {
            //    AddCheckpointTarget(LogManager::global_min_gsn_flushed.load());
            //  }
          }
        }
      }
    }

    // Reset WAL offset when it reaches the end
    if (w_offset_ + LogWorker::WAL_BUFFER_SIZE >= w_end_offset_) {
      // Reset the write offset to start from the beginning
      w_offset_ = w_start_offset_;
      checkpoint_added_ = false;
      last_target_shard_idx = 0;

      // Optionally trim the WAL if flag is set
      if (FLAGS_use_trim && !FLAGS_use_ZNS && !FLAGS_wal_path.empty()) {
        TrimWAL();
      }
    }
    /**
     * Persist all WAL to SSD using async IO
     */

    auto wal_gct_cursor = logger.gct_cursor.load();
    if (worker_states_[w_i].w_written_offset > wal_gct_cursor) {
      const auto lower_offset = wal_gct_cursor;
      const auto upper_offset = worker_states_[w_i].w_written_offset;
      Ensure(upper_offset <= LogWorker::WAL_BUFFER_SIZE);
      auto size_aligned = upper_offset - lower_offset;

      PrepareWrite(logger.wal_buffer + lower_offset, size_aligned, w_offset_);
      w_offset_ += size_aligned;
      if (start_profiling &&
          !FLAGS_wal_path.empty()) { // if(!FLAGS_wal_path.empty()){
        statistics::recovery::gct_write_bytes += size_aligned;
      }
      // LOG_INFO("logger: %lu written_offset larger w_offset: %lu
      // wal_gct_cursor: %lu written_offset: %lu", w_i, w_offset_/MB,
      // wal_gct_cursor, worker_states_[w_i].w_written_offset);
    } else if (worker_states_[w_i].w_written_offset < wal_gct_cursor) {
      {
        // const u64 lower_offset = DownAlign(wal_gct_cursor);
        const u64 lower_offset = wal_gct_cursor;
        const u64 upper_offset = LogWorker::WAL_BUFFER_SIZE;
        auto size_aligned = upper_offset - lower_offset;
        PrepareWrite(logger.wal_buffer + lower_offset, size_aligned, w_offset_);
        w_offset_ += size_aligned;
        if (start_profiling && !FLAGS_wal_path.empty()) {
          statistics::recovery::gct_write_bytes += size_aligned;
        }
        // LOG_INFO("logger: %lu written_offset downalign w_offset: %lu
        // wal_gct_cursor: %lu written_offset: %lu", w_i, w_offset_/GB,
        // wal_gct_cursor, worker_states_[w_i].w_written_offset/MB);
      }
      {
        const u64 lower_offset = 0;
        // const u64 upper_offset  =
        // UpAlign(worker_states_[w_i].w_written_offset);
        const u64 upper_offset = worker_states_[w_i].w_written_offset;
        const auto size_aligned = upper_offset - lower_offset;
        PrepareWrite(logger.wal_buffer, size_aligned, w_offset_);
        w_offset_ += size_aligned;
        if (start_profiling && !FLAGS_wal_path.empty()) {
          statistics::recovery::gct_write_bytes += size_aligned;
        }
        // LOG_INFO("logger: %lu written_offset upalign w_offset: %lu
        // wal_gct_cursor: %lu written_offset: %lu", w_i, w_offset_/MB,
        // wal_gct_cursor, worker_states_[w_i].w_written_offset/MB);
      }
    }
  }
}

void GroupCommitExecutor::AddCheckpointTarget(logid_t min_all_workers_gsn_) {
  // Add the checkpoint target
  u32 shard_idx = (w_offset_ - w_start_offset_) / shard_wal_size_;
  cp_target_[shard_idx] = std::make_tuple(w_offset_, min_all_workers_gsn_);

  // Log the checkpoint
  // LOG_INFO("wid: %lu w_offset: %lu gsn: %lu cp_target.size: %d shard_idx:
  // %lu",
  //          worker_thread_id, w_offset_, min_all_workers_gsn_,
  //          cp_target_.size(), shard_idx);
  checkpoint_added_ = true;
}

/**
 * @brief Flush all logs to disk
 */
void GroupCommitExecutor::PhaseTwo() {
  if (FLAGS_wal_fsync) {
    fdatasync(wal_fd_);
  }
}

void GroupCommitExecutor::PhaseThree() {
  for (size_t w_i = 0; w_i < total_logger_cnt_; w_i++) {
    auto &logger = all_loggers_[w_i];
    /**
     * @brief Persist all BLOBs for all transactions
     */
    if (FLAGS_blob_logging_variant >= 0) {
      for (size_t tx_i = 0; tx_i < ready_to_commit_cut_[w_i]; tx_i++) {
        PrepareLargePageWrite(logger.precommitted_queue[tx_i]);
      }
      for (size_t tx_i = 0; tx_i < ready_to_commit_rfa_cut_[w_i]; tx_i++) {
        PrepareLargePageWrite(logger.precommitted_queue_rfa[tx_i]);
      }
    }
  }
}

void GroupCommitExecutor::PhaseFour() {
  if (has_blob_) {
    if (FLAGS_wal_fsync) {
      fdatasync(wal_fd_);
    }

    if (FLAGS_blob_logging_variant >= 0) {
      for (auto [start_pid, pg_cnt, extent_pid, extent_sz] : lp_req_) {
        if (!completed_lp_.contains(start_pid)) {
          completed_lp_.insert(start_pid);
          if (FLAGS_enable_blob) {
            buffer_->EvictExtent(extent_pid, extent_sz);
          }
        }
      }
    }
  }
}

void GroupCommitExecutor::PhaseFive() {
  for (size_t w_i = 0; w_i < total_logger_cnt_; w_i++) {
    auto &logger = all_loggers_[w_i];

    logger.gct_cursor.store(worker_states_[w_i].w_written_offset,
                            std::memory_order_release);
    // -------------------------------------------------------------------------------------
    // Commit transactions in precommitted_queue

    u64 tx_i = 0;

    for (tx_i = 0;
         tx_i < ready_to_commit_cut_[w_i] &&
         logger.precommitted_queue[tx_i].max_observed_gsn_ <=
             min_all_workers_gsn_ &&
         logger.precommitted_queue[tx_i].StartTS() <= min_hardened_commit_ts_;
         tx_i++) {
      if (FLAGS_enable_blob) {
        CompleteTxnBlobs(logger.precommitted_queue[tx_i]);
      }
      // logger.precommitted_queue[tx_i].state_ =
      //     transaction::Transaction::State::COMMITTED;
      if (FLAGS_fp_enable) {
        buffer_->GetFreePageManager()->PublicFreeRanges(
            logger.precommitted_queue[tx_i].ToFreeExtents());
      }
    }
    Ensure(tx_i <= ready_to_commit_cut_[w_i]);
    if (tx_i > 0) {
      {
        std::unique_lock<std::mutex> g(logger.precommitted_queue_mutex);
        logger.precommitted_queue.erase(logger.precommitted_queue.begin(),
                                        logger.precommitted_queue.begin() +
                                            tx_i);
      }
      if (w_i < FLAGS_worker_count) {
        completed_txn_ += tx_i;
      }
    }
    // -------------------------------------------------------------------------------------
    // Commit transactions in precommitted_queue_rfa
    for (tx_i = 0; tx_i < ready_to_commit_rfa_cut_[w_i]; tx_i++) {
      if (FLAGS_enable_blob) {
        CompleteTxnBlobs(logger.precommitted_queue_rfa[tx_i]);
      }
      // logger.precommitted_queue_rfa[tx_i].state_ =
      //     transaction::Transaction::State::COMMITTED;

      if (FLAGS_fp_enable) {
        buffer_->GetFreePageManager()->PublicFreeRanges(
            logger.precommitted_queue_rfa[tx_i].ToFreeExtents());
      }
    }
    if (tx_i > 0) {
      {
        std::unique_lock<std::mutex> g(logger.precommitted_queue_mutex);
        logger.precommitted_queue_rfa.erase(
            logger.precommitted_queue_rfa.begin(),
            logger.precommitted_queue_rfa.begin() + tx_i);
      }
      if (w_i < FLAGS_worker_count) {
        completed_txn_ += tx_i;
      }
    }
  }
}

// -------------------------------------------------------------------------------------
void GroupCommitExecutor::PrepareLargePageWrite(transaction::Transaction &txn) {
  assert(FLAGS_blob_logging_variant >= 0);
  Ensure(txn.ToFlushedLargePages().size() == txn.ToEvictedExtents().size());
  if (txn.ToFlushedLargePages().size() > 0) {
    has_blob_ = true;
  }
  if (FLAGS_enable_blob) {
    for (auto &[pid, pg_cnt] : txn.ToFlushedLargePages()) {
      if (!already_prep_.contains(pid)) {
        already_prep_.emplace(pid);
        if (FLAGS_blob_normal_buffer_pool) {
          for (auto id = pid; id < pid + pg_cnt; id++) {
            PrepareWrite(reinterpret_cast<u8 *>(buffer_->ToPtr(id)), PAGE_SIZE,
                         id * PAGE_SIZE);
          }
        } else {
          PrepareWrite(reinterpret_cast<u8 *>(buffer_->ToPtr(pid)),
                       pg_cnt * PAGE_SIZE, pid * PAGE_SIZE);
        }
      }
    }
  }

  /* Update large-page requirements for txn commit */
  for (size_t idx = 0; idx < txn.ToFlushedLargePages().size(); idx++) {
    auto &[pid, pg_cnt] = txn.ToFlushedLargePages()[idx];
    auto &[extent_pid, extent_sz] = txn.ToEvictedExtents()[idx];
    if (start_profiling) {
      statistics::blob::blob_logging_io += pg_cnt * PAGE_SIZE;
    }
    lp_req_.emplace_back(pid, pg_cnt, extent_pid, extent_sz);
  }
}

void GroupCommitExecutor::CompleteTxnBlobs(transaction::Transaction &txn) {
  if (FLAGS_blob_logging_variant >= 0) {
    for (auto &lp : txn.ToFlushedLargePages()) {
      completed_lp_.erase(lp.start_pid);
    }
  }
}

size_t GroupCommitExecutor::GetWalFileOffset() { return w_offset_; }

size_t GroupCommitExecutor::UpdateCheckpointOffset(u64 offset) {
  checkpoint_offset_ += offset;
  return checkpoint_offset_;
}

void GroupCommitExecutor::TrimWAL() {
  SendBlkdiscardReq(w_start_offset_, w_end_offset_ - w_start_offset_);
}

void GroupCommitExecutor::SendBlkdiscardReq(u64 start_offset, u64 size) {
  u64 start_lba = start_offset / sector_size_;
  u32 num_sectors = size / sector_size_;
  try {
    std::string command =
        "echo " + FLAGS_user_pwd + " | sudo -S nvme dsm " + FLAGS_db_path +
        " --ad --slbs=" + std::to_string(start_lba) +
        " --blocks=" + std::to_string(num_sectors) + " 2>/dev/null";
    std::string output = ExecCommand(command);
    return;
  } catch (const std::runtime_error &e) {
    // std::cerr << "Error: " << e.what() << std::endl;
  }
  return;
}

std::string GroupCommitExecutor::ExecCommand(const std::string &cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  return result;
}

void GroupCommitExecutor::UpdateCheckpointOffsetUntilCurGSN(
    u32 shard_idx, logid_t cur_cp_gsn) {
  std::unique_lock<std::mutex> x(cp_lock);
  if (LogManager::checkpoint_gsn.load() > cur_cp_gsn) {
    LOG_INFO("strange things happening");
  }

  size_t target_offset = std::get<0>(cp_target_[shard_idx]);
  logid_t target_gsn = std::get<1>(cp_target_[shard_idx]);
  checkpoint_offset_ = target_offset;
  // LOG_INFO("shard_idx: %lu target_offset: %lu target_gsn: %lu
  // cp_target.size(): %lu", shard_idx, target_offset, target_gsn,
  // cp_target_.size());
  cp_target_[shard_idx] = std::make_tuple(0, target_gsn);
}

size_t GroupCommitExecutor::GetCheckpointOffset() { return checkpoint_offset_; }

// -------------------------------------------------------------------------------------
Checkpointer::Checkpointer(buffer::BufferManager *buffer,
                           GroupCommitExecutor *group_committer,
                           std::atomic<bool> &keep_running, u32 shard_cnt)
    : buffer_(buffer), group_committer_(group_committer),
      keep_running_(&keep_running), shard_cnt(shard_cnt) {
  // Initialize each element to false
  shard_checkpointed_gsn_.resize(shard_cnt, 0);
  last_cp_shard_idx = 0;
}

void Checkpointer::UpdateShardCheckpointGSN(u32 shard_idx, logid_t gsn) {
  Ensure(shard_checkpointed_gsn_[shard_idx] <= gsn);
  shard_checkpointed_gsn_[shard_idx] = gsn;
}

void Checkpointer::UpdateCheckpointOffsetUntilShard(u32 shard_idx) {
  logid_t min_cp_gsn = shard_checkpointed_gsn_[shard_idx];

  Ensure(LogManager::checkpoint_gsn.load() <= min_cp_gsn);
  LogManager::checkpoint_gsn.store(min_cp_gsn);
  group_committer_->UpdateCheckpointOffsetUntilCurGSN(
      shard_idx, LogManager::checkpoint_gsn.load());
}

bool Checkpointer::RunCheckpointThread(u32 shard_idx, bool gc) {
  /* check whether we need to start checkpoint*/
  if (group_committer_ == nullptr) {
    LOG_INFO("nullptr");
    return true;
  }

  if (buffer_->alloc_cnt_ < FLAGS_bm_physical_gb * GB / PAGE_SIZE * 0.99) {
    return true;
  }

  if (gc) {
    if (buffer_->sspace_->GetBlockListSize(storage::space::State::GC_READ) ||
        buffer_->sspace_->GetBlockListSize(storage::space::State::GC_IN_USE) ||
        buffer_->sspace_->GetGCTargetSize()) {
      logid_t min_flushed_gsn =
          buffer_->FuzzyCheckpoint(1, shard_idx, shard_cnt);
      // for(u32 s = 0; s< shard_cnt; s++){
      //   logid_t min_flushed_gsn =
      //   buffer_->FuzzyCheckpoint(1, s, shard_cnt);
      // }
      return true;
    }
  }

  logid_t target_gsn = std::get<1>(group_committer_->cp_target_[shard_idx]);

  if (target_gsn == 0) {
    return true;
  }

  /* 1. Get checkpoint_target gsn from gct */
  logid_t max_target_gsn = LogManager::global_min_gsn_flushed.load();

  if (!max_target_gsn || max_target_gsn < shard_checkpointed_gsn_[shard_idx]) {
    return true;
  }

  if (target_gsn <= LogManager::checkpoint_gsn.load()) {
    std::unique_lock<std::mutex> x(group_committer_->cp_lock);
    UpdateShardCheckpointGSN(shard_idx, LogManager::checkpoint_gsn.load());
    return true;
  }

  //  LOG_INFO("wid: %lu shard_idx: %lu target_gsn: %lu shard_gsn: %lu "
  //        "cp_gsn : %lu",
  //        worker_thread_id, shard_idx, target_gsn,
  //        shard_checkpointed_gsn_[shard_idx],
  //        LogManager::checkpoint_gsn.load());

  Ensure(target_gsn >= shard_checkpointed_gsn_[shard_idx]);

  if (target_gsn < shard_checkpointed_gsn_[shard_idx]) {
    group_committer_->UpdateCheckpointOffsetUntilCurGSN(
        shard_idx, shard_checkpointed_gsn_[shard_idx]);
    return true;
  }

  // LOG_INFO("shard_idx: %lu max_target_gsn: %lu target_gsn: %lu", shard_idx,
  // max_target_gsn, target_gsn);

  /* 3. Gather dirty pages with p_gsn < target_gsn in each shard and flush them
   */
  logid_t min_flushed_gsn =
      buffer_->FuzzyCheckpoint(target_gsn, shard_idx, shard_cnt);

  UpdateShardCheckpointGSN(shard_idx, min_flushed_gsn);

  UpdateCheckpointOffsetUntilShard(shard_idx);

  // LOG_INFO("wid: %lu shard_idx: %lu target_gsn: %lu min_flushed_gsn: %lu "
  //          "cp_gsn : %lu",
  //          worker_thread_id, shard_idx, target_gsn, min_flushed_gsn,
  //          LogManager::checkpoint_gsn.load());

  return true;
}

}; // namespace leanstore::recovery