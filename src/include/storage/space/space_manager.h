#pragma once

#include "common/typedefs.h"
#include "device.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "liburing.h"
#include "storage/page.h"
#include "transaction/transaction.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace leanstore::storage::space {

constexpr size_t TRIM_BUFFER_SIZE = 16; // Buffer size for reusable PIDs

static constexpr size_t MAX_REUSABLE_PIDS = 64 * KB; // Adjust as needed

enum class State : u16 {
  EMPTY = 0,
  IN_USE = 1,
  OPEN = 2,
  FULL = 3,
  GC_IN_USE = 4,
  GC_READ = 5,
  GC_WRITE = 6,
  FULL_PAD = 7
};

struct PID2Offset {
  u64 packed_data __attribute__((packed)); // now using full 64 bits

  void set(u64 offset, u16 comp_size) {
    // Ensure comp_size is within valid range before any bitwise operations
    Ensure(comp_size <=
           0x7FFF); // comp_size now can be between 0 and 32767 (15 bits)

    offset &= ((1ULL << 50) - 1); // 50 bits for offset
    comp_size &= 0x7FFF;          // 15 bits for comp_size (0x7FFF = 32767 max)

    packed_data = (offset << 15) |
                  comp_size; // shift by 15 bits to allocate space for comp_size
  }

  u64 get_offset() const {
    return (packed_data >> 15) &
           ((1ULL << 50) - 1); // shift right by 15 bits to get the offset
  }

  u16 get_comp_sz() const {
    return packed_data & 0x7FFF; // extract the lower 15 bits for comp_size
  }
} __attribute__((packed));

struct Offset2PIDs {
  std::vector<pageid_t> pids;
  u16 cnt = 0;
} __attribute__((packed));

struct Block {
  std::vector<Offset2PIDs> pages;
};

struct BlockMetadata {
  u64 cur_size = 0;
  u32 total_pid_cnt = 0;
  u32 invalidated_pid_cnt = 0;
  u16 block_invalidation_cnt = 0;
  State state = State::EMPTY; // Assuming `State` fits in u16  or u16
  logid_t edt = 0;            // Ensure alignment at end
  logid_t block_min_edt = std::numeric_limits<logid_t>::max();
  logid_t block_max_edt = 0;
  // std::vector<logid_t> invalidation_history_gsn;

  BlockMetadata(logid_t edt_val = 0, u32 size = 0, State st = State::EMPTY,
                std::size_t history_size = 1)
      : cur_size(size), total_pid_cnt(0), invalidated_pid_cnt(0),
        block_invalidation_cnt(0), state(st), edt(edt_val) {
    //  invalidation_history_gsn.reserve(history_size);
  }
};

struct StorageSpace {
  std::vector<Block> blocks;                 // Actual blocks
  std::vector<BlockMetadata> block_metadata; // Metadata for each block
  std::unordered_map<State, std::vector<blockid_t>> block_lists;
  std::unordered_map<State, std::mutex>
      state_mutexes; // Mutexes for each state list
  std::shared_mutex blk_state_mutex_;
  std::shared_mutex padding_mutex_;
  u32 min_comp_size_;
  u32 page_cnt_per_block_;
  u64 block_cnt_;  // total number of blocks
  u64 block_size_; // zone alignment size
  u64 max_w_ptr_;  // block_size_ and max_w_ptr_ can be different when using ZNS
  u64 max_mapped_pages_cnt_ = 0; // == max_db_capacity_gb * GB /PAGE_SIZE

  u32 max_open_block_;
  std::vector<blockid_t> active_block_list_;
  std::atomic<u32> active_block_cnt_{0};

  std::vector<std::vector<blockid_t>> write_group_;

  std::atomic<bool> gc_triggered{false};

  std::atomic<u32> not_compensation_write{0};
  std::atomic<bool> compensation_write{false};

  StorageSpace(u64 block_cnt, u32 page_cnt_per_block,
               std::size_t write_history_length, u32 min_comp_size,
               u64 block_size, u64 max_w_ptr, u64 max_open_block_cnt);

  bool MoveBlockToList(blockid_t bid, State from, State to) {
    if (!IsBlockInList(bid, from)) {
      return false;
    }

    // Acquire locks for the "from" and "to" states in a deadlock-free manner
    std::unique_lock<std::mutex> from_lock(state_mutexes[from],
                                           std::defer_lock);
    std::unique_lock<std::mutex> to_lock(state_mutexes[to], std::defer_lock);
    std::lock(from_lock, to_lock); // Deadlock-free locking

    Ensure(block_metadata[bid].state == from);

    block_metadata[bid].state = to;

    auto &from_list = block_lists[from];
    auto &to_list = block_lists[to];
    Ensure(std::find(to_list.begin(), to_list.end(), bid) == to_list.end());
    auto it = std::find(from_list.begin(), from_list.end(), bid);
    if (it != from_list.end()) {
      from_list.erase(it);
    } else {
      return false;
    }

    // Add to target list
    if (to == State::EMPTY) {
      block_lists[to].insert(block_lists[to].begin(), bid);
    } else {
      block_lists[to].push_back(bid);
    }

    // if (to == State::FULL) {
    //   std::cout << "[NOWA] bid: " << bid << " closed" << std::endl;
    //   Ensure(block_metadata[bid].cur_size == max_w_ptr_);
    // }

    // if (from == State::EMPTY || from == State::GC_WRITE) {
    //   std::cout << "[NOWA] bid: " << bid << " opened" << std::endl;
    // }

    return true;
  }

  void UpdateBlockMetadata(blockid_t bid, u64 size, logid_t edt, State state) {
    // Check if the block ID is valid
    Ensure(bid < block_cnt_);
    auto &metadata = block_metadata[bid];
    metadata.cur_size = size;
    metadata.edt = edt;
    metadata.state = state;
  }

  // Method to get the size of the block list for a given State
  std::size_t GetBlockListSize(State type) { return block_lists[type].size(); }

  bool IsBlockInList(blockid_t bid, State state) {
    auto &block_list = block_lists[state];
    auto it = std::find(block_list.begin(), block_list.end(), bid);
    return it != block_list.end();
  }

  u32 GetGCTargetSize() {
    u32 cnt = 0;
    auto &inuse_list = block_lists[State::IN_USE];
    for (blockid_t bid : inuse_list) {
      if (block_metadata[bid].cur_size == max_w_ptr_)
        cnt++;
    }

    auto &gcinuse_list = block_lists[State::GC_IN_USE];
    for (blockid_t bid : gcinuse_list) {
      cnt++;
    }
    return cnt;
  }

  u32 GetActiveBlockSize() {
    // either open or inuse user write
    u32 cnt = 0;
    auto &inuse_list = block_lists[State::IN_USE];
    for (blockid_t bid : inuse_list) {
      if (block_metadata[bid].cur_size != max_w_ptr_)
        cnt++;
    }

    auto &gcinuse_list = block_lists[State::OPEN];
    for (blockid_t bid : gcinuse_list) {
      cnt++;
    }
    return cnt;
  }
};

// SpaceManager is responsible for data placement when using out-of-place write
class SpaceManager {
public:
  // Constructor and Destructor
  SpaceManager(int blockfd, u64 max_byte_offset, u64 block_alginment_size,
               u64 block_size, u64 block_cnt, u32 max_open_block_cnt,
               u64 max_mapped_pages_cnt, u32 sector_size_, NVMeDevice *ssdsim);
  // Initialization and Setup
  void Construction();
  void Init();
  void AllocMappingTableSpace();
  void Destroy();

  // selecting block id for data placement
  blockid_t SelectBlockIDToWrite(u64 max_write_sz, logid_t cur_gsn,
                                 logid_t avg_wgap, u32 pid_cnt, u32 group_idx,
                                 logid_t min_edt, logid_t max_edt);
  blockid_t SelectOpenBlockID(u64 max_write_sz, logid_t avg_edt,
                              logid_t cur_gsn, u32 pid_cnt, u32 group_idx,
                              logid_t min_edt, logid_t max_edt);
  blockid_t SelectOpenBlockIDWithLowestFillFactor(logid_t avg_edt,
                                                  u64 max_write_sz,
                                                  u32 pid_cnt);
  blockid_t SelectOpenBlockIDWithClosestEDT(logid_t avg_edt, u64 max_write_sz,
                                            u32 pid_cnt, u32 group_idx,
                                            logid_t min_edt, logid_t max_edt);
  blockid_t SelectOpenBlockIDRandom(u64 max_write_sz, logid_t avg_edt,
                                    u32 pid_cnt);
  blockid_t SelectEmptyBlockID(u64 max_write_sz, logid_t avg_edt,
                               logid_t cur_gsn, u32 pid_cnt, logid_t min_edt,
                               logid_t max_edt);
  blockid_t SelectFullBlockID(u64 max_write_sz, logid_t avg_edt,
                              logid_t cur_gsn, u32 pid_cnt);
  blockid_t SelectEmptyBlockRandom();
  blockid_t SelectEmptyBlockWithMinInvalidationCnt();
  blockid_t SelectFullBlockToGCRead();
  std::vector<blockid_t> SelectReadGCedBlocksToGCWrite(u64 write_sz);
  blockid_t SelectUnfilledOpenBlockID(u64 max_write_sz, logid_t avg_edt,
                                      logid_t cur_gsn, u32 pid_cnt);
  blockid_t SelectGCInUseBlockToGCRead();
  bool ShouldOpenNewBlockID(logid_t global_min_edt, logid_t global_max_edt,
                            logid_t avg_edt, u64 write_size);
  blockid_t AdjustEDTRange(logid_t global_min_edt, logid_t global_max_edt,
                           logid_t avg_edt, blockid_t bid);
  bool IssuePaddingWritesToFullBlock(blockid_t bid);
  bool AddBlockIDToActiveBlockList(blockid_t bid);
  bool BlockIDInActiveBlockList(blockid_t bid);
  bool RemoveBlockIDFromActiveBlockList(blockid_t bid);
  void AddWriteGroupHistory();
  void MoveGCedBlockToOpenList(blockid_t bid, logid_t prev_min_edt,
                               logid_t lsn_gap, logid_t global_min_edt,
                               logid_t global_max_edt);
  logid_t GetBlockEDTGap();
  void SetMinMaxEDTPerBlock(blockid_t bid, logid_t min_edt, logid_t max_edt);
  void SetMinMaxEDTToMinimum(blockid_t bid, logid_t prev_min, logid_t lsn_gap);

  // single block operation
  logid_t GetAvgWriteGap(blockid_t bid);
  u64 GetBlockOffset(blockid_t bid);
  u64 GetBlockWriteOffset(blockid_t bid);
  bool BlockNeedsGC(blockid_t bid);
  bool BlockEDTIsOutdated(blockid_t bid, logid_t cur_gsn);
  logid_t CalcBlockAvgEDT(blockid_t bid, logid_t avg_edt, u32 pid_cnt);
  void UpdateBlockMetadataAfterWrite(blockid_t bid, u64 write_sz, u32 pid_cnt,
                                     logid_t edt);
  void UpdateBlockMetadataAfterSyncGCWrite(blockid_t bid, u64 write_sz,
                                           u32 pid_cnt);
  void UpdateBlockMetadataAfterAsyncGCWrite(blockid_t bid, u64 write_sz,
                                            u32 pid_cnt);
  u64 CountWrittenPIDsInBlock(blockid_t bid);
  blockid_t GetMinEDTBlockID();
  void EraseBlock(blockid_t bid, logid_t gsn, bool sync_gc);
  void ResetBlockMetadata(blockid_t bid);
  void ResetBlockSpace(blockid_t bid);
  void AddBlockInvalidationHistory(blockid_t bid);
  void ValidPIDsInSecurePlace(blockid_t bid);
  void CloseAllOpenBlocks();
  logid_t GetHighestEDT();

  // Page-Level Operations
  bool PageIsValid(blockid_t bid, u32 p, u16 pidx);
  bool PIDOffsetIsValid(pageid_t pid);
  bool PIDIsDeallocated(pageid_t pid);
  bool PIDOffsetIsInBlock(pageid_t pid, blockid_t bid);
  blockid_t PIDOffsetBlock(pageid_t pid);
  bool PIDInGCBlock(pageid_t pid);
  bool PIDInGCRBlock(pageid_t pid);
  bool PIDInGCWBlock(pageid_t pid);
  blockid_t PIDBlock(pageid_t pid);
  void UpdatePID2Offset(pageid_t pid, u64 offset, u16 comp_sz, blockid_t bid,
                        logid_t gsn, bool sync_gc);
  void UpdateOffset2PIDs(pageid_t pid, blockid_t bid, u64 offset);
  void ResetOffset2PIDs(blockid_t bid, u32 pn, u16 pcnt, logid_t gsn);
  void ConvertOffset2PIDs(u64 page_offset);
  void UpdateBlockInvalidCount(u64 prev_offset, pageid_t pid);
  u64 ValidPIDsCntInBlock(blockid_t bid);
  u64 TotalPageCntInBlock(blockid_t bid);
  bool IsUniqueOffset(u64 offset);
  pageid_t GetPIDFromOffset(blockid_t bid, u32 p, u16 pidx);
  bool UpdateBlockMetadataAfterReadGC(blockid_t bid);
  void UpdateBlockMetadataAfterGCWrite(blockid_t bid, u64 write_sz, u32 pid_cnt,
                                       logid_t edt);
  void MovePaddedBlocksToFullList();
  void CheckOpenBlockForPadding(u64 write_sz);
  void CloseOneOpenBlock();
  int EmptySlotCountInActiveBlockList();
  blockid_t FindFirstGCInUseBlockInActiveList();
  int GCInProgessBlockCountInActiveList();

  // I/O Operations
  void HotUringSubmit();
  void CheckpointMappingTable();

  // WAL log
  void LogPID2Offset(pageid_t pid);
  void LogValidPage(pageid_t pid, storage::Page *page);

  // Dealloc list
  void AddPIDToDeallocList(u32 t_id, pageid_t pid, logid_t p_gsn);
  pageid_t AssignPIDUponAlloc(u32 t_id);
  bool HasReusablePID(u32 t_id);
  void AddReusablePID(size_t t_id, pageid_t pid);
  u32 BlockGCReadCnt();

  // Trim Thread Management
  void StartTrimThread();
  void StopTrimThread();
  void ProcessTrimQueue();
  void ProcessDeallocList(u32 t_id, logid_t p_gsn);
  void TrimPID(pageid_t pid);
  void TrimBlock(blockid_t bid);
  bool IsPIDInDeallocatedList(pageid_t pid);
  void SendBlkdiscardReq(u64 start_offset, u64 size);
  std::string ExecCommand(const std::string &cmd);

  // space management
  float GetDBSize();
  bool ReachedAllocSize();
  bool ShouldTriggerGC(u64 write_size);
  bool ShouldTriggerGCZNS(u64 write_size);

  u64 block_cnt_; // total number of blocks
  u32 page_cnt_per_block_;
  u64 block_size_; // zone alignment size
  u64 max_w_ptr_;  // maximum block fill factor
  u32 max_open_block_cnt_ = 0;
  u32 cur_max_open_block_cnt_ = 0;
  u32 sector_size_ = 0;
  u64 max_userspace_capacity_;
  PID2Offset *PID2Offset_table_;
  StorageSpace *sspace_;
  NVMeDevice *device_;
  bool discarded = false;

  u64 max_mapped_pages_cnt_; // == max_db_capacity_gb * GB /PAGE_SIZE
  std::vector<std::atomic<size_t> *> reusable_pids_heads;
  std::vector<std::atomic<size_t> *> reusable_pids_tails;
  std::vector<std::vector<pageid_t>> reusable_pids_buffers;
  std::mutex trim_mutex;

private:
  // Member Variables
  size_t mt_offset_;     // Mapping table offset (PID2Offset mapping table)
  size_t mt_end_offset_; // Mapping table end offset (PID2Offset mapping table)
  int mt_fd_; //  Mapping table file descriptor (default in the same device)

  struct io_uring ring_;
  u32 submitted_io_cnt_{0};
  u64 virtual_cnt_;
  u32 min_comp_size_;
  u32 io_handler_cnt_;

  // Trim Thread
  std::thread trim_thread_;
  std::atomic<bool> trim_thread_active_{false};
  logid_t last_trim_gsn = 0;
  std::unordered_map<u32, std::deque<std::pair<pageid_t, logid_t>>>
      deallocated_pid_list_;
  // handler
  std::vector<std::vector<pageid_t>> reusable_pids; // Set of reusable PIDs
  std::queue<std::pair<u64, u64>> trim_queue;       // Queue of trim tasks

  std::mutex reusable_mutex;
  size_t optimal_x = 0;
  std::mt19937 rng_;
};

} // namespace leanstore::storage::space
