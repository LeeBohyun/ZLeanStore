#include "storage/space/space_manager.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "liburing.h"
#include "storage/space/device.h"
#include "storage/space/garbage_collector.h"
#include "transaction/transaction_manager.h"
#include "gtest/gtest_prod.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <libnvme.h>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <nvme/types.h>
#include <queue>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <thread>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

using LogEntry = leanstore::recovery::LogEntry;

namespace leanstore::storage::space {

StorageSpace::StorageSpace(u64 block_cnt, u32 page_cnt_per_block,
                           std::size_t write_history_length, u32 min_comp_size,
                           u64 block_size, u64 max_w_ptr,
                           u64 max_open_block_cnt)
    : blocks(block_cnt), page_cnt_per_block_(page_cnt_per_block),
      max_open_block_(max_open_block_cnt), block_metadata(block_cnt),
      min_comp_size_(min_comp_size), block_cnt_(block_cnt),
      block_size_(block_size), max_w_ptr_(max_w_ptr) {
  if (FLAGS_use_out_of_place_write) {
    blocks.reserve(block_cnt);
    block_metadata.reserve(block_cnt);
    for (std::size_t i = 0; i < block_cnt; ++i) {
      // Initialize pages with the correct size
      blocks[i].pages.resize(page_cnt_per_block);

      block_metadata[i] =
          BlockMetadata(0, 0, State::EMPTY, write_history_length);

      for (u32 p = 0; p < page_cnt_per_block; p++) {
        // Reserve memory for each page's pids
        if (FLAGS_use_compression) {
          blocks[i].pages[p].pids.reserve(PAGE_SIZE / MIN_COMP_SIZE);
        } else {
          blocks[i].pages[p].pids.reserve(1);
        }
      }
    }

    // Initialize the block lists based on states
    block_lists[State::EMPTY] = std::vector<blockid_t>();
    block_lists[State::IN_USE] = std::vector<blockid_t>();
    block_lists[State::OPEN] = std::vector<blockid_t>();
    block_lists[State::FULL] = std::vector<blockid_t>();
    block_lists[State::GC_IN_USE] = std::vector<blockid_t>();
    block_lists[State::GC_READ] = std::vector<blockid_t>();
    block_lists[State::GC_WRITE] = std::vector<blockid_t>();
    block_lists[State::FULL_PAD] = std::vector<blockid_t>();

    // Reserve memory for each list based on block_cnt_
    block_lists[State::EMPTY].reserve(block_cnt);
    block_lists[State::IN_USE].reserve(block_cnt);
    block_lists[State::OPEN].reserve(block_cnt);
    block_lists[State::FULL].reserve(block_cnt);
    block_lists[State::GC_IN_USE].reserve(block_cnt);
    block_lists[State::GC_READ].reserve(block_cnt);
    block_lists[State::GC_WRITE].reserve(block_cnt);
    block_lists[State::FULL_PAD].reserve(block_cnt);

    u64 storage_space_size = block_cnt * sizeof(block_lists);
    storage_space_size += sizeof(Offset2PIDs) * (PAGE_SIZE / MIN_COMP_SIZE) *
                          page_cnt_per_block * block_cnt;

    fprintf(stderr, "max storage space metadata size: %.2f MB\n",
            (double)storage_space_size / MB);

    active_block_list_.assign(max_open_block_cnt, block_cnt);

    fprintf(stderr, "max open block cnt: %lu\n", max_open_block_);

    gc_triggered.store(false, std::memory_order_relaxed);
  }
}

SpaceManager::SpaceManager(int blockfd, u64 max_byte_offset,
                           u64 block_alignment_size, u64 block_size,
                           u64 block_cnt, u32 max_open_block_cnt,
                           u64 max_mapped_pages_cnt, u32 sector_size,
                           NVMeDevice *device)
    : mt_fd_(blockfd), max_userspace_capacity_(max_byte_offset),
      block_size_(block_alignment_size), max_w_ptr_(block_size),
      block_cnt_(block_cnt), max_open_block_cnt_(max_open_block_cnt),
      max_mapped_pages_cnt_(max_mapped_pages_cnt), sector_size_(sector_size),
      device_(device) {
  cur_max_open_block_cnt_ = max_open_block_cnt;
  Construction();
  if (FLAGS_simulator_mode) {
    assert(device_ != nullptr && "ssdsim_ not initialized.");
    assert(device_->ssdsim_ != nullptr && "ssdsim_ not initialized.");
    assert(device_->gc_ != nullptr && "gc_ not initialized.");
  }
}

void SpaceManager::Construction() {
  // Initialize the maximum counts based on configurations

  io_handler_cnt_ =
      FLAGS_worker_count + FLAGS_page_provider_thread + FLAGS_wal_enable +
      FLAGS_checkpointer_cnt * FLAGS_wal_enable + FLAGS_garbage_collector_cnt;

  // Initialize deallocated PID list
  if (!FLAGS_use_out_of_place_write) {
    reusable_pids.resize(io_handler_cnt_);
    reusable_pids_heads.resize(io_handler_cnt_);
    reusable_pids_tails.resize(io_handler_cnt_);
    reusable_pids_buffers.resize(io_handler_cnt_,
                                 std::vector<pageid_t>(MAX_REUSABLE_PIDS));
    // Allocate `std::atomic` objects for each thread
    for (size_t t = 0; t < io_handler_cnt_; ++t) {
      reusable_pids_heads[t] =
          new std::atomic<size_t>(0); // Initialize head to 0
      reusable_pids_tails[t] =
          new std::atomic<size_t>(0); // Initialize tail to 0
    }
  }

  if (FLAGS_use_trim && !FLAGS_use_out_of_place_write) {
    // Start the trim thread
    StartTrimThread();
  }

  if (FLAGS_use_out_of_place_write) {
    page_cnt_per_block_ = block_size_ / PAGE_SIZE;
    min_comp_size_ = MIN_COMP_SIZE;
  }

  Init();

  if (FLAGS_use_out_of_place_write) {
    fprintf(stderr, "block cnt: %lu block_size: %lu max_space: %lu\n",
            block_cnt_, block_size_ / MB, max_userspace_capacity_ / MB);

    Ensure(block_size_ == FLAGS_block_size_mb * MB);
    Ensure(block_cnt_ * block_size_ == max_userspace_capacity_);
    Ensure(block_cnt_ == sspace_->block_cnt_);
  }

  std::random_device rd;
  rng_ = std::mt19937(rd());
}

void SpaceManager::Init() {
  // Allocate memory for PID2Offset_table_
  PID2Offset_table_ = new PID2Offset[max_mapped_pages_cnt_];

  fprintf(stderr, "maximum mapping table size: %2f (MB)\n",
          (float)(max_mapped_pages_cnt_ * sizeof(PID2Offset)) / MB);

  // Pre-touch to trigger physical memory allocation
  for (size_t i = 0; i < max_mapped_pages_cnt_; i++) {
    PID2Offset_table_[i].set(0,
                             0); // Access (read is enough, but write is better)
  }

  if (FLAGS_use_out_of_place_write) {
    fprintf(stderr, "block size: %lu w_ptr: %lu \n", block_size_, max_w_ptr_);
    Ensure(FLAGS_bm_evict_batch_size * PAGE_SIZE /
               FLAGS_write_buffer_partition_cnt <=
           block_size_);

    // Allocate and initialize sspace_
    sspace_ = new StorageSpace(block_cnt_, page_cnt_per_block_,
                               WRITE_HISTORY_LENGTH, min_comp_size_,
                               block_size_, max_w_ptr_, max_open_block_cnt_);
    fprintf(
        stderr,
        "block_cnt: %lu page_cnt_per_block: %lu cur_max_open_block_cnt_: %lu\n",
        block_cnt_, page_cnt_per_block_, cur_max_open_block_cnt_);
  }

  if (FLAGS_use_out_of_place_write) {
    // Initialize blocks and their metadata
    for (blockid_t bid = 0; bid < block_cnt_; bid++) {
      // Initialize Block Metadata
      ResetBlockMetadata(bid);
      ResetBlockSpace(bid);
      // Add block to the free list
      sspace_->block_lists[State::EMPTY].push_back(bid);
    }

    // Clear other block lists (as they are populated by block state changes)
    sspace_->block_lists[State::IN_USE].clear();
    sspace_->block_lists[State::OPEN].clear();
    sspace_->block_lists[State::FULL].clear();
    sspace_->block_lists[State::GC_READ].clear();
    sspace_->block_lists[State::FULL_PAD].clear();

    sspace_->max_mapped_pages_cnt_ = max_mapped_pages_cnt_;
  }
}

void SpaceManager::AllocMappingTableSpace() {
  // mapping info offset is after the dwb offset
  // TODO needs fixing
  mt_offset_ =
      FLAGS_bm_virtual_gb * GB + FLAGS_max_wal_capacity_gb * GB + block_size_;

  // Align the mapping table size with the block size
  u64 required_space = sizeof(PID2Offset_table_) * max_mapped_pages_cnt_;

  fprintf(stderr, "required space for mapping table: %lu", required_space);

  //  Ensure(required_space % block_size_ == 0);

  // Calculate the end offset for the mapping table
  mt_end_offset_ = mt_offset_ + required_space;

  // Ensure the allocated space matches the calculated space
  Ensure((mt_end_offset_ - mt_offset_) >= required_space);
}

//  Get available bid to serve write request
blockid_t SpaceManager::SelectBlockIDToWrite(u64 write_sz, logid_t cur_gsn,
                                             logid_t avg_edt, u32 pid_cnt,
                                             u32 group_idx, logid_t min_edt,
                                             logid_t max_edt) {
try_again:
  blockid_t bid = block_cnt_;
  // 1. check whether there is an open block appropriate to serve write request
  if (FLAGS_max_open_block_cnt) {
    //  if(!ShouldOpenNewBlockID(min_edt, max_edt, avg_edt, write_sz)){
    bid = SelectOpenBlockID(write_sz, avg_edt, cur_gsn, pid_cnt, group_idx,
                            min_edt, max_edt);
    if (bid < block_cnt_) {
      return bid;
    }
    if (sspace_->gc_triggered.load(std::memory_order_relaxed)) {
      bid = FindFirstGCInUseBlockInActiveList();
    }
    if (bid < block_cnt_) {
      return bid;
    } else {
      if (!ShouldOpenNewBlockID(min_edt, max_edt, avg_edt, write_sz) &&
          sspace_->gc_triggered.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        goto try_again;
      }
    }
    //  }
  } else {
    bid = SelectOpenBlockID(write_sz, avg_edt, cur_gsn, pid_cnt, group_idx,
                            min_edt, max_edt);
    if (bid < block_cnt_) {
      return bid;
    }

    if (sspace_->gc_triggered.load(std::memory_order_relaxed)) {
      bid = FindFirstGCInUseBlockInActiveList();
      if (bid < block_cnt_) {
        return bid;
      }
    }
  }

  bool trigger_gc = ShouldTriggerGC(write_sz);
  if (!trigger_gc) {
    bid = SelectEmptyBlockID(write_sz, avg_edt, cur_gsn, pid_cnt, min_edt,
                             max_edt);
    if (bid < block_cnt_) {
      return bid;
    }
  }

  if (bid == block_cnt_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    if (trigger_gc) { // && ShouldOpenNewBlockID(min_edt, max_edt, avg_edt,
                      // write_sz)
      SelectFullBlockToGCRead();
    }
    goto try_again;
  }
  // if there is no free or open block available, perform GC
  // select read gc targets
  Ensure(bid < block_cnt_);

  return bid;
}

blockid_t SpaceManager::SelectUnfilledOpenBlockID(u64 max_write_sz,
                                                  logid_t avg_edt,
                                                  logid_t cur_gsn,
                                                  u32 pid_cnt) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);

  blockid_t closest_bid = block_cnt_;
  logid_t min_diff = std::numeric_limits<logid_t>::max();

  for (u32 idx = 0; idx < max_open_block_cnt_; ++idx) {
    blockid_t active_bid = sspace_->active_block_list_[idx];
    if (active_bid == block_cnt_) {
      continue; // skip uninitialized slots
    }

    BlockMetadata &meta = sspace_->block_metadata[active_bid];
    // Make sure the block is OPEN
    if (meta.state != State::OPEN) {
      continue;
    }
    if (meta.cur_size >= max_w_ptr_) {
      continue;
    }

    // Accept EDT = 0 as priority (first write to the block)
    if (meta.edt == 0) {
      return active_bid;
    }

    logid_t diff = std::abs(static_cast<int64_t>(avg_edt) -
                            static_cast<int64_t>(meta.edt));

    if (diff < min_diff) {
      min_diff = diff;
      closest_bid = active_bid;
    }
  }

  // Transition selected block from OPEN → IN_USE and update metadata
  if (closest_bid != block_cnt_) {
    Ensure(sspace_->block_metadata[closest_bid].state == State::OPEN);

    if (sspace_->MoveBlockToList(closest_bid, State::OPEN, State::IN_USE)) {
      // std::cout<< " active bid: " << closest_bid
      //         << " fill factor: " << GetBlockWriteOffset(closest_bid)
      //         << " state: " <<
      //         static_cast<int>(sspace_->block_metadata[closest_bid].state )
      //                       << std::endl;
      return closest_bid;
    }
  }

  // No suitable block found
  return block_cnt_;
}

bool SpaceManager::AddBlockIDToActiveBlockList(blockid_t bid) {
  if (!FLAGS_max_open_block_cnt) {
    return true;
  }

  Ensure(sspace_->block_metadata[bid].state == State::EMPTY ||
         sspace_->block_metadata[bid].state == State::GC_IN_USE);

  for (size_t idx = 0; idx < cur_max_open_block_cnt_; ++idx) {
    blockid_t cur_bid = sspace_->active_block_list_[idx];

    // If this slot is empty (i.e., uninitialized or invalid)
    if (cur_bid == block_cnt_) {
      sspace_->active_block_list_[idx] = bid;
      sspace_->active_block_cnt_.fetch_add(1, std::memory_order_relaxed);
      return true;
    }

    Ensure(sspace_->active_block_cnt_.load() <= cur_max_open_block_cnt_);

    // if(FLAGS_use_SSDWA1_pattern){
    //             std::cout<< "add idx: " << idx << " active bid: " << cur_bid
    // << " fill factor: " << GetBlockWriteOffset(cur_bid)
    // << " state: " <<  static_cast<int>(sspace_->block_metadata[cur_bid].state
    // )
    // << " active cnt: " << sspace_->active_block_cnt_.load()   << std::endl;
    // }
  }

  return false; // No suitable slot found
}

bool SpaceManager::BlockIDInActiveBlockList(blockid_t bid) {

  if (!FLAGS_max_open_block_cnt)
    return true;

  for (size_t idx = 0; idx < cur_max_open_block_cnt_; ++idx) {
    blockid_t cur_bid = sspace_->active_block_list_[idx];

    // If this slot is empty (i.e., uninitialized or invalid)
    if (cur_bid == bid) {
      return true;
    }
  }

  return false; // No suitable slot found
}

int SpaceManager::EmptySlotCountInActiveBlockList() {
  if (!FLAGS_max_open_block_cnt)
    return max_open_block_cnt_;
  int cnt = 0;
  for (size_t idx = 0; idx < cur_max_open_block_cnt_; ++idx) {
    blockid_t cur_bid = sspace_->active_block_list_[idx];

    // Check for an uninitialized or invalid slot
    if (cur_bid == block_cnt_) {
      cnt++;
    }
  }
  return cnt; // No empty slot found
}

bool SpaceManager::RemoveBlockIDFromActiveBlockList(blockid_t bid) {
  if (!FLAGS_max_open_block_cnt)
    return true;
  bool success = false;
  u32 full_cnt = 0;
  for (size_t idx = 0; idx < cur_max_open_block_cnt_; ++idx) {
    blockid_t cur_bid = sspace_->active_block_list_[idx];

    if (cur_bid == bid) {
      // bid is inside the active block list

      success = true;
      if (!FLAGS_use_SSDWA1_pattern) {
        sspace_->active_block_list_[idx] = block_cnt_;
        sspace_->active_block_cnt_.fetch_sub(1, std::memory_order_relaxed);
      }
    }

    if (FLAGS_use_SSDWA1_pattern) {
      if (GetBlockWriteOffset(cur_bid) == max_w_ptr_ &&
              (sspace_->block_metadata[cur_bid].state == State::FULL) ||
          cur_bid == block_cnt_) {
        full_cnt++;
      }
    }

    //                   std::cout<< "remove idx: " << idx << " active bid: " <<
    //                   cur_bid
    // << " fill factor: " << GetBlockWriteOffset(cur_bid)
    // << " state: " <<  static_cast<int>(sspace_->block_metadata[cur_bid].state
    // )
    // << " active cnt: " << sspace_->active_block_cnt_.load()  << " full cnt: "
    // << full_cnt << std::endl;
  }
  if (FLAGS_use_SSDWA1_pattern) {
    if (full_cnt == cur_max_open_block_cnt_) {
      AddWriteGroupHistory();
      sspace_->active_block_list_.assign(max_open_block_cnt_, block_cnt_);
      sspace_->active_block_cnt_.store(0, std::memory_order_relaxed);
      cur_max_open_block_cnt_ = max_open_block_cnt_;
    }
  }

  Ensure(sspace_->active_block_cnt_.load() <= cur_max_open_block_cnt_);

  return success;
}

void SpaceManager::AddWriteGroupHistory() {
  // u32 ssdop = std::min( (u32)(max_open_block_cnt_ * 3) ,
  // (u32)(block_cnt_/2)); if(sspace_->GetBlockListSize(State::EMPTY) < ssdop )
  // return;
  if (sspace_->compensation_write.load()) {
    sspace_->not_compensation_write.store(0, std::memory_order_relaxed);
    return;
  } else {
    u32 notfirst = 0;
    for (u32 i = 0; i < max_open_block_cnt_; ++i) {
      blockid_t bid = sspace_->active_block_list_[i];
      if (bid != block_cnt_) {
        if (sspace_->block_metadata[bid].block_invalidation_cnt > 0)
          notfirst++;
      }
    }
    if (notfirst > 0) {
      sspace_->not_compensation_write.fetch_add(1, std::memory_order_relaxed);
    }
  }

  u32 cnt = sspace_->active_block_cnt_.load(std::memory_order_relaxed);
  if (cnt < max_open_block_cnt_)
    return; // nothing to record

  std::vector<blockid_t> group;
  group.reserve(cnt);
  u32 first = 0;

  for (u32 i = 0; i < cnt; ++i) {
    blockid_t bid = sspace_->active_block_list_[i];
    if (bid != block_cnt_)
      group.emplace_back(bid); // skip sentinel slots
    if (sspace_->block_metadata[bid].block_invalidation_cnt == 0)
      first++;
  }

  // if(first == cnt) return;

  std::sort(group.begin(), group.end()); // <= only once per group
  sspace_->write_group_.emplace_back(std::move(group));
}

blockid_t SpaceManager::AdjustEDTRange(logid_t global_min_edt,
                                       logid_t global_max_edt, logid_t avg_edt,
                                       blockid_t bid) {
  if (!FLAGS_use_edt)
    return block_cnt_;

  if (bid == block_cnt_) {
    return block_cnt_;
  }

  std::vector<std::pair<logid_t, logid_t>> edt_ranges;

  // Collect EDT ranges from all *other* active blocks
  for (u32 i = 0; i < cur_max_open_block_cnt_; ++i) {
    blockid_t other_bid = sspace_->active_block_list_[i];
    if (other_bid == block_cnt_)
      continue;

    const auto &other_meta = sspace_->block_metadata[other_bid];
    if ((other_meta.state != State::OPEN &&
         other_meta.state != State::IN_USE) ||
        other_meta.cur_size == sspace_->max_w_ptr_) {
      continue;
    }

    edt_ranges.emplace_back(other_meta.block_min_edt, other_meta.block_max_edt);
  }

  // Sort by start
  std::sort(edt_ranges.begin(), edt_ranges.end());

  logid_t cursor = global_min_edt;
  blockid_t last_extended_bid = block_cnt_;
  bool extended_any = false;

  for (const auto &[start, end] : edt_ranges) {
    if (cursor < start) {
      // Found a gap
      logid_t gap_start = cursor;
      logid_t gap_end = start;

      // Find a block to extend (closest adjacent)
      blockid_t extend_bid = block_cnt_;
      bool extend_upper = false;

      for (u32 i = 0; i < cur_max_open_block_cnt_; ++i) {
        blockid_t other_bid = sspace_->active_block_list_[i];
        if (other_bid == block_cnt_)
          continue;

        auto &other_meta = sspace_->block_metadata[other_bid];
        if ((other_meta.state != State::OPEN &&
             other_meta.state != State::IN_USE) ||
            other_meta.cur_size == sspace_->max_w_ptr_) {
          continue;
        }

        if (other_meta.block_min_edt == std::numeric_limits<logid_t>::max() &&
            other_meta.block_max_edt == 0) {
          continue;
        }

        if (other_meta.block_max_edt == gap_start) {
          extend_bid = other_bid;
          extend_upper = true;
          break;
        }

        if (other_meta.block_min_edt == gap_end) {
          extend_bid = other_bid;
          extend_upper = false;
          break;
        }
      }

      if (extend_bid != block_cnt_) {
        auto &meta = sspace_->block_metadata[extend_bid];
        if (extend_upper) {
          // std::cout << "[AdjustEDTRange] Extending block " << extend_bid
          //           << " upper bound from " << meta.block_max_edt
          //           << " to " << gap_end << "\n";
          meta.block_max_edt = gap_end;
        } else {
          // std::cout << "[AdjustEDTRange] Extending block " << extend_bid
          //           << " lower bound from " << meta.block_min_edt
          //           << " to " << gap_start << "\n";
          meta.block_min_edt = gap_start;
        }
        last_extended_bid = extend_bid;
        extended_any = true;
      }
    }

    // Advance cursor to the furthest end seen so far
    cursor = std::max(cursor, end);
  }

  // Check tail gap
  if (cursor < global_max_edt) {
    logid_t gap_start = cursor;
    logid_t gap_end = global_max_edt;
    // if (sspace_->gc_triggered.load()) {
    //     std::cout << "[AdjustEDTRange] Missing tail range: [" << gap_start <<
    //     ", " << gap_end << ")\n";
    // }

    // Try to find the block with the highest max_edt to extend
    blockid_t extend_bid = block_cnt_;
    logid_t highest_max = std::numeric_limits<logid_t>::min();
    for (u32 i = 0; i < cur_max_open_block_cnt_; ++i) {
      blockid_t other_bid = sspace_->active_block_list_[i];
      auto &other_meta = sspace_->block_metadata[other_bid];
      if (other_meta.block_max_edt > highest_max) {
        highest_max = other_meta.block_max_edt;
        extend_bid = other_bid;
      }
    }

    if (extend_bid != block_cnt_) {
      auto &meta = sspace_->block_metadata[extend_bid];
      // if (sspace_->gc_triggered.load()) {
      //     std::cout << "[AdjustEDTRange] Extending block " << extend_bid
      //               << " upper bound from " << meta.block_max_edt
      //               << " to " << gap_end << "\n";
      // }
      meta.block_max_edt = gap_end;
      meta.block_min_edt = std::min(meta.block_min_edt, gap_start);
      last_extended_bid = extend_bid;
      extended_any = true;
    }
  }

  return extended_any ? last_extended_bid : block_cnt_;
}

bool SpaceManager::ShouldOpenNewBlockID(logid_t global_min_edt,
                                        logid_t global_max_edt, logid_t avg_edt,
                                        u64 write_size) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  if (!FLAGS_max_open_block_cnt)
    return true;

  if (!sspace_->active_block_cnt_.load())
    return true;

  if (sspace_->active_block_cnt_.load(std::memory_order_relaxed) >=
      cur_max_open_block_cnt_) {
    return false;
  } else {
    // Check if avg_edt is already covered by any active block
    if (!FLAGS_use_edt) {
      return true;
    }
    for (u32 i = 0; i < cur_max_open_block_cnt_; ++i) {
      blockid_t bid = sspace_->active_block_list_[i];
      auto &meta = sspace_->block_metadata[bid];

      // If avg_edt falls within the block's EDT range, no need to open a new
      // block
      if (bid == block_cnt_)
        continue;
      if (meta.cur_size >= sspace_->max_w_ptr_)
        continue;
      if (avg_edt >= meta.block_min_edt && avg_edt <= meta.block_max_edt)
        return false;
      // if( meta.block_max_edt - meta.block_min_edt < (global_max_edt -
      // global_min_edt) / FLAGS_write_buffer_partition_cnt ) return false;
    }
    return true;
  }
}

bool SpaceManager::UpdateBlockMetadataAfterReadGC(blockid_t bid) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  Ensure(sspace_->block_metadata[bid].state == State::IN_USE);
  Ensure(sspace_->MoveBlockToList(bid, State::IN_USE, State::GC_READ));
  Ensure(sspace_->block_metadata[bid].state == State::GC_READ);
  Ensure(sspace_->IsBlockInList(bid, State::GC_READ));
  return true;
}

blockid_t SpaceManager::SelectOpenBlockID(u64 max_write_sz, logid_t avg_edt,
                                          logid_t cur_gsn, u32 pid_cnt,
                                          u32 group_idx, logid_t min_edt,
                                          logid_t max_edt) {
try_again:
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  blockid_t bid = block_cnt_;

  // Case 1: No open blocks at all
  if (sspace_->block_lists.at(State::OPEN).empty()) {
    return bid;
  }

  // Case 2: Max open block count not exceeded
  if (!FLAGS_max_open_block_cnt) {
    if (sspace_->block_lists.at(State::OPEN).size() <
        io_handler_cnt_ * FLAGS_write_buffer_partition_cnt) {
      return bid;
    }
  }

  // Case 3: Use EDT-based or random block selection
  if (FLAGS_use_edt) {
    AdjustEDTRange(min_edt, max_edt, avg_edt, bid);
    bid = SelectOpenBlockIDWithClosestEDT(avg_edt, max_write_sz, pid_cnt,
                                          group_idx, min_edt, max_edt);
  } else {
    bid = SelectOpenBlockIDRandom(max_write_sz, avg_edt, pid_cnt);
  }

  // Case 4: Fallback - pick any OPEN block that has space
  if (FLAGS_use_edt) {
    if (sspace_->GetActiveBlockSize() == 1 &&
        sspace_->active_block_cnt_.load() >= cur_max_open_block_cnt_) {
      bid = SelectOpenBlockIDRandom(max_write_sz, avg_edt, pid_cnt);
    }
  }

  // Case 5: Still not found
  if (bid == block_cnt_) {
    return bid;
  }

  // Move selected block to IN_USE
  if (!sspace_->MoveBlockToList(bid, State::OPEN, State::IN_USE)) {
    return block_cnt_;
  }
  return bid;
}

logid_t SpaceManager::GetBlockEDTGap() {
  const logid_t threshold = WRITE_HISTORY_LENGTH;
  std::vector<logid_t> edt_values;

  for (blockid_t bid = 0; bid < block_cnt_; ++bid) {
    const auto &bm = sspace_->block_metadata[bid];

    // Skip empty blocks
    if (bm.state == State::EMPTY) {
      continue;
    }

    logid_t edt = bm.edt;
    if (edt > threshold) {
      edt_values.push_back(edt);
    }
  }

  // Case 1: All EDTs are 0 or <= threshold
  if (edt_values.empty()) {
    // Check if *all* edt values are zero
    bool all_zero = true;
    for (blockid_t bid = 0; bid < block_cnt_; ++bid) {
      const auto &bm = sspace_->block_metadata[bid];
      if (bm.state == State::EMPTY)
        continue;
      if (bm.edt != 0) {
        all_zero = false;
        break;
      }
    }

    if (all_zero) {
      return 0;
    }
  }

  // Case 2: Compute average gap among EDTs > threshold
  std::sort(edt_values.begin(), edt_values.end());
  logid_t total_gap = 0;
  for (size_t i = 1; i < edt_values.size(); ++i) {
    total_gap += edt_values[i] - edt_values[i - 1];
  }

  return logid_t(total_gap / (edt_values.size() - 1));
}

blockid_t SpaceManager::SelectOpenBlockIDWithClosestEDT(
    logid_t avg_edt, u64 max_write_sz, u32 pid_cnt, u32 group_idx,
    logid_t min_edt, logid_t max_edt) {
  blockid_t closest_bid = block_cnt_;
  logid_t closest_gap = std::numeric_limits<logid_t>::max();

  std::vector<logid_t> open_block_min_edts;
  blockid_t uninitialized_edt_bid = block_cnt_;

  blockid_t best_fit_bid = block_cnt_;
  logid_t best_fit_max_edt = std::numeric_limits<logid_t>::max();

  bool all_blocks_maxedt_le_avg = true;
  bool all_blocks_minedt_ge_avg = true;

  // Scan OPEN blocks
  for (u32 idx = 0; idx < sspace_->GetBlockListSize(State::OPEN); ++idx) {
    blockid_t bid = sspace_->block_lists[State::OPEN][idx];
    auto &bm = sspace_->block_metadata[bid];
    if (bm.state != State::OPEN)
      continue;

    if (bm.cur_size + max_write_sz > max_w_ptr_) {
      Ensure(sspace_->MoveBlockToList(bid, State::OPEN, State::FULL_PAD));
      IssuePaddingWritesToFullBlock(bid);
      continue;
    }

    if (bm.edt == std::numeric_limits<logid_t>::max()) {
      uninitialized_edt_bid = bid;
      continue;
    }

    open_block_min_edts.push_back(bm.block_min_edt);

    // Track min gap
    logid_t gap = (bm.edt > avg_edt) ? bm.edt - avg_edt : avg_edt - bm.edt;
    if (gap < closest_gap) {
      closest_gap = gap;
      closest_bid = bid;
    }

    // Track condition checks
    if (bm.block_max_edt > avg_edt) {
      all_blocks_maxedt_le_avg = false;
    }
    if (bm.block_min_edt < avg_edt) {
      all_blocks_minedt_ge_avg = false;
    }

    if (bm.block_max_edt < avg_edt && bm.block_max_edt < best_fit_max_edt) {
      best_fit_max_edt = bm.block_max_edt;
      best_fit_bid = bid;
    }
  }

  // Scan IN_USE blocks for comparisons
  for (u32 idx = 0; idx < sspace_->GetBlockListSize(State::IN_USE); ++idx) {
    blockid_t bid = sspace_->block_lists[State::IN_USE][idx];
    auto &bm = sspace_->block_metadata[bid];

    if (bm.state != State::IN_USE || bm.cur_size == max_write_sz ||
        bm.cur_size == 0 || bm.block_max_edt == 0 || bm.edt == 0)
      continue;

    logid_t gap = (bm.edt > avg_edt) ? bm.edt - avg_edt : avg_edt - bm.edt;
    if (gap < closest_gap) {
      return block_cnt_; // In-use block is closer; skip OPENs
    }

    if (bm.block_max_edt > avg_edt) {
      all_blocks_maxedt_le_avg = false;
    }
    if (bm.block_min_edt < avg_edt) {
      all_blocks_minedt_ge_avg = false;
    }
  }

  // Prefer uninitialized EDT block if avg_edt ≤ all known block_min_edts

  if (closest_bid != block_cnt_)
    return closest_bid;

  if (all_blocks_maxedt_le_avg && best_fit_bid != block_cnt_) {
    return best_fit_bid;
  }
  return block_cnt_;
}

blockid_t SpaceManager::SelectOpenBlockIDRandom(u64 max_write_sz,
                                                logid_t avg_edt, u32 pid_cnt) {
  auto &open_list = sspace_->block_lists[State::OPEN];
  if (open_list.empty()) {
    return block_cnt_; // No open blocks
  }

  std::vector<blockid_t> candidates;
  for (blockid_t bid : open_list) {
    if (bid == block_cnt_)
      continue;

    auto &bm = sspace_->block_metadata[bid];
    if (bm.state != State::OPEN)
      continue;

    // Skip blocks that cannot fit the max_write_sz
    if (bm.cur_size + max_write_sz > max_w_ptr_) {
      Ensure(sspace_->MoveBlockToList(bid, State::OPEN, State::FULL_PAD));
      IssuePaddingWritesToFullBlock(bid);
      continue;
    }

    candidates.push_back(bid);
  }

  if (candidates.empty()) {
    return block_cnt_; // No valid open block found
  }

  // Use thread-local RNG for better performance in multithreaded environments
  thread_local std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);

  return candidates[dist(rng)];
}

// blockid_t SpaceManager::SelectOpenBlockIDRandom(u64 max_write_sz,
//                                                 logid_t avg_edt, u32 pid_cnt)
//                                                 {
//   blockid_t selected_bid = block_cnt_; // Invalid block ID
//   u64 min_fill = std::numeric_limits<u64>::max();
//   // select the most unfilled

//   auto &open_list = sspace_->block_lists[State::OPEN];
//   if (open_list.empty()) {
//     return block_cnt_;
//   }

//   for (blockid_t bid : open_list) {
//     if (bid == block_cnt_) continue;

//     auto &bm = sspace_->block_metadata[bid];
//     if (bm.state != State::OPEN) continue;

//     // Skip blocks that cannot fit the max_write_sz
//     if (bm.cur_size + max_write_sz > max_w_ptr_) {
//       Ensure(sspace_->MoveBlockToList(bid, State::OPEN, State::FULL_PAD));
//       IssuePaddingWritesToFullBlock(bid);
//       continue;
//     }

//     if (bm.cur_size < min_fill) {
//       min_fill = bm.cur_size;
//       selected_bid = bid;
//     }
//   }
//   return selected_bid;
// }

// blockid_t SpaceManager::SelectOpenBlockIDRandom(u64 max_write_sz,
//                                                 logid_t avg_edt, u32 pid_cnt)
//                                                 {
//   blockid_t best_bid = block_cnt_;
//   u64 min_cur_size = std::numeric_limits<u64>::max();
//   auto &open_list = sspace_->block_lists[State::OPEN];

//   for (blockid_t bid : open_list) {
//     auto &meta = sspace_->block_metadata[bid];

//     if (meta.state != State::OPEN)
//       continue;

//     u64 new_size = meta.cur_size + max_write_sz;

//     if (new_size > max_w_ptr_) {
//       // Mark the block as full and move to FULL_PAD state
//       Ensure(sspace_->MoveBlockToList(bid, State::OPEN, State::FULL_PAD));
//       IssuePaddingWritesToFullBlock(bid);
//       continue;
//     }

//     // Track the block with minimum cur_size that has enough space
//     if (meta.cur_size < min_cur_size) {
//       best_bid = bid;
//       min_cur_size = meta.cur_size;
//     }
//   }

//   return best_bid;
// }

blockid_t SpaceManager::SelectEmptyBlockID(u64 max_write_sz, logid_t avg_edt,
                                           logid_t cur_gsn, u32 pid_cnt,
                                           logid_t global_min_edt,
                                           logid_t global_max_edt) {
  blockid_t bid = block_cnt_;
  auto &empty_blocks = sspace_->block_lists[State::EMPTY];
  if (empty_blocks.empty()) {
    return bid;
  }

  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);

  if (FLAGS_use_edt) {
    for (size_t idx = 0; idx < cur_max_open_block_cnt_; ++idx) {
      blockid_t cur_bid = sspace_->active_block_list_[idx];
      if (cur_bid == block_cnt_)
        continue;
      const auto &meta = sspace_->block_metadata[cur_bid];
      if (meta.block_min_edt <= avg_edt && meta.block_max_edt > avg_edt &&
          meta.cur_size + max_write_sz <= sspace_->max_w_ptr_) {
        return bid;
      }
    }
  }

  // Select block based on history or randomly
  if (FLAGS_invalidation_history_based_empty_block_selection) {
    bid = SelectEmptyBlockWithMinInvalidationCnt();
  } else {
    bid = SelectEmptyBlockRandom();
  }

  if (bid >= block_cnt_) {
    return bid; // No valid block, exit early
  }

  if (!AddBlockIDToActiveBlockList(bid)) {
    return block_cnt_;
  } else {
    Ensure(sspace_->MoveBlockToList(bid, State::EMPTY, State::IN_USE));
    // ResetBlockSpace(bid);
  }

  // AdjustEDTRange(global_min_edt, global_max_edt, avg_edt, bid);

  Ensure(bid < block_cnt_);
  return bid;
}

blockid_t SpaceManager::SelectEmptyBlockWithMinInvalidationCnt() {
  const auto &empty_list = sspace_->block_lists[State::EMPTY];

  blockid_t min_bid = empty_list[0];
  u64 min_block_invalidation_cnt =
      sspace_->block_metadata[min_bid].block_invalidation_cnt;
  if (min_block_invalidation_cnt == 0) {
    return min_bid;
  }

  // Find block with the minimum invalidation count
  for (u32 idx = 1; idx < empty_list.size(); ++idx) {
    blockid_t current_bid = empty_list[idx];
    u64 block_invalidation_cnt =
        sspace_->block_metadata[current_bid].block_invalidation_cnt;
    if (block_invalidation_cnt == 0) {
      return current_bid;
    }

    if (block_invalidation_cnt < min_block_invalidation_cnt) {
      min_bid = current_bid;
      min_block_invalidation_cnt = block_invalidation_cnt;
    }
  }
  return min_bid;
}

blockid_t SpaceManager::SelectEmptyBlockRandom() {
  const auto &empty_list = sspace_->block_lists[State::EMPTY];
  if (empty_list.empty()) {
    return block_cnt_;
  }

  return empty_list[0];

  // if (FLAGS_use_edt) {
  //   return empty_list[0];
  // }

  // std::uniform_int_distribution<u32> dist(0, empty_list.size() - 1);
  // return empty_list[dist(rng_)];
}

blockid_t SpaceManager::SelectFullBlockID(u64 max_write_sz, logid_t avg_edt,
                                          logid_t cur_gsn, u32 pid_cnt) {
  blockid_t bid = block_cnt_;
  const auto &full_list = sspace_->block_lists[State::FULL];

  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);

  for (u32 idx = 0; idx < full_list.size(); ++idx) {
    if (sspace_->block_metadata[bid].cur_size + max_write_sz <= max_w_ptr_) {

      if (!sspace_->MoveBlockToList(bid, State::FULL, State::IN_USE)) {
        continue;
      }
    }
  }

  return bid;
}

void SpaceManager::CheckOpenBlockForPadding(u64 write_sz) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);

  if (!sspace_->GetBlockListSize(State::OPEN)) {
    return;
  } else {
    auto &open_list = sspace_->block_lists[State::OPEN];
    for (u32 idx = 0; idx < open_list.size(); idx++) {
      blockid_t bid = open_list[idx];
      if (sspace_->block_metadata[bid].state != State::OPEN) {
        continue;
      }
      if (sspace_->block_metadata[bid].cur_size + write_sz >
          sspace_->max_w_ptr_) {
        if (sspace_->MoveBlockToList(bid, State::OPEN, State::FULL_PAD)) {
          IssuePaddingWritesToFullBlock(bid);
          Ensure(sspace_->block_metadata[bid].state == State::FULL);
        }
      }
    }
  }
}

void SpaceManager::MovePaddedBlocksToFullList() {
  auto &padding_list = sspace_->block_lists[State::FULL_PAD];

  for (u32 idx = 0; idx < padding_list.size(); idx++) {
    blockid_t bid = padding_list[idx];
    Ensure(sspace_->MoveBlockToList(bid, State::FULL_PAD, State::FULL));
    Ensure(sspace_->block_metadata[bid].state == State::FULL);
  }
}

int SpaceManager::GCInProgessBlockCountInActiveList() {
  if (FLAGS_max_open_block_cnt) {
    int cnt = 0;
    // std::cout << "Active Block List Contents (max_open_block_cnt_ = " <<
    // max_open_block_cnt_ << "):\n";
    for (size_t idx = 0; idx < max_open_block_cnt_; ++idx) {
      blockid_t bid = sspace_->active_block_list_[idx];
      if (bid == block_cnt_)
        continue;
      const auto &meta = sspace_->block_metadata[bid];
      if (meta.state == State::IN_USE && meta.cur_size == sspace_->max_w_ptr_) {
        // GC in progress count
        cnt++;
      }
    }
    return cnt;

  } else {
    return 0;
  }
}

u32 SpaceManager::BlockGCReadCnt() {
  u32 cnt = 0;
  auto &inuse_list = sspace_->block_lists[State::IN_USE];
  for (blockid_t bid : inuse_list) {
    if (sspace_->block_metadata[bid].cur_size == max_w_ptr_)
      cnt++;
  }

  return cnt;
}

blockid_t SpaceManager::FindFirstGCInUseBlockInActiveList() {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  blockid_t max_bid = block_cnt_;
  u64 max_invalidated_cnt = 0;
  logid_t edt = 0;

  if (FLAGS_max_open_block_cnt) {
    for (size_t idx = 0; idx < max_open_block_cnt_; ++idx) {
      blockid_t bid = sspace_->active_block_list_[idx];
      if (bid == block_cnt_)
        continue;

      const auto &meta = sspace_->block_metadata[bid];

      // if (meta.state == State::GC_IN_USE && meta.invalidated_pid_cnt >
      // max_invalidated_cnt) {
      //   max_bid = bid;
      //   max_invalidated_cnt = meta.invalidated_pid_cnt;
      // }
      if (FLAGS_use_edt) {
        if (meta.state == State::GC_IN_USE && meta.edt > edt) {
          max_bid = bid;
          edt = meta.edt;
        }
      } else {
        if (meta.state == State::GC_IN_USE &&
            meta.invalidated_pid_cnt > max_invalidated_cnt) {
          max_bid = bid;
          max_invalidated_cnt = meta.invalidated_pid_cnt;
        }
      }
    }
  } else {
    auto &list = sspace_->block_lists[State::GC_IN_USE];
    for (size_t idx = 0; idx < list.size(); ++idx) {
      blockid_t bid = list[idx];
      if (bid == block_cnt_)
        continue;

      const auto &meta = sspace_->block_metadata[bid];

      if (meta.state == State::GC_IN_USE &&
          meta.invalidated_pid_cnt > max_invalidated_cnt) {
        max_bid = bid;
        max_invalidated_cnt = meta.invalidated_pid_cnt;
      }
    }
  }

  auto &list = sspace_->block_lists[State::GC_IN_USE];

  if (max_bid == block_cnt_ && !list.empty()) {
    max_bid = list.front(); // fallback to any GC_IN_USE block
  }

  if (max_bid != block_cnt_) {
    Ensure(sspace_->MoveBlockToList(max_bid, State::GC_IN_USE, State::IN_USE));
    Ensure(sspace_->IsBlockInList(max_bid, State::IN_USE));
    Ensure(!sspace_->IsBlockInList(max_bid, State::GC_IN_USE));
    Ensure(sspace_->block_metadata[max_bid].state == State::IN_USE);
  }
  return max_bid;
}

blockid_t SpaceManager::SelectFullBlockToGCRead() {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  // first check whether there is GC_IN_USE blocks
  // waiting to be GC_READ from the previous selection
  int slotcnt = 1;
  u32 mincnt = 0;

  std::unique_ptr<storage::space::GarbageCollector> gc =
      std::make_unique<storage::space::GarbageCollector>(
          PID2Offset_table_, sspace_, mt_fd_, worker_thread_id);

  if (FLAGS_max_open_block_cnt) {
    // if( sspace_->GetBlockListSize(State::GC_READ)) return block_cnt_;
    if (sspace_->active_block_cnt_.load(std::memory_order_relaxed)) {
      return block_cnt_;
    } else
      slotcnt = EmptySlotCountInActiveBlockList();
    // if(FLAGS_use_SSDWA1_pattern){
    //    if (sspace_->active_block_cnt_.load(std::memory_order_relaxed)){return
    //    block_cnt_;} else slotcnt = EmptySlotCountInActiveBlockList();
    // }else{
    // // first secure a slot in active block list
    //   mincnt = sspace_->page_cnt_per_block_ / ( sspace_->page_cnt_per_block_
    //   - gc->ValidPIDsCntInBlock(gc->GreedyGC(true)))  + 1; slotcnt =
    //   std::min((u32)EmptySlotCountInActiveBlockList(), mincnt) ;

    //   if()
    // }
    // // need to select a new set of blocks to GC

    // if (!slotcnt) {
    //   return block_cnt_;
    // }
  }

  if (!FLAGS_max_open_block_cnt) {
    if (sspace_->GetBlockListSize(State::OPEN) >
        io_handler_cnt_ * FLAGS_write_buffer_partition_cnt) {
      return block_cnt_;
    }
    slotcnt = io_handler_cnt_ * FLAGS_write_buffer_partition_cnt -
              sspace_->GetGCTargetSize();
    if (!slotcnt)
      return block_cnt_;
  }

  std::vector<blockid_t> selected_bids =
      gc->SelectVictimBlockToGCRead(slotcnt, lock);
  if (selected_bids.empty())
    return block_cnt_;

  if (FLAGS_use_SSDWA1_pattern) {
    cur_max_open_block_cnt_ = selected_bids.size();
    sspace_->max_open_block_ = cur_max_open_block_cnt_;
  }

  for (auto bid : selected_bids) {
    if (bid < block_cnt_) {
      Ensure(sspace_->block_metadata[bid].state == State::GC_IN_USE);
      Ensure(AddBlockIDToActiveBlockList(bid));
    }
  }
  return block_cnt_;
}

void SpaceManager::CloseAllOpenBlocks() {
  for (size_t idx = 0; idx < max_open_block_cnt_; ++idx) {
    blockid_t bid = sspace_->active_block_list_[idx];

    if (bid == block_cnt_)
      continue; // Skip invalid entries

    if (sspace_->block_metadata[bid].state != State::OPEN)
      continue;

    if (sspace_->block_metadata[bid].cur_size > 0) {
      if (sspace_->MoveBlockToList(bid, State::OPEN, State::FULL_PAD)) {
        IssuePaddingWritesToFullBlock(bid);
        Ensure(sspace_->block_metadata[bid].state == State::FULL);
      }
    }
  }
}

void SpaceManager::CloseOneOpenBlock() {
  auto &open_list = sspace_->block_lists[State::OPEN];

  if (open_list.empty())
    return;

  blockid_t best_bid = block_cnt_; // invalid bid by default
  u64 max_size = 0;
  u32 idx = 0;
  u32 max_idx = 0;

  // Find the OPEN block with the largest cur_size
  for (blockid_t bid : open_list) {

    if (sspace_->block_metadata[bid].state == State::OPEN) {
      u64 cur_size = sspace_->block_metadata[bid].cur_size;
      if (cur_size > max_size) {
        max_size = cur_size;
        best_bid = bid;
        max_idx = idx;
      }
    }
    idx++;
  }
  // Close and pad the selected block
  if (best_bid != block_cnt_ &&
      sspace_->MoveBlockToList(best_bid, State::OPEN, State::FULL_PAD)) {
    IssuePaddingWritesToFullBlock(best_bid);
    Ensure(sspace_->block_metadata[best_bid].state == State::FULL);
  }
  sspace_->active_block_list_[max_idx] = block_cnt_;
  sspace_->active_block_cnt_.fetch_sub(1, std::memory_order_relaxed);
}

// select victim blocks to write among GC_READ bids
// can be multiple bids, depending on the situation of active bids and GC victim
// selection algorithm
std::vector<blockid_t>
SpaceManager::SelectReadGCedBlocksToGCWrite(u64 write_sz) {
try_again:
  std::vector<blockid_t> selected_bids;
  blockid_t bid = block_cnt_; // default invalid value
                              // Ensure there are GC_READ blocks available
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  // Construct GC engine
  std::unique_ptr<storage::space::GarbageCollector> gc =
      std::make_unique<storage::space::GarbageCollector>(
          PID2Offset_table_, sspace_, mt_fd_, worker_thread_id);

  // Select victim GC blocks
  selected_bids = gc->SelectVictimBlocksToGCWrite(write_sz, lock);

  if (!selected_bids.empty()) {
    // Step 2: Add all selected bids to active block list
    for (blockid_t bid : selected_bids) {
      Ensure(bid < block_cnt_);
      Ensure(BlockIDInActiveBlockList(bid));
      Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
    }
  }

  return selected_bids; // No block selected
}

void SpaceManager::EraseBlock(blockid_t bid, logid_t gsn, bool sync_gc) {
  // Initialize cleaned block info'""
  Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
  TrimBlock(bid);
  for (u32 p = 0; p < page_cnt_per_block_; p++) {
    u16 pcnt = sspace_->blocks[bid].pages[p].cnt;
    for (u16 pidx = 0; pidx < pcnt; pidx++) {
      pageid_t pid = sspace_->blocks[bid].pages[p].pids[pidx];
      u64 page_offset = bid * block_size_ + p * PAGE_SIZE;
      if (page_offset != 0 &&
          PID2Offset_table_[pid].get_offset() == page_offset &&
          !PIDIsDeallocated(pid)) {
        // valid PIDs are not fully read
        // UnreachableCode();
      }
      ResetOffset2PIDs(bid, p, pidx, gsn);
    }
  }

  if (gsn > 0) {
    AddBlockInvalidationHistory(bid);
  }

  ResetBlockMetadata(bid);
}

// debug function
void SpaceManager::ValidPIDsInSecurePlace(blockid_t bid) {
  u32 valid_page_cnt = 0;
  for (u32 p = 0; p < page_cnt_per_block_; p++) {
    u16 pcnt = sspace_->blocks[bid].pages[p].cnt;
    for (u16 pidx = 0; pidx < pcnt; pidx++) {
      pageid_t pid = sspace_->blocks[bid].pages[p].pids[pidx];
      if (pid != 0) {
        Ensure(PIDOffsetIsValid(pid));
      }
    }
  }
}

bool SpaceManager::IssuePaddingWritesToFullBlock(blockid_t bid) {
  // Fast path: already full
  if (sspace_->IsBlockInList(bid, State::FULL)) {
    Ensure(max_w_ptr_ == GetBlockWriteOffset(bid));
    Ensure(RemoveBlockIDFromActiveBlockList(bid));
    return true;
  }

  if (!sspace_->IsBlockInList(bid, State::FULL_PAD)) {
    return false;
  }

  // Transition FULL_PAD → IN_USE before writing
  Ensure(sspace_->MoveBlockToList(bid, State::FULL_PAD, State::IN_USE));
  Ensure(sspace_->block_metadata[bid].state == State::IN_USE);

  if (max_w_ptr_ > GetBlockWriteOffset(bid)) {
    u64 padding_size = max_w_ptr_ - GetBlockWriteOffset(bid);
    void *buffer = malloc(padding_size);
    memset(buffer, 0, padding_size);

    if (FLAGS_use_SSDWA1_pattern || FLAGS_use_ZNS) {
      auto *iob = new storage::space::backend::IOBackend(
          mt_fd_, this, PID2Offset_table_, sspace_);
      iob->WritePagesOutOfPlace(GetBlockWriteOffset(bid) + GetBlockOffset(bid),
                                padding_size, buffer,
                                true, // sync
                                false // not GC write
      );
      delete iob;
    }
    free(buffer);

    auto &metadata = sspace_->block_metadata[bid];

    for (u32 i = 0; i < padding_size / PAGE_SIZE; i++) {
      u64 updated_offset =
          GetBlockWriteOffset(bid) + GetBlockOffset(bid) + i * PAGE_SIZE;
      UpdateOffset2PIDs(max_mapped_pages_cnt_, bid, updated_offset);
    }

    metadata.cur_size += padding_size;

    Ensure(max_w_ptr_ == GetBlockWriteOffset(bid));
  }

  // Transition IN_USE → FULL after writing
  Ensure(sspace_->MoveBlockToList(bid, State::IN_USE, State::FULL));
  Ensure(sspace_->block_metadata[bid].state == State::FULL);

  Ensure(max_w_ptr_ == GetBlockWriteOffset(bid));
  Ensure(RemoveBlockIDFromActiveBlockList(bid));
  return true;
}

bool SpaceManager::IsUniqueOffset(u64 offset) {
  for (u64 p = 0; p < max_mapped_pages_cnt_; p++) {
    if (PID2Offset_table_[p].get_comp_sz() == 0 ||
        PID2Offset_table_[p].get_comp_sz() == PAGE_SIZE - 1) {
      continue;
    }
    if (PID2Offset_table_[p].get_offset() == offset) {
      std::cout << "pid: " << p << std::endl;
      return false;
    }
  }
  return true;
}

blockid_t SpaceManager::GetMinEDTBlockID() {
  if (sspace_->block_lists.at(State::OPEN).empty()) {
    return block_cnt_;
  }
  blockid_t min_bid = sspace_->block_lists.at(State::OPEN).front();
  logid_t min_edt = sspace_->block_metadata[min_bid].edt;

  for (const auto &bid : sspace_->block_lists.at(State::OPEN)) {
    if (sspace_->block_metadata[bid].edt < min_edt) {
      min_bid = bid;
      min_edt = sspace_->block_metadata[bid].edt;
    }
  }
  return min_bid;
}

bool SpaceManager::BlockEDTIsOutdated(blockid_t bid, logid_t cur_gsn) {
  // Check if the block's EDT is outdated compared to the current GSN
  if (bid >= block_cnt_) {
    return true;
  }
  return sspace_->block_metadata[bid].edt <= cur_gsn;
}

u64 SpaceManager::CountWrittenPIDsInBlock(blockid_t bid) {
  return (sspace_->block_metadata[bid].total_pid_cnt);
}

logid_t SpaceManager::CalcBlockAvgEDT(blockid_t bid, logid_t avg_edt,
                                      u32 pid_cnt) {
  u64 bid_pids_cnt = CountWrittenPIDsInBlock(bid);
  logid_t prev_edt = sspace_->block_metadata[bid].edt;

  if (bid_pids_cnt == 0)
    return avg_edt;

  double updated_edt = (static_cast<double>(prev_edt) * bid_pids_cnt +
                        static_cast<double>(avg_edt) * pid_cnt) /
                       (bid_pids_cnt + pid_cnt);

  return static_cast<logid_t>(std::round(updated_edt));
}

void SpaceManager::UpdateBlockMetadataAfterWrite(blockid_t bid, u64 write_sz,
                                                 u32 pid_cnt, logid_t avg_edt) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  Ensure(sspace_->block_metadata[bid].state == State::IN_USE);
  Ensure(sspace_->block_metadata[bid].cur_size + write_sz <= max_w_ptr_);

  logid_t calc_edt = CalcBlockAvgEDT(bid, avg_edt, pid_cnt);
  auto &metadata = sspace_->block_metadata[bid];
  metadata.cur_size += write_sz;
  metadata.total_pid_cnt += pid_cnt;
  metadata.edt = calc_edt;

  Ensure(sspace_->MoveBlockToList(bid, State::IN_USE, State::OPEN));
  Ensure(sspace_->block_metadata[bid].state == State::OPEN);
}

void SpaceManager::SetMinMaxEDTPerBlock(blockid_t bid, logid_t min_edt,
                                        logid_t max_edt) {
  auto &meta = sspace_->block_metadata[bid];

  if (min_edt < meta.block_min_edt) {
    meta.block_min_edt = min_edt;
  }

  if (max_edt > meta.block_max_edt) {
    meta.block_max_edt = max_edt;
  }
}

void SpaceManager::SetMinMaxEDTToMinimum(blockid_t bid, logid_t prev_min,
                                         logid_t lsn_gap) {
  // Get current flushed log sequence number
  logid_t curts = recovery::LogManager::global_min_gsn_flushed.load();

  logid_t width =
      lsn_gap /
      FLAGS_write_buffer_partition_cnt; // /
                                        // (static_cast<u64>(FLAGS_write_buffer_partition_cnt)
                                        // * (FLAGS_write_buffer_partition_cnt +
                                        // 1) / 2);

  // Set the min_edt of the block to the current flushed timestamp
  sspace_->block_metadata[bid].block_min_edt = std::max(curts, prev_min);

  // Initialize max_edt to the highest possible value
  logid_t min_edt_among_active_blocks = std::numeric_limits<logid_t>::max();

  // Iterate over all active blocks to find the minimum block_min_edt
  for (size_t idx = 0; idx < max_open_block_cnt_; ++idx) {
    blockid_t active_bid = sspace_->active_block_list_[idx];
    if (active_bid == block_cnt_)
      continue;

    if (active_bid == bid)
      continue;

    const auto &meta = sspace_->block_metadata[active_bid];
    if (meta.cur_size != sspace_->max_w_ptr_ &&
        (meta.state == State::OPEN || meta.state == State::IN_USE)) {
      // Use this block's min_edt to find the minimum
      min_edt_among_active_blocks =
          std::min(min_edt_among_active_blocks, meta.block_min_edt);
    }
  }

  if (min_edt_among_active_blocks < curts)
    min_edt_among_active_blocks =
        sspace_->block_metadata[bid].block_min_edt + width;

  // Set the max_edt of the block to the minimum EDT among active blocks
  sspace_->block_metadata[bid].block_max_edt =
      std::min(min_edt_among_active_blocks, prev_min);
  sspace_->block_metadata[bid].edt =
      (sspace_->block_metadata[bid].block_max_edt +
       sspace_->block_metadata[bid].block_min_edt) /
      2;
}

void SpaceManager::UpdateBlockMetadataAfterGCWrite(blockid_t bid, u64 write_sz,
                                                   u32 pid_cnt,
                                                   logid_t avg_edt) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  // Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
  Ensure(sspace_->block_metadata[bid].cur_size + write_sz <= max_w_ptr_);
  auto &metadata = sspace_->block_metadata[bid];
  logid_t edt = CalcBlockAvgEDT(bid, avg_edt, pid_cnt);

  metadata.edt = edt;
  metadata.cur_size += write_sz;
  metadata.total_pid_cnt += pid_cnt;

  if (sspace_->block_metadata[bid].state != State::GC_WRITE) {
    Ensure(sspace_->MoveBlockToList(bid, State::IN_USE, State::OPEN));
    Ensure(sspace_->block_metadata[bid].state == State::OPEN);
  }
}

void SpaceManager::UpdateBlockMetadataAfterAsyncGCWrite(blockid_t bid,
                                                        u64 write_sz,
                                                        u32 pid_cnt) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
  Ensure(sspace_->block_metadata[bid].cur_size + write_sz <= max_w_ptr_);

  auto &metadata = sspace_->block_metadata[bid];
  metadata.cur_size += write_sz;
  metadata.total_pid_cnt += pid_cnt;
}

void SpaceManager::UpdateBlockMetadataAfterSyncGCWrite(blockid_t bid,
                                                       u64 write_sz,
                                                       u32 pid_cnt) {

  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
  Ensure(sspace_->block_metadata[bid].cur_size + write_sz <=
         sspace_->max_w_ptr_);

  auto &metadata = sspace_->block_metadata[bid];
  metadata.cur_size += write_sz;
  metadata.total_pid_cnt += pid_cnt;
}

void SpaceManager::MoveGCedBlockToOpenList(blockid_t bid, logid_t prev_min_edt,
                                           logid_t lsn_gap,
                                           logid_t global_min_edt,
                                           logid_t global_max_edt) {
  std::unique_lock<std::shared_mutex> lock(sspace_->blk_state_mutex_);
  if (FLAGS_use_edt) {
    if (sspace_->block_metadata[bid].cur_size == 0) {
      SetMinMaxEDTToMinimum(bid, prev_min_edt, lsn_gap);
    } else {
      AdjustEDTRange(global_min_edt, global_max_edt,
                     sspace_->block_metadata[bid].edt, bid);
    }
  }

  //  std::cout<< " active bid: " <<  bid
  //   << " fill factor: " << GetBlockWriteOffset(bid)
  //   << " state: " <<
  //   static_cast<int>(sspace_->block_metadata[ bid].state )
  //   << " active cnt: " << sspace_->active_block_cnt_.load()
  //   << "block edt: " << sspace_->block_metadata[ bid].edt
  //   << " block min: " << sspace_->block_metadata[ bid].block_min_edt
  //   << " block max: " << sspace_->block_metadata[ bid].block_max_edt <<
  //   std::endl;
  for (size_t idx = 0; idx < cur_max_open_block_cnt_; ++idx) {
    blockid_t cur_bid = sspace_->active_block_list_[idx];
    if (cur_bid == block_cnt_)
      continue;

    // If this slot is empty (i.e., uninitialized or invalid)
    // std::cout<< "openblock idx: " << idx << " active bid: " <<
    // sspace_->active_block_list_[idx]
    //     << " fill factor: " << GetBlockWriteOffset(
    //     sspace_->active_block_list_[idx])
    //     << " state: " <<
    //     static_cast<int>(sspace_->block_metadata[
    //     sspace_->active_block_list_[idx]].state )
    //     << " active cnt: " << sspace_->active_block_cnt_.load()
    //     << "block edt: " << sspace_->block_metadata[
    //     sspace_->active_block_list_[idx]].edt
    //     << " block min: " << sspace_->block_metadata[
    //     sspace_->active_block_list_[idx]].block_min_edt
    //     << " block max: " << sspace_->block_metadata[
    //     sspace_->active_block_list_[idx]].block_max_edt
    //     << std::endl;
  }

  Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
  Ensure(sspace_->block_metadata[bid].cur_size <= sspace_->max_w_ptr_);
  Ensure(sspace_->MoveBlockToList(bid, State::GC_WRITE, State::OPEN));
  Ensure(sspace_->block_metadata[bid].state == State::OPEN);
  Ensure(!sspace_->IsBlockInList(bid, State::GC_WRITE));
}

logid_t SpaceManager::GetHighestEDT() {
  // Initialize to max to find the minimum among all valid EDTs.
  logid_t lowest_edt = std::numeric_limits<logid_t>::max();

  // Lambda to process a list of blocks by state
  auto process_block_list = [&](State state) {
    auto &block_list = sspace_->block_lists[state];
    for (u32 idx = 0; idx < block_list.size(); ++idx) {
      blockid_t bid = block_list[idx];
      auto &bm = sspace_->block_metadata[bid];

      // Confirm the block is still in the expected state
      if (bm.state != state)
        continue;

      // Track the minimum EDT seen
      if (bm.edt > lowest_edt) {
        lowest_edt = bm.edt;
      }
    }
  };

  // Process both OPEN and FULL block lists
  process_block_list(State::OPEN);
  // process_block_list(State::FULL);

  // Fallback: if no valid blocks were found
  if (lowest_edt == std::numeric_limits<logid_t>::max()) {
    lowest_edt = recovery::LogManager::global_min_gsn_flushed.load();
  }

  return lowest_edt;
}

u64 SpaceManager::GetBlockOffset(blockid_t bid) {
  return static_cast<u64>(bid) * static_cast<u64>(block_size_);
}

u64 SpaceManager::GetBlockWriteOffset(blockid_t bid) {
  // Access cur_size from block_metadata structure
  return sspace_->block_metadata[bid].cur_size;
}

bool SpaceManager::BlockNeedsGC(blockid_t bid) {
  // Access cur_size from block_metadata structure
  return (sspace_->block_metadata[bid].state == State::IN_USE &&
          sspace_->block_metadata[bid].cur_size == max_w_ptr_);
}

void SpaceManager::ResetBlockMetadata(blockid_t bid) {
  sspace_->block_metadata[bid].cur_size = 0;
  sspace_->block_metadata[bid].edt = 0;
  sspace_->block_metadata[bid].block_min_edt =
      std::numeric_limits<logid_t>::max();
  sspace_->block_metadata[bid].block_max_edt = 0;
  sspace_->block_metadata[bid].total_pid_cnt = 0;
  sspace_->block_metadata[bid].invalidated_pid_cnt = 0;

  if (!sspace_->block_metadata[bid].block_invalidation_cnt) {
    sspace_->block_metadata[bid].state = State::EMPTY;
  }
}

void SpaceManager::ResetBlockSpace(blockid_t bid) {
  // sspace_->block_metadata[bid].invalidation_history_gsn.resize(
  //     WRITE_HISTORY_LENGTH, 0);

  // Initialize each page within the block
  for (u32 pn = 0; pn < page_cnt_per_block_; pn++) {
    sspace_->blocks[bid].pages[pn].cnt = 0;
    size_t new_size = FLAGS_use_compression ? PAGE_SIZE / min_comp_size_ : 1;
    sspace_->blocks[bid].pages[pn].pids.resize(new_size,
                                               0); // Resize pids vector
  }
}

void SpaceManager::AddBlockInvalidationHistory(blockid_t bid) {
  u32 min_hist_idx = 0;
  // const auto &history =
  // sspace_->block_metadata[bid].invalidation_history_gsn;

  // for (u32 idx = 1; idx < history.size(); ++idx) {
  //   if (history[idx] < history[min_hist_idx]) {
  //     min_hist_idx = idx;
  //   }
  // }
  // sspace_->block_metadata[bid].invalidation_history_gsn[min_hist_idx] =
  //     recovery::LogManager::global_min_gsn_flushed.load();
  sspace_->block_metadata[bid].block_invalidation_cnt++;
}

logid_t SpaceManager::GetAvgWriteGap(blockid_t bid) {
  logid_t avg_wgap = 0;
  u32 valid_count = 0;

  // const auto &write_history =
  //     sspace_->block_metadata[bid].invalidation_history_gsn;
  // for (u32 idx = 0; idx < write_history.size(); ++idx) {
  //   logid_t gsn = write_history[idx];
  //   if (gsn == 0) {
  //     break;
  //   }
  //   avg_wgap += gsn;
  //   ++valid_count;
  // }

  if (valid_count == 0) {
    return 0; // No valid history, return 0 to indicate no gap.
  }

  return avg_wgap / valid_count;
}

u64 SpaceManager::ValidPIDsCntInBlock(blockid_t bid) {
  u64 valid_page_cnt = 0;
  u64 iterate_valid_cnt = 0;
  Ensure(sspace_->block_metadata[bid].total_pid_cnt >=
         sspace_->block_metadata[bid].invalidated_pid_cnt);
  valid_page_cnt = sspace_->block_metadata[bid].total_pid_cnt -
                   sspace_->block_metadata[bid].invalidated_pid_cnt;

  // for (u32 p = 0; p < page_cnt_per_block_; p++) {
  //   u16 pcnt = sspace_->blocks[bid].pages[p].cnt;
  //   if (pcnt == 0)
  //     continue;
  //   for (u16 pidx = 0; pidx < pcnt; pidx++) {
  //     if (PageIsValid(bid, p, pidx)) {
  //       valid_page_cnt++;
  //     }
  //   }
  // }

  return valid_page_cnt;
}

u64 SpaceManager::TotalPageCntInBlock(blockid_t bid) {
  return sspace_->block_metadata[bid].total_pid_cnt;
}

bool SpaceManager::PIDInGCBlock(pageid_t pid) {
  if (!FLAGS_use_out_of_place_write) {
    return false;
  }

  u64 offset = PID2Offset_table_[pid].get_offset();
  blockid_t bid = offset / block_size_;

  if (sspace_->block_metadata[bid].state == State::GC_IN_USE &&
      sspace_->block_metadata[bid].cur_size == max_w_ptr_) {
    return true;
  }
  return false;
}

bool SpaceManager::PIDInGCWBlock(pageid_t pid) {
  if (!FLAGS_use_out_of_place_write) {
    return false;
  }

  u64 offset = PID2Offset_table_[pid].get_offset();
  blockid_t bid = offset / block_size_;

  if (sspace_->block_metadata[bid].state == State::GC_WRITE) {
    return true;
  }
  return false;
}

bool SpaceManager::PIDInGCRBlock(pageid_t pid) {
  if (!FLAGS_use_out_of_place_write) {
    return false;
  }

  u64 offset = PID2Offset_table_[pid].get_offset();
  blockid_t bid = offset / block_size_;

  if (sspace_->block_metadata[bid].state == State::GC_READ) {
    return true;
  }
  return false;
}

blockid_t SpaceManager::PIDBlock(pageid_t pid) {
  if (!FLAGS_use_out_of_place_write) {
    return 0;
  }

  u64 offset = PID2Offset_table_[pid].get_offset();
  return offset / block_size_;
}

bool SpaceManager::PIDIsDeallocated(pageid_t pid) {
  u64 offset = PID2Offset_table_[pid].get_offset();
  if (PID2Offset_table_[pid].get_comp_sz() == PAGE_SIZE - 1) {
    return true;
  }

  return false;
}

bool SpaceManager::PIDOffsetIsValid(pageid_t pid) {
  u64 offset = PID2Offset_table_[pid].get_offset();

  if (offset == 0 && PID2Offset_table_[pid].get_comp_sz() == PAGE_SIZE - 1) {
    // means it has been deallocated
    return true;
  }

  if (!FLAGS_use_out_of_place_write) {
    return true;
  }

  if (offset == 0 && PID2Offset_table_[pid].get_comp_sz() == 0) {
    // has never been written before
    return false;
  }

  blockid_t bid = PID2Offset_table_[pid].get_offset() / block_size_;

  u32 p = (PID2Offset_table_[pid].get_offset() % block_size_) / PAGE_SIZE;
  u16 pcnt = sspace_->blocks[bid].pages[p].cnt;
  u16 pidx = 0;

  for (pidx = 0; pidx < pcnt; pidx++) {
    if (sspace_->blocks[bid].pages[p].pids[pidx] == pid) {
      return true;
    }
  }

  return false;
}

bool SpaceManager::PIDOffsetIsInBlock(pageid_t pid, blockid_t bid) {
  // Ensure(PIDOffsetIsValid(pid));
  if (PID2Offset_table_[pid].get_offset() / block_size_ == bid) {
    return true;
  }
  return false;
}

blockid_t SpaceManager::PIDOffsetBlock(pageid_t pid) {
  return PID2Offset_table_[pid].get_offset() / block_size_;
}

void SpaceManager::UpdatePID2Offset(pageid_t pid, u64 offset, u16 comp_sz,
                                    blockid_t bid, logid_t gsn, bool sync_gc) {
  if (FLAGS_wal_enable) {
    LogPID2Offset(pid);
  }

  u64 prev_offset = PID2Offset_table_[pid].get_offset();
  u16 prev_sz = PID2Offset_table_[pid].get_comp_sz();

  PID2Offset_table_[pid].set(offset, comp_sz);
  Ensure(PID2Offset_table_[pid].get_comp_sz() == comp_sz);
  Ensure(PID2Offset_table_[pid].get_offset() == offset);

  if (FLAGS_use_ZNS) {
    Ensure(PID2Offset_table_[pid].get_offset() % block_size_ <= max_w_ptr_);
  }

  if (FLAGS_use_out_of_place_write) {
    if (((prev_sz > 0 && prev_offset > 0) || comp_sz == PAGE_SIZE - 1) &&
        FLAGS_use_out_of_place_write && !sync_gc) {
      UpdateBlockInvalidCount(prev_offset, pid);
    }
  }
}

void SpaceManager::UpdateOffset2PIDs(pageid_t pid, blockid_t bid,
                                     u64 page_offset) {

  u32 p = 0;
  p = (page_offset % block_size_) / PAGE_SIZE;
  Ensure(page_offset / block_size_ == bid);
  Ensure(
      sspace_->blocks[bid].pages[p].pids[sspace_->blocks[bid].pages[p].cnt] !=
      max_mapped_pages_cnt_);
  // Ensure(PID2Offset_table_[pid].get_comp_sz() > 0);
  sspace_->blocks[bid].pages[p].pids[sspace_->blocks[bid].pages[p].cnt] = pid;
  sspace_->blocks[bid].pages[p].cnt++;

  Ensure(sspace_->blocks[bid].pages[p].cnt <= PAGE_SIZE / MIN_COMP_SIZE);
}

void SpaceManager::ConvertOffset2PIDs(u64 page_offset) {
  u32 p = 0;
  u16 pcnt = 0;

  p = (page_offset % block_size_) / PAGE_SIZE;
  pcnt = sspace_->blocks[page_offset / block_size_].pages[p].cnt;
  for (u16 pidx = 0; pidx < pcnt; pidx++) {
    fprintf(stderr, "bid: %lu pid: %lu\t", page_offset / block_size_,
            sspace_->blocks[page_offset / block_size_].pages[p].pids[pidx]);
  }
  fprintf(stderr, "\n");
}

void SpaceManager::UpdateBlockInvalidCount(u64 page_offset, pageid_t pid) {
  blockid_t bid = (page_offset / block_size_);
  u32 p = (page_offset % block_size_) / PAGE_SIZE;
  u16 pcnt = sspace_->blocks[page_offset / block_size_].pages[p].cnt;
  for (u16 pidx = 0; pidx < pcnt; pidx++) {
    if (pid == sspace_->blocks[bid].pages[p].pids[pidx]) {
      sspace_->block_metadata[bid].invalidated_pid_cnt++;
      return;
    }
  }
}

void SpaceManager::Destroy() {
  // Cleanup resources if needed.

  if (FLAGS_use_out_of_place_write) {
    delete[] sspace_;
  }

  delete[] PID2Offset_table_;

  for (size_t t = 0; t < io_handler_cnt_; ++t) {
    delete reusable_pids_heads[t];
    delete reusable_pids_tails[t];
  }

  if (FLAGS_use_trim && !FLAGS_use_out_of_place_write) {
    StopTrimThread();
  }
}

void SpaceManager::HotUringSubmit() {
  if (submitted_io_cnt_) {
    auto ret = io_uring_submit_and_wait(&ring_, submitted_io_cnt_);
    if (ret < 0) {
      // Log the error or handle it accordingly
      std::cerr << "io_uring_submit_and_wait error: " << strerror(-ret)
                << std::endl;
    }

    // Handle cases where fewer than expected entries were submitted
    Ensure(ret == static_cast<int>(submitted_io_cnt_));
    io_uring_cq_advance(&ring_, submitted_io_cnt_);
  }
}

void SpaceManager::ResetOffset2PIDs(blockid_t bid, u32 pn, u16 pcnt,
                                    logid_t gsn) {

  // pageid_t pid = sspace_->blocks[bid].pages[pn].pids[pcnt];
  sspace_->blocks[bid].pages[pn].pids[pcnt] = 0;
  sspace_->blocks[bid].pages[pn].cnt--;
}

// check whether page is still valid upon GC
bool SpaceManager::PageIsValid(blockid_t bid, u32 p, u16 pidx) {
  // Compute the page offset based on the block and page information

  pageid_t cur_pid = sspace_->blocks[bid].pages[p].pids[pidx];

  if (cur_pid == max_mapped_pages_cnt_) {
    return false;
  }

  if (PIDIsDeallocated(cur_pid)) {
    return false;
  }

  if (PID2Offset_table_[cur_pid].get_comp_sz() == PAGE_SIZE - 1 ||
      PID2Offset_table_[cur_pid].get_comp_sz() == 0) {
    return false;
  }

  u64 page_offset = bid * block_size_ + p * PAGE_SIZE;

  u64 page_start_offset = PID2Offset_table_[cur_pid].get_offset() -
                          (PID2Offset_table_[cur_pid].get_offset() % PAGE_SIZE);

  // u64 cur_pid_start_ptr = (PID2Offset_table_[cur_pid].get_offset() %
  // PAGE_SIZE);

  if (page_start_offset != page_offset) {
    return false;
  }

  return (page_start_offset == page_offset);
}

pageid_t SpaceManager::GetPIDFromOffset(blockid_t bid, u32 p, u16 pidx) {
  return (sspace_->blocks[bid].pages[p].pids[pidx]);
}

// log PID2Offset mapping info to WAL
void SpaceManager::LogPID2Offset(pageid_t pid) {
  auto &txn = transaction::TransactionManager::active_txn;
  // Check if there is an active transaction
  if (!txn.IsRunning()) {
    return;
  }
  // Ensure PID is within the valid range (if applicable)
  if (pid >= max_mapped_pages_cnt_) {
    throw std::out_of_range("Page ID out of range");
  }
  // Proceed only if WAL logging is enabled
  if (FLAGS_wal_enable) {
    auto &logger = txn.LogWorker();
    // Retrieve the offset for the given PID
    u64 offset = PID2Offset_table_[pid].get_offset();
    // Reserve WAL space for the PID2Offset log entry
    auto &entry = logger.ReservePID2OffsetLog(pid, offset);
    entry.type = LogEntry::Type::PID2OFFSET;
    // Submit the log entry to the WAL
    logger.SubmitActiveLogEntry();
  }
}

// log entire page image to WAL
void SpaceManager::LogValidPage(pageid_t pid, storage::Page *page) {
  auto &txn = transaction::TransactionManager::active_txn;
  Ensure(txn.IsRunning());

  if (FLAGS_wal_enable) {
    auto &logger = txn.LogWorker();
    u64 offset = PID2Offset_table_[pid].get_offset();

    // Insert PID2Offset log entry to WAL
    auto &entry = logger.ReservePageImageLog(PAGE_SIZE, pid, offset);
    std::memcpy(entry.payload, page, PAGE_SIZE);
    entry.type = LogEntry::Type::PAGE_IMG;
    logger.SubmitActiveLogEntry();
  }
}

bool SpaceManager::HasReusablePID(u32 t_id) {
  // Access the head and tail for the specified thread ID
  auto *head = reusable_pids_heads[t_id];
  auto *tail = reusable_pids_tails[t_id];

  // Check if there are reusable PIDs available
  size_t head_pos = head->load(std::memory_order_relaxed);
  size_t tail_pos = tail->load(std::memory_order_relaxed);

  // Return true if the buffer is not empty (head != tail)
  return head_pos != tail_pos;
}

// Assigns a PID from the reusable list for a specific I/O handler, if available
pageid_t SpaceManager::AssignPIDUponAlloc(u32 t_id) {
  // Ensure the thread ID is within the valid range
  if (t_id >= io_handler_cnt_) {
    throw std::out_of_range("Invalid I/O handler index");
  }

  // Access the buffer, head, and tail for the thread
  auto *head = reusable_pids_heads[t_id];
  auto *tail = reusable_pids_tails[t_id];
  auto &buffer = reusable_pids_buffers[t_id];

  // Load head and tail indices
  size_t head_pos = head->load(std::memory_order_relaxed);
  size_t tail_pos = tail->load(std::memory_order_relaxed);

  if (head_pos != tail_pos) { // Buffer is not empty
    pageid_t pid = buffer[head_pos % MAX_REUSABLE_PIDS];
    buffer[head_pos % MAX_REUSABLE_PIDS] = max_mapped_pages_cnt_; // optional

    // Update the head position with relaxed ordering
    head->store((head_pos + 1) % MAX_REUSABLE_PIDS, std::memory_order_relaxed);
    Ensure(PID2Offset_table_[pid].get_comp_sz() == PAGE_SIZE - 1);
    return pid;
  }

  // Fallback to global allocation if no reusable PIDs are available
  throw std::out_of_range("no reusable pids");
}

// Checks if a specific PID exists in any deallocated list
bool SpaceManager::IsPIDInDeallocatedList(pageid_t pid) {
  for (u32 t_id = 0; t_id < io_handler_cnt_; ++t_id) {
    auto &pid_list = deallocated_pid_list_[t_id];
    // Check if the PID exists in the list
    if (std::any_of(pid_list.begin(), pid_list.end(), [pid](const auto &pair) {
          return pair.first == pid; // Compare the PID part of the pair
        })) {
      return true; // PID found
    }
  }
  return false; // PID not found
}

/// Adds a PID to the reusable list in FIFO order
void SpaceManager::AddReusablePID(size_t t_id, pageid_t pid) {
  auto *tail = reusable_pids_tails[t_id];
  auto &buffer = reusable_pids_buffers[t_id];

  size_t tail_pos = tail->load(std::memory_order_acquire);
  buffer[tail_pos % MAX_REUSABLE_PIDS] = pid;
  tail->store((tail_pos + 1) % MAX_REUSABLE_PIDS, std::memory_order_release);
}

// When PID is deallocated
void SpaceManager::AddPIDToDeallocList(u32 t_id, pageid_t pid, logid_t p_gsn) {
  // append to FIFO list
  deallocated_pid_list_[t_id].emplace_back(pid, p_gsn);
}

// Called periodically or at allocation time to recycle oldest PIDs
void SpaceManager::ProcessDeallocList(u32 t_id, logid_t p_gsn) {
  auto &pid_list = deallocated_pid_list_[t_id];

  // Process oldest entry first
  if (!pid_list.empty()) {
    auto [pid_to_trim, gsn_to_trim] = pid_list.front();

    // Example: GSN-based safe reuse condition
    if ((gsn_to_trim <= p_gsn)) {
      pid_list.pop_front();
      AddReusablePID(t_id, pid_to_trim);
    }
  }
}

// Submits a trim command for a specific PID
void SpaceManager::TrimPID(pageid_t pid) {
  u64 start_offset = static_cast<u64>(pid * PAGE_SIZE);
  u64 size = PAGE_SIZE;

  {
    std::lock_guard<std::mutex> lock(trim_mutex);
    trim_queue.emplace(start_offset, size);
  }
}

// Submits a trim command for a specific block
void SpaceManager::TrimBlock(blockid_t bid) {
  if (!FLAGS_use_trim) {
    return;
  }
  if (FLAGS_use_ZNS) {
    return;
  }
  u64 start_offset = static_cast<u64>(bid * block_size_);
  SendBlkdiscardReq(start_offset, block_size_);
}

void SpaceManager::StartTrimThread() {
  if (!FLAGS_use_trim) {
    return;
  }

  // Check if the thread is already running
  if (trim_thread_active_.load()) {
    return;
  }

  trim_thread_active_ = true;

  // Start the trim thread
  trim_thread_ = std::thread([this]() { ProcessTrimQueue(); });
}

void SpaceManager::StopTrimThread() {
  if (!trim_thread_active_.load()) {
    return;
  }

  trim_thread_active_ = false;

  if (trim_thread_.joinable()) {
    trim_thread_.join();
  }
}

void SpaceManager::ProcessTrimQueue() {
  while (trim_thread_active_.load()) {
    std::pair<u64, u64> trim_task;

    if (trim_queue.empty()) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(1)); // Avoid busy waiting
      continue;
    }
    std::lock_guard<std::mutex> lock(trim_mutex);
    if (!trim_queue.empty()) {
      //   std::lock_guard<std::mutex> lock(trim_mutex);
      trim_task = trim_queue.front();
      trim_queue.pop();
    }

    // Process the trim task
    SendBlkdiscardReq(trim_task.first, trim_task.second);
  }
}

void SpaceManager::CheckpointMappingTable() {
  auto sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    HotUringSubmit(); // Submit existing SQEs
    sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      throw std::runtime_error(
          "Failed to get SQE after submitting existing IOs");
    }
  }

  u64 write_size = mt_end_offset_ - mt_offset_;
  io_uring_prep_write(sqe, mt_fd_, reinterpret_cast<void *>(PID2Offset_table_),
                      write_size, mt_offset_);
  submitted_io_cnt_++;

  // Delay the submission until a later point (possibly in a batch loop).
  // Optionally check here if more SQEs can be added before submitting.
  HotUringSubmit();

  // Statistics should be updated only after ensuring successful submission.
  statistics::storage::space::checkpoint_writes += write_size;
}

void SpaceManager::SendBlkdiscardReq(u64 start_offset, u64 size) {
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

// Executes a shell command and returns the output
std::string SpaceManager::ExecCommand(const std::string &cmd) {
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

  // Remove only trailing newline
  if (!result.empty() && result.back() == '\n') {
    result.pop_back();
  }

  return result;
}

float SpaceManager::GetDBSize() {
  if (!FLAGS_use_out_of_place_write) {
    return static_cast<float>((statistics::buffer::alloc_cnt.load() -
                               statistics::buffer::dealloc_cnt.load()) *
                              PAGE_SIZE / MB);
  }
  u64 dbsize = 0;
  // if(FLAGS_use_ZNS){
  //    return static_cast<float>(
  //            (block_cnt_ - sspace_->GetBlockListSize(State::EMPTY)) *
  //            max_w_ptr_) /
  //        MB;
  // }else{
  dbsize = sspace_->GetBlockListSize(State::FULL) * max_w_ptr_;

  for (auto &bid : sspace_->block_lists[State::OPEN]) {
    if (sspace_->block_metadata[bid].state != State::OPEN) {
      continue;
    }
    dbsize += sspace_->block_metadata[bid].cur_size;
  }

  for (auto &bid : sspace_->block_lists[State::IN_USE]) {
    if (sspace_->block_metadata[bid].state != State::IN_USE) {
      continue;
    }
    dbsize += sspace_->block_metadata[bid].cur_size;
  }
  for (auto &bid : sspace_->block_lists[State::GC_IN_USE]) {
    if (sspace_->block_metadata[bid].state != State::GC_IN_USE) {
      continue;
    }
    dbsize += sspace_->block_metadata[bid].cur_size;
  }

  for (auto &bid : sspace_->block_lists[State::GC_READ]) {
    if (sspace_->block_metadata[bid].state != State::GC_READ) {
      continue;
    }
    dbsize += sspace_->block_metadata[bid].cur_size;
  }

  for (auto &bid : sspace_->block_lists[State::GC_WRITE]) {
    if (sspace_->block_metadata[bid].state != State::GC_WRITE) {
      continue;
    }
    dbsize += sspace_->block_metadata[bid].cur_size;
  }
  //}
  return static_cast<float>(dbsize) / MB;
}

bool SpaceManager::ReachedAllocSize() {

  if (FLAGS_use_ZNS) {
    if (sspace_->block_lists[State::EMPTY].empty()) {
      return true;
    }
  }

  u64 w_size = statistics::storage::space::cum_total_writes;
  u64 w_cnt = statistics::buffer::cum_total_writes_cnt;

  if (w_size < FLAGS_bm_physical_gb * GB) {
    return false;
  }

  if (w_size < FLAGS_bm_physical_gb * GB) {
    return false;
  }

  if (w_size == 0 || w_cnt == 0) {
    return false;
  }

  float comp_ratio = (!FLAGS_use_compression)
                         ? 1.0f
                         : static_cast<float>(w_cnt * leanstore::PAGE_SIZE) /
                               static_cast<float>(w_size);

  // Adjust allocated dataset size with compression ratio
  float alloc_minus_dealloc =
      static_cast<float>((statistics::buffer::alloc_cnt.load() -
                          statistics::buffer::dealloc_cnt.load()) *
                         PAGE_SIZE);

  if (comp_ratio <= 1.0f && FLAGS_use_compression) {
    comp_ratio = 2.0f;
  }

  // original
  float adjusted_logical_size = (alloc_minus_dealloc) / comp_ratio;

  return (GetDBSize() * MB > adjusted_logical_size + block_size_ * 2);
}

bool SpaceManager::ShouldTriggerGCZNS(u64 write_size) {
  if (FLAGS_max_open_block_cnt) {
    if ((sspace_->GetBlockListSize(State::FULL) >=
             block_cnt_ - cur_max_open_block_cnt_ &&
         sspace_->GetBlockListSize(State::EMPTY) ==
             0 // block_cnt_ % max_open_block_cnt_
         )) {
      return true;
    } else {
      return false;
    }
  } else {
    if ((sspace_->GetBlockListSize(State::FULL) == block_cnt_ &&
         !sspace_->GetBlockListSize(State::OPEN))) {
      return true;
    } else {
      return false;
    }
  }
}

bool SpaceManager::ShouldTriggerGC(u64 write_size) {
  bool trigger = false;
  trigger = ShouldTriggerGCZNS(write_size);
  sspace_->gc_triggered.store(trigger, std::memory_order_relaxed);
  return trigger;
  // when using ZNS
  if (FLAGS_use_ZNS || FLAGS_use_SSDWA1_pattern ||
      FLAGS_max_open_block_cnt) { //|| FLAGS_use_SSDWA1_pattern
    // trigger = ShouldTriggerGCZNS(write_size);
    // sspace_->gc_triggered.store(trigger, std::memory_order_relaxed);

  } else {
    if (!ReachedAllocSize()) {
      trigger = false;
    } else {
      // Get the current allocation and deallocation counts (in pages)
      u64 alloc_cnt =
          statistics::buffer::alloc_cnt.load(); // Number of allocated pages
      u64 dealloc_cnt =
          statistics::buffer::dealloc_cnt.load(); // Number of deallocated pages

      // Calculate the net allocated space (in bytes)
      u64 alloc_minus_dealloc = (alloc_cnt - dealloc_cnt) * PAGE_SIZE;

      // Ensure that logical_dataset_size is non-negative
      float logical_dataset_size = static_cast<float>(alloc_minus_dealloc);

      // Get the total database size and max WAL capacity (in bytes)
      u64 used_space =
          static_cast<u64>(GetDBSize()) * MB + FLAGS_max_wal_capacity_gb * GB;

      // SSD logical capacity and physical capacity (in bytes)
      float ssd_logical_capacity = static_cast<float>(
          max_userspace_capacity_ + (FLAGS_max_wal_capacity_gb)*GB); // SSD size
      float ssd_physical_capacity =
          ssd_logical_capacity * (1.0f + FLAGS_SSD_OP);

      // Total write size and total write count
      u64 w_size = statistics::storage::space::cum_total_writes;
      u64 w_cnt = statistics::buffer::cum_total_writes_cnt;

      // Compute the compression ratio if compression is enabled
      float comp_ratio =
          (!FLAGS_use_compression || w_size == 0 || w_cnt == 0)
              ? 1.0f
              : static_cast<float>(w_cnt * leanstore::PAGE_SIZE) /
                    static_cast<float>(w_size);

      if (FLAGS_use_compression && comp_ratio <= 1.0f) {
        comp_ratio = 2.0f;
      }

      // Adjust logical dataset size if compression is used
      if (FLAGS_use_compression) {
        logical_dataset_size = static_cast<float>(
            static_cast<double>(logical_dataset_size) / comp_ratio);
      }

      // Function to calculate write amplification (WA)
      auto calculateWA = [&](float x) {
        float denominator_1 =
            (x > logical_dataset_size)
                ? (ssd_logical_capacity - logical_dataset_size) /
                      (2.0f * (x - logical_dataset_size))
                : 1.0f;
        float denominator_2 =
            (x < ssd_physical_capacity)
                ? (ssd_physical_capacity) / (2.0f * (ssd_physical_capacity - x))
                : 1.0f;

        if (denominator_1 < 1.0f) {
          denominator_1 = 1.0f;
        }

        if (denominator_2 < 1.0f) {
          denominator_2 = 1.0f;
        }

        // Calculate the write amplification for the given value of x
        float result = denominator_1 * denominator_2;
        return result;
      };

      // Ensure logical dataset size does not exceed DB size
      float db_size_f =
          static_cast<float>(GetDBSize()) * static_cast<float>(MB);

      // Find the optimal dataset size with minimum write amplification
      float optimal_x = ssd_logical_capacity;
      float min_wa = std::numeric_limits<float>::max();
      float step = 1.0f * static_cast<float>(block_size_);

      for (float x = db_size_f;
           x <= ssd_logical_capacity -
                    static_cast<float>((FLAGS_max_wal_capacity_gb)*GB);
           x += step) {
        float current_wa = calculateWA(x);
        if (current_wa <= min_wa) {
          min_wa = current_wa;
          optimal_x = x;
        }
      }

      u64 gc_trigger = static_cast<u64>(optimal_x);

      if (write_size > 0) {
        // worker thread
        trigger = (used_space >= gc_trigger + block_cnt_);
      } else {
        // GC thread, should be triggered earlier when space usage exceeds
        // threshold minus a buffer
        u32 bound = 2;

        u64 buffer_pages = block_size_ * bound;

        u64 threshold = gc_trigger - (buffer_pages);

        trigger = used_space >= threshold;

        sspace_->gc_triggered.store(trigger, std::memory_order_relaxed);
      }
    }
  }
  return trigger;
}

} // namespace leanstore::storage::space
