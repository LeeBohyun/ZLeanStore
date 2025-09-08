#include "storage/space/garbage_collector.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "storage/space/space_manager.h"
#include "transaction/transaction_manager.h"
#include <atomic>
#include <chrono>
#include <libnvme.h>
#include <mutex>
#include <nvme/types.h>
#include <random>
#include <shared_mutex>
#include <string>
#include <vector>

using LogEntry = leanstore::recovery::LogEntry;

namespace leanstore::storage::space {
GarbageCollector::GarbageCollector(PID2Offset *PID2Offset_table,
                                   StorageSpace *storage_space, int fd,
                                   u32 gc_id)
    : PID2Offset_table_(PID2Offset_table), sspace_(storage_space), fd_(fd),
      thread_id_(gc_id) {
  block_size_ = sspace_->block_size_;
  block_cnt_ = sspace_->block_cnt_;
  page_cnt_per_block_ = block_size_ / PAGE_SIZE;
  max_w_ptr_ = sspace_->max_w_ptr_;
}

bool GarbageCollector::AddBlockAsGCVictim(blockid_t bid) {
  if (sspace_->block_metadata[bid].state != State::FULL) {
    return false;
  }

  if (!MoveBlockFromFullToGCBlockList(bid)) {
    return false;
  } else {
    Ensure(sspace_->block_metadata[bid].state == State::GC_IN_USE);
  }

  return true;
}

bool GarbageCollector::ShouldTrustEDT() {
  u32 edt_zero_bid_cnt = 0;
  const auto &full_list = sspace_->block_lists[State::FULL];
  Ensure(!full_list.empty());
  u32 list_size = full_list.size();
  for (u32 i = 0; i < list_size; i++) {
    blockid_t bid = sspace_->block_lists[State::FULL][i];
    if (sspace_->block_metadata[bid].edt == 0) {
      edt_zero_bid_cnt++;
    }
  }
  return edt_zero_bid_cnt < sspace_->GetBlockListSize(State::FULL) / 2;
}

bool GarbageCollector::BlockIsGCVictim(blockid_t victim_bid) {

  if (sspace_->block_lists[State::GC_IN_USE].empty() &&
      sspace_->block_lists[State::GC_READ].empty() &&
      sspace_->block_lists[State::GC_WRITE].empty()) {
    return false;
  }

  for (auto bid : sspace_->block_lists[State::GC_IN_USE]) {
    if (victim_bid == bid) {
      return true;
    }
  }

  for (auto bid : sspace_->block_lists[State::GC_READ]) {
    if (victim_bid == bid) {
      return true;
    }
  }

  for (auto bid : sspace_->block_lists[State::GC_WRITE]) {
    if (victim_bid == bid) {
      return true;
    }
  }

  return false;
}

bool GarbageCollector::ShouldSelectImbalancedBlock(int victim_cnt) {
  if (!FLAGS_use_SSDWA1_pattern) {
    return false;
  }

  u32 active_group_threshold = static_cast<u32>(
      (sspace_->block_cnt_ * FLAGS_SSD_OP) / FLAGS_max_open_block_cnt);
  u32 imbalanced_cnt = 0;

  // iterate through sspace_->write_group_ vector lists
  // to check whether is there any imbalance in invalidation freqency of a block
  // within the same group invalidation count stored in
  // sspace_->block_metadata[min_bid].block_invalidation_cnt;
  constexpr u32 kImbalanceThreshold = 1; // can tune based on profiling

  for (const auto &group : sspace_->write_group_) {
    if (group.size() <= 1)
      continue; // no imbalance possible

    u32 min_inv = std::numeric_limits<u32>::max();
    u32 max_inv = 0;

    for (blockid_t bid : group) {
      u32 cnt = sspace_->block_metadata[bid].block_invalidation_cnt;
      min_inv = std::min(min_inv, cnt);
      max_inv = std::max(max_inv, cnt);
    }

    if ((max_inv - min_inv) >= kImbalanceThreshold) {
      imbalanced_cnt++;
      if (imbalanced_cnt > 0 &&
          sspace_->not_compensation_write.load() >= imbalanced_cnt - 1) {
        return true;
      }
      // return true; // found a group with significant imbalance
    }
  }

  return false;
}

int GarbageCollector::GreedyAmongImbalancedGroups(int victim_cnt) {
  if (!FLAGS_use_SSDWA1_pattern)
    return -1;

  constexpr u32 kImbalanceThreshold = 1; // can tune this

  int selected_group_index = -1;
  double lowest_valid_ratio = 1.0;
  u64 lowest_valid_cnt = UINT_MAX;

  for (size_t group_idx = 0; group_idx < sspace_->write_group_.size();
       ++group_idx) {
    const auto &group = sspace_->write_group_[group_idx];
    if (group.size() <= 1)
      continue;

    // Skip group if any block is not in FULL state
    bool all_full = true;
    for (blockid_t bid : group) {
      if (sspace_->block_metadata[bid].state != State::FULL) {
        all_full = false;
        break;
      }
    }
    if (!all_full)
      continue;

    u32 min_inv = std::numeric_limits<u32>::max();
    u32 max_inv = 0;
    u64 total_valid_pages = 0;
    bool full = false;
    u32 mincnt = 0;
    for (blockid_t bid : group) {
      u32 inv_cnt = sspace_->block_metadata[bid].block_invalidation_cnt;
      min_inv = std::min(min_inv, inv_cnt);
      max_inv = std::max(max_inv, inv_cnt);
      if (ValidPIDsCntInBlock(bid) ==
          sspace_->block_metadata[bid].total_pid_cnt) {
        full = true;
      }
    }

    if ((max_inv - min_inv) < kImbalanceThreshold || full)
      continue;

    for (blockid_t bid : group) {
      u32 inv_cnt = sspace_->block_metadata[bid].block_invalidation_cnt;
      if (inv_cnt < max_inv) {
        total_valid_pages += ValidPIDsCntInBlock(bid);
        mincnt++;
      }
    }

    if (total_valid_pages / (mincnt) < lowest_valid_cnt) {
      lowest_valid_cnt = total_valid_pages;
      selected_group_index = static_cast<int>(group_idx);
      // std::cout << "imbalanced group " << group_idx << " has "
      //           << total_valid_pages << " valid pages while cur min group "
      //           << selected_group_index << " has " << lowest_valid_cnt
      //           << std::endl;
    }
  }

  return selected_group_index;
}

std::vector<blockid_t> GarbageCollector::SelectVictimBlockToGCRead(
    int victim_cnt, std::unique_lock<std::shared_mutex> &lock) {
try_again:
  std::vector<blockid_t> victim_bids;
  u32 it = 0;

  if (!victim_cnt)
    return victim_bids;

  if (ShouldSelectImbalancedBlock(victim_cnt)) {
    int group_idx = GreedyAmongImbalancedGroups(victim_cnt);
    if (group_idx != -1) {
      blockid_t max_inv_bid = sspace_->write_group_[group_idx][0];
      u32 max_inv_cnt =
          sspace_->block_metadata[max_inv_bid].block_invalidation_cnt;

      for (blockid_t bid : sspace_->write_group_[group_idx]) {
        u32 inv_cnt = sspace_->block_metadata[bid].block_invalidation_cnt;
        if (inv_cnt > max_inv_cnt) {
          max_inv_cnt = inv_cnt;
          max_inv_bid = bid;
        }
      }

      std::vector<blockid_t> moved_bids;
      for (blockid_t bid : sspace_->write_group_[group_idx]) {
        if (sspace_->block_metadata[bid].block_invalidation_cnt ==
            max_inv_cnt) {
          continue;
        }
        moved_bids.push_back(bid);
      }

      std::sort(moved_bids.begin(), moved_bids.end(),
                [this](blockid_t a, blockid_t b) {
                  return ValidPIDsCntInBlock(a) < ValidPIDsCntInBlock(b);
                });

      for (blockid_t bid : moved_bids) {
        if ((int)victim_bids.size() >= victim_cnt)
          break;
        if (!AddBlockAsGCVictim(bid)) {
          victim_bids.clear();
          goto try_again;
        }
        Ensure(sspace_->block_metadata[bid].state == State::GC_IN_USE);
        victim_bids.push_back(bid);
      }

      if (!victim_bids.empty()) {
        sspace_->compensation_write.store(true, std::memory_order_relaxed);
        fprintf(stderr, "select bid compensation write\n");
        return victim_bids;
      }
    }
  }

  while ((int)victim_bids.size() < victim_cnt) {
    blockid_t victim_bid = block_cnt_;
    std::vector<blockid_t> victims;
    switch (FLAGS_gc_victim_block_selection_algorithm) {
    case 0:
      victim_bid = GreedyGC(true);
      break;
    case 1:
      victim_bid = EDTBasedGC(true);
      break;
    case 2:
      victim_bid = RandomGC();
      break;
    case 3: {
      u32 k = sspace_->GetBlockListSize(State::FULL) * 0.01;
      if (k == 0) {
        k = sspace_->GetBlockListSize(State::FULL) >= 8
                ? 8
                : sspace_->GetBlockListSize(State::FULL);
      }
      victim_bid = KGreedyGC(k);
      break;
    }
    case 4: {
      if (FLAGS_use_SSDWA1_pattern) {
        // select multiple victim blocks at once
        victims = GreedyGC(true, victim_cnt);

        if (victims.size() == victim_cnt) {
          sspace_->compensation_write.store(false, std::memory_order_relaxed);
          return victims;
        }
      } else {
        victim_bid = GreedyGC(true);
      }
      break;
    }
    default:
      victim_bid = GreedyGC(true);
      break;
    }

    if (victim_bid == block_cnt_) {
      break;
    }

    Ensure(AddBlockAsGCVictim(victim_bid));
    Ensure(sspace_->block_metadata[victim_bid].state == State::GC_IN_USE);
    victim_bids.push_back(victim_bid);
    victim_bid = block_cnt_;
  }

  if (!victim_bids.empty()) {
    // fprintf(stderr, "select bid on pure greedy\n");
    sspace_->compensation_write.store(false, std::memory_order_relaxed);
  }

  return victim_bids;
}

std::vector<blockid_t> GarbageCollector::SelectVictimBlocksToGCWrite(
    u64 write_sz, std::unique_lock<std::shared_mutex> &lock) {
  std::vector<blockid_t> selected_bids;
  blockid_t victim_bid = block_cnt_;

  // fprintf(stderr, "start selecting vicitm blocks to gcwrite\n");

  if (!sspace_->GetBlockListSize(State::GC_READ)) {
    return selected_bids;
  }

  switch (FLAGS_gc_victim_block_selection_algorithm) {
  case 0:
    victim_bid = GreedyGC(false);
    selected_bids.push_back(victim_bid);
    break;
  case 1:
    victim_bid = EDTBasedGC(false);
    selected_bids.push_back(victim_bid);
    break;
  case 2:
    victim_bid = RandomGC();
    selected_bids.push_back(victim_bid);
    break;
  case 4: {
    // 2R Greedy
    selected_bids = TwoRGreedyGC(false);
    break;
  }
  default:
    victim_bid = GreedyGC(false);
    selected_bids.push_back(victim_bid);
    break;
  }

  u64 valid_sz = 0;
  for (blockid_t bid : selected_bids) {
    if (bid == block_cnt_) {
      selected_bids.clear();
      return selected_bids;
    }
  }

  for (blockid_t bid : selected_bids) {
    Ensure(bid < block_cnt_);
    // Move block to GC_WRITE state only when it is selected
    Ensure(sspace_->MoveBlockToList(bid, State::GC_READ, State::GC_WRITE));
    Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
    Ensure(sspace_->IsBlockInList(bid, State::GC_WRITE));
  }
  return selected_bids;
}

blockid_t GarbageCollector::GreedyGC(bool read) {
  const auto &list = read ? sspace_->block_lists[State::FULL]
                          : sspace_->block_lists[State::GC_READ];

  u64 min_valid_page_cnt = page_cnt_per_block_ * (PAGE_SIZE / MIN_COMP_SIZE);
  blockid_t min_bid = block_cnt_;

  logid_t min_flushed = recovery::LogManager::global_min_gsn_flushed.load();

  for (blockid_t bid : list) {
    auto &bm = sspace_->block_metadata[bid];

    if ((read && bm.state != State::FULL) ||
        (!read && bm.state != State::GC_READ)) {
      continue;
    }
    if (ValidPIDsCntInBlock(bid) == sspace_->block_metadata[bid].total_pid_cnt)
      continue;

    // if (FLAGS_use_edt && read && bm.block_invalidation_cnt > 0) {
    //   if (FLAGS_write_buffer_partition_cnt > 0) {
    //     double min_gsn =
    //     static_cast<double>(recovery::LogManager::global_min_gsn_flushed);
    //     double edt = static_cast<double>(bm.edt);
    //     double margin = 1.0 + 1.0 / (FLAGS_write_buffer_partition_cnt *
    //     (FLAGS_write_buffer_partition_cnt+1) / 2);

    //     if (edt > min_gsn && edt <= min_gsn * margin) {
    //       continue;
    //     }
    //   }
    // }

    // Skip blocks overlapping with active blocks

    u64 valid_page_cnt = ValidPIDsCntInBlock(bid);
    if (valid_page_cnt == 0)
      return bid;

    if (valid_page_cnt < min_valid_page_cnt) {
      min_valid_page_cnt = valid_page_cnt;
      min_bid = bid;
    }
  }

  return min_bid;
}

std::vector<blockid_t> GarbageCollector::GreedyGC(bool read, int victim_cnt) {
  // greedy implemenataion for SSDWA1 write pattern
  std::vector<blockid_t> victims;

  while (victims.size() < victim_cnt) {

    blockid_t min_bid = GreedyGC(read);

    // Now: Imbalance-aware refinement
    bool min_triggers_imbalance = WillTriggerImbalance(min_bid);

    if (min_triggers_imbalance) {
      min_bid = SearchForOtherCandidate(min_bid, victims);
    }

    Ensure(AddBlockAsGCVictim(min_bid));
    Ensure(sspace_->block_metadata[min_bid].state == State::GC_IN_USE);
    victims.push_back(min_bid);

    if (victims.size() == victim_cnt) {
      break;
    }
  }

  return victims;
}

blockid_t
GarbageCollector::SearchForOtherCandidate(blockid_t min_bid,
                                          std::vector<blockid_t> &cur_group) {
  // Compute baseline valid data for the current victim block
  u64 min_group_valid_sum = AvgGroupValidPages(min_bid, cur_group);

  blockid_t candidate = min_bid;

  for (blockid_t bid : sspace_->block_lists[State::FULL]) {
    if (sspace_->block_metadata[bid].state != State::FULL)
      continue;
    if (bid == min_bid)
      continue; // Avoid comparing to itself

    bool will_imbalance = WillTriggerImbalance(bid);
    u64 cur_valid_cnt = will_imbalance ? AvgGroupValidPages(bid, cur_group)
                                       : ValidPIDsCntInBlock(bid);

    if (min_group_valid_sum > cur_valid_cnt) {
      // std::cout << "original min bid: " << candidate  <<  " has this: " <<
      // min_group_valid_sum << " changed to bid: " << bid  << " which has this:
      // " << cur_valid_cnt<< std::endl;
      candidate = bid;
      min_group_valid_sum = cur_valid_cnt;
    }
  }

  return candidate;
}

u64 GarbageCollector::AvgGroupValidPages(blockid_t bid,
                                         std::vector<blockid_t> &cur_group) {
  // Calculate the sum of all the valid pages in all the imbalanced groups
  u64 total_valid = 0;
  u32 bidcnt = 0;

  for (const auto &group : sspace_->write_group_) {
    // Skip groups that do not contain the target bid
    if (std::find(group.begin(), group.end(), bid) == group.end())
      continue;

    for (blockid_t other : group) {
      // Skip the bid itself and blocks already in the current group
      if (other == bid || std::find(cur_group.begin(), cur_group.end(),
                                    other) != cur_group.end())
        continue;

      total_valid += ValidPIDsCntInBlock(other);
      bidcnt++;
    }
  }

  if (bidcnt == 0)
    return ValidPIDsCntInBlock(bid); // Avoid division by zero
  return total_valid / bidcnt;
}

bool GarbageCollector::WillTriggerImbalance(blockid_t bid) {
  if (!FLAGS_use_SSDWA1_pattern) {
    return false;
  }
  u32 imbalance_cnt = 0;

  constexpr u32 kImbalanceThreshold = 1;

  for (const auto &group : sspace_->write_group_) {
    // Check only groups that contain `bid`
    if (std::find(group.begin(), group.end(), bid) == group.end()) {
      continue;
    }

    if (group.size() <= 1) {
      continue; // skip trivial groups
    }

    u32 min_inv = std::numeric_limits<u32>::max();
    u32 max_inv = 0;

    for (blockid_t other_bid : group) {
      u32 cnt = sspace_->block_metadata[other_bid].block_invalidation_cnt;
      if (other_bid == bid) {
        cnt += 1; // simulate effect of selecting this block
      }
      min_inv = std::min(min_inv, cnt);
      max_inv = std::max(max_inv, cnt);
    }

    if ((max_inv - min_inv) >= kImbalanceThreshold) {
      return true;

      // return true; // imbalance would be triggered in this group
    }
  }

  return false; // no imbalance triggered in any group
}

std::vector<blockid_t> GarbageCollector::TwoRGreedyGC(bool read) {
  std::vector<blockid_t> selected_bids;

  const auto &src_list = read ? sspace_->block_lists[State::FULL]
                              : sspace_->block_lists[State::GC_READ];

  u32 mincnt =
      sspace_->page_cnt_per_block_ / (sspace_->page_cnt_per_block_ -
                                      ValidPIDsCntInBlock(GreedyGC(false))) +
      1;
  u32 max_target_size = std::min(mincnt, (u32)sspace_->max_open_block_);
  u64 block_data_capacity = sspace_->max_w_ptr_;

  if (src_list.size() < mincnt && (sspace_->GetGCTargetSize() != 0)) {
    return selected_bids;
  }

  if (src_list.size() == 1) {
    selected_bids.push_back(src_list[0]);
    return selected_bids;
  }

  if (ValidPIDsCntInBlock(GreedyGC(false)) < sspace_->page_cnt_per_block_ / 8) {
    selected_bids.push_back(GreedyGC(false));
    return selected_bids;
  }

  struct BlockInfo {
    blockid_t bid;
    u64 invalid_size;
    u64 invalidation_cnt;
    logid_t edt;
  };

  std::vector<BlockInfo> candidates;

  for (blockid_t bid : src_list) {
    if (sspace_->block_metadata[bid].state != State::GC_READ) {
      continue;
    }

    u64 valid_size = ValidPIDsSizeInBlock(bid);
    u64 invalid_size =
        block_data_capacity > valid_size ? block_data_capacity - valid_size : 0;
    u64 invalidation_cnt = sspace_->block_metadata[bid].block_invalidation_cnt;

    candidates.push_back({bid, invalid_size, invalidation_cnt,
                          sspace_->block_metadata[bid].edt});
  }

  // Step 1: Sort candidates by descending invalid_size (most reclaimable space
  // first)
  std::sort(candidates.begin(), candidates.end(),
            [](const BlockInfo &a, const BlockInfo &b) {
              return a.invalid_size > b.invalid_size;
            });

  // Step 2: Pick top-k candidates until we accumulate enough invalid data
  // also if the amount of total valid size % sspace_->max_w_ptr_ smaller before
  // adding the final candidate, break
  u64 accumulated_invalid = 0;
  u64 accumulated_valid = 0;
  std::vector<BlockInfo> chosen;

  for (size_t i = 0; i < candidates.size() && chosen.size() < max_target_size;
       ++i) {
    const auto &cand = candidates[i];
    u64 cand_valid = block_data_capacity - cand.invalid_size;

    u64 new_accumulated_invalid = accumulated_invalid + cand.invalid_size;
    u64 new_accumulated_valid = accumulated_valid + cand_valid;

    chosen.push_back(cand);
    accumulated_invalid = new_accumulated_invalid;
    accumulated_valid = new_accumulated_valid;

    if (accumulated_invalid >= block_data_capacity) {
      break;
    }
  }

  // Step 3: Sort chosen blocks by ascending invalidation_cnt, then by
  // invalid_size
  // descending order of edt

  std::sort(
      chosen.begin(), chosen.end(),
      [](const BlockInfo &a, const BlockInfo &b) { return a.edt > b.edt; });

  // Step 4: Extract block IDs
  for (const auto &info : chosen) {
    selected_bids.push_back(info.bid);
  }

  return selected_bids;
}

blockid_t GarbageCollector::KGreedyGC(u32 k) {
  std::vector<blockid_t> candidates; // To store randomly selected blocks
  std::vector<u32> valid_page_counts;

  // Get the list of candidate blocks from the FULL block list
  std::vector<blockid_t> full_blocks = sspace_->block_lists[State::FULL];

  if (full_blocks.size() <= k) {
    candidates = full_blocks;
  } else {
    if (FLAGS_invalidation_history_based_empty_block_selection) {
      // Sort the full blocks based on invalidation count and select k blocks
      // with minimum invalidation count
      std::vector<std::pair<blockid_t, u64>> block_invalidation_pairs;

      // Populate the vector with block IDs and their invalidation counts
      for (blockid_t block_id : full_blocks) {
        u64 invalidation_cnt =
            sspace_->block_metadata[block_id].block_invalidation_cnt;
        block_invalidation_pairs.push_back({block_id, invalidation_cnt});
      }

      // Sort the vector by invalidation count (ascending order)
      std::sort(block_invalidation_pairs.begin(),
                block_invalidation_pairs.end(),
                [](const std::pair<blockid_t, u64> &a,
                   const std::pair<blockid_t, u64> &b) {
                  return a.second < b.second; // Compare by invalidation count
                });

      // Select the k blocks with minimum invalidation count
      for (u32 i = 0; i < k; ++i) {
        candidates.push_back(block_invalidation_pairs[i].first);
      }

    } else {
      // Randomly select k blocks from the FULL block list
      std::sample(full_blocks.begin(), full_blocks.end(),
                  std::back_inserter(candidates), k,
                  std::mt19937{std::random_device{}()});
    }
  }

  blockid_t min_bid = block_cnt_; // Initialize with an invalid block ID
  u64 min_valid_page_cnt = page_cnt_per_block_ * (PAGE_SIZE / MIN_COMP_SIZE);

  for (auto bid : candidates) {
    if (sspace_->block_metadata[bid].state != State::FULL ||
        BlockIsGCVictim(bid)) {
      continue;
    }

    u64 valid_page_cnt = ValidPIDsCntInBlock(bid);
    // Select the block with the minimum valid pages
    if (valid_page_cnt < min_valid_page_cnt) {
      min_valid_page_cnt = valid_page_cnt;
      min_bid = bid;
    }
  }

  return min_bid;
}

blockid_t GarbageCollector::FIFOGC() {
  blockid_t bid = sspace_->block_lists[State::FULL][0];
  return bid;
}

blockid_t GarbageCollector::RandomGC() {
  u32 idx = rand() % sspace_->GetBlockListSize(State::FULL);
  blockid_t bid = sspace_->block_lists[State::FULL][idx];
  return bid;
}

blockid_t GarbageCollector::EDTBasedGC(bool read) {
  blockid_t victim_bid = block_cnt_; // Default to invalid bid
  logid_t min_edt = std::numeric_limits<logid_t>::max();
  int min_valid_pids = std::numeric_limits<int>::max();

  const auto &list = read ? sspace_->block_lists[State::FULL]
                          : sspace_->block_lists[State::GC_READ];

  for (blockid_t bid : list) {
    State expected_state = read ? State::FULL : State::GC_READ;
    if (sspace_->block_metadata[bid].state != expected_state ||
        BlockIsGCVictim(bid)) {
      continue;
    }

    logid_t bid_edt = sspace_->block_metadata[bid].edt;
    int valid_pids = ValidPIDsCntInBlock(bid);

    if (bid_edt < min_edt ||
        (bid_edt == min_edt && valid_pids < min_valid_pids)) {
      min_edt = bid_edt;
      min_valid_pids = valid_pids;
      victim_bid = bid;
    }
  }

  return victim_bid;
}

void GarbageCollector::AddBlockInvalidationHistory(blockid_t bid) {
  auto &txn = transaction::TransactionManager::active_txn;
  auto &logger = txn.LogWorker();
  if (!txn.IsRunning()) {
    return;
  }

  logid_t w_gsn = logger.GetCurrentGSN();

  u32 min_hist_idx = 0;
  // const auto &history =
  // sspace_->block_metadata[bid].invalidation_history_gsn;

  // for (u32 idx = 1; idx < history.size(); ++idx) {
  //   if (history[idx] < history[min_hist_idx]) {
  //     min_hist_idx = idx;
  //   }
  // }
  // sspace_->block_metadata[bid].invalidation_history_gsn[min_hist_idx] =
  // w_gsn;
  sspace_->block_metadata[bid].block_invalidation_cnt++;
}

std::vector<pageid_t> &GarbageCollector::GetValidPIDsInBlock(blockid_t bid) {
  // Create a static vector to hold the valid page IDs.
  static std::vector<pageid_t> valid_pids;
  valid_pids.clear();

  if (bid >= block_cnt_) {
    return valid_pids; // Return an empty vector if the block ID is invalid.
  }

  for (u32 p = 0; p < page_cnt_per_block_; p++) {
    u16 pcnt = sspace_->blocks[bid].pages[p].cnt;
    if (pcnt == 0)
      continue;

    for (u16 pidx = 0; pidx < pcnt; pidx++) {
      if (PageIsValid(bid, p, pidx)) {
        valid_pids.push_back(ConvertOffset2PID(bid, p, pidx));
      }
    }
  }
  return valid_pids;
}

u64 GarbageCollector::ValidPIDsCntInBlock(blockid_t bid) {
  u64 valid_page_cnt = 0;
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
  Ensure(sspace_->block_metadata[bid].total_pid_cnt >=
         sspace_->block_metadata[bid].invalidated_pid_cnt);
  valid_page_cnt = sspace_->block_metadata[bid].total_pid_cnt -
                   sspace_->block_metadata[bid].invalidated_pid_cnt;
  return valid_page_cnt;
}

u64 GarbageCollector::ValidPIDsSizeInBlock(blockid_t bid) {
  if (bid == block_cnt_)
    return max_w_ptr_;

  u64 valid_pid_cnt = 0;
  u64 total_comp_size = 0;
  for (u32 p = 0; p < page_cnt_per_block_; p++) {
    u16 pcnt = sspace_->blocks[bid].pages[p].cnt;
    for (u16 pidx = 0; pidx < pcnt; pidx++) {
      if (PageIsValid(bid, p, pidx)) {
        valid_pid_cnt++;
        total_comp_size += (PAGE_SIZE / pcnt) + pcnt;
      }
    }
  }

  double comp_ratio =
      (!FLAGS_use_compression && valid_pid_cnt == 0)
          ? 1.0
          : static_cast<double>(total_comp_size) /
                (static_cast<double>(valid_pid_cnt) * leanstore::PAGE_SIZE);

  // return static_cast<u64>(valid_pid_cnt * PAGE_SIZE * comp_ratio);  //
  // Explicit cast to u64
  return total_comp_size;
}

bool GarbageCollector::MoveBlockFromFullToGCBlockList(blockid_t bid) {
  if (sspace_->MoveBlockToList(bid, State::FULL, State::GC_IN_USE)) {
    Ensure(sspace_->IsBlockInList(bid, State::GC_IN_USE));
    Ensure(sspace_->block_metadata[bid].state == State::GC_IN_USE);
    return true;
  }
  return false;
}

void GarbageCollector::MoveBlocksFromGCToEmptyBlockList() {
  if (!victim_bids_.size()) {
    return;
  }

  for (u32 idx = 0; idx < victim_bids_.size(); idx++) {
    blockid_t bid = victim_bids_[idx];
    auto &metadata = sspace_->block_metadata[bid];
    sspace_->MoveBlockToList(bid, State::GC_READ, State::EMPTY);
  }
}

void GarbageCollector::MoveBlocksFromGCToOpenBlockList() {
  if (!victim_bids_.size()) {
    return;
  }
  // Use the general-purpose method to move the block
  for (u32 idx = 0; idx < victim_bids_.size(); idx++) {
    blockid_t bid = victim_bids_[idx];
    sspace_->MoveBlockToList(bid, State::GC_READ, State::OPEN);

    // Update the block's state
    sspace_->UpdateBlockMetadata(bid, sspace_->block_metadata[bid].cur_size,
                                 sspace_->block_metadata[bid].edt, State::OPEN);
  }
}

void GarbageCollector::EraseBlock(blockid_t bid) {

  // Ensure the block is in the GCED state before erasing
  if (bid >= sspace_->block_metadata.size() ||
      sspace_->block_metadata[bid].state != State::GC_READ) {
    throw std::runtime_error("Block is not in GCED state or out of range");
  }

  if (bid >= block_cnt_) {
    return;
  }

  // Initialize cleaned block info
  for (u32 p = 0; p < page_cnt_per_block_; p++) {
    u16 pcnt = sspace_->blocks[bid].pages[p].cnt;
    ResetOffset2PIDs(bid, p, pcnt);
  }
}

void GarbageCollector::TrimBlock(blockid_t bid) {
  if (bid < block_cnt_) {
    DiscardBlockLBA(bid);
  }
}

void GarbageCollector::DiscardBlockLBA(blockid_t bid) {
  u64 lba_offset = bid * block_size_;
  SendBlkdiscardReq(lba_offset, block_size_);
}

void GarbageCollector::SendBlkdiscardReq(u64 start_offset, u64 size) {
  u64 start_lba = start_offset / SECTOR_SIZE;
  u32 num_sectors = size / SECTOR_SIZE;
  try {
    std::string command =
        "echo " + FLAGS_user_pwd + " | sudo -S nvme dsm " + FLAGS_db_path +
        " --ad --slbs=" + std::to_string(start_lba) +
        " --blocks=" + std::to_string(num_sectors) + " 2>/dev/null";
    std::string output = ExecCommand(command);
    return;
  } catch (const std::runtime_error &e) {
  }
  return;
}

std::string GarbageCollector::ExecCommand(const std::string &cmd) {
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

u64 GarbageCollector::GetPageOffset(blockid_t bid, u32 p) {
  return bid * block_size_ + p * PAGE_SIZE;
}

void GarbageCollector::ResetOffset2PIDs(blockid_t bid, u32 pn, u16 pcnt) {
  auto &pages = sspace_->blocks[bid].pages[pn];
  pages.pids.assign(pcnt, 0);
  pages.cnt = 0;
}

pageid_t GarbageCollector::ConvertOffset2PID(blockid_t bid, u32 p, u16 pidx) {
  pageid_t pid = sspace_->blocks[bid].pages[p].pids[pidx];
  return pid;
}

void GarbageCollector::LogValidPage(pageid_t pid, storage::Page *page) {
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

bool GarbageCollector::PageIsValid(blockid_t bid, u32 p, u16 pidx) {

  u64 page_offset = bid * block_size_ + p * PAGE_SIZE;
  pageid_t cur_pid = sspace_->blocks[bid].pages[p].pids[pidx];

  if (cur_pid == sspace_->max_mapped_pages_cnt_) {
    return false;
  }

  if (PID2Offset_table_[cur_pid].get_comp_sz() == PAGE_SIZE * 2 ||
      PID2Offset_table_[cur_pid].get_comp_sz() == 0) {
    return false;
  }
  if (PID2Offset_table_[cur_pid].get_offset() == 0 &&
      PID2Offset_table_[cur_pid].get_comp_sz() == 0) {
    return false;
  }
  u64 page_start_offset = PID2Offset_table_[cur_pid].get_offset() -
                          (PID2Offset_table_[cur_pid].get_offset() % PAGE_SIZE);
  // fprintf(stderr, "bid: %lu p: %lu pidx: %lu page_offset: %lu cur_pid :%lu
  // page_start_offset: %lu\n", bid, p, pidx, page_offset, cur_pid,
  // page_start_offset);
  return (page_start_offset == page_offset);
}

} // namespace leanstore::storage::space
