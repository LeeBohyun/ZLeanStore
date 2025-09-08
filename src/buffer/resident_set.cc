#include "buffer/resident_set.h"
#include "common/exceptions.h"
#include "common/utils.h"

#include "share_headers/logger.h"

#include <bit>
#include <cassert>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <sys/mman.h>

namespace leanstore::buffer {

ResidentPageSet::ResidentPageSet(u64 max_count, sync::PageState *page_state,
                                 u32 iohandler)
    : capacity_(max_count),
      count_(std::bit_ceil(static_cast<u64>(capacity_ * 1.8))), // 1.5
      mask_(count_ - 1), clock_pos_(0), page_state_(page_state),
      iohandler_cnt_(iohandler) {
  slots_ = static_cast<Entry *>(AllocHuge(count_ * sizeof(Entry)));
  memset(static_cast<void *>(slots_), 0xFF, count_ * sizeof(Entry));
  slots_per_shard = count_ / iohandler;
  valid_counts_ = std::make_unique<std::atomic<u64>[]>(iohandler);
  for (u32 i = 0; i < iohandler; ++i) {
    valid_counts_[i].store(0);
  }
}

ResidentPageSet::~ResidentPageSet() {
  munmap(slots_, count_ * sizeof(u64));
  slots_ = nullptr; // this trivial line is simply to quiet clang-tidy
}

auto ResidentPageSet::Count() -> u64 { return count_; }

auto ResidentPageSet::Capacity() -> u64 { return capacity_; }

u64 ResidentPageSet::GetShardForSlot(u64 slot_idx) {
  return slot_idx / slots_per_shard;
}

u64 ResidentPageSet::GetShardForPID(pageid_t pid) {
  u64 slot_idx = HashFn(pid) % count_;
  return slot_idx / slots_per_shard;
}

auto ResidentPageSet::Contain(pageid_t page_id) -> bool {
  // u64 pos = HashFn(page_id) & mask_;
  u64 pos = HashFn(page_id) % count_;
  u64 start_pos = pos;
  do {
    u64 curr = slots_[pos].pid.load();
    if (curr == ResidentPageSet::EMPTY) {
      return false;
    }
    if (curr == page_id) {
      return true;
    }
    pos = (pos + 1) % count_;
  } while (pos != start_pos);

  return false;
}

auto ResidentPageSet::Insert(pageid_t page_id) -> bool {
  assert(GetPageState(page_id).LockState() == sync::PageStateMode::EXCLUSIVE);
  u64 pos = HashFn(page_id) % count_;
  u64 curr;

  // Try to insert the page_id using linear probing
  while (true) {
    curr = slots_[pos].pid.load();

    // If the page_id is already in the table, do nothing
    if (curr == page_id) {
      return false; // Page ID already present, no need to insert
    }

    // If the slot is empty or marked as tombstone, attempt to insert the
    // page_id
    if ((curr == ResidentPageSet::EMPTY) ||
        (curr == ResidentPageSet::TOMBSTONE)) {
      if (slots_[pos].pid.compare_exchange_strong(curr, page_id)) {
        // u64 shard = pos / slots_per_shard;
        // valid_counts_[shard].fetch_add(1, std::memory_order_relaxed);
        return true; // Insertion successful
      }
    }

    // Move to the next position if current slot is occupied
    pos = (pos + 1) % count_;
  }
  return false;
}

auto ResidentPageSet::Remove(pageid_t page_id) -> bool {
  assert(GetPageState(page_id).LockState() == sync::PageStateMode::EXCLUSIVE);

  u64 pos = HashFn(page_id) % count_;
  while (true) {
    u64 curr = slots_[pos].pid.load();
    if (curr == ResidentPageSet::EMPTY) {
      return false;
    }
    if (curr == page_id) {
      if (slots_[pos].pid.compare_exchange_strong(curr,
                                                  ResidentPageSet::TOMBSTONE)) {
        // u64 shard = pos / slots_per_shard;
        // valid_counts_[shard].fetch_sub(1, std::memory_order_relaxed);
        return true;
      }
    }

    pos = (pos + 1) % count_;
  }

  // Can't find the page_id in the hash table
  return false;
}

void ResidentPageSet::IterateClockBatch(
    u64 batch, const std::function<void(pageid_t)> &evict_fn) {
  u64 pos;
  u64 new_position;

  do {
    pos = clock_pos_.load();
    new_position = (pos + batch) % count_;
  } while (!clock_pos_.compare_exchange_strong(pos, new_position));

  pos = clock_pos_.load();

  for (u64 idx = 0; idx < batch; idx++) {
    u64 curr = slots_[pos].pid.load();
    if ((curr != ResidentPageSet::TOMBSTONE) &&
        (curr != ResidentPageSet::EMPTY)) {
      evict_fn(curr);
    }

    pos = (pos + 1) % count_;
  }
}

pageid_t ResidentPageSet::GetPID(u64 cnt) {
  u64 pos;
  pos = cp_pos_.load();
  cnt =
      ((cnt + pos) % count_); //  Apply mask to keep cnt within [0, count_ - 1]

  // Start linear probing from the given cnt index
  pageid_t curr = slots_[cnt].pid.load();
  cp_pos_.compare_exchange_weak(pos, cnt);

  // Check if the current slot has a valid PID
  if ((curr != ResidentPageSet::TOMBSTONE) &&
      (curr != ResidentPageSet::EMPTY)) {
    return curr;
  }

  // If no valid PID is found, return an invalid PID or indicate failure
  return ResidentPageSet::TOMBSTONE; // Returning EMPTY to indicate no valid PID
                                     // was found
}

u64 ResidentPageSet::GetShardWithMostValidPages() {
  u64 max_shard = 0;
  u64 max_count = 0;
  for (u64 i = 0; i < iohandler_cnt_; ++i) {
    u64 count = valid_counts_[i].load(std::memory_order_relaxed);
    if (count > max_count) {
      max_shard = i;
      max_count = count;
    }
  }
  return max_shard;
}

void ResidentPageSet::SetGCPosToCP() {
  u64 cnt = 0;
  u64 cur_gc_pos = 0;
  u64 pos = 0;
  do {
    pos = (cp_pos_.load() + cnt) % count_;
    cur_gc_pos = gc_pos_.load();
    cnt++;
  } while (!gc_pos_.compare_exchange_strong(cur_gc_pos, pos));
}

pageid_t ResidentPageSet::GetPIDToEvict(u64 cnt, u64 batch) {
  u64 pos;
  pos = clock_pos_.load();
  cnt = ((batch + pos) %
         count_); //  Apply mask to keep cnt within [0, count_ - 1]

  // Start linear probing from the given cnt index
  pageid_t curr = slots_[cnt].pid.load();

  clock_pos_.compare_exchange_strong(pos, cnt);

  // Check if the current slot has a valid PID
  if ((curr != ResidentPageSet::TOMBSTONE) &&
      (curr != ResidentPageSet::EMPTY)) {
    return curr;
  }

  // If no valid PID is found, return an invalid PID or indicate failure
  return ResidentPageSet::TOMBSTONE; // Returning EMPTY to indicate no valid PID
}

} // namespace leanstore::buffer