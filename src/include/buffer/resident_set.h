#pragma once

#include "common/typedefs.h"
#include "common/utils.h"
#include "sync/page_state.h"

#include <atomic>
#include <functional>
#include <shared_mutex>

namespace leanstore::buffer {

class ResidentPageSet {
public:
  static constexpr u64 EMPTY = ~0ULL;
  static constexpr u64 TOMBSTONE = (~0ULL) - 1;

  explicit ResidentPageSet(u64 max_count, sync::PageState *page_state,
                           u32 io_handler_cnt);
  ~ResidentPageSet();
  auto Capacity() -> u64;
  auto Count() -> u64;
  auto Contain(pageid_t page_id) -> bool;
  auto Insert(pageid_t page_id) -> bool;
  auto Remove(pageid_t page_id) -> bool;
  void IterateClockBatch(u64 batch,
                         const std::function<void(pageid_t)> &evict_fn);
  pageid_t GetPID(u64 cnt);
  pageid_t GetPIDToEvict(u64 cnt, u64 batch);
  void SetGCPosToCP();
  u64 GetShardForSlot(u64 slot_idx);
  u64 GetShardForPID(pageid_t pid);
  u64 GetShardWithMostValidPages();

private:
  auto GetPageState(pageid_t pid) -> sync::PageState & {
    return page_state_[pid];
  }

  struct EvictBatch {
    u64 start;
    u64 valid_count;
  };

  class Entry {
  public:
    std::atomic<pageid_t> pid;
  };

  const u64 capacity_;
  const u64 count_;
  const u64 mask_;
  std::atomic<u64> clock_pos_;
  std::atomic<u64> cp_pos_;
  std::atomic<u64> gc_pos_;
  Entry *slots_;
  sync::PageState *page_state_;

  u32 iohandler_cnt_ = 0;
  u64 slots_per_shard = 0;
  std::unique_ptr<std::atomic<u64>[]> valid_counts_;
};

} // namespace leanstore::buffer