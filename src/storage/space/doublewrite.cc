#include <storage/space/doublewrite.h>

#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"

namespace leanstore::storage::space {
Doublewrite::Doublewrite(int blockfd, u64 wale_offset)
    : fd_(blockfd), wal_end_offset(wale_offset) {
  wal_end_offset +=
      (FLAGS_wal_path.empty() ? 0 : FLAGS_max_wal_capacity_gb * GB);
  Ensure(wal_end_offset % PAGE_SIZE == 0);
  dwb_start_offset_ =
      wal_end_offset + static_cast<size_t>(worker_thread_id * MB * 384);
  dwb_end_offset_ =
      wal_end_offset + static_cast<size_t>((1 + worker_thread_id) * MB * 384);
  dwb_cur_w_offset_ = dwb_start_offset_;
}

void Doublewrite::SetWriteOffset() {
  dwb_start_offset_ =
      wal_end_offset + static_cast<size_t>(worker_thread_id * MB * 384);
  dwb_end_offset_ =
      wal_end_offset + static_cast<size_t>((1 + worker_thread_id) * MB * 384);
}

void Doublewrite::AdvanceWriteOffset() {

  if (cnt == MB * 384 / PAGE_SIZE - 1) {
    cnt = 0;
  } else {
    cnt++;
  }

  dwb_cur_w_offset_ = dwb_start_offset_ + PAGE_SIZE * cnt;

  Ensure(dwb_cur_w_offset_ >= dwb_start_offset_);
  Ensure(dwb_cur_w_offset_ < dwb_end_offset_);
}

} // namespace leanstore::storage::space