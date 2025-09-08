#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"

#include <shared_mutex>

namespace leanstore::storage::space {
// current implementation only mocks the DWB behavior, needs to be properly
// implmented in the future if FLAGS_enable_doublewrite, write pages to dwb file
// before writing to db file offset doublewrite offset is after WAL end offset
// (w_end_offset_) in case FLAGS_use_out_of_place_write is enabled, mapping
// table is stored instead the total size of doublewrite file is PAGE_SIZE *
// io_interface_cnt w_end_offset_ ~ w_end_offset_ + PAGE_SIZE * io_interface_cnt
// per thread offset: [w_end_offset_ + worker_thread_id * PAGE_SIZE,
// w_end_offset_ + (1 + worker_thread_id) * PAGE_SIZE)
// this object is created per thread
class Doublewrite {
public:
  Doublewrite(int blockfd, u64 wale_offset);
  void SetWriteOffset();
  void AdvanceWriteOffset();
  size_t dwb_start_offset_;
  size_t dwb_end_offset_;
  size_t dwb_cur_w_offset_;
  size_t cnt = 0;
  int fd_;
  size_t wal_end_offset = 0;
};
} // namespace leanstore::storage::space