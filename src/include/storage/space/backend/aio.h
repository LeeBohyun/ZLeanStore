#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "storage/extent/large_page.h"
#include "storage/page.h"

#include "liburing.h"

#include <vector>

namespace leanstore::storage::space::backend {

class LibaioInterface {
  static constexpr u32 MAX_IOS = 2048;

public:
  LibaioInterface(int blockfd);
  ~LibaioInterface() = default;
  void IssueWriteRequest(u64 offset, u32 write_sz, void *buffer,
                         bool is_synchronous);
  void IssueReadRequest(u64 offset, u16 read_sz, void *buffer,
                        bool is_synchronous);
  void UringSubmit(size_t submit_cnt, io_uring *ring);
  void UringSubmitRead();
  void UringSubmitWrite();

private:
  int blockfd_;
  /* io_uring properties */
  struct io_uring read_ring_;
  struct io_uring write_ring_;
  u32 write_submit_cnt;
  u32 read_submit_cnt;
};

} // namespace leanstore::storage::space::backend