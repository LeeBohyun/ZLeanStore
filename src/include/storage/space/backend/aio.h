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
  struct PendingIO {
    u64 offset;
    u64 size;
    void *buffer;
  };

  // Reap `cnt` completions and verify each transferred its full size;
  // failed or short transfers are redone synchronously
  void ReapAndVerify(io_uring *ring, size_t cnt, std::vector<PendingIO> &pending,
                     bool is_write);

  int blockfd_;
  /* io_uring properties */
  struct io_uring read_ring_;
  struct io_uring write_ring_;
  u32 write_submit_cnt;
  u32 read_submit_cnt;
  std::vector<PendingIO> pending_writes_;
  std::vector<PendingIO> pending_reads_;
};

} // namespace leanstore::storage::space::backend