#include "storage/space/backend/aio.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "share_headers/logger.h"

#include "liburing.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace leanstore::storage::space::backend {

LibaioInterface::LibaioInterface(int blockfd) : blockfd_(blockfd) {
  assert(FLAGS_bm_aio_qd <= MAX_IOS);
  // Previously the return value of io_uring_queue_init was discarded. When
  // init fails (e.g. EMFILE under heavy fd usage, ENOMEM, lockable-memory
  // exhaustion), the io_uring struct is left in a zeroed state where
  // io_uring_get_sqe() returns null forever and io_uring_sq_space_left() is
  // 0 — which then trips Ensure(sqe != nullptr) inside IssueWriteRequest /
  // IssueReadRequest at the first I/O. Surface the failure loudly so it's
  // not a confusing assertion later.
  int r = io_uring_queue_init(FLAGS_bm_aio_qd, &read_ring_, 0);
  if (r != 0) {
    throw std::runtime_error(std::string("io_uring_queue_init(read_ring) failed: ") +
                             strerror(-r));
  }
  r = io_uring_queue_init(FLAGS_bm_aio_qd, &write_ring_, 0);
  if (r != 0) {
    throw std::runtime_error(std::string("io_uring_queue_init(write_ring) failed: ") +
                             strerror(-r));
  }
  read_submit_cnt = 0;
  write_submit_cnt = 0;
  // Sanity check: liburing inline functions read ring->sq.ring_entries to
  // know the queue size. If headers and runtime library are out of ABI sync
  // (e.g. headers from liburing 2.5 + runtime 2.2 from a foreign install),
  // ring_entries lands at a different offset and silently reads as 0 here —
  // which then makes io_uring_get_sqe() return null forever.
  if (write_ring_.sq.ring_entries == 0 || read_ring_.sq.ring_entries == 0) {
    throw std::runtime_error(
        "io_uring init succeeded but ring_entries==0; liburing header/runtime "
        "version mismatch? Check `ldd` for liburing.so.2 path.");
  }
}

void LibaioInterface::UringSubmit(size_t submit_cnt, io_uring *ring) {
  if (submit_cnt > 0) {
    // Submit the requests and wait for at least `submit_cnt` completions
    auto ret = io_uring_submit_and_wait(ring, submit_cnt);
    Ensure(ret >= 0); // Ensure the submission was successful

    // Process the completion queue for the submitted requests
    for (size_t i = 0; i < submit_cnt; ++i) {
      io_uring_cqe *cqe;
      int wait_ret =
          io_uring_wait_cqe(ring, &cqe); // Wait for a completion entry
      Ensure(wait_ret == 0); // Ensure a CQE was retrieved successfully

      // Check the result of the completed request
      if (cqe->res < 0) {
        LOG_ERROR("IO request failed with error: %d wid: %u ", cqe->res,
                  worker_thread_id);
      }

      // Mark the completion entry as seen
      io_uring_cqe_seen(ring, cqe);
    }
  }
}

void LibaioInterface::UringSubmitRead() {
  if (read_submit_cnt > 0) {
    auto ret = io_uring_submit_and_wait(&read_ring_, read_submit_cnt);
    io_uring_cq_advance(&read_ring_, read_submit_cnt);
    Ensure(ret == static_cast<int>(read_submit_cnt));
    read_submit_cnt = 0;
  }
}

void LibaioInterface::UringSubmitWrite() {
  if (write_submit_cnt > 0) {
    auto ret = io_uring_submit_and_wait(&write_ring_, write_submit_cnt);
    io_uring_cq_advance(&write_ring_, write_submit_cnt);
    Ensure(ret == static_cast<int>(write_submit_cnt));
    write_submit_cnt = 0;
  }
}

void LibaioInterface::IssueWriteRequest(u64 offset, u32 write_sz, void *buffer,
                                        bool is_synchronous) {
  auto sqe = io_uring_get_sqe(&write_ring_);
  if (sqe == nullptr) {
    UringSubmit(write_submit_cnt, &write_ring_);
    sqe = io_uring_get_sqe(&write_ring_);
    Ensure(sqe != nullptr);
    write_submit_cnt = 0;
  }
  io_uring_prep_write(sqe, blockfd_, buffer, write_sz, offset);
  write_submit_cnt++;

  if (is_synchronous || write_submit_cnt >= FLAGS_bm_aio_qd) {
    UringSubmit(write_submit_cnt, &write_ring_);
    write_submit_cnt = 0;
  }
}

void LibaioInterface::IssueReadRequest(u64 offset, u16 read_sz, void *buffer,
                                       bool is_synchronous) {
  if (is_synchronous) {
    int ret = pread(blockfd_, buffer, read_sz, offset);
    if (ret == read_sz) {
      // LOG_INFO("return value is a request size offset: %lu size: %lu ret:
      // %d",
      //  offset, read_sz, ret);
      return;
    } else {
      LOG_INFO(
          "return value is not a request size offset: %lu size: %u ret: %d",
          offset, read_sz, ret);
      Ensure(ret == read_sz);
      return;
    }
  }
  auto sqe = io_uring_get_sqe(&read_ring_);
  if (sqe == nullptr) {
    UringSubmit(read_submit_cnt, &read_ring_);
    sqe = io_uring_get_sqe(&read_ring_);
    Ensure(sqe != nullptr);
    read_submit_cnt = 0;
  }

  io_uring_prep_read(sqe, blockfd_, buffer, read_sz, offset);
  read_submit_cnt++;

  if (is_synchronous) {
    UringSubmit(read_submit_cnt, &read_ring_);
    read_submit_cnt = 0;
  }
}

} // namespace leanstore::storage::space::backend
