#include "storage/space/io_interface.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "lz4.h"
#include "recovery/log_manager.h"
#include "storage/page.h"
#include "storage/space/backend/io_backend.h"
#include "storage/space/doublewrite.h"
#include "storage/space/space_manager.h"
#include "zlib.h"
#include "zstd.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <shared_mutex>
#include <vector>

namespace leanstore::storage::space {

IOInterface::IOInterface(int blockfd, Page *virtual_mem, u64 virtual_cnt,
                         SpaceManager *sm, PID2Offset *PID2Offset_table,
                         StorageSpace *storage_space)
    : blockfd_(blockfd), virtual_mem_(virtual_mem), virtual_cnt_(virtual_cnt),
      sm_(sm), PID2Offset_table_(PID2Offset_table), sspace_(storage_space),
      io_backend_(blockfd, sm, PID2Offset_table, storage_space),
      doublewrite_(blockfd, sm->max_userspace_capacity_) {
  Construction();
}

void IOInterface::Construction() {
  min_comp_size_ = PAGE_SIZE / 4; // expect average compression ratio of 4
  wbuffer_.buffer = nullptr;
  wbuffer_.zpage_info = nullptr;

  // Resize containers based on group count and group count
  if (FLAGS_use_out_of_place_write) {
    group_cnt_ = FLAGS_write_buffer_partition_cnt;
    page_cnt_per_block_ = sm_->page_cnt_per_block_;
    block_cnt_ = sm_->block_cnt_;
    wbuffer_group_.resize(group_cnt_);
    bins.resize(group_cnt_);
    selected_bids_.reserve(group_cnt_);
    prev_gced_bids_.reserve(group_cnt_);

    ResetReadBuffer(0);
    bin_gc_ = std::make_unique<Bins>();

    if (FLAGS_use_ZNS) {
      Ensure(FLAGS_bm_evict_batch_size <= 64);
    }

    for (u32 idx = 0; idx < group_cnt_; idx++) {
      prev_gced_bids_[idx] = block_cnt_;
    }
  }

  if (FLAGS_enable_doublewrite) {
    doublewrite_.SetWriteOffset();
  }
}

void IOInterface::ResetReadBuffer(u32 page_cnt) {
  ResetBuffer(&rbuffer_, page_cnt);
}

void IOInterface::ReadPage(pageid_t pid, bool gcread) {
  if (!gcread) {
    ResetReadBuffer(4);
  }

  if (!FLAGS_use_out_of_place_write) {
    ReadPageInPlace(pid);
  } else {
    if (gcread) {
      std::vector<pageid_t> to_read;
      to_read.push_back(pid);
      ReadPagesOutOfPlace(to_read);
      to_read.clear();
    } else {
      ReadPageOutOfPlace(pid);
    }
  }
}

void IOInterface::ReadPageInPlace(pageid_t pid) {
  // read when using in-place write:
  // offset ==  pid * PAGE_SIZE : for nondeallocated pages
  // offset !=  pid * PAGE_SIZE : reuse deallocated space
  u64 page_offset = PID2Offset_table_[pid].get_comp_sz() == PAGE_SIZE
                        ? PID2Offset_table_[pid].get_offset()
                        : pid * PAGE_SIZE;

  if (pid != 0 && page_offset == 0 &&
      PID2Offset_table_[pid].get_comp_sz() == PAGE_SIZE) {
    throw std::runtime_error("Read deallocated pid, which should never happen");
  }

  void *space_to_read = static_cast<void *>(&(virtual_mem_[pid]));

  io_backend_.ReadPage(page_offset, space_to_read, PAGE_SIZE, true);
}

void IOInterface::ReadPageOutOfPlace(pageid_t pid) {
  // use out-of-place write, refer to Offset2PID mapping table
  // 1. get offset and compressed size from mapping table
  Ensure(PID2Offset_table_[pid].get_comp_sz() > 0 &&
         PID2Offset_table_[pid].get_comp_sz() <= PAGE_SIZE);

  u64 pid_offset = GetPID2Offset(pid);
  u16 comp_sz = GetPIDCompSize(pid);

  Ensure(sm_->PIDOffsetIsValid(pid));

  if (FLAGS_use_compression) {
    if (FLAGS_use_binpacking) {
      HandleCompressedBinpackedRead(pid, pid_offset, comp_sz, true);
    } else {
      HandleCompressedRead(pid, pid_offset, comp_sz, true);
    }
  } else {
    HandleUncompressedRead(pid, pid_offset, comp_sz, true);
  }
  UpdateReadCachedPageInfo(pid, comp_sz);
}

void IOInterface::HandleCompressedRead(pageid_t pid, u64 pid_offset,
                                       u16 comp_sz, bool sync) {
  // Use pre-allocated read buffer
  void *read_buffer = static_cast<void *>(rbuffer_.buffer);
  void *space_to_read = &(virtual_mem_[pid]);
  Ensure(comp_sz <= PAGE_SIZE && comp_sz > 0);

  if (FLAGS_read_size == PAGE_SIZE) {
    // Calculate aligned start and end offsets
    u64 start_ptr = pid_offset % PAGE_SIZE;
    u64 start_offset = pid_offset - start_ptr; // Align to page boundary
    u64 end_offset = pid_offset + comp_sz;

    // Read the first page
    io_backend_.ReadPage(start_offset, read_buffer, PAGE_SIZE, sync);

    // If the data spans into the next page, read the second page
    if (start_ptr + comp_sz >= PAGE_SIZE) {
      u64 second_page_offset = start_offset + PAGE_SIZE;
      io_backend_.ReadPage(second_page_offset,
                           static_cast<char *>(read_buffer + PAGE_SIZE),
                           PAGE_SIZE, sync);
    }
    // Extract and decompress the relevant data
    DecompressPage(static_cast<char *>(read_buffer),
                   static_cast<Page *>(space_to_read), start_ptr, comp_sz, pid);
    return;
  }
  if (FLAGS_read_size < PAGE_SIZE) {
    // read size is FLAGS_read_size bytes
    // for FLAGS_read_size < PAGE_SIZE
    u64 start_ptr =
        pid_offset %
        FLAGS_read_size; // Align start pointer to FLAGS_read_size-byte boundary
    u64 first_read_offset =
        (pid_offset / FLAGS_read_size) *
        FLAGS_read_size; // Align start offset to FLAGS_read_size-byte boundary
    u64 last_read_offset =
        ((pid_offset + comp_sz + FLAGS_read_size - 1) / FLAGS_read_size) *
        FLAGS_read_size;

    u64 total_reads = (last_read_offset - first_read_offset) / FLAGS_read_size;

    // Read in FLAGS_read_size-byte chunks
    for (u64 i = 0; i < total_reads; i++) {
      u64 cur_offset = first_read_offset + (i * FLAGS_read_size);
      io_backend_.ReadPage(
          cur_offset, static_cast<char *>(read_buffer) + (i * FLAGS_read_size),
          FLAGS_read_size, sync);
    }

    // Extract and decompress the relevant data
    DecompressPage(static_cast<char *>(read_buffer),
                   static_cast<Page *>(space_to_read), start_ptr, comp_sz, pid);
    return;
  }
  Ensure(FLAGS_read_size > PAGE_SIZE);
}

void IOInterface::HandleCompressedBinpackedRead(pageid_t pid, u64 pid_offset,
                                                u16 comp_sz, bool sync) {
  u64 start_ptr = pid_offset % PAGE_SIZE; // Offset within the page
  u64 page_start_offset =
      pid_offset - start_ptr; // Aligned to the start of the page

  // Pre-allocate buffer for the read operation
  void *read_buffer = static_cast<void *>(rbuffer_.buffer);
  void *space_to_read = &(virtual_mem_[pid]);

  if (FLAGS_read_size == PAGE_SIZE) {
    // Case when the read size is exactly one page
    io_backend_.ReadPage(page_start_offset, read_buffer, FLAGS_read_size, sync);
  } else {
    // Case when FLAGS_read_size is smaller than PAGE_SIZE
    u64 first_read_offset =
        page_start_offset; // Start reading from the page start
    u64 total_reads = (PAGE_SIZE + FLAGS_read_size - 1) /
                      FLAGS_read_size; // Total number of reads

    // Read the page in chunks of FLAGS_read_size
    for (u64 i = 0; i < total_reads; i++) {
      u64 cur_offset = first_read_offset + (i * FLAGS_read_size);
      u64 cur_size = (i == total_reads - 1)
                         ? (PAGE_SIZE - (i * FLAGS_read_size))
                         : FLAGS_read_size;

      // Read the chunk of data
      io_backend_.ReadPage(
          cur_offset, static_cast<char *>(read_buffer) + (i * FLAGS_read_size),
          cur_size, sync);
    }
  }

  // Decompress the read data into virtual memory
  DecompressPage(static_cast<char *>(read_buffer),
                 static_cast<Page *>(space_to_read), start_ptr, comp_sz, pid);
}

void IOInterface::HandleUncompressedRead(pageid_t pid, u64 pid_offset,
                                         u16 comp_sz, bool sync) {
  // Ensure the size of the data to read is exactly one page (uncompressed)
  Ensure(comp_sz == PAGE_SIZE);

  // If FLAGS_read_size is equal to PAGE_SIZE, read the full page in one go
  if (FLAGS_read_size == PAGE_SIZE) {
    io_backend_.ReadPage(pid_offset,
                         reinterpret_cast<char *>(&virtual_mem_[pid]),
                         PAGE_SIZE, sync);
    return;
  }

  // If FLAGS_read_size is smaller than PAGE_SIZE, read in chunks
  if (FLAGS_read_size < PAGE_SIZE) {
    u64 bytes_read = 0;
    while (bytes_read < PAGE_SIZE) {
      // Calculate how much more to read
      u64 remaining_bytes = PAGE_SIZE - bytes_read;
      u32 current_read_size = (remaining_bytes < FLAGS_read_size)
                                  ? remaining_bytes
                                  : FLAGS_read_size;

      // Perform the read
      io_backend_.ReadPage(pid_offset + bytes_read,
                           reinterpret_cast<char *>(&virtual_mem_[pid]) +
                               bytes_read,
                           current_read_size, sync);

      // Update the bytes read
      bytes_read += current_read_size;
    }
  }
}

void IOInterface::ReadPagesOutOfPlace(std::vector<pageid_t> &to_read) {
  std::vector<u64> r_offsets;
  std::vector<u64> r_start_ptrs;
  std::vector<u16> r_sizes;
  std::vector<void *> r_buffers;
  std::vector<void *> pid_r_buffers;

  u64 align_size = (FLAGS_read_size == 512) ? 512 : PAGE_SIZE;

  // Adjust buffer allocation based on read size
  if (FLAGS_read_size == PAGE_SIZE) {
    ResetBuffer(&rbuffer_gc_, to_read.size() * PAGE_SIZE / MIN_COMP_SIZE);
  } else {
    ResetBuffer(&rbuffer_gc_, to_read.size() * 8);
  }

  // Step 1: Prepare read requests
  for (auto &pid : to_read) {
    Ensure(PID2Offset_table_[pid].get_comp_sz() != 0);

    u64 pid_offset = GetPID2Offset(pid);
    u16 comp_sz = GetPIDCompSize(pid);

    u64 start_ptr =
        pid_offset % align_size; // Offset within first read-aligned block
    u64 r_start_offset = pid_offset - start_ptr; // Align read start
    u64 r_end_offset = pid_offset + comp_sz;     // End of compressed page

    // Allocate buffer for reading
    void *r_buffer =
        FLAGS_use_compression
            ? static_cast<void *>(rbuffer_gc_.buffer + rbuffer_gc_.cur_size)
            : &(virtual_mem_[pid]);

    // Store metadata for decompression
    r_offsets.push_back(r_start_offset);
    r_buffers.push_back(r_buffer);
    pid_r_buffers.push_back(r_buffer);
    r_sizes.push_back(comp_sz);
    r_start_ptrs.push_back(start_ptr);

    rbuffer_gc_.zpage_cnt++;
    rbuffer_gc_.cur_size += align_size;

    // **Handle multiple page reads when using 512-byte alignment**
    if (FLAGS_use_compression && !FLAGS_use_binpacking) {
      if (start_ptr + comp_sz > align_size) {
        // Compute total number of required reads (including first one)
        u64 total_reads =
            (r_end_offset - r_start_offset + align_size - 1) / align_size;

        // Perform additional reads beyond the first one
        for (u64 i = 1; i < total_reads;
             i++) { // Start at 1 (first read already counted)
          u64 extra_read_offset = r_start_offset + (i * align_size);
          void *extra_buffer =
              static_cast<void *>(rbuffer_gc_.buffer + rbuffer_gc_.cur_size);

          r_offsets.push_back(extra_read_offset);
          r_buffers.push_back(extra_buffer);
          rbuffer_gc_.cur_size += align_size;
        }
      }
    }
  }

  // Step 2: Perform batched read operation
  io_backend_.ReadPagesOutOfPlace(to_read, r_offsets, r_buffers, r_sizes,
                                  false);

  // Step 3: Perform decompression after all reads are done
  if (FLAGS_use_compression) {
    for (u32 cnt = 0; cnt < to_read.size(); ++cnt) {
      void *space_to_read = &(virtual_mem_[to_read[cnt]]);
      DecompressPage(static_cast<char *>(pid_r_buffers[cnt]),
                     static_cast<Page *>(space_to_read), r_start_ptrs[cnt],
                     r_sizes[cnt], to_read[cnt]);
    }
  }

  ResetBuffer(&rbuffer_gc_, 0);
}

// used when buffer manager assumes the pid has been evicted but it is actually
// stored in write buffer
bool IOInterface::ReadPageFromWBuffer(pageid_t pid) {
  u64 start_ptr;
  u16 comp_sz = PAGE_SIZE;

  // Check main write buffer
  {
    std::unique_lock<std::shared_mutex> lock(wbuffer_.mtx);
    for (u32 idx = 0; idx < wbuffer_.zpage_cnt; idx++) {
      if (wbuffer_.zpage_info[idx].pid == pid) {
        start_ptr = wbuffer_.zpage_info[idx].start_ptr;
        comp_sz = wbuffer_.zpage_info[idx].comp_sz;

        if (FLAGS_use_compression) {
          // Decompress a page to virtual mem
          DecompressPage(wbuffer_.buffer, &(virtual_mem_[pid]), start_ptr,
                         comp_sz, pid);
        } else {
          // Copy a page to virtual mem
          memcpy(&(virtual_mem_[pid]), wbuffer_.buffer + start_ptr, comp_sz);
        }
        return true;
      }
    }
  }

  // Check garbage collection write buffer
  {
    std::unique_lock<std::shared_mutex> lock(wbuffer_gc_.mtx);
    for (u32 idx = 0; idx < wbuffer_gc_.zpage_cnt; idx++) {
      if (wbuffer_gc_.zpage_info[idx].pid == pid) {
        start_ptr = wbuffer_gc_.zpage_info[idx].start_ptr;
        comp_sz = wbuffer_gc_.zpage_info[idx].comp_sz;

        if (FLAGS_use_compression) {
          // Decompress a page to virtual mem
          DecompressPage(wbuffer_gc_.buffer, &(virtual_mem_[pid]), start_ptr,
                         comp_sz, pid);
        } else {
          // Copy a page to virtual mem
          memcpy(&(virtual_mem_[pid]), wbuffer_gc_.buffer + start_ptr, comp_sz);
        }
        return true;
      }
    }
  }
  return false;
}

void IOInterface::DecompressPage(char *rbuffer, storage::Page *page,
                                 u64 start_ptr, u16 comp_sz, u64 pid) {
  u16 decomp_sz = 0;

  comp_sz = PID2Offset_table_[pid].get_comp_sz();
  Ensure(comp_sz < PAGE_SIZE && comp_sz > 0);

  switch (FLAGS_compression_algorithm) {
  case 0: // LZ4
  {
    int temp_decomp_sz = LZ4_decompress_safe_partial(
        rbuffer + start_ptr, reinterpret_cast<char *>(page), comp_sz, PAGE_SIZE,
        PAGE_SIZE);

    if (temp_decomp_sz != PAGE_SIZE) {
      LOG_ERROR("LZ4 decompression failed: Expected: %d, Got: %d", PAGE_SIZE,
                temp_decomp_sz);
      LOG_INFO(
          "wid: %d pid: %lu decompress size: %d compressed size: %d offset: "
          "%lu start_ptr: %lu",
          worker_thread_id, pid, temp_decomp_sz, comp_sz,
          PID2Offset_table_[pid].get_offset(), start_ptr);
      sm_->ConvertOffset2PIDs(PID2Offset_table_[pid].get_offset());
      Ensure(false); // Or handle error gracefully
    } else {
      decomp_sz = static_cast<u16>(temp_decomp_sz);
    }
  } break;

  case 1: // Zstd
  {
    decomp_sz = ZSTD_decompress(reinterpret_cast<char *>(page), PAGE_SIZE,
                                rbuffer + start_ptr, comp_sz);
    if (ZSTD_isError(decomp_sz)) {
      LOG_ERROR("Zstd decompression failed: %s", ZSTD_getErrorName(decomp_sz));
      Ensure(false);
    }
    if (decomp_sz != PAGE_SIZE) {
      LOG_ERROR("Zstd decompression failed: Decompressed size mismatch. "
                "Expected: %d, Got: %lu",
                PAGE_SIZE, decomp_sz);
      Ensure(false); // Or handle error gracefully
    }
  } break;
  case 2: // Zlib
  {
    uLongf dest_len = PAGE_SIZE;
    int ret = uncompress(reinterpret_cast<Bytef *>(page), &dest_len,
                         reinterpret_cast<const Bytef *>(rbuffer + start_ptr),
                         comp_sz);
    decomp_sz = dest_len;
    if (ret != Z_OK) {
      LOG_ERROR("Zlib decompression failed with error code: %d pid: %lu", ret,
                pid);
      Ensure(false);
    }
    if (decomp_sz != PAGE_SIZE) {
      LOG_ERROR("Zlib decompression failed: Decompressed size mismatch. "
                "Expected: %d, Got: %lu",
                PAGE_SIZE, decomp_sz);
      Ensure(false); // Or handle error gracefully
    }
  } break;

  default:
    LOG_ERROR("Unknown compression algorithm");
    Ensure(false);
    break;
  }

  Ensure(decomp_sz == PAGE_SIZE);
}

u16 IOInterface::CompressPage(Page *page, char *wbuffer, u64 start_ptr,
                              u64 pid) {
  u16 comp_sz = 0;

  switch (FLAGS_compression_algorithm) {
  case 0: // LZ4
  {
    int temp_comp_sz = LZ4_compress_default(
        reinterpret_cast<char *>(page),
        reinterpret_cast<char *>(wbuffer + start_ptr), PAGE_SIZE, PAGE_SIZE);
    if (temp_comp_sz >= PAGE_SIZE) {
      LOG_ERROR("LZ4 compression failed: Compressed size mismatch. Expected: "
                "%d, Got: %u",
                PAGE_SIZE, comp_sz);
      //   Ensure(false);  // Or handle error gracefully
    } else {
      comp_sz = static_cast<u16>(temp_comp_sz);
    }
  } break;

  case 1: // Zstd
    comp_sz = ZSTD_compress(wbuffer + start_ptr, PAGE_SIZE,
                            reinterpret_cast<char *>(page), PAGE_SIZE,
                            1); // Level 1 for speed
    if (ZSTD_isError(comp_sz)) {
      LOG_ERROR("Zstd compression failed: %s", ZSTD_getErrorName(comp_sz));
      Ensure(false);
    }
    if (comp_sz < PAGE_SIZE) {
      // Compression was successful, but compressed data size is smaller than
    } else {
      // Compression did not reduce the size as expected (maybe uncompressible
      // data)
      LOG_INFO("Zstd compression did not reduce size, comp_sz: %llu", comp_sz);
    }
    break;
  case 2: // Zlib
  {
    uLongf dest_len = PAGE_SIZE;
    int ret = compress2(reinterpret_cast<Bytef *>(wbuffer + start_ptr),
                        &dest_len, reinterpret_cast<const Bytef *>(page),
                        PAGE_SIZE, Z_BEST_SPEED);
    comp_sz = dest_len;
    if (ret != Z_OK) {
      LOG_ERROR("Zlib compression failed with error code: %d", ret);
      Ensure(false);
    }
  } break;

  default:
    LOG_ERROR("Unknown compression algorithm");
    Ensure(false);
    break;
  }

  Ensure(comp_sz <= PAGE_SIZE);
  return comp_sz;
}

bool IOInterface::PageInUserWBuffer(pageid_t pid) {
  {
    std::shared_lock<std::shared_mutex> lock(wbuffer_.mtx);
    // Early exit if there are no compressed pages
    if (wbuffer_.zpage_cnt == 0) {
      return false;
    }
    for (u32 idx = 0; idx < wbuffer_.zpage_cnt; idx++) {
      if (wbuffer_.zpage_info[idx].pid == pid &&
          wbuffer_.zpage_info[idx].comp_sz > 0) {
        return true;
      }
    }
  }

  return false;
}

bool IOInterface::PageInUserWBufferPerGroup(pageid_t pid, u32 gidx) {
  {
    std::shared_lock<std::shared_mutex> lock(wbuffer_group_[gidx].mtx);
    // Early exit if there are no compressed pages
    if (wbuffer_group_[gidx].zpage_cnt == 0) {
      return false;
    }
    for (u32 idx = 0; idx < wbuffer_group_[idx].zpage_cnt; idx++) {
      if (wbuffer_group_[gidx].zpage_info[idx].pid == pid &&
          wbuffer_group_[gidx].zpage_info[idx].comp_sz > 0) {
        return true;
      }
    }
  }

  return false;
}

bool IOInterface::PageInGCWBuffer(pageid_t pid) {
  {
    std::unique_lock<std::shared_mutex> lock(wbuffer_gc_.mtx);
    // Early exit if there are no compressed pages
    if (wbuffer_gc_.zpage_cnt == 0) {
      return false;
    }
    for (u32 idx = 0; idx < wbuffer_gc_.zpage_cnt; idx++) {
      if (wbuffer_gc_.zpage_info[idx].pid == pid &&
          wbuffer_gc_.zpage_info[idx].comp_sz > 0) {
        return true;
      }
    }
  }

  return false;
}

bool IOInterface::PageInWBuffer(pageid_t pid) {
  {
    std::shared_lock<std::shared_mutex> lock(wbuffer_.mtx);
    // Early exit if there are no compressed pages
    if (wbuffer_.zpage_cnt == 0) {
      return false;
    }
    for (u32 idx = 0; idx < wbuffer_.zpage_cnt; idx++) {
      if (wbuffer_.zpage_info[idx].pid == pid &&
          wbuffer_.zpage_info[idx].comp_sz > 0) {
        return true;
      }
    }
  }
  {
    std::unique_lock<std::shared_mutex> lock(wbuffer_gc_.mtx);
    // Early exit if there are no compressed pages
    if (wbuffer_gc_.zpage_cnt == 0) {
      return false;
    }
    for (u32 idx = 0; idx < wbuffer_gc_.zpage_cnt; idx++) {
      if (wbuffer_gc_.zpage_info[idx].pid == pid &&
          wbuffer_gc_.zpage_info[idx].comp_sz > 0) {
        return true;
      }
    }
  }

  return false;
}

bool IOInterface::PageInReadBuffer(pageid_t pid) {
  {
    // Early exit if there are no compressed pages
    std::unique_lock<std::shared_mutex> lock(rbuffer_.mtx);
    if (rbuffer_.zpage_cnt == 0) {
      return false;
    }
    u32 read_cnt = FLAGS_use_compression ? PAGE_SIZE / MIN_COMP_SIZE : 1;
    for (u32 idx = 0; idx < read_cnt; idx++) {
      if (rbuffer_.zpage_info[idx].pid == pid &&
          rbuffer_.zpage_info[idx].comp_sz != 0) {
        return true;
      }
    }
  }

  return false;
}

void IOInterface::UpdateCachedPageInfo(ZipPage *wbuffer, pageid_t pid,
                                       u16 comp_sz, logid_t edt) {
  /*add zpage info */
  wbuffer->zpage_info[wbuffer->zpage_cnt].pid = pid;
  wbuffer->zpage_info[wbuffer->zpage_cnt].comp_sz = comp_sz;
  wbuffer->zpage_info[wbuffer->zpage_cnt].death_time = edt;
  wbuffer->zpage_info[wbuffer->zpage_cnt].start_ptr = wbuffer->cur_size;
  wbuffer->cur_size += comp_sz;
  wbuffer->zpage_cnt = wbuffer->zpage_cnt + 1;

  // compress stats update
  statistics::storage::space::total_size_after_compression += comp_sz;
}

void IOInterface::UpdateReadCachedPageInfo(pageid_t pid, u16 comp_sz) {
  /*add zpage info */
  // for (u32 i = 0; i < PAGE_SIZE / MIN_COMP_SIZE; i++) {
  if (rbuffer_.zpage_info[rbuffer_.zpage_cnt].pid == 0 &&
      rbuffer_.zpage_info[rbuffer_.zpage_cnt].comp_sz == 0) {
    rbuffer_.zpage_info[rbuffer_.zpage_cnt].pid = pid;
    rbuffer_.zpage_info[rbuffer_.zpage_cnt].comp_sz = comp_sz;
    rbuffer_.zpage_cnt++;
    return;
  }
  // }
}

void IOInterface::ResetUserWriteBuffer(u32 page_cnt) {
  ResetBuffer(&wbuffer_, page_cnt);
  u32 page_cnt_per_group = (page_cnt);
  u32 wbuffer_size_per_group = page_cnt_per_group * PAGE_SIZE;

  if (FLAGS_use_out_of_place_write) {
    ResetWBufferPerGroup(wbuffer_size_per_group, page_cnt);
    for (u32 idx = 0; idx < group_cnt_; idx++) {
      selected_bids_.clear();
    }
  }
}

void IOInterface::ResetGCWriteBuffer(u64 wbuffer_size, u32 cached_cnt) {
  ResetBuffer(&wbuffer_gc_, cached_cnt);
  ResetBuffer(&binpacked_gc_, cached_cnt);

  if (bin_gc_.get()->sum) {
    delete[] bin_gc_.get()->sum;
    delete[] bin_gc_.get()->count;
    delete[] bin_gc_.get()->start_ptr;
  }
  bin_gc_.get()->sum = new u32[cached_cnt];
  bin_gc_.get()->count = new u32[cached_cnt];
  bin_gc_.get()->start_ptr = new u64[cached_cnt];

  for (u32 idx = 0; idx < cached_cnt; idx++) {
    bin_gc_.get()->sum[idx] = 0;
    bin_gc_.get()->count[idx] = 0;
    bin_gc_.get()->start_ptr[idx] = 0;
  }
  bin_gc_.get()->bincount = 0;
}

void IOInterface::ResetWBufferPerGroup(u64 wbuffer_size, u32 page_cnt) {
  for (u32 gidx = 0; gidx < group_cnt_; gidx++) {
    ResetBuffer(&(wbuffer_group_[gidx]), page_cnt);

    // Delete old memory only if it exists
    if (bins[gidx].sum) {
      delete[] bins[gidx].sum;
    }
    if (bins[gidx].count) {
      delete[] bins[gidx].count;
    }
    if (bins[gidx].start_ptr) {
      delete[] bins[gidx].start_ptr;
    }

    // Allocate new memory
    bins[gidx].sum = new u32[page_cnt];
    bins[gidx].count = new u32[page_cnt];
    bins[gidx].start_ptr = new u64[page_cnt];

    for (u32 idx = 0; idx < page_cnt; idx++) {
      bins[gidx].sum[idx] = 0;
      bins[gidx].count[idx] = 0;
      bins[gidx].start_ptr[idx] = 0;
    }
    bins[gidx].bincount = 0;
  }
}

void IOInterface::ResetBuffer(ZipPage *zpage, u32 page_cnt) {
  Ensure(zpage != nullptr);
  {
    std::unique_lock<std::shared_mutex> lock(zpage->mtx);

    // Compute required buffer size
    u64 buffer_size = page_cnt * PAGE_SIZE;
    Ensure(buffer_size % PAGE_SIZE == 0);

    // Reset ZipInfo array
    if (zpage->zpage_info) {
      delete[] zpage->zpage_info;
      zpage->zpage_info = nullptr;
    }

    if (page_cnt > 0) {
      zpage->zpage_info = new ZipInfo[page_cnt](); // Zero-initialize
    } else {
      zpage->zpage_info = nullptr;
    }

    zpage->zpage_cnt = 0;
    zpage->cur_size = 0;
    zpage->avg_edt = 0;

    // Free existing buffer
    if (zpage->buffer) {
      free(zpage->buffer);
      zpage->buffer = nullptr;
    }

    // Allocate new buffer only if needed
    if (buffer_size > 0) {
      zpage->buffer =
          static_cast<char *>(aligned_alloc(PAGE_SIZE, buffer_size));
      Ensure(zpage->buffer != nullptr); // Check for allocation failure
      Ensure(IsAligned(PAGE_SIZE, zpage->buffer, buffer_size));
    } else {
      zpage->buffer = nullptr;
    }
  }
}

u16 IOInterface::AddPageToWriteBuffer(storage::Page *page, pageid_t pid,
                                      logid_t edt) {
  if (!FLAGS_use_out_of_place_write && FLAGS_enable_doublewrite) {
    doublewrite_.SetWriteOffset();
    doublewrite_.AdvanceWriteOffset();
    io_backend_.WritePageSynchronously(pid, doublewrite_.dwb_cur_w_offset_,
                                       reinterpret_cast<void *>(page), 999);
  }

  std::unique_lock<std::shared_mutex> lock(wbuffer_.mtx);

  u16 comp_sz = PAGE_SIZE;
  u64 cur_write_size = wbuffer_.cur_size;

  if (FLAGS_use_compression) {
    comp_sz = CompressPage(page, wbuffer_.buffer, cur_write_size, pid);
    Ensure(comp_sz < PAGE_SIZE);
  } else {
    memcpy(reinterpret_cast<void *>(wbuffer_.buffer + cur_write_size),
           reinterpret_cast<void *>(page), PAGE_SIZE);
  }
  Ensure(comp_sz > 0);

  UpdateCachedPageInfo(&wbuffer_, pid, comp_sz, edt);
  return comp_sz;
}

u16 IOInterface::AddPageToGCWriteBuffer(storage::Page *page, pageid_t pid,
                                        logid_t edt) {

  // Ensure(!PageInGCWBuffer(pid) && !PageInUserWBuffer(pid));
  std::unique_lock<std::shared_mutex> lock(wbuffer_gc_.mtx);

  u16 comp_sz = PAGE_SIZE;
  u64 cur_write_size = wbuffer_gc_.cur_size;

  if (FLAGS_use_compression) {
    /* compress page using configured compression algorithm */
    comp_sz = CompressPage(page, wbuffer_gc_.buffer, cur_write_size, pid);
    Ensure(comp_sz < PAGE_SIZE);
  } else {
    memcpy(reinterpret_cast<void *>(wbuffer_gc_.buffer + cur_write_size),
           reinterpret_cast<void *>(page), PAGE_SIZE);
  }
  Ensure(comp_sz > 0);

  UpdateCachedPageInfo(&wbuffer_gc_, pid, comp_sz, edt);

  return comp_sz;
}

void IOInterface::FlushWriteBuffer() {
  std::vector<pageid_t> w_pids;
  std::vector<u64> w_offsets;
  std::vector<void *> w_buffers;
  std::vector<indexid_t> w_idxs;

  for (u32 idx = 0; idx < wbuffer_.zpage_cnt; idx++) {
    pageid_t pid = wbuffer_.zpage_info[idx].pid;
    indexid_t w_idx = virtual_mem_[pid].idx_id;
    // PAGE_SIZE means pid is resused
    u64 p_offset = PID2Offset_table_[pid].get_comp_sz() == PAGE_SIZE
                       ? PID2Offset_table_[pid].get_offset()
                       : pid * PAGE_SIZE;
    // if(PID2Offset_table_[pid].get_comp_sz() == PAGE_SIZE){
    // //  LOG_INFO("reuse space");
    // }
    w_pids.push_back(pid);
    w_offsets.push_back(p_offset);
    w_buffers.push_back(&virtual_mem_[pid]);
    w_idxs.push_back(w_idx);
  }
  if (!w_pids.empty()) {
    io_backend_.WritePagesInPlace(w_pids, w_offsets, w_buffers, w_idxs, false);
  }
  w_pids.clear();
  w_offsets.clear();
  w_buffers.clear();
  w_idxs.clear();

  return;
}

u64 IOInterface::GetWbufferPIDCntPerGroup(u32 idx) {
  return wbuffer_group_[idx].zpage_cnt;
}

u64 IOInterface::GetWbufferSizePerGroup(u32 idx) {
  return wbuffer_group_[idx].cur_size;
}

pageid_t IOInterface::GetWbufferPIDPerGroup(u32 idx, u32 pidx) {
  return wbuffer_group_[idx].zpage_info[pidx].pid;
}

void IOInterface::FlushWriteBufferPerGroup(u32 group_idx, blockid_t cur_bid,
                                           logid_t min_edt, logid_t max_edt) {
  blockid_t bid = block_cnt_;
  if (cur_bid == block_cnt_) {
    // select open block after GC
    while (bid >= sspace_->block_cnt_) {
      bid = sm_->SelectOpenBlockID(
          wbuffer_group_[group_idx].cur_size, wbuffer_group_[group_idx].avg_edt,
          recovery::LogManager::global_min_gsn_flushed.load(),
          wbuffer_group_[group_idx].zpage_cnt, group_idx, min_edt, max_edt);
    }
  } else {
    bid = cur_bid;
  }
  Ensure(sspace_->block_metadata[bid].state == State::IN_USE);

  ZipPage *wbuffer = &wbuffer_group_[group_idx]; // Get the corresponding
                                                 // wbuffer for this group
  if (FLAGS_use_edt) {
    logid_t minedt =
        wbuffer->zpage_cnt > 1
            ? wbuffer->zpage_info[wbuffer->zpage_cnt - 1].death_time
            : wbuffer->zpage_info[0].death_time;
    if (minedt < recovery::LogManager::global_min_gsn_flushed.load())
      minedt = recovery::LogManager::global_min_gsn_flushed.load();
    logid_t maxedt = wbuffer->zpage_info[0].death_time;
    sm_->SetMinMaxEDTPerBlock(bid, minedt, maxedt);
  }
  if (FLAGS_batch_writes) {
    io_backend_.WritePagesOutOfPlace(
        sm_->GetBlockWriteOffset(bid) + sm_->GetBlockOffset(bid),
        GetWbufferWriteSize(wbuffer), reinterpret_cast<void *>(wbuffer->buffer),
        false, false);
  } else {
    for (u32 idx = 0; idx < wbuffer->cur_size / PAGE_SIZE; idx++) {
      io_backend_.WritePagesOutOfPlace(
          sm_->GetBlockWriteOffset(bid) + sm_->GetBlockOffset(bid) +
              idx * PAGE_SIZE,
          PAGE_SIZE,
          reinterpret_cast<void *>(wbuffer->buffer + idx * PAGE_SIZE), false,
          false);
    }
  }
  UpdateSpaceMetadata(wbuffer, bid);
}

bool IOInterface::BlockNeedsGC(blockid_t bid) {
  if (sspace_->block_metadata[bid].state == State::GC_IN_USE) {
    return true;
  }
  return false;
}

logid_t IOInterface::GetMinEDTFromGCWBuffer() {
  ZipPage *wbuffer = &binpacked_gc_;

  logid_t minedt = wbuffer->zpage_cnt > 1
                       ? wbuffer->zpage_info[wbuffer->zpage_cnt - 1].death_time
                       : wbuffer->zpage_info[0].death_time;
  return minedt;
}

void IOInterface::FlushGCWriteBuffer(blockid_t bid, bool sync, logid_t min_edt,
                                     logid_t max_edt, bool find_other_block) {
  if (sspace_->block_metadata[bid].cur_size == sspace_->max_w_ptr_) {
    sm_->EraseBlock(bid, recovery::LogManager::global_min_gsn_flushed.load(),
                    true);
    Ensure(sspace_->block_metadata[bid].cur_size == 0);
  }

  if (wbuffer_gc_.zpage_cnt > 0) {
    logid_t avg_edt = 0;

    if (FLAGS_use_binpacking) {
      avg_edt = BinPackingPerBlock(&wbuffer_gc_, &binpacked_gc_, bin_gc_.get(),
                                   0, wbuffer_gc_.zpage_cnt - 1);
    } else {
      ;
      avg_edt = CopyCompressedPages(&wbuffer_gc_, &binpacked_gc_, 0,
                                    wbuffer_gc_.zpage_cnt - 1);
    }

    binpacked_gc_.avg_edt = avg_edt;
    binpacked_gc_.zpage_cnt = wbuffer_gc_.zpage_cnt;

    ZipPage *wbuffer = &binpacked_gc_;

    Ensure(sspace_->block_metadata[bid].state == State::GC_WRITE);
    // sm_->AdjustEDTRange(min_edt, max_edt, avg_edt, bid);
    if (FLAGS_use_edt) {
      logid_t minedt =
          wbuffer->zpage_cnt > 1
              ? wbuffer->zpage_info[wbuffer->zpage_cnt - 1].death_time
              : wbuffer->zpage_info[0].death_time;
      // if(minedt < recovery::LogManager::global_min_gsn_flushed.load()) minedt
      // = recovery::LogManager::global_min_gsn_flushed.load();
      logid_t maxedt = wbuffer->zpage_info[0].death_time;
      if (find_other_block) {
        blockid_t candidate = sm_->SelectOpenBlockID(
            GetWbufferWriteSize(wbuffer), avg_edt,
            recovery::LogManager::global_min_gsn_flushed.load(),
            binpacked_gc_.zpage_cnt, FLAGS_write_buffer_partition_cnt, min_edt,
            max_edt);
        if (candidate != sm_->block_cnt_) {
          bid = candidate;
        }
      }
      // LOG_INFO("gcwrite bid: %lu min_edt: %lu max_edt: %lu curts: %lu avg_edt
      // :%lu", bid, minedt, maxedt,
      // recovery::LogManager::global_min_gsn_flushed.load(), wbuffer->avg_edt);
      sm_->SetMinMaxEDTPerBlock(bid, minedt, maxedt);
    }

    if (FLAGS_batch_writes) {
      io_backend_.WritePagesOutOfPlace(
          sm_->GetBlockWriteOffset(bid) + sm_->GetBlockOffset(bid),
          GetWbufferWriteSize(wbuffer),
          reinterpret_cast<void *>(wbuffer->buffer), true, true);
    } else {
      // if((!FLAGS_use_binpacking && FLAGS_use_compression)){
      for (u32 idx = 0; idx < wbuffer->cur_size / PAGE_SIZE; idx++) {
        io_backend_.WritePagesOutOfPlace(
            sm_->GetBlockWriteOffset(bid) + sm_->GetBlockOffset(bid) +
                idx * PAGE_SIZE,
            PAGE_SIZE,
            reinterpret_cast<void *>(wbuffer->buffer + idx * PAGE_SIZE), sync,
            true);
      }
    }
  }
  /* update data placement info */
  UpdateSpaceMetadata(&binpacked_gc_, bid);
  ResetGCWriteBuffer(0, 0);
}

blockid_t IOInterface::SelectBlockIDToWrite(u32 idx, logid_t min_edt,
                                            logid_t max_edt) {
  // select blocks to place the data
  blockid_t bid = sm_->block_cnt_;
  u64 max_write_size = wbuffer_group_[idx].cur_size;
  u64 total_cnt = wbuffer_group_[idx].zpage_cnt;
  logid_t avg_edt = wbuffer_group_[idx].avg_edt;
  logid_t cur_ts = recovery::LogManager::global_min_gsn_flushed.load();
  if (!total_cnt)
    return bid;
  bid = sm_->SelectBlockIDToWrite(max_write_size, cur_ts, avg_edt, total_cnt,
                                  idx, min_edt, max_edt);
  selected_bids_.emplace_back(bid);
  return bid;
}

std::vector<logid_t> IOInterface::ExtractUniqueEDTs() {
  std::vector<logid_t> unique_edts;
  unique_edts.reserve(wbuffer_.zpage_cnt);
  for (u32 i = 0; i < wbuffer_.zpage_cnt; ++i) {
    if (unique_edts.empty() ||
        unique_edts.back() != wbuffer_.zpage_info[i].death_time) {
      unique_edts.push_back(wbuffer_.zpage_info[i].death_time);
    }
  }
  return unique_edts;
}

u64 IOInterface::BinPackGroups(
    const std::vector<std::pair<u32, u32>> &group_ranges) {
  u64 total_cnt = 0;
  u32 group_idx = 0;
  for (auto &[start_idx, end_idx] : group_ranges) {
    logid_t avg_edt = 0;
    if (FLAGS_use_binpacking) {
      avg_edt = BinPackingPerBlock(&wbuffer_, &(wbuffer_group_[group_idx]),
                                   &(bins[group_idx]), start_idx, end_idx);
      total_cnt += wbuffer_group_[group_idx].zpage_cnt;
    } else {
      avg_edt = CopyCompressedPages(&wbuffer_, &(wbuffer_group_[group_idx]),
                                    start_idx, end_idx);
    }

    wbuffer_group_[group_idx].avg_edt = avg_edt;
    ++group_idx;
  }
  return total_cnt;
}

u64 IOInterface::UniformBinPacking() {
  u64 total_cnt = 0;
  // std::shuffle(wbuffer_.zpage_info, wbuffer_.zpage_info + wbuffer_.zpage_cnt,
  //              std::default_random_engine{std::random_device{}()});

  for (u32 idx = 0; idx < group_cnt_; idx++) {
    u32 start_idx = (wbuffer_.zpage_cnt / group_cnt_) * idx;
    u32 end_idx = (idx != (group_cnt_ - 1))
                      ? (wbuffer_.zpage_cnt / group_cnt_) * (idx + 1) - 1
                      : wbuffer_.zpage_cnt - 1;
    logid_t avg_edt = 0;

    if (FLAGS_use_binpacking) {
      avg_edt = BinPackingPerBlock(&wbuffer_, &(wbuffer_group_[idx]),
                                   &(bins[idx]), start_idx, end_idx);
      total_cnt += wbuffer_group_[idx].zpage_cnt;
    } else {
      avg_edt = CopyCompressedPages(&wbuffer_, &(wbuffer_group_[idx]),
                                    start_idx, end_idx);
    }

    wbuffer_group_[idx].avg_edt = avg_edt;
  }

  return total_cnt;
}

std::vector<std::pair<u32, u32>>
IOInterface::GroupPagesByEDT(const std::vector<logid_t> &unique_edts,
                             logid_t min_edt, logid_t max_edt) {

  std::vector<std::pair<u32, u32>> group_ranges;
  u32 actual_group_cnt = std::min<u32>(sspace_->max_open_block_, group_cnt_);
  if (actual_group_cnt == 0)
    actual_group_cnt = 1;

  logid_t edt_range = max_edt - min_edt + 1;

  // Compute thresholds with bucket widths proportional to (i + 1)
  std::vector<logid_t> thresholds;
  thresholds.push_back(min_edt);

  // Total weight: 1 + 2 + ... + N = N * (N + 1) / 2
  u64 total_weight =
      static_cast<u64>(actual_group_cnt) * (actual_group_cnt + 1) / 2;

  logid_t current = min_edt;
  logid_t assigned = 0;

  for (u32 i = 0; i < actual_group_cnt - 1; ++i) {
    u64 weight = static_cast<u64>(i + 1);
    logid_t width = static_cast<logid_t>((weight * edt_range) / total_weight);

    if (width == 0)
      width = 1;
    assigned += width;
    current = std::min(min_edt + assigned, max_edt);
    thresholds.push_back(current);
  }

  thresholds.push_back(max_edt + 1); // exclusive upper bound for last bucket

  std::vector<std::pair<u32, u32>> group_bounds(actual_group_cnt,
                                                {UINT32_MAX, 0});

  for (u32 i = 0; i < wbuffer_.zpage_cnt; ++i) {
    logid_t edt = wbuffer_.zpage_info[i].death_time;

    // Find group_id by checking bucket thresholds
    u32 group_id = actual_group_cnt - 1; // default to last group
    for (u32 g = 0; g < actual_group_cnt; ++g) {
      if (edt < thresholds[g + 1]) {
        group_id = g;
        break;
      }
    }

    if (group_bounds[group_id].first == UINT32_MAX) {
      group_bounds[group_id].first = i;
    }
    group_bounds[group_id].second = i;
  }

  for (u32 group_idx = 0; group_idx < actual_group_cnt; ++group_idx) {
    auto [start_idx, end_idx] = group_bounds[group_idx];
    if (start_idx == UINT32_MAX)
      continue; // skip empty group

    group_ranges.emplace_back(start_idx, end_idx);

    logid_t group_max_edt = wbuffer_.zpage_info[start_idx].death_time;
    logid_t group_min_edt = wbuffer_.zpage_info[end_idx].death_time;
  }

  return group_ranges;
}

void IOInterface::SortPagesByEDT() {
  std::sort(wbuffer_.zpage_info, wbuffer_.zpage_info + wbuffer_.zpage_cnt,
            [](const ZipInfo &a, const ZipInfo &b) {
              return a.death_time > b.death_time;
            });
}

void IOInterface::BinPacking(logid_t cur_ts, logid_t min_edt, logid_t max_edt) {
  u64 total_cnt = 0;
  if (wbuffer_.zpage_cnt == 0)
    return;

  if (FLAGS_use_edt) {
    SortPagesByEDT();
    std::vector<logid_t> unique_edts = ExtractUniqueEDTs();
    if (unique_edts.size() == 1) {
      total_cnt = UniformBinPacking();
    } else {
      std::vector<std::pair<u32, u32>> group_ranges =
          GroupPagesByEDT(unique_edts, min_edt, max_edt);
      total_cnt = BinPackGroups(group_ranges);
    }

  } else {
    total_cnt = UniformBinPacking();
  }

  Ensure(total_cnt == wbuffer_.zpage_cnt);
}

logid_t IOInterface::CopyCompressedPages(ZipPage *src_wbuffer,
                                         ZipPage *dest_wbuffer, u64 start_idx,
                                         u64 end_idx) {
  Ensure(src_wbuffer->zpage_cnt > 0);
  logid_t avg_edt = 0;

  // Initialize destination buffer pointer
  char *wbuffer_ptr = dest_wbuffer->buffer;
  u64 copied_pages_cnt = 0;

  // Keep track of compressed page count per PAGE_SIZE
  u64 pages_per_page_size = 0;

  for (u64 idx = start_idx; idx <= end_idx; idx++) {
    u16 cur_size = src_wbuffer->zpage_info[idx].comp_sz;

    // Check if the current PAGE_SIZE has reached its limit
    if (pages_per_page_size >= PAGE_SIZE / MIN_COMP_SIZE) {
      // Align the wbuffer_ptr to the next PAGE_SIZE offset
      u64 offset = wbuffer_ptr - dest_wbuffer->buffer;
      u64 next_page_offset = ((offset + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
      wbuffer_ptr = dest_wbuffer->buffer + next_page_offset;

      // Reset pages per PAGE_SIZE counter
      pages_per_page_size = 0;
    }

    // Copy the compressed page directly
    dest_wbuffer->zpage_info[copied_pages_cnt] = src_wbuffer->zpage_info[idx];
    dest_wbuffer->zpage_info[copied_pages_cnt].start_ptr =
        wbuffer_ptr - dest_wbuffer->buffer;

    std::memcpy(wbuffer_ptr,
                src_wbuffer->buffer + src_wbuffer->zpage_info[idx].start_ptr,
                cur_size);

    // Update destination buffer pointer and metadata
    wbuffer_ptr += cur_size;
    dest_wbuffer->zpage_cnt++;
    dest_wbuffer->cur_size = wbuffer_ptr - dest_wbuffer->buffer;
    avg_edt += src_wbuffer->zpage_info[idx].death_time;
    copied_pages_cnt++;
    pages_per_page_size++;
  }

  Ensure(copied_pages_cnt == end_idx - start_idx + 1);

  // Align the final write size to PAGE_SIZE
  u64 aligned_write_size =
      (GetWbufferWriteSize(dest_wbuffer) + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
  dest_wbuffer->cur_size = aligned_write_size + PAGE_SIZE;

  return (copied_pages_cnt > 0) ? avg_edt / copied_pages_cnt : 0;
}

logid_t IOInterface::BinPackingPerBlock(ZipPage *src_wbuffer,
                                        ZipPage *dest_wbuffer, Bins *bins,
                                        u64 start_idx, u64 end_idx) {
  logid_t avg_edt = 0;

  std::sort(
      src_wbuffer->zpage_info + start_idx,
      src_wbuffer->zpage_info + end_idx + 1,
      [](const ZipInfo &a, const ZipInfo &b) { return a.comp_sz > b.comp_sz; });

  char *wbuffer_ptr = dest_wbuffer->buffer;
  u32 binpacked_pages_cnt = 0;
  u32 physical_pages_cnt = 0;

  std::multimap<u32, int> freespace_to_bin;

  // Constants
  const u32 max_pages_per_bin = PAGE_SIZE / MIN_COMP_SIZE;

  // Initialize multimap with available bins
  for (int b = 0; b < bins->bincount; ++b) {
    if (bins->count[b] < max_pages_per_bin) {
      u32 remaining = PAGE_SIZE - bins->sum[b];
      freespace_to_bin.insert({remaining, b});
    }
  }

  for (u64 idx = start_idx; idx <= end_idx; idx++) {
    const ZipInfo &src_info = src_wbuffer->zpage_info[idx];
    u16 cur_size = src_info.comp_sz;

    ZipInfo &dest_info = dest_wbuffer->zpage_info[binpacked_pages_cnt];
    dest_info = src_info;

    auto it = freespace_to_bin.lower_bound(cur_size);
    int chosen_bin = -1;

    // Find a suitable bin with enough space and under the page limit
    while (it != freespace_to_bin.end()) {
      int b = it->second;

      if (bins->count[b] < max_pages_per_bin) {
        chosen_bin = b;
        freespace_to_bin.erase(it);
        break;
      }

      ++it;
    }

    if (chosen_bin == -1) {
      // No suitable bin found — start a new physical page
      dest_info.start_ptr = physical_pages_cnt * PAGE_SIZE;
      std::memcpy(wbuffer_ptr, src_wbuffer->buffer + src_info.start_ptr,
                  cur_size);

      bins->sum[bins->bincount] = cur_size;
      bins->start_ptr[bins->bincount] = dest_info.start_ptr;
      bins->count[bins->bincount] = 1;

      if (max_pages_per_bin > 1) {
        u32 remaining = PAGE_SIZE - cur_size;
        freespace_to_bin.insert({remaining, bins->bincount});
      }

      physical_pages_cnt++;
      bins->bincount++;
      wbuffer_ptr = dest_wbuffer->buffer + physical_pages_cnt * PAGE_SIZE;
    } else {
      // Place page in existing bin
      dest_info.start_ptr = bins->start_ptr[chosen_bin] + bins->sum[chosen_bin];
      std::memcpy(dest_wbuffer->buffer + dest_info.start_ptr,
                  src_wbuffer->buffer + src_info.start_ptr, cur_size);

      bins->sum[chosen_bin] += cur_size;
      bins->count[chosen_bin]++;

      // Only reinsert if not full
      if (bins->count[chosen_bin] < max_pages_per_bin) {
        u32 remaining = PAGE_SIZE - bins->sum[chosen_bin];
        freespace_to_bin.insert({remaining, chosen_bin});
      }
    }

    avg_edt += src_info.death_time;
    dest_wbuffer->zpage_cnt++;
    binpacked_pages_cnt++;
  }

  Ensure(bins->bincount == physical_pages_cnt);

  Ensure(dest_wbuffer->zpage_cnt == (end_idx - start_idx + 1));
  dest_wbuffer->cur_size = physical_pages_cnt * PAGE_SIZE;
  logid_t total_edt =
      (dest_wbuffer->zpage_cnt > 0) ? (avg_edt) / dest_wbuffer->zpage_cnt : 0;
  dest_wbuffer->avg_edt = total_edt;

  return total_edt;
}

u64 IOInterface::GetWbufferWriteSize(ZipPage *wbuffer) {
  if (FLAGS_use_binpacking) {
    Ensure(wbuffer->cur_size % PAGE_SIZE == 0);
  }

  return wbuffer->cur_size;
}

u32 IOInterface::GetWbufferWriteCnt(ZipPage *wbuffer) {
  if (FLAGS_use_binpacking) {
    Ensure(wbuffer->cur_size % PAGE_SIZE == 0);
  }

  return wbuffer->zpage_cnt;
}

void IOInterface::UpdateSpaceMetadata(ZipPage *wbuffer, blockid_t bid) {
  Ensure(GetWbufferWriteSize(wbuffer) + sm_->GetBlockWriteOffset(bid) <=
         sm_->max_w_ptr_);

  for (u32 idx = 0; idx < wbuffer->zpage_cnt; idx++) {
    pageid_t pid = wbuffer->zpage_info[idx].pid;
    u64 offset = wbuffer->zpage_info[idx].start_ptr;
    u16 comp_sz = wbuffer->zpage_info[idx].comp_sz;

    u64 updated_offset = static_cast<u64>(sm_->GetBlockOffset(bid)) +
                         static_cast<u64>(sm_->GetBlockWriteOffset(bid)) +
                         offset;

    Ensure(comp_sz > 0);

    bool copyback_write = wbuffer == &binpacked_gc_ ? true : false;

    sm_->UpdatePID2Offset(pid, updated_offset, comp_sz, bid,
                          recovery::LogManager::global_min_gsn_flushed,
                          copyback_write);

    sm_->UpdateOffset2PIDs(pid, bid, updated_offset);
  }

  if (wbuffer->zpage_cnt == 0) {
    wbuffer->avg_edt = 0;
  }

  if (wbuffer == &binpacked_gc_) {
    sm_->UpdateBlockMetadataAfterGCWrite(bid, GetWbufferWriteSize(wbuffer),
                                         GetWbufferWriteCnt(wbuffer),
                                         wbuffer->avg_edt);
  } else {
    sm_->UpdateBlockMetadataAfterWrite(bid, GetWbufferWriteSize(wbuffer),
                                       GetWbufferWriteCnt(wbuffer),
                                       wbuffer->avg_edt);
    // for debugging purposes
  }
}

blockid_t IOInterface::GetPrevGCedBlock(u32 group_idx) {
  return prev_gced_bids_[group_idx];
}

void IOInterface::SetPrevGCedBlock(u32 group_idx, blockid_t newbid) {
  blockid_t bid = prev_gced_bids_[group_idx];
  prev_gced_bids_[group_idx] = newbid;
}

u64 IOInterface::GetPID2Offset(pageid_t pid) {
  return PID2Offset_table_[pid].get_offset();
}

u16 IOInterface::GetPIDCompSize(pageid_t pid) {
  u16 comp_sz = PID2Offset_table_[pid].get_comp_sz();
  Ensure(comp_sz <= PAGE_SIZE);
  return comp_sz;
}

} // namespace leanstore::storage::space
