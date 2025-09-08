#pragma once

#include "backend/io_backend.h"
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "doublewrite.h"
#include "fsst.h"
#include "lz4.h"
#include "space_manager.h"
#include "storage/page.h"
#include "zlib.h"
#include "zstd.h"
#include "gtest/gtest_prod.h"
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <span>
#include <vector>

namespace leanstore::storage::space {
// Custom bin state for tracking and sorting
struct BinState {
  u16 sum;       // used size in the bin
  u16 count;     // number of items in the bin
  u64 start_ptr; // offset into the destination buffer
  int bin_idx;   // index of the bin in bins array

  bool operator<(const BinState &other) const {
    // reverse order for min-heap (smallest sum first)
    return sum > other.sum;
  }
};

// Structure to hold information about bins in the bin packing process.
struct Bins {
  u32 bincount = 0;
  u32 *sum = nullptr;
  u32 *count = nullptr;
  u64 *start_ptr = nullptr;
};

// Structure to hold compressed page information.
struct ZipInfo {
  pageid_t pid = 0;
  u16 comp_sz = 0;
  logid_t death_time = 0;
  u64 start_ptr = 0;
};

// Structure representing a page used for compression.
struct ZipPage {
  char *buffer = nullptr;
  ZipInfo *zpage_info = nullptr;
  u64 cur_size = 0;
  u32 zpage_cnt = 0;
  logid_t avg_edt = 0;
  u32 allocated_page_cnt = 0; // <-- New field
  std::shared_mutex mtx;

  // Default constructor
  ZipPage() = default;

  // Move constructor
  ZipPage(ZipPage &&other) noexcept
      : buffer(other.buffer), zpage_info(other.zpage_info),
        cur_size(other.cur_size), zpage_cnt(other.zpage_cnt) {
    // Null out the original to prevent double free
    other.buffer = nullptr;
    other.zpage_info = nullptr;
  }
};

// Interface for I/O operations on storage pages.
class IOInterface {
public:
  IOInterface(int blockfd, Page *virtual_mem, u64 virtual_cnt, SpaceManager *sm,
              PID2Offset *PID2Offset_table, StorageSpace *storage_space);
  // Read operations
  void ResetReadBuffer(u32 page_cnt);
  void ReadPage(pageid_t pid, bool gcread);
  void ReadPageInPlace(pageid_t pid);
  void ReadPageOutOfPlace(pageid_t pid);
  void ReadPagesOutOfPlace(std::vector<pageid_t> &to_read);
  void HandleCompressedRead(pageid_t pid, u64 pid_offset, u16 comp_sz,
                            bool sync);
  void HandleCompressedBinpackedRead(pageid_t pid, u64 pid_offset, u16 comp_sz,
                                     bool sync);
  void HandleUncompressedRead(pageid_t pid, u64 pid_offset, u16 comp_sz,
                              bool sync);
  bool ReadPageFromWBuffer(pageid_t pid);
  void DecompressPage(char *read_buffer, storage::Page *page, u64 start_ptr,
                      u16 comp_sz, u64 pid);
  void UpdateReadCachedPageInfo(pageid_t pid, u16 comp_sz);
  u64 GetPID2Offset(pageid_t pid);
  u16 GetPIDCompSize(pageid_t pid);
  blockid_t GetPrevGCedBlock(u32 group_idx);
  blockid_t GetReadGCedBlock(blockid_t bid);
  void SetPrevGCedBlock(u32 group_idx, blockid_t newbid);

  // Write operations
  u16 AddPageToWriteBuffer(storage::Page *page, pageid_t pid, logid_t edt);
  u16 AddPageToGCWriteBuffer(storage::Page *page, pageid_t pid, logid_t edt);
  bool PageInUserWBuffer(pageid_t pid);
  bool PageInUserWBufferPerGroup(pageid_t pid, u32 idx);
  bool PageInReadBuffer(pageid_t pid);
  bool PageInGCWBuffer(pageid_t pid);
  bool PageInWBuffer(pageid_t pid);

  // Write buffer management
  void FlushWriteBuffer();
  void FlushWriteBufferPerGroup(u32 group_idx, blockid_t bid, logid_t min_edt,
                                logid_t max_edt);
  void FlushGCWriteBuffer(blockid_t bid, bool sync, logid_t min_edt,
                          logid_t max_edt,
                          bool find_other_block); // synchronous GC
  bool BlockNeedsGC(blockid_t bid);

  // Compression and bin packing
  u16 CompressPage(Page *page, char *wbuffer, u64 start_ptr, u64 pid);
  void UpdateCachedPageInfo(ZipPage *wbuffer, pageid_t pid, u16 comp_sz,
                            logid_t edt);
  void BinPacking(logid_t cur_ts, logid_t min_edt, logid_t max_edt);
  void SortPagesByEDT();
  std::vector<logid_t> ExtractUniqueEDTs();
  std::vector<std::pair<u32, u32>>
  GroupPagesByEDT(const std::vector<logid_t> &unique_edts, logid_t min_edt,
                  logid_t max_edt);
  u64 BinPackGroups(const std::vector<std::pair<u32, u32>> &group_ranges);
  u64 UniformBinPacking();
  u64 GetWbufferPIDCntPerGroup(u32 idx);
  pageid_t GetWbufferPIDPerGroup(u32 idx, u32 pidx);
  logid_t BinPackingPerBlock(ZipPage *src, ZipPage *dest, Bins *bins,
                             u64 start_idx, u64 end_idx);
  logid_t CopyCompressedPages(ZipPage *src, ZipPage *dest, u64 start_idx,
                              u64 end_idx);
  blockid_t SelectBlockIDToWrite(u32 idx, logid_t min_edt, logid_t max_edt);
  // Buffer reset functions
  void ResetBuffer(ZipPage *zpage, u32 page_cnt);
  void ResetUserWriteBuffer(u32 page_cnt);
  void ResetWBufferPerGroup(u64 wbuffer_size, u32 page_cnt);
  u64 GetWbufferSizePerGroup(u32 idx);
  logid_t GetMinEDTFromGCWBuffer();
  void ResetGCWriteBuffer(u64 wbuffer_size, u32 cached_cnt);
  u64 GetWbufferWriteSize(ZipPage *wbuffer);
  u32 GetWbufferWriteCnt(ZipPage *wbuffer);
  void UpdateSpaceMetadata(ZipPage *wbuffer, blockid_t bid);

  std::vector<blockid_t> selected_bids_; // List of selected block IDs
private:
  void Construction();
  friend class leanstore::storage::space::backend::IOBackend;

  // Main write buffer for in-place and out-of-place writes
  ZipPage wbuffer_;

  // For grouping pages with similar EDT
  u64 min_comp_size_;
  u32 group_cnt_;
  u32 page_cnt_per_group_;

  // Write buffer used for bin packing (for out-of-place writes)
  std::vector<ZipPage> wbuffer_group_;
  std::vector<Bins> bins;

  // For synchronous garbage collection
  ZipPage wbuffer_gc_;
  ZipPage binpacked_gc_;
  std::unique_ptr<Bins> bin_gc_;
  std::vector<blockid_t> prev_gced_bids_;

  // Read buffer for cached pages
  ZipPage rbuffer_;    // for synchronous read
  ZipPage rbuffer_gc_; // for asynchronous gc read

  // IO backend
  int blockfd_;
  u64 virtual_cnt_;
  Page *virtual_mem_;
  storage::space::backend::IOBackend io_backend_;

  // doublewrite buffer
  storage::space::Doublewrite doublewrite_;

  // Space manager
  storage::space::SpaceManager *sm_;
  storage::space::PID2Offset *PID2Offset_table_;
  storage::space::StorageSpace *sspace_;
  u64 page_cnt_per_block_;
  u64 block_cnt_;
};

} // namespace leanstore::storage::space
