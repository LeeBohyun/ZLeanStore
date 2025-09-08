#pragma once

#include "buffer/buffer_frame.h"
#include "buffer/resident_set.h"
#include "common/typedefs.h"
#include "storage/extent/large_page.h"
#include "storage/free_page_manager.h"
#include "storage/page.h"
#include "storage/space/garbage_collector.h"
#include "storage/space/io_interface.h"
#include "sync/page_state.h"

#include "gtest/gtest_prod.h"

#ifdef ENABLE_EXMAP
#include <linux/exmap.h>
Ensure(FLAGS_enable_exmap);
#endif

#include <atomic>
#include <signal.h>
#include <span>
#include <sys/ioctl.h>
#include <thread>
#include <vector>

namespace leanstore {
// Forward class declaration
class LeanStore;

namespace transaction {
class Transaction;
}

namespace storage {
class FreePageManager;
} // namespace storage

namespace storage::blob {
class BlobManager;
struct PageAliasGuard;
} // namespace storage::blob

namespace storage::space {
class IOInterface;
class SpaceManager;
struct ZipInfo;
struct PID2Offset;
struct Offset2PIDs;
struct StorageSpace;
struct Block;
struct ZipPage;
struct VictimBlock;
struct GCedBlock;
} // namespace storage::space

// Exmap env
void HandleExmapSEGFAULT(int signo, siginfo_t *info, void *extra);
void RegisterSEGFAULTHandler();
} // namespace leanstore

namespace leanstore::buffer {

class BufferManager {
public:
  explicit BufferManager(int blockfd, u64 virtual_page_count,
                         std::atomic<bool> &keep_running,
                         storage::FreePageManager *fp,
                         storage::space::SpaceManager *space,
                         std::atomic<bool> &is_loading);
  BufferManager(int blockfd, u64 virtual_page_count, u64 physical_page_count,
                u64 extra_page_count, u64 evict_size,
                std::atomic<bool> &keep_running, storage::FreePageManager *fp,
                storage::space::SpaceManager *space,
                std::atomic<bool> &is_loading);
  ~BufferManager();

  // Misc utilities
  auto GetWalInfo() -> std::pair<int, u64>;
  void AllocMetadataPage();
  void RunPageProviderThreads();
  void FlushAll();
  pageid_t GetMaxPID();

  // Free Pages utilities
  auto GetFreePageManager() -> storage::FreePageManager *;

  // Misc helpers
  auto GetPageState(pageid_t page_id) -> sync::PageState &;
  void ValidatePID(pageid_t pid);
  auto IsValidPtr(void *page) -> bool;
  auto ToPID(void *page) -> pageid_t;
  auto ToPtr(pageid_t page_id) -> storage::Page *;
  auto BufferFrame(pageid_t page_id) -> buffer::BufferFrame &;
  void ChunkOperation(pageid_t start_pid, u64 no_bytes,
                      const std::function<void(u64, std::span<u8>)> &func);
  void MakePageClean(pageid_t page_id);
  void MakePageDirty(pageid_t page_id);

  // Main page operation
  void EnsureFreePages();
  auto AllocPage() -> storage::Page *;
  pageid_t ReuseDeallocatedSpace();
  void DeallocPage(pageid_t pid);
  void HandlePageFault(pageid_t page_id);

  void ReadPage(pageid_t page_id, bool gc);
  void WritePages(std::vector<pageid_t> &to_write, bool gcwrite, bool evict);

  // eviction
  void Evict(u32 evict_cnt);
  // evict clean pages only
  u64 EvictCleanPages(u64 evict_cnt, blockid_t bid);
  // evict a deallocated page
  bool EvictDeallocPage(pageid_t pid);

  // incremental checkpointing called by the checkpointer threads
  logid_t FuzzyCheckpoint(logid_t target_gsn, u32 shard_idx, u32 shard_cnt);

  // Extent utilities
  auto TryReuseExtent(u64 required_page_cnt, bool is_special_block,
                      pageid_t &out_start_pid) -> bool;
  auto AllocExtent(extidx_t extent_idx, u64 fixed_page_cnt) -> pageid_t;
  void PrepareExtentEviction(pageid_t start_pid);
  void EvictExtent(pageid_t pid, u64 page_cnt);
  void ReadExtents(const storage::LargePageList &large_pages);
  auto RequestAliasingArea(u64 requested_size) -> pageid_t;
  void ReleaseAliasingArea();

  // for GC operations
  u64 WriteDirtyPages(u32 write_cnt, u32 shard_idx, u32 shard_cnt, bool gc);
  void WritePagesInPlace(std::vector<pageid_t> &to_write, bool evict);
  void WritePagesOutOfPlace(std::vector<pageid_t> &to_write, bool evict);
  void HandlePageFaultGC(pageid_t page_id);
  blockid_t GCValidPIDs(blockid_t bid, bool sync_gc, u32 group_idx);
  std::vector<pageid_t> GCReadValidPIDs(blockid_t bid, bool sync_gc);
  u64 GCWriteValidPIDs(u32 gidx);
  u64 GCValidPIDCntInDisk(blockid_t bid);
  std::vector<pageid_t> GCReadValidPIDsInBlockOnDisk(blockid_t bid);
  u64 GCValidPIDCntInBlockOnDisk(blockid_t bid);
  bool FixExclusiveOnDiskGC(pageid_t pid, blockid_t bid, u32 p, u8 pidx);
  auto FixShareImplGC(pageid_t pid, blockid_t bid, u32 p, u8 pidx,
                      u64 *dirtified_cnt, u32 gidx) -> bool;

  // Synchronization
  auto FixExclusive(pageid_t page_id) -> storage::Page *;
  auto FixShare(pageid_t page_id) -> storage::Page *;
  void UnfixExclusive(pageid_t page_id);
  void UnfixShare(pageid_t page_id);

  // dummy page-level operations for flashbench
  void UpdatePID(pageid_t pid);
  void ReadPID(pageid_t pid);
  void ModifyPageContent(storage::Page *page);
  void CalcEDT(pageid_t pid);
  void UpdateMinMaxEDT(pageid_t pid);
  void RemoveMinMaxEDT(pageid_t pid);
  void UpdateLSNGap();

  void FlushValidPIDsToBlock(blockid_t bid, std::vector<pageid_t> gc_target,
                             bool final, logid_t prev_min_edt,
                             bool find_other_block);

  /* main metadata for space management*/
  storage::space::SpaceManager *sm_;
  storage::space::PID2Offset *PID2Offset_table_;
  storage::space::StorageSpace *sspace_;
  int blockfd_;

  u64 min_clean_cnt_ = 0; /* Number of minimum clean pages for GC*/
  std::atomic<bool>
      *is_loading_; /* Whether the database is running or was stopped */
                    /* Buffer pool statistics */
  std::atomic<u64> alloc_cnt_;   /* Number of allocated pages in VMCache */
  std::atomic<u64> dealloc_cnt_; /* Number of allocated pages in VMCache */
  buffer::BufferFrame *frame_;   /* The buffer frames of all stored pages */

private:
  static constexpr u8 NO_BLOCKS_PER_LOCK = sizeof(u64) * CHAR_BIT;
  friend class leanstore::transaction::Transaction;
  friend class leanstore::storage::blob::BlobManager;
  friend struct leanstore::storage::blob::PageAliasGuard;
  friend class leanstore::LeanStore;
  friend class leanstore::storage::space::IOInterface;
  friend class leanstore::storage::space::SpaceManager;

  FRIEND_TEST(TestBlobManager, InsertNewBlob);
  FRIEND_TEST(TestBlobManager, GrowExistingBlob);
  FRIEND_TEST(TestBufferManager, BasicTest);
  FRIEND_TEST(TestBufferManager, BasicTestWithExtent);
  FRIEND_TEST(TestBufferManager, AllocFullCapacity);
  FRIEND_TEST(TestBufferManager, SharedAliasingLock);
  FRIEND_TEST(TestBufferManager, ConcurrentRequestAliasingArea);

  // Various private utilities
  void Construction();
  void ExmapAlloc(pageid_t pid, size_t mem_alloc_sz = 1);
  auto FixShareImpl(pageid_t page_id) -> bool;
  auto FixShareCachedPage(pageid_t pid) -> bool;
  auto ToggleShalasLocks(bool set_op, u64 &block_start, u64 block_end) -> bool;
  void PrepareExtentEnv(pageid_t start_pid, u64 page_cnt);
  void PrepareTailExtent(bool already_lock_1st_extent, pageid_t start_pid,
                         u64 page_cnt);

  /* Exmap environment */
  int exmapfd_;
  std::vector<struct exmap_user_interface *> exmap_interface_;

  /* Buffer pool environment */
  const u64 virtual_size_; /* Size of the buffer pool manager */
  const u64
      physical_size_; /* Size of OS memory used for LeanStore buffer manager */
  const u64 alias_size_;   /* Size of the worker-local memory used for various
                              utilities */
  const u64 virtual_cnt_;  /* Number of supported pages in virtual mem */
  const u64 physical_cnt_; /* Number of supported physical pages */

  const u64
      alias_pg_cnt_; /* Number of pages in the aliasing region per worker */
  const u64 evict_batch_; /* Size of batch for eviction */

  std::atomic<bool>
      *keep_running_; /* Whether the database is running or was stopped */

  std::atomic<u64> physical_used_cnt_; /* Number of active pages in OS memory */
  std::atomic<u64> maximum_page_size_{1}; /* The maximum size of a large page */
  std::atomic<u64> dirty_cnt_{0};     /* Number of clean pages in buffer pool */
  std::atomic<u64> reuse_cnt_{0};     /* Number of supported physical pages */
  std::atomic<u64> nonreused_cnt_{0}; /* Number of supported physical pages */

  /* Free Extents/Pages manager */
  storage::FreePageManager *free_pages_;

  /* Page info & addresses */
  storage::Page
      *virtual_mem_; /* The virtual memory addresses to refer to DB pages */
  sync::PageState *page_state_; /* The state of all database pages */

  ResidentPageSet resident_set_; /* The clock replacer impl */

  /* Aliasing memory regions with block-granular range lock on Shalas area */
  std::vector<u64>
      wl_alias_ptr_;            /* Current Ptr in Worker-local aliasing area */
  std::atomic<u64> shalas_ptr_; /* Current block ptr in shared aliasing area */
  storage::Page *shalas_area_;  /* Shared-area for memory aliasing */
  const u64 shalas_no_blocks_;  /* Number of blocks in shared aliasing area,
                                   every block is as big as alias_size_ */
  const u64 shalas_no_locks_;   /* Number of locks of `shalas_lk_`, each
                                   atomic<u64> manages 64 locks */
  std::unique_ptr<std::atomic<u64>[]>
      shalas_lk_; /* Range lock impl with Bitmap, each bit corresponds to a
                     block */
  std::vector<std::vector<std::pair<u64, u64>>>
      shalas_lk_acquired_; /* For UNDO the lock after a successful lock */

  /* IO page translation layer */
  std::vector<storage::space::IOInterface> io_interface_;
  // Workers + Page providers + Group committer + Checkpointer +  Garbage
  // collector
  u32 io_handler_cnt_ = 0; // number of threads that issue I/O requests

  std::atomic<u64> valid_page_cnt_after_gc_{
      0}; /* Number of valid pages after GC */
  std::atomic<u64> cleaned_block_cnt_{
      0};                                /* Number of cleaned blocks after GC */
  std::atomic<u64> cleaned_page_cnt_{0}; /* Number of cleaned pages after GC */
  std::atomic<logid_t> max_edt{0};
  std::atomic<logid_t> min_edt{std::numeric_limits<logid_t>::max()};
  std::atomic<logid_t> prev_lsn_gap_per_wgroup{
      0}; /* Number of supported physical pages */
  std::atomic<logid_t> cur_lsn_gap_per_wgroup{
      0};                          /* Number of supported physical pages */
  std::atomic<logid_t> lsn_gap{0}; /* Number of supported physical pages */
  std::atomic<u64> write_cnt_per_interval{0};
  std::atomic<u64> next_write_threshold{
      0}; // new: threshold to trigger next update
};

} // namespace leanstore::buffer
