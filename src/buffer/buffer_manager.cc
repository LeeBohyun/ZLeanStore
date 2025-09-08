#include "buffer/buffer_manager.h"
#include "common/exceptions.h"
#include "common/format.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/env.h"
#include "leanstore/statistics.h"
#include "recovery/log_manager.h"
#include "storage/blob/blob_handler.h"

#include "fmt/ranges.h"
#include "share_headers/logger.h"
#include "storage/space/io_interface.h"
#include "storage/space/space_manager.h"

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <sys/stat.h>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

constexpr u64 MAX_EVICT_TIME_MS = 1000;

// Set the first ((i) % NO_BLOCKS_PER_LOCK) bits
#define SET_BITS(i) ((1UL << ((i) % NO_BLOCKS_PER_LOCK)) - 1)

#define NEW_ALLOC_PAGE(pid)                                                    \
  ({                                                                           \
    auto &ps = GetPageState(pid);                                              \
    u64 v = ps.StateAndVersion().load();                                       \
    bool success = ps.TryLockExclusive(v);                                     \
    Ensure(success);                                                           \
  })

#define PAGE_FAULT(pid, page_cnt)                                              \
  ({                                                                           \
    physical_used_cnt_ += (page_cnt);                                          \
    EnsureFreePages();                                                         \
    /* Check if EXMAP is enabled */                                            \
    if (FLAGS_use_exmap) {                                                     \
      ExmapAlloc(pid, page_cnt);                                               \
    }                                                                          \
  })

#define EXTENT_FRAME_SET(pid, size)                                            \
  ({                                                                           \
    frame_[pid].Reset();                                                       \
    frame_[pid].SetFlag(PageFlagIdx::IS_EXTENT);                               \
    frame_[pid].evict_pg_count = (size);                                       \
  })

#define PREPARE_EXTENT(pid, page_cnt)                                          \
  ({                                                                           \
    resident_set_.Insert(pid);                                                 \
    frame_[pid].extent_size = (page_cnt);                                      \
    if (FLAGS_bm_enable_fair_eviction) {                                       \
      UpdateMax(maximum_page_size_, page_cnt);                                 \
    }                                                                          \
  })

// For your info, please check virtual_alloc_size's description in
// BufferManager::Construction()
#define ALIAS_LOCAL_PTR_START(w_id) (virtual_cnt_ + 1 + (w_id)*alias_pg_cnt_)
#define ALIAS_AREA_CAPABLE(index, w_id)                                        \
  ((index) < ALIAS_LOCAL_PTR_START((w_id) + 1))

#define NULL_PID (~0ULL) - 1

using ExtentList = leanstore::storage::ExtentList;

namespace leanstore {

void HandleExmapSEGFAULT([[maybe_unused]] int signo, siginfo_t *info,
                         [[maybe_unused]] void *extra) {
  void *page = info->si_addr;
  for (auto &buffer_pool : all_buffer_pools) {
    if (buffer_pool->IsValidPtr(page)) {
      throw sync::RestartException();
    }
  }
  LOG_ERROR("SEGFAULT - addr %p wid: %d", page, worker_thread_id);
  throw ex::EnsureFailed("SEGFAULT");
}

void RegisterSEGFAULTHandler() {
  struct sigaction action;
  action.sa_flags = SA_SIGINFO;
  action.sa_sigaction = HandleExmapSEGFAULT;
  if (sigaction(SIGSEGV, &action, nullptr) == -1) {
    perror("sigusr: sigaction");
    throw leanstore::ex::EnsureFailed();
  }
}

} // namespace leanstore

namespace leanstore::buffer {

BufferManager::BufferManager(int blockfd, u64 virtual_page_count,
                             std::atomic<bool> &keep_running,
                             storage::FreePageManager *fp,
                             storage::space::SpaceManager *sm,
                             std::atomic<bool> &is_loading)
    : blockfd_(blockfd), virtual_size_(virtual_page_count * PAGE_SIZE),
      physical_size_(static_cast<u64>(FLAGS_bm_physical_gb * GB)),
      alias_size_(FLAGS_bm_wl_alias_mb * MB), virtual_cnt_(virtual_page_count),
      physical_cnt_(physical_size_ / PAGE_SIZE),
      alias_pg_cnt_(alias_size_ / PAGE_SIZE),
      evict_batch_(FLAGS_bm_evict_batch_size), keep_running_(&keep_running),
      io_handler_cnt_(FLAGS_worker_count + FLAGS_page_provider_thread +
                      FLAGS_wal_enable + FLAGS_checkpointer_cnt +
                      FLAGS_garbage_collector_cnt),
      min_clean_cnt_((FLAGS_block_size_mb * MB / PAGE_SIZE) * io_handler_cnt_),
      free_pages_(fp), page_state_(static_cast<sync::PageState *>(
                           AllocHuge(virtual_cnt_ * sizeof(sync::PageState)))),
      sm_(sm), is_loading_(&is_loading),
      resident_set_(physical_cnt_, page_state_,
                    FLAGS_worker_count + FLAGS_page_provider_thread +
                        FLAGS_wal_enable + FLAGS_checkpointer_cnt +
                        FLAGS_garbage_collector_cnt),
      shalas_no_blocks_(virtual_cnt_ / alias_pg_cnt_),
      shalas_no_locks_(std::ceil(static_cast<float>(shalas_no_blocks_) /
                                 NO_BLOCKS_PER_LOCK)) {
  Construction();
}

BufferManager::BufferManager(int blockfd, u64 virtual_page_count,
                             u64 physical_page_count, u64 extra_page_count,
                             u64 evict_size, std::atomic<bool> &keep_running,
                             storage::FreePageManager *fp,
                             storage::space::SpaceManager *sm,
                             std::atomic<bool> &is_loading)
    : blockfd_(blockfd), virtual_size_(virtual_page_count * PAGE_SIZE),
      physical_size_(physical_page_count * PAGE_SIZE),
      alias_size_(extra_page_count * PAGE_SIZE),
      alias_pg_cnt_(extra_page_count), evict_batch_(FLAGS_bm_evict_batch_size),
      io_handler_cnt_(FLAGS_worker_count + FLAGS_page_provider_thread +
                      FLAGS_wal_enable + FLAGS_checkpointer_cnt +
                      FLAGS_garbage_collector_cnt),
      min_clean_cnt_((FLAGS_block_size_mb * MB / PAGE_SIZE) * io_handler_cnt_),
      virtual_cnt_(virtual_page_count), physical_cnt_(physical_page_count),
      keep_running_(&keep_running), free_pages_(fp), sm_(sm),
      is_loading_(&is_loading),
      page_state_(static_cast<sync::PageState *>(
          AllocHuge(virtual_cnt_ * sizeof(sync::PageState)))),
      resident_set_(physical_cnt_, page_state_,
                    FLAGS_worker_count + FLAGS_page_provider_thread +
                        FLAGS_wal_enable + FLAGS_checkpointer_cnt +
                        FLAGS_garbage_collector_cnt),
      shalas_no_blocks_(virtual_cnt_ / alias_pg_cnt_),
      shalas_no_locks_(std::ceil(static_cast<float>(shalas_no_blocks_) /
                                 NO_BLOCKS_PER_LOCK)) {
  Construction();
}

BufferManager::~BufferManager() {
  close(exmapfd_);
  close(blockfd_);
}

void BufferManager::Construction() {
  assert(virtual_size_ >= physical_size_);
  u64 virtual_alloc_size = (FLAGS_enable_blob == true)
                               ? virtual_size_ + PAGE_SIZE +
                                     alias_size_ * FLAGS_worker_count +
                                     virtual_size_
                               : virtual_size_ + (1 << 16);

  physical_used_cnt_ = 0;
  alloc_cnt_ = 0;
  dealloc_cnt_ = 0;

  if (FLAGS_use_exmap) {
#ifdef ENABLE_EXMAP
    // exmap allocation
    exmapfd_ = open(FLAGS_exmap_path.c_str(), O_RDWR);
    if (exmapfd_ < 0) {
      throw leanstore::ex::GenericException(
          "Open exmap file-descriptor error. Did you load the module?");
    }
    struct exmap_ioctl_setup buffer;
    buffer.fd = blockfd_;
    buffer.max_interfaces = io_handler_cnt_;
    buffer.buffer_size = physical_cnt_;
    buffer.flags =
        (FLAGS_worker_pin_thread) ? exmap_flags::EXMAP_CPU_AFFINITY : 0;
    auto ret = ioctl(exmapfd_, EXMAP_IOCTL_SETUP, &buffer);
    if (ret < 0) {
      throw leanstore::ex::GenericException("ioctl: exmap_setup error");
    }

    exmap_interface_.resize(io_handler_cnt_);
    for (size_t idx = 0; idx < io_handler_cnt_; idx++) {
      exmap_interface_[idx] = static_cast<exmap_user_interface *>(
          mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, exmapfd_,
               EXMAP_OFF_INTERFACE(idx)));
      if (exmap_interface_[idx] == MAP_FAILED) {
        throw leanstore::ex::GenericException("Setup exmap_interface_ error");
      };
    }

    // Setup virtual mem on top of exmap
    virtual_mem_ = static_cast<storage::Page *>(
        mmap(nullptr, virtual_alloc_size, PROT_READ | PROT_WRITE, MAP_SHARED,
             exmapfd_, 0));
#endif
  } else {
    // Setup virtual mem without exmap
    fprintf(stderr, "Setup virtual mem without exmap\n");
    virtual_mem_ = static_cast<storage::Page *>(
        mmap(nullptr, virtual_alloc_size, PROT_READ | PROT_WRITE,
             MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    madvise(virtual_mem_, virtual_alloc_size, MADV_NOHUGEPAGE);
  }

  if (virtual_mem_ == MAP_FAILED) {
    throw leanstore::ex::GenericException("mmap failed");
  };

  if (FLAGS_enable_blob) {
    // Setup aliasing areas
    wl_alias_ptr_.resize(FLAGS_worker_count);
    for (size_t idx = 0; idx < FLAGS_worker_count; idx++) {
      wl_alias_ptr_[idx] = ALIAS_LOCAL_PTR_START(idx);
    }
    Ensure(virtual_cnt_ % alias_pg_cnt_ == 0);
    shalas_ptr_ = 0;
    shalas_area_ = &virtual_mem_[ALIAS_LOCAL_PTR_START(FLAGS_worker_count)];
    shalas_lk_ = std::make_unique<std::atomic<u64>[]>(shalas_no_locks_);
    shalas_lk_acquired_.resize(FLAGS_worker_count);
  }

  frame_ = static_cast<buffer::BufferFrame *>(
      AllocHuge(virtual_cnt_ * sizeof(buffer::BufferFrame)));

  // space manager per PID
  PID2Offset_table_ = sm_->PID2Offset_table_;

  if (FLAGS_use_out_of_place_write) {
    // constuct storage space for GC
    sspace_ = sm_->sspace_;
    // minimum number of clean pages to maintain inside the buffer pool for GC
    if (FLAGS_use_compression) {
      min_clean_cnt_ *= (PAGE_SIZE / MIN_COMP_SIZE);
    }
  } else {
    min_clean_cnt_ = 0;
  }

  // Construct IO interfaces per IO handler
  io_interface_.reserve(io_handler_cnt_);
  for (size_t idx = 0; idx < io_handler_cnt_; idx++) {
    io_interface_.emplace_back(storage::space::IOInterface(
        blockfd_, virtual_mem_, virtual_cnt_, sm_, PID2Offset_table_, sspace_));
  }

  // log buffer manager info
  fprintf(stderr,
          "VMCache: Path(%s), VirtualGB(%lu), VirtualCount(%lu), PhysGB(%lu), "
          "PhysCount(%lu), EvictSize(%lu), "
          "VirtualAlloccnt(%lu)\n",
          FLAGS_db_path.c_str(), virtual_size_ / GB, virtual_cnt_,
          physical_size_ / GB, physical_cnt_, evict_batch_,
          virtual_alloc_size / PAGE_SIZE);
}

// ------------------------------------------------------------------------------------------------------

void BufferManager::AllocMetadataPage() {
  // Alloc page 0 for metadata
  AllocPage();
  GetPageState(0).UnlockExclusive();
}

pageid_t BufferManager::GetMaxPID() { return alloc_cnt_.load(); }

auto BufferManager::GetWalInfo() -> std::pair<int, u64> {
  size_t w_offset_ = FLAGS_bm_virtual_gb * GB;
  return std::make_pair(blockfd_, w_offset_);
}

void BufferManager::RunPageProviderThreads() {
  for (u32 t_id = 0; t_id < FLAGS_page_provider_thread; t_id++) {
    std::thread page_provider([&, t_id]() {
      pthread_setname_np(pthread_self(), "page_provider");
      worker_thread_id = FLAGS_worker_count + 1 + t_id;
      try {
        while (keep_running_->load()) {
          if (physical_used_cnt_ >= physical_cnt_ * 0.99) {
            Evict(evict_batch_);
          }
          AsmYield();
        }
      } catch (...) {
        // Similar reason to GroupCommitExecutor::StartExecution()
      }
    });
    page_provider.detach();
  }
}

// ------------------------------------------------------------------------------------------------------
auto BufferManager::GetFreePageManager() -> storage::FreePageManager * {
  return free_pages_;
}

// ------------------------------------------------------------------------------------------------------
auto BufferManager::GetPageState(pageid_t pid) -> sync::PageState & {
  return page_state_[pid];
}

void BufferManager::ValidatePID(pageid_t pid) {
  if (pid >= virtual_cnt_) {
    throw std::runtime_error("Page id " + std::to_string(pid) + " is invalid");
  }
}

auto BufferManager::IsValidPtr(void *page) -> bool {
  return (page >= virtual_mem_) && (page < (virtual_mem_ + virtual_size_ + 16));
}

auto BufferManager::ToPID(void *page) -> pageid_t {
  return reinterpret_cast<storage::Page *>(page) - virtual_mem_;
}

auto BufferManager::ToPtr(pageid_t pid) -> storage::Page * {
  if (FLAGS_blob_normal_buffer_pool) {
    resident_set_.Contain(pid);
  }
  return &virtual_mem_[pid];
}

auto BufferManager::BufferFrame(pageid_t pid) -> buffer::BufferFrame & {
  return frame_[pid];
}

/**
 * @brief Chunk operation to simulate overheads of normal Buffer Manager
 */
void BufferManager::ChunkOperation(
    pageid_t start_pid, u64 no_bytes,
    const std::function<void(u64, std::span<u8>)> &func) {
  Ensure(FLAGS_blob_normal_buffer_pool);
  auto offset = 0UL;
  while (offset < no_bytes) {
    auto to_op_size = std::min(no_bytes - offset, PAGE_SIZE);
    auto to_op_pid = start_pid + offset / PAGE_SIZE;
    func(offset, {reinterpret_cast<u8 *>(ToPtr(to_op_pid)), to_op_size});
    offset += to_op_size;
  }
  Ensure(offset == no_bytes);
}

void BufferManager::MakePageClean(pageid_t pid) {
  ToPtr(pid)->dirty = false;
  frame_[pid].dirty = false;
  frame_[pid].min_p_gsn = 0;
  dirty_cnt_.fetch_sub(1, std::memory_order_relaxed);
}

void BufferManager::MakePageDirty(pageid_t pid) {
  ToPtr(pid)->dirty = true;
  frame_[pid].dirty = true;

  dirty_cnt_.fetch_add(1, std::memory_order_relaxed);
}

void BufferManager::EnsureFreePages() { //+ PAGE_CNT_PER_BLOCK
  if (FLAGS_page_provider_thread > 0) {
    // Page Provider thread is enabled, then we should sleep to wait for free
    // pages
    while ((physical_used_cnt_ >= physical_cnt_ * 0.99) &&
           (keep_running_->load())) {
      AsmYield();
    }
    return;
  }

  u64 range = physical_cnt_ / io_handler_cnt_ / 100;

  while ((physical_used_cnt_ >= physical_cnt_ * 0.98 + range) &&
         (keep_running_->load())) {
    Evict(evict_batch_);
  }
}

void BufferManager::ExmapAlloc(pageid_t pid, size_t mem_alloc_sz) {
// TODO: Support ExmapAlloc with `mem_alloc_sz > EXMAP_PAGE_MAX_PAGES *
// EXMAP_USER_INTERFACE_PAGES`
#ifdef ENABLE_EXMAP
  int idx = 0;
  for (; mem_alloc_sz > 0; idx++) {
    auto alloc_sz =
        std::min(static_cast<size_t>(EXMAP_PAGE_MAX_PAGES - 1), mem_alloc_sz);
    exmap_interface_[worker_thread_id]->iov[idx].page = pid;
    exmap_interface_[worker_thread_id]->iov[idx].len = alloc_sz;
    pid += alloc_sz;
    mem_alloc_sz -= alloc_sz;
  }
  Ensure(idx < EXMAP_USER_INTERFACE_PAGES);
  while (ExmapAction(exmapfd_, EXMAP_OP_ALLOC, idx) < 0) {
    LOG_ERROR("Exmap Alloc Page errno '%d', pid '%lu', worker_id '%d'", errno,
              pid, worker_thread_id);
    EnsureFreePages();
  }
#endif
}

auto BufferManager::AllocPage() -> storage::Page * {
  // pid increments everytime a new page is allocated
  pageid_t pid = 0;

  bool reuse = false;
  pageid_t space_pid = 0;

  if (!FLAGS_use_out_of_place_write) {
    // check whether there is a reusable space whose pid has been deallocated
    space_pid = ReuseDeallocatedSpace();
    if (space_pid != 0) {
      // the space has been reused
      reuse = true;
    }
  }
  // increment the pid and stats
  statistics::buffer::alloc_cnt++;
  ValidatePID(pid);
  pid = alloc_cnt_.fetch_add(1, std::memory_order_relaxed);
  if (pid > virtual_cnt_) {
    throw std::runtime_error("Ran out of virtual memory");
  }

  // lock the newly allocated page exclusively
  NEW_ALLOC_PAGE(pid);
  Ensure(GetPageState(pid).LockState() == sync::PageStateMode::EXCLUSIVE);

  // ensure that the free buffer frame is available
  PAGE_FAULT(pid, 1);
  Ensure(physical_used_cnt_ < physical_cnt_);

  // zero out page upon alloc for better compression
  if (FLAGS_use_compression) {
    memset(ToPtr(pid), 0, PAGE_SIZE);
  }

  if (reuse) {
    // if the space has been reused, update the mapping info
    sm_->UpdatePID2Offset(pid, space_pid * PAGE_SIZE, PAGE_SIZE,
                          sm_->block_cnt_, 0, false);
    Ensure(PID2Offset_table_[space_pid].get_comp_sz() == 0);
    Ensure(frame_[space_pid].deallocated);
  }

  // make page dirty to write it to the disk for the first time
  MakePageDirty(pid);

  // insert to the buffer pool
  Ensure(resident_set_.Insert(pid));
  return ToPtr(pid);
}

pageid_t BufferManager::ReuseDeallocatedSpace() {
  if (!dealloc_cnt_.load())
    return 0;

  // Try to move oldest deallocated PID to reusable list
  sm_->ProcessDeallocList(worker_thread_id,
                          recovery::LogManager::global_min_gsn_flushed.load());

  pageid_t space_pid = 0;
  if (sm_->HasReusablePID(worker_thread_id)) {
    space_pid = sm_->AssignPIDUponAlloc(worker_thread_id);
    Ensure(PID2Offset_table_[space_pid].get_comp_sz() == PAGE_SIZE - 1);
    Ensure(frame_[space_pid].deallocated);
    sm_->UpdatePID2Offset(space_pid, 0, 0, sm_->block_cnt_, 0, false);
    reuse_cnt_.fetch_add(1, std::memory_order_relaxed);
  } else {
    nonreused_cnt_.fetch_add(1, std::memory_order_relaxed);
  }
  return space_pid;
}

void BufferManager::DeallocPage(pageid_t pid) {
  Ensure(GetPageState(pid).LockState() == sync::PageStateMode::EXCLUSIVE);
  Ensure(!frame_[pid].deallocated);
  Ensure(!sm_->PIDIsDeallocated(pid));

  frame_[pid].deallocated = true;
  MakePageClean(pid);

  dealloc_cnt_.fetch_add(1, std::memory_order_relaxed);
  statistics::buffer::dealloc_cnt++;
}

/**
 * @brief Try to ensure that there are free frames (i.e. virtual mem in vmcache)
 * in the buffer manager. After that, read the page and load it into the buffer
 * pool
 */
void BufferManager::HandlePageFault(pageid_t pid) {
  Ensure(GetPageState(pid).LockState() == sync::PageStateMode::EXCLUSIVE);
  Ensure(!sm_->PIDIsDeallocated(pid));
  Ensure(PID2Offset_table_[pid].get_comp_sz() != PAGE_SIZE - 1);
  if (sm_->PIDIsDeallocated(pid)) {
    // this should never happen since reading deallocated page doesn't make
    // sense
    return;
  }
  // read PID from the disk
  ReadPage(pid, false);
  // make PID clean
  MakePageClean(pid);

  // ensure enough free buffer frame
  EnsureFreePages();

  // insert the pid to the buffer pool
  Ensure(resident_set_.Insert(pid));
  physical_used_cnt_++;
  // increment the buffer miss counter
  statistics::buffer::buffer_miss_cnt++;
}

void BufferManager::HandlePageFaultGC(pageid_t pid) {
  // GC read operation
  Ensure(GetPageState(pid).LockState() == sync::PageStateMode::EXCLUSIVE);
  Ensure(sm_->PIDOffsetIsValid(pid));
  Ensure(!frame_[pid].deallocated);

  if (physical_used_cnt_ >= physical_cnt_ - 1) {
    // handle page fault gc triggerd, need to evict more clean pages
    // cannot evict dirty pages
    EvictCleanPages(evict_batch_, sm_->PIDBlock(pid));
  }

  Ensure(physical_used_cnt_ < physical_cnt_);

  // issue GC read request
  io_interface_[worker_thread_id].ReadPage(pid, true);

  MakePageDirty(pid);
  // mark the location to indicate it is going through GC
  sm_->UpdatePID2Offset(pid, 0, 0, 0, 0, true);
  // must dirtify a page since it has to be relocated

  // mark that the page has been read bc of gc purpose
  ToPtr(pid)->gc_read = true;
  // insert to the buffer pool
  Ensure(resident_set_.Insert(pid));
  physical_used_cnt_++;
  statistics::buffer::buffer_miss_cnt++;
}

void BufferManager::ReadPage(pageid_t pid, bool gcread) {
  if (FLAGS_use_exmap) {
#ifdef ENABLE_EXMAP
    for (u64 repeat_counter = 0;; repeat_counter++) {
      struct iovec vec[1];
      vec[0].iov_base = ToPtr(pid);
      vec[0].iov_len = PAGE_SIZE;
      int ret = preadv(exmapfd_, &vec[0], 1, worker_thread_id);
      if (ret == PAGE_SIZE) {
        return;
      }
    }
#endif
  } else {
    Ensure(!sm_->PIDIsDeallocated(pid));
    Ensure(sm_->PIDOffsetIsValid(pid));
    io_interface_[worker_thread_id].ReadPage(pid, gcread);
  }
  statistics::buffer::read_cnt++;
}

void BufferManager::Evict(u32 evict_cnt) {
  std::vector<pageid_t> to_evict(0); // store all clean MARKED pages
  std::vector<pageid_t> to_write(0); // store all dirty MARKED pages
  u32 iteration = 0;
  // 0. find evict candidates, lock dirty ones in shared mode
  for (u64 idx = 0; idx < resident_set_.Capacity(); idx++) {
    // we gather enough pages for the eviction batch, break
    if (to_write.size() + to_evict.size() >= evict_cnt &&
        FLAGS_write_buffer_partition_cnt <= to_write.size()) {
      break;
    }
    auto flushed_log_gsn = recovery::LogManager::global_min_gsn_flushed.load();
    resident_set_.IterateClockBatch(evict_batch_, [this, &to_evict, &to_write,
                                                   &flushed_log_gsn, &evict_cnt,
                                                   &iteration](pageid_t pid) {
      iteration++;
      auto page = ToPtr(pid);
      auto &ps = GetPageState(pid);
      u64 v = ps.StateAndVersion();
      if (to_write.size() + to_evict.size() >= evict_cnt &&
          FLAGS_write_buffer_partition_cnt <= to_write.size()) {
        return;
      }
      // Example logic for processing a page
      switch (sync::PageState::LockState(v)) {
      case sync::PageState::MARKED:
        if (frame_[pid].prevent_evict.load()) {
          return; // Continue iteration
        }

        if ((frame_[pid].IsExtent() && frame_[pid].ToWriteExtent()) ||
            (!frame_[pid].IsExtent() &&
             ToPtr(pid)->dirty)) { //  && page->p_gsn <= flushed_log_gsn
          if (ps.TryLockShared(v)) {
            to_write.push_back(pid);
          }
        } else {
          if (!frame_[pid].dirty && !ToPtr(pid)->dirty) {
            to_evict.push_back(pid);
          }
        }
        break;
      case sync::PageState::UNLOCKED:
        ps.TryMark(v);
        break;
      default:
        break;
      }
    });

    if (iteration >= resident_set_.Capacity() - 1) {
      break;
    }
  }

  // we can't do anything, return
  if (to_evict.size() + to_write.size() == 0) {
    return;
  }

  if (to_write.size() >= FLAGS_write_buffer_partition_cnt) {
    WritePages(to_write, false, true);
  }
  // 2. try to X lock all clean page candidates
  std::erase_if(to_evict, [&](pageid_t pid) {
    sync::PageState &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();
    bool should_erase = false;

    if (sync::PageState::LockState(v) != sync::PageState::MARKED) {
      return true;
    }
    bool locked = ps.UnmarkAndLockExclusive(v);
    if (!locked) {
      u64 unused;
      ps.Unmark(v, unused);
      return true;
    } else {
      if (!sm_->PIDOffsetIsValid(pid) || frame_[pid].dirty ||
          frame_[pid].deallocated || ToPtr(pid)->dirty) {
        should_erase = true;
        ps.UnlockExclusive();
      } else {
        return false;
      }
    }
    return should_erase;
  });

  // 3. try to upgrade lock for dirty page candidates

  for (auto &pid : to_write) {
    sync::PageState &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();

    if (to_write.size() >= FLAGS_write_buffer_partition_cnt &&
        sm_->PIDOffsetIsValid(pid)) {
      if (ps.UpgradeLock(v) && !frame_[pid].prevent_evict) {
        // only this evict thread uses this page, we can remove it from buffer
        // pool
        to_evict.push_back(pid);
      } else {
        // others are using this page, thus we
        // don't evict it from buffer pool
        ps.UnlockShared();
      }
    } else {
      ps.UnlockShared();
    }
  }

  // 4. remove from page table
  u64 evict_size = 0;

  if (FLAGS_use_exmap) {
#ifdef ENABLE_EXMAP
    Ensure(to_evict.size() <= EXMAP_USER_INTERFACE_PAGES);
    for (size_t idx = 0; idx < to_evict.size(); idx++) {
      auto pid = to_evict[idx];
      evict_size = evict_size + frame_[pid].evict_pg_count;

      // For normal buffer pool, simulate lookup here
      for (auto idx = 0UL; idx < frame_[pid].evict_pg_count; idx++) {
        resident_set_.Contain(pid + idx);
      }
      exmap_interface_[worker_thread_id]->iov[idx].page = pid;
      exmap_interface_[worker_thread_id]->iov[idx].len =
          frame_[pid].evict_pg_count;
      madvise(ToPtr(pid), PAGE_SIZE, MADV_DONTNEED);

      // 5. remove from hash table and unlock
      bool ret = resident_set_.Remove(pid);
      if (!ret) {
        throw leanstore::ex::GenericException(
            fmt::format("Evict page {} but it not reside in Buffer Pool", pid));
      }
      GetPageState(pid).UnlockExclusiveAndEvict();
    }
    if (ExmapAction(exmapfd_, EXMAP_OP_FREE, to_evict.size()) < 0) {
      throw leanstore::ex::GenericException("ioctl: EXMAP_OP_FREE error");
    }
#endif
  } else {
    for (size_t idx = 0; idx < to_evict.size(); idx++) {
      pageid_t pid = to_evict[idx];
      evict_size++;
      logid_t p_gsn = ToPtr(pid)->p_gsn;
      // RemoveMinMaxEDT(pid);
      if (frame_[pid].deallocated) {

        EvictDeallocPage(pid);
      } else {
        madvise(ToPtr(pid), PAGE_SIZE, MADV_DONTNEED);
        Ensure(GetPageState(pid).LockState() == sync::PageStateMode::EXCLUSIVE);
        // 5. remove from hash table and unlock
        bool ret = resident_set_.Remove(pid);
        if (!ret) {
          throw leanstore::ex::GenericException(fmt::format(
              "Evict page {} but it not reside in Buffer Pool", pid));
        }
      }
      GetPageState(pid).UnlockExclusiveAndEvict();
    }
  }

  // update evict stats
  physical_used_cnt_ -= evict_size;
}

u64 BufferManager::EvictCleanPages(u64 evict_cnt, blockid_t bid) {
  if (evict_cnt == 0) {
    return 0;
  }

  std::vector<pageid_t> to_evict; // store all clean pages and lock exclusive
  u64 clean_cnt = 0;
  u64 dirty_cnt = 0;
  u64 iteration = 0;
  u64 marked_cnt = 0;
  u64 unlocked_cnt = 0;
  u64 invalid_space = 0;
  u64 gcwblock = 0;
  u64 invalid_pid = 0;
  u64 gc_block_pid = 0;
  u64 unmark_fail = 0;
  u64 null_pid = 0;
  u64 ee_pids = 0;
  u64 slocked = 0;
  u64 marked = 0;
  u64 unlocked = 0;

  // 0. find evict candidates, lock dirty ones in shared mode
  const u64 resident_size = resident_set_.Count() - 1;

  u64 start_idx = 0;

  // static thread_local std::mt19937_64 rng(std::random_device{}());
  // std::uniform_int_distribution<u64> dist(0, io_handler_cnt_ -
  //                                                1); // Exclusive upper bound
  start_idx = worker_thread_id * (resident_size / io_handler_cnt_);

  for (u64 idx = start_idx;; idx++) {
    if (to_evict.size() >= evict_cnt) {
      break;
    }
    if (idx >= resident_set_.Count() - 1) {
      idx = 0;
    }

    pageid_t pid = resident_set_.GetPIDToEvict(iteration, 1);

    iteration++;
    auto page = ToPtr(pid);

    if (pid == NULL_PID || sm_->PIDBlock(pid) == bid ||
        !sm_->PIDOffsetIsValid(pid)) {
      null_pid++;
      continue;
    }

    if (ToPtr(pid)->dirty) {
      dirty_cnt++;
      continue;
    }

    auto &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();
    switch (sync::PageState::LockState(v)) {
    case sync::PageState::UNLOCKED:
      unlocked++;
      if (ps.TryLockExclusive(v)) {
        to_evict.push_back(pid);
      }
      break;
    case sync::PageState::MARKED:
      if (ps.UnmarkAndLockExclusive(v)) {
        if (sm_->PIDOffsetIsValid(pid)) {
          to_evict.push_back(pid);
        } else {
          unmark_fail++;
          GetPageState(pid).UnlockExclusive();
        }
      }
      break;
    case sync::PageState::EXCLUSIVE:
    case sync::PageState::EVICTED:
      ee_pids++;
      break;
    default:
      slocked++;
      break;
    }
  }

  // 4. remove from page table
  u64 evict_size = 0;

  if (FLAGS_use_exmap) {
#ifdef ENABLE_EXMAP
    Ensure(to_evict.size() <= EXMAP_USER_INTERFACE_PAGES);
    for (size_t idx = 0; idx < to_evict.size(); idx++) {
      auto pid = to_evict[idx];
      evict_size = evict_size + frame_[pid].evict_pg_count;

      // For normal buffer pool, simulate lookup here
      for (auto idx = 0UL; idx < frame_[pid].evict_pg_count; idx++) {
        resident_set_.Contain(pid + idx);
      }
      exmap_interface_[worker_thread_id]->iov[idx].page = pid;
      exmap_interface_[worker_thread_id]->iov[idx].len =
          frame_[pid].evict_pg_count;
    }
    if (ExmapAction(exmapfd_, EXMAP_OP_FREE, to_evict.size()) < 0) {
      throw leanstore::ex::GenericException("ioctl: EXMAP_OP_FREE error");
    }
#endif
  } else {
    for (size_t idx = 0; idx < to_evict.size(); idx++) {
      pageid_t pid = to_evict[idx];
      evict_size++;
      madvise(ToPtr(pid), PAGE_SIZE, MADV_DONTNEED);
    }
  }

  // 5. remove from hash table and unlock
  for (auto &pid : to_evict) {
    bool ret = resident_set_.Remove(pid);
    if (!ret) {
      throw leanstore::ex::GenericException(fmt::format(
          "Evict clean page {} but it not reside in Buffer Pool", pid));
    }
    sync::PageState &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();
    ps.UnlockExclusiveAndEvict();
  }

  // we can't do anything, return
  if (evict_size == 0) {
    return 0;
  }

  // update evict stats
  physical_used_cnt_ -= evict_size;

  // evict size smaller than the target size
  if (evict_size < evict_cnt) {
    evict_cnt -= evict_size;
  }
  return evict_size;
}

bool BufferManager::EvictDeallocPage(pageid_t pid) {
  Ensure(GetPageState(pid).LockState() == sync::PageStateMode::EXCLUSIVE);
  logid_t p_gsn = ToPtr(pid)->p_gsn;

  // remove the deallocated pid from the buffer pool
  madvise(ToPtr(pid), PAGE_SIZE, MADV_DONTNEED);
  bool ret = resident_set_.Remove(pid);
  if (!ret) {
    throw leanstore::ex::GenericException(fmt::format(
        "Evict deallocated page {} but it not reside in Buffer Pool", pid));
  } else {
    physical_used_cnt_--;
  }

  // update space info, size PAGE_SIZE - 1 means pid has been deallocated
  if (FLAGS_use_out_of_place_write) {
    sm_->UpdatePID2Offset(pid, 0, PAGE_SIZE - 1, 0, 0, false);
  } else {
    sm_->UpdatePID2Offset(pid, 0, PAGE_SIZE - 1, sm_->block_cnt_, 0, false);
    sm_->AddPIDToDeallocList(
        worker_thread_id, pid,
        recovery::LogManager::global_min_gsn_flushed.load());
  }
  // dealloc succeeded
  return true;
}

void BufferManager::WritePages(std::vector<pageid_t> &to_write, bool gcwrite,
                               bool evict) {
  if (to_write.size() == 0) {
    return;
  }

  // reset write buffer
  io_interface_[worker_thread_id].ResetUserWriteBuffer(to_write.size());

  for (auto &pid : to_write) {
    logid_t edt = 0;
    // EDT enabled, collect write histories per PID
    if (FLAGS_use_edt) {
      CalcEDT(pid);
      if (ToPtr(pid)->CountValidWriteHistories() < WRITE_HISTORY_LENGTH) {
        edt = ToPtr(pid)->CountValidWriteHistories() - 1;
      } else {
        edt = ToPtr(pid)->CountValidWriteHistories() - 1;
      }

      edt = ToPtr(pid)->EstimateDeathTime(cur_lsn_gap_per_wgroup.load());
    }
    // add page to write buffer
    io_interface_[worker_thread_id].AddPageToWriteBuffer(ToPtr(pid), pid, edt);
    // update min max edt if necessary
    UpdateMinMaxEDT(pid);
  }

  // flush write buffer
  if (!FLAGS_use_out_of_place_write) {
    WritePagesInPlace(to_write, evict);
  } else {
    WritePagesOutOfPlace(to_write, evict);
    UpdateLSNGap();
  }

  io_interface_[worker_thread_id].ResetUserWriteBuffer(0);

  // update buffer stats
  statistics::buffer::write_cnt += to_write.size();
  statistics::buffer::cum_total_writes_cnt += to_write.size();
}

void BufferManager::UpdateLSNGap() {
  u64 current_writes =
      statistics::buffer::cum_total_writes_cnt.load(std::memory_order_relaxed);
  u64 current_threshold = next_write_threshold.load(std::memory_order_relaxed);

  // First time setup
  if (current_threshold == 0) {
    current_threshold = sm_->page_cnt_per_block_ * sspace_->max_open_block_;
    next_write_threshold.store(current_threshold, std::memory_order_relaxed);
  }

  // Check if we've passed the threshold
  if (current_writes >= current_threshold) {
    u64 global_min_gsn = recovery::LogManager::global_min_gsn_flushed.load(
        std::memory_order_relaxed);

    if (prev_lsn_gap_per_wgroup.load(std::memory_order_relaxed) == 0 &&
        cur_lsn_gap_per_wgroup.load(std::memory_order_relaxed) == 0) {

      cur_lsn_gap_per_wgroup.store(global_min_gsn, std::memory_order_relaxed);
      lsn_gap.store(global_min_gsn, std::memory_order_relaxed);
      prev_lsn_gap_per_wgroup.store(global_min_gsn, std::memory_order_relaxed);

    } else {
      cur_lsn_gap_per_wgroup.store(global_min_gsn, std::memory_order_relaxed);

      u64 prev_gsn = prev_lsn_gap_per_wgroup.load(std::memory_order_relaxed);
      u64 gap = global_min_gsn - prev_gsn;
      if (gap >= 100) {
        lsn_gap.store(gap, std::memory_order_relaxed);
        prev_lsn_gap_per_wgroup.store(global_min_gsn,
                                      std::memory_order_relaxed);
      }
    }

    u64 prev_write_cnt = write_cnt_per_interval.load(std::memory_order_relaxed);
    write_cnt_per_interval.store(current_writes - prev_write_cnt,
                                 std::memory_order_relaxed);

    // Move threshold forward to the next multiple
    next_write_threshold.store(current_threshold + (sm_->page_cnt_per_block_ *
                                                    sspace_->max_open_block_),
                               std::memory_order_relaxed);
  }
}

void BufferManager::WritePagesInPlace(std::vector<pageid_t> &to_write,
                                      bool evict) {
  io_interface_[worker_thread_id].FlushWriteBuffer();

  for (auto &pid : to_write) {
    MakePageClean(pid);
    if (!evict) {
      sync::PageState &ps = GetPageState(pid);
      ps.UnlockShared();
    }
  }
  return;
}

void BufferManager::WritePagesOutOfPlace(std::vector<pageid_t> &to_write,
                                         bool evict) {
  // oop write

  // binpacking into groups
  io_interface_[worker_thread_id].BinPacking(
      recovery::LogManager::global_min_gsn_flushed.load(), min_edt.load(),
      max_edt.load());

  // write binpacked pages per group
  for (u32 idx = 0; idx < FLAGS_write_buffer_partition_cnt; idx++) {
    // select a block to write
    blockid_t bid = io_interface_[worker_thread_id].SelectBlockIDToWrite(
        idx, min_edt.load(), max_edt.load());

    // check if bid requires gc
    if (sm_->BlockNeedsGC(bid)) {
      blockid_t wbid = GCValidPIDs(bid, true, idx);
      io_interface_[worker_thread_id].FlushWriteBufferPerGroup(
          idx, wbid, min_edt.load(), max_edt.load());
    } else {
      // flush write buffer per group
      io_interface_[worker_thread_id].FlushWriteBufferPerGroup(
          idx, bid, min_edt.load(), max_edt.load());
    }
    for (u32 p = 0;
         p < io_interface_[worker_thread_id].GetWbufferPIDCntPerGroup(idx);
         p++) {
      pageid_t pid =
          io_interface_[worker_thread_id].GetWbufferPIDPerGroup(idx, p);
      MakePageClean(pid);
      if (!evict) {
        sync::PageState &ps = GetPageState(pid);
        ps.UnlockShared();
      }
    }
  }
}

// group idx pass it as a parameter
u64 BufferManager::GCValidPIDs(blockid_t read_bid, bool sync_gc,
                               u32 group_idx) {
  // write previously gc-read blocks
  blockid_t write_bid = sspace_->block_cnt_;
  write_bid = GCWriteValidPIDs(group_idx);

  // get the number of pids on disk to read
  u64 clean_evict_cnt = GCValidPIDCntInBlockOnDisk(read_bid);

  // read valid pids from the disk
  std::vector<pageid_t> read_pids = GCReadValidPIDs(read_bid, sync_gc);

  read_pids.clear();
  return sm_->block_cnt_;
}

blockid_t BufferManager::GCWriteValidPIDs(u32 idx) {
  std::vector<blockid_t> write_bids = sm_->SelectReadGCedBlocksToGCWrite(
      io_interface_[worker_thread_id].GetWbufferSizePerGroup(idx));

  if (write_bids.empty()) {
    return sm_->block_cnt_;
  }

  logid_t curts = recovery::LogManager::global_min_gsn_flushed.load();

  u64 dirtified_cnt = 0;
  std::vector<pageid_t> to_write_pids;
  u64 write_cnt = 0;
  u32 write_bid_cnt = write_bids.size();

  // Phase 1: Collect candidate PIDs from selected GC victim blocks
  for (blockid_t bid : write_bids) {
    for (u32 p = 0; p < sspace_->page_cnt_per_block_; ++p) {
      u32 pcnt = sspace_->blocks[bid].pages[p].cnt;
      for (u8 pidx = 0; pidx < pcnt; ++pidx) {
        pageid_t pid = sspace_->blocks[bid].pages[p].pids[pidx];
        if (FixShareImplGC(pid, bid, p, pidx, &dirtified_cnt, idx)) {
          to_write_pids.push_back(pid);
          // Do not call CalcEDT here — only after validation!
        }
      }
    }
  }

  // Optional: Reorder blocks based on WA-awareness
  if ((FLAGS_use_SSDWA1_pattern) && write_bids.size() > 1) {
    std::sort(write_bids.begin(), write_bids.end(),
              [&](blockid_t a, blockid_t b) {
                return sspace_->block_metadata[a].block_invalidation_cnt >
                       sspace_->block_metadata[b].block_invalidation_cnt;
              });
  }

  // Phase 2: Filter and sort by EstimateDeathTime (EDT)
  std::vector<pageid_t> sorted_valid_pids;

  for (auto it = to_write_pids.begin(); it != to_write_pids.end();) {
    pageid_t pid = *it;
    auto &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();

    // Skip invalid/evicted/exclusive pages
    if (sync::PageState::LockState(v) == sync::PageState::EVICTED ||
        sync::PageState::LockState(v) == sync::PageState::EXCLUSIVE ||
        !IsValidPtr(ToPtr(pid))) {
      it = to_write_pids.erase(it);
      continue;
    }

    // Compute EDT only after page is validated
    logid_t edt = 0;
    if (FLAGS_use_edt) {
      edt = ToPtr(pid)->EstimateNextGCWrite(curts);
    }

    // Sort by descending EDT (higher EDT = later death = lower priority)
    auto sorted_it =
        std::lower_bound(sorted_valid_pids.begin(), sorted_valid_pids.end(),
                         edt, [&](pageid_t other_pid, logid_t val) {
                           logid_t other_edt =
                               ToPtr(other_pid)->EstimateNextGCWrite(curts);
                           return other_edt > val;
                         });
    sorted_valid_pids.insert(sorted_it, pid);
    it = to_write_pids.erase(it); // remove from original list
  }

  to_write_pids = std::move(sorted_valid_pids); // overwrite original list

  // Phase 3: Flush valid pages in batches
  std::vector<pageid_t> gc_batch;
  blockid_t cur_w_bid = write_bids[0];
  u32 cur_w_idx = 0;
  u64 remaining_bytes_in_block = sm_->max_w_ptr_;
  u64 gc_batch_ = sspace_->page_cnt_per_block_;
  u64 batch_bytes = gc_batch_ * PAGE_SIZE;
  logid_t prev_min_edt = 0;
  logid_t min_edt = std::numeric_limits<logid_t>::max(); // initialize to max
  u32 bidcnt = write_bids.size();

  io_interface_[worker_thread_id].ResetGCWriteBuffer(batch_bytes, gc_batch_);

  for (auto it = to_write_pids.begin(); it != to_write_pids.end();) {
    pageid_t pid = *it;
    auto &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();

    if (sync::PageState::LockState(v) == sync::PageState::EVICTED ||
        sync::PageState::LockState(v) == sync::PageState::EXCLUSIVE) {
      it = to_write_pids.erase(it);
      continue;
    }

    if (!FixShareCachedPage(pid)) {
      it = to_write_pids.erase(it);
      continue;
    }

    logid_t edt = 0;
    if (FLAGS_use_edt) {
      edt = ToPtr(pid)->EstimateNextGCWrite(curts);
      ToPtr(pid)->UpdateLastGCWrite(curts);
      ToPtr(pid)->gc_read = false;
      min_edt = std::min(min_edt, edt); // Track minimum EDT in current block
    }

    gc_batch.push_back(pid);
    write_cnt++;

    io_interface_[worker_thread_id].AddPageToGCWriteBuffer(ToPtr(pid), pid,
                                                           edt);

    remaining_bytes_in_block -= PAGE_SIZE;
    it = to_write_pids.erase(it); // handled

    if (gc_batch.size() >= gc_batch_ || remaining_bytes_in_block < PAGE_SIZE) {
      bool is_final_flush = remaining_bytes_in_block < PAGE_SIZE;
      bool find_other_block =
          ((edt != curts * 2) && FLAGS_use_edt) ? true : false;
      FlushValidPIDsToBlock(cur_w_bid, gc_batch, is_final_flush, prev_min_edt,
                            find_other_block);
      gc_batch.clear();

      if (is_final_flush) {
        write_bids.erase(write_bids.begin() + cur_w_idx);
        if (write_bids.empty())
          break;
        cur_w_bid = write_bids[cur_w_idx];
        remaining_bytes_in_block = sm_->max_w_ptr_;

        // propagate current min_edt to prev_min_edt and reset
        prev_min_edt = min_edt;
        min_edt = std::numeric_limits<logid_t>::max(); // reset for next block
      }
    }
  }

  // Phase 4: Cleanup — restore valid PIDs we skipped due to invalid state
  if (write_bids.empty() && !to_write_pids.empty()) {
    for (pageid_t pid : to_write_pids) {
      if (FixShareCachedPage(pid)) {
        MakePageDirty(pid);
        GetPageState(pid).UnlockShared();
      }
    }
  }

  // Phase 5: Flush any leftovers
  if (!write_bids.empty()) {
    for (blockid_t bid : write_bids) {
      cur_w_bid = bid;
      FlushValidPIDsToBlock(cur_w_bid, gc_batch, true, min_edt, false);
      gc_batch.clear();
    }
  }

  io_interface_[worker_thread_id].ResetGCWriteBuffer(0, 0);

  write_bids.clear();
  to_write_pids.clear();
  return cur_w_bid;
}

void BufferManager::FlushValidPIDsToBlock(blockid_t bid,
                                          std::vector<pageid_t> gc_target,
                                          bool final, logid_t prev_min_edt,
                                          bool find_other_block) {
  io_interface_[worker_thread_id].FlushGCWriteBuffer(
      bid, final, min_edt.load(), max_edt.load(), find_other_block);

  statistics::buffer::write_cnt += gc_target.size();
  statistics::buffer::cum_total_writes_cnt += gc_target.size();

  for (pageid_t pid : gc_target) {
    MakePageClean(pid);
    GetPageState(pid).UnlockShared();
  }

  if (final) {
    sm_->MoveGCedBlockToOpenList(bid, prev_min_edt, lsn_gap.load(),
                                 min_edt.load(), max_edt.load());
  }
  u64 gc_batch_ = sspace_->page_cnt_per_block_;
  io_interface_[worker_thread_id].ResetGCWriteBuffer(gc_batch_ * PAGE_SIZE,
                                                     gc_batch_);
}

void BufferManager::CalcEDT(pageid_t pid) {
  logid_t cur_ts = recovery::LogManager::global_min_gsn_flushed.load();
  Ensure(cur_ts != 0);

  if (ToPtr(pid)->CountValidWriteHistories() >= 2) {
    logid_t prev_edt =
        ToPtr(pid)->EstimateDeathTime(cur_lsn_gap_per_wgroup.load());
    logid_t gap =
        (prev_edt > cur_ts) ? (prev_edt - cur_ts) : (cur_ts - prev_edt);
    statistics::buffer::edt_gap += gap;
    statistics::buffer::edt_cnt++;
  }

  ToPtr(pid)->AddWriteGSN(cur_ts);
  Ensure(ToPtr(pid)->CountValidWriteHistories());

  // Update min_edt (atomically)
  UpdateMinMaxEDT(pid);
}

void BufferManager::UpdateMinMaxEDT(pageid_t pid) {
  if (!FLAGS_use_edt)
    return;
  logid_t edt = ToPtr(pid)->EstimateDeathTime(cur_lsn_gap_per_wgroup.load());

  if (ToPtr(pid)->gc_read) {
    return;
    edt = ToPtr(pid)->EstimateNextGCWrite(
        recovery::LogManager::global_min_gsn_flushed.load());
  }

  if (min_edt.load() < cur_lsn_gap_per_wgroup.load()) {
    min_edt.store(cur_lsn_gap_per_wgroup.load(), std::memory_order_relaxed);
  }

  // Update min_edt
  logid_t cur_min = min_edt.load(std::memory_order_relaxed);
  while (edt < cur_min &&
         !min_edt.compare_exchange_weak(cur_min, edt, std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
    // cur_min is updated with the latest observed value
  }

  // Update max_edt
  logid_t cur_max = max_edt.load(std::memory_order_relaxed);
  while (edt > cur_max &&
         !max_edt.compare_exchange_weak(cur_max, edt, std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
    // cur_max is updated with the latest observed value
  }
}

void BufferManager::RemoveMinMaxEDT(pageid_t pid) {
  if (!FLAGS_use_edt)
    return;

  logid_t target_edt =
      ToPtr(pid)->EstimateDeathTime(cur_lsn_gap_per_wgroup.load());

  if (ToPtr(pid)->gc_read &&
      ToPtr(pid)->prev_gcwrite <
          recovery::LogManager::global_min_gsn_flushed.load()) {
    target_edt =
        ToPtr(pid)->EstimateDeathTime(cur_lsn_gap_per_wgroup.load()) >=
                recovery::LogManager::global_min_gsn_flushed.load()
            ? ToPtr(pid)->EstimateDeathTime(cur_lsn_gap_per_wgroup.load())
            : ToPtr(pid)->EstimateNextGCWrite(
                  recovery::LogManager::global_min_gsn_flushed
                      .load()); //  * (sspace_->block_metadata[cur_w_bid].block_invalidation_cnt + 1)
  }

  if (target_edt < recovery::LogManager::global_min_gsn_flushed.load())
    return;

  // Invalidate min_edt if this page held it
  logid_t cur_min = min_edt.load(std::memory_order_relaxed);
  if (target_edt == cur_min) {
    min_edt.store(std::numeric_limits<logid_t>::max(),
                  std::memory_order_relaxed);
  }

  // Invalidate max_edt if this page held it
  logid_t cur_max = max_edt.load(std::memory_order_relaxed);
  if (target_edt == cur_max) {
    max_edt.store(0, std::memory_order_relaxed);
  }
}

std::vector<pageid_t> BufferManager::GCReadValidPIDs(blockid_t bid,
                                                     bool sync_gc) {
  // read valid pids from disk
  u64 valid_cnt = sm_->ValidPIDsCntInBlock(bid);
  u64 gced_cnt = sspace_->page_cnt_per_block_;

  statistics::storage::space::valid_page_cnt += valid_cnt;
  statistics::storage::space::gced_page_cnt += gced_cnt;

  std::vector<pageid_t> read_pids_from_disk = GCReadValidPIDsInBlockOnDisk(bid);

  sm_->UpdateBlockMetadataAfterReadGC(bid);

  read_pids_from_disk.clear();

  return read_pids_from_disk;
}

std::vector<pageid_t>
BufferManager::GCReadValidPIDsInBlockOnDisk(blockid_t bid) {
  std::vector<pageid_t> to_read;
  std::vector<pageid_t> all_read;
  u64 gc_batch = sm_->ValidPIDsCntInBlock(bid);

  for (u32 p = 0; p < sspace_->page_cnt_per_block_; p++) {
    u32 pcnt = sspace_->blocks[bid].pages[p].cnt;

    for (u8 pidx = 0; pidx < pcnt; pidx++) {
      pageid_t pid = sm_->GetPIDFromOffset(bid, p, pidx);
      if (!sm_->PageIsValid(bid, p, pidx) || bid != sm_->PIDBlock(pid)) {
        continue;
      }

      if (physical_used_cnt_ >= physical_cnt_ * 0.99) {
        break;
      }

      sync::PageState &ps = GetPageState(pid);
      u64 v = ps.StateAndVersion();

      if (sync::PageState::LockState(v) == sync::PageState::EVICTED) {
        if (FixExclusiveOnDiskGC(pid, bid, p, pidx)) {
          to_read.push_back(pid);
        }
      }

      if (to_read.size() >= gc_batch) {
        break;
      }
    }

    if (to_read.size() >= gc_batch) {
      break;
    }
  }

  // === Perform a single batched Read ===
  if (!to_read.empty()) {
    io_interface_[worker_thread_id].ReadPagesOutOfPlace(to_read);

    // === Evict clean pages for this batch ===
    u64 evicted_cnt = EvictCleanPages(to_read.size(), bid);

    // === Insert to buffer pool ===
    for (auto &pid : to_read) {
      Ensure(resident_set_.Insert(pid));
      physical_used_cnt_++;
      MakePageDirty(pid);
      ToPtr(pid)->gc_read = true;
      GetPageState(pid).UnlockExclusive();
      all_read.push_back(pid);
    }

    statistics::buffer::buffer_miss_cnt += all_read.size();
  }

  return all_read;
}

u64 BufferManager::GCValidPIDCntInBlockOnDisk(blockid_t bid) {
  u64 valid_cnt = 0;
  u64 it = 0;
  for (u32 p = 0; p < sspace_->page_cnt_per_block_; p++) {
    u32 pcnt = sspace_->blocks[bid].pages[p].cnt;
    for (u8 pidx = 0; pidx < pcnt; pidx++) {
      pageid_t pid = sm_->GetPIDFromOffset(bid, p, pidx);
      sync::PageState &ps = GetPageState(pid);
      u64 v = ps.StateAndVersion();
      if (sync::PageState::LockState(v) == sync::PageState::EVICTED) {

        if (sm_->PageIsValid(bid, p, pidx) && sm_->PIDOffsetIsValid(pid)) {
          valid_cnt++;
        }
      }
    }
  }
  return valid_cnt;
}

u64 BufferManager::WriteDirtyPages(u32 write_cnt, u32 shard_idx, u32 shard_cnt,
                                   bool gc) {
  if (write_cnt == 0) {
    return 0;
  }

  std::vector<pageid_t> to_write(0); // store all dirty MARKED pages
  std::vector<pageid_t> to_evict(0); // store all clean MARKED pages
  u64 iterations = 0;
  u64 clean_cnt = 0;
  u64 total_write_cnt = 0;
  u64 shard_size = resident_set_.Count() / shard_cnt;
  u64 idx = 0;

  // u32 start_idx = shard_idx ==0 ? shard_cnt -1: shard_idx - 1;
  for (idx = shard_idx * shard_size;; idx++) {
    // Gather enough pages for to write, then break
    pageid_t pid = resident_set_.GetPID(idx);
    iterations++;
    if (idx >= resident_set_.Count() - 1) {
      idx = 0;
    }
    auto page = ToPtr(pid);

    if (pid == NULL_PID || pid == 0) {
      continue;
    }

    if (to_write.size() >= write_cnt) {
      break;
    }
    auto &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();

    switch (sync::PageState::LockState(v)) {
    case sync::PageState::EVICTED:
    case sync::PageState::EXCLUSIVE:
      break;
      // If this page is prevented from eviction, skip it
    default:
      if (!frame_[pid].dirty && !ToPtr(pid)->dirty) {
        clean_cnt++;
      }

      if ((ToPtr(pid)->dirty)) {
        if (ps.TryLockShared(v)) {
          to_write.push_back(pid);
        }
      }
    }

    if (iterations >= resident_set_.Count()) {
      break;
    }
  }
  // we can't do anything, return
  if (to_write.size() == 0) {
    return 0;
  }
  // write dirty pages and make it clean
  if (to_write.size() >= FLAGS_write_buffer_partition_cnt) { //
    bool gcwrite = (shard_cnt == io_handler_cnt_) ? true : false;
    WritePages(to_write, gcwrite, false);
    total_write_cnt = to_write.size();
  }

  return total_write_cnt;
}

logid_t BufferManager::FuzzyCheckpoint(logid_t target_gsn, u32 shard_idx,
                                       u32 shard_cnt) {
  u64 shard_size = (resident_set_.Count() / shard_cnt);
  u64 start_idx = shard_idx * shard_size;
  u64 idx = 0;
  u64 end_idx = (1 + shard_idx) * shard_size;
  u64 last_batch_idx = end_idx - (end_idx % evict_batch_) - 1;
  u64 clean_page_cnt = 0;
  u32 cped_cnt = 0;

  logid_t flushed_log_gsn;
  pageid_t pid;
  logid_t min_flushed_gsn = UINT64_MAX;
  logid_t p_min_p_gsn = target_gsn;
  u64 invalid_cnt = 0;

  std::vector<pageid_t> batch_pids;
  batch_pids.reserve(evict_batch_);

  for (idx = start_idx; idx < end_idx; idx++) {
    pid = resident_set_.GetPID(idx);
    flushed_log_gsn = recovery::LogManager::global_min_gsn_flushed.load();
    auto page = ToPtr(pid);

    if (pid == NULL_PID || pid == 0 || !IsValidPtr(page)) {
      invalid_cnt++;
      continue;
    }

    sync::PageState &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion();
    if (sync::PageState::LockState(v) != sync::PageState::EVICTED) {
      p_min_p_gsn = frame_[pid].min_p_gsn;
      // Check conditions for checkpoint
      if ((ToPtr(pid)->dirty && frame_[pid].min_p_gsn > 0 &&
           !frame_[pid].deallocated && frame_[pid].min_p_gsn <= target_gsn &&
           page->p_gsn <= flushed_log_gsn)) {

        if (FixShareCachedPage(pid)) {
          batch_pids.push_back(pid);
          cped_cnt++;

          if (min_flushed_gsn > p_min_p_gsn && !p_min_p_gsn) {
            min_flushed_gsn = p_min_p_gsn;
          }
        } else {
          if (!frame_[pid].dirty && !ToPtr(pid)->dirty) {
            clean_page_cnt++;
          }
        }

        if (batch_pids.size() >= evict_batch_ && idx != last_batch_idx) {
          clean_page_cnt += batch_pids.size();
          WritePages(batch_pids, false, false);
          batch_pids.clear();
        }
      }
    }
  }

  if (batch_pids.size() >= FLAGS_write_buffer_partition_cnt) {
    WritePages(batch_pids, false, false);
    clean_page_cnt += batch_pids.size();
  }
  if (!batch_pids.empty() &&
      batch_pids.size() < FLAGS_write_buffer_partition_cnt) {
    for (auto &pid : batch_pids) {
      sync::PageState &ps = GetPageState(pid);
      ps.UnlockShared();
    }
  }

  batch_pids.clear();

  min_flushed_gsn = target_gsn;

  // if ( FLAGS_use_out_of_place_write && clean_page_cnt <
  // sspace_->page_cnt_per_block_ / shard_cnt ) {
  //   u64 write_cnt = target_gsn == 1
  //                       ? sspace_->page_cnt_per_block_ / shard_cnt
  //                       : sspace_->page_cnt_per_block_ / shard_cnt;

  //   // write_cnt = RoundUpToPowerOfTwo(write_cnt);

  //   u64 cur_w_cnt = 0;
  //   while (cur_w_cnt < write_cnt) {
  //     u64 target_cnt = write_cnt - cur_w_cnt >= evict_batch_
  //                          ? evict_batch_
  //                          : write_cnt - cur_w_cnt;
  //     u64 cur_cnt = WriteDirtyPages(target_cnt, shard_idx, shard_cnt, false);
  //     cur_w_cnt += cur_cnt;
  //     if (cur_cnt < evict_batch_) {
  //       break;
  //     }
  //   }
  // }else{
  //   if(!FLAGS_use_out_of_place_write){
  //   u64 write_cnt  = (shard_size  - clean_page_cnt) /  shard_cnt;
  //   u64 cur_w_cnt = 0;
  //   while (cur_w_cnt < write_cnt) {
  //     u64 target_cnt = write_cnt - cur_w_cnt >= evict_batch_
  //                          ? evict_batch_
  //                          : write_cnt - cur_w_cnt;
  //     u64 cur_cnt = WriteDirtyPages(target_cnt, shard_idx, shard_cnt, false);
  //     cur_w_cnt += cur_cnt;
  //     if (cur_cnt < evict_batch_) {
  //       break;
  //     }
  //   }
  //   }

  // }
  return min_flushed_gsn;
}

void BufferManager::FlushAll() {
  while (physical_used_cnt_) {
    Evict(evict_batch_);
  }
}
// ------------------------------------------------------------------------------------------------------

/**
 * @brief Try to find a reusable free extent in the Free Page manager
 * If there is one + that found extent is larger than the asked size,
 *  split that extent into multiple smaller extents and set necessary env
 */
auto BufferManager::TryReuseExtent(u64 required_page_cnt, bool is_special_block,
                                   pageid_t &out_start_pid) -> bool {
  storage::TierList split_extents;
  bool found_ext_was_evicted = false;

  auto found_range = free_pages_->RequestFreeExtent(
      required_page_cnt,
      [&](pageid_t pid) {
        auto &ps = GetPageState(pid);
        u64 v = ps.StateAndVersion().load();
        if (!ps.TryLockExclusive(v)) {
          return false;
        }
        if (sync::PageState::LockState(v) == sync::PageState::EVICTED) {
          found_ext_was_evicted = true;
        }
        return true;
      },
      out_start_pid, split_extents);

  /* TODO(Duy): The below is too complicated, should simplify it */
  if (found_range) {
    if (found_ext_was_evicted) {
      PAGE_FAULT(out_start_pid, required_page_cnt);
      if (is_special_block) {
        PrepareTailExtent(true, out_start_pid, required_page_cnt);
      } else {
        PrepareExtentEnv(out_start_pid, required_page_cnt);
      }
    } else {
      // Resident Set should already contain this `out_start_pid`
      if (is_special_block) {
        storage::TailExtent::SplitToExtents(
            out_start_pid, required_page_cnt,
            [&](pageid_t pid, extidx_t index) {
              if (pid != out_start_pid) {
                NEW_ALLOC_PAGE(pid);
                resident_set_.Insert(pid);
              }
              EXTENT_FRAME_SET(pid, ExtentList::ExtentSize(index));
            });
      } else {
        EXTENT_FRAME_SET(out_start_pid, required_page_cnt);
      }
      /**
       * If the splitted extents are already in memory, we need to:
       * - Acquire X-lock on them before modify their state, i.e.
       * NEW_ALLOC_PAGE()
       * - Add them to resident_set and set the evict_pg_count, i.e.
       * PrepareExtentEnv()
       * - Unlock X-lock to allow later transaction reuse them
       */
      for (auto &extent : split_extents) {
        NEW_ALLOC_PAGE(extent.start_pid);
        PrepareExtentEnv(extent.start_pid,
                         ExtentList::ExtentSize(extent.tier_index));
        UnfixExclusive(extent.start_pid);
      }
    }
  }

  return found_range;
}

/**
 * @brief Allocate a single extent whose id is `extent_id`
 * `fixed_page_cnt` = 0 most of the time, and should only be set for Blob's
 * TailExtent
 */
auto BufferManager::AllocExtent(extidx_t extent_idx, u64 fixed_page_cnt)
    -> pageid_t {
  Ensure(fixed_page_cnt <= ExtentList::ExtentSize(extent_idx));
  auto is_special_block = fixed_page_cnt != 0;
  u64 required_page_cnt =
      is_special_block ? fixed_page_cnt : ExtentList::ExtentSize(extent_idx);

  // Alloc new extent
  pageid_t start_pid;
  auto found_range =
      TryReuseExtent(required_page_cnt, is_special_block, start_pid);

  if (!found_range) {
    Ensure(alloc_cnt_ + required_page_cnt < virtual_cnt_);
    PAGE_FAULT(start_pid, required_page_cnt);

    // Determine whether we need to prepare env for all split extents or a
    // single one
    if (!is_special_block) {
      NEW_ALLOC_PAGE(start_pid);
      PrepareExtentEnv(start_pid, ExtentList::ExtentSize(extent_idx));
    } else {
      Ensure(fixed_page_cnt < ExtentList::ExtentSize(extent_idx));
      PrepareTailExtent(false, start_pid, fixed_page_cnt);
    }
  }

  return start_pid;
}

void BufferManager::PrepareExtentEnv(pageid_t start_pid, u64 page_cnt) {
  assert(ExtentList::TierIndex(page_cnt, true) < ExtentList::NO_TIERS);
  resident_set_.Insert(start_pid);
  EXTENT_FRAME_SET(start_pid, page_cnt);
  if (FLAGS_bm_enable_fair_eviction) {
    UpdateMax(maximum_page_size_, page_cnt);
  }
}

void BufferManager::PrepareTailExtent(bool already_lock_1st_extent,
                                      pageid_t start_pid, u64 page_cnt) {
  storage::TailExtent::SplitToExtents(
      start_pid, page_cnt, [&](pageid_t pid, extidx_t index) {
        if (pid != start_pid || !already_lock_1st_extent) {
          NEW_ALLOC_PAGE(pid);
        }
        PrepareExtentEnv(pid, ExtentList::ExtentSize(index));
      });
}

/**
 * @brief Prepare necessary env for the extent eviction later
 *
 * The caller must ensure that `start_pid` is the start PID of an actual Extent
 */
void BufferManager::PrepareExtentEviction(pageid_t start_pid) {
  if (FLAGS_blob_logging_variant < 0) {
    frame_[start_pid].SetFlag(PageFlagIdx::EXTENT_TO_WRITE);
  } else {
    frame_[start_pid].prevent_evict = true;
  }
  UnfixExclusive(start_pid);
}

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Evict an extent from the buffer manager
 * The caller is responsible for ensuring that [start_pid..start_pid+page_cnt)
 * should refer to an existing extent
 */
void BufferManager::EvictExtent(pageid_t pid, u64 page_cnt) {
  assert(FLAGS_blob_logging_variant >= 0);

  /**
   * It's possible that this Extent was already evicted in a batch
   * The scenario is:
   *  Txn A commits Extent X -> Txn B remove X -> Txn C reuse X and commits ->
   * Group Commit flush (A, B, C) In this scenario, X will be EvictExtent()
   * twice Because a single EvictExtent() is enough to commit all A, B, and C,
   * we shouldn't EvictExtent() 1 more time
   */
  if (frame_[pid].prevent_evict.load()) {
    frame_[pid].prevent_evict = false;

    auto &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion().load();
    if (ps.TryLockExclusive(v)) {
      switch (FLAGS_blob_logging_variant) {
      case 0:
#ifdef ENABLE_EXMAP
        exmap_interface_[worker_thread_id]->iov[0] = {.page = pid,
                                                      .len = page_cnt};
        if (ExmapAction(exmapfd_, EXMAP_OP_FREE, 0) < 0) {
          throw leanstore::ex::GenericException("ioctl: EXMAP_OP_FREE error");
        }
#endif
        resident_set_.Remove(pid);
        physical_used_cnt_ -= page_cnt;
        // statistics::buffer::evict_cnt += page_cnt;
        ps.UnlockExclusiveAndEvict();
        break;
      case 1:
        ps.UnlockExclusive();
        break;
      case 2:
        ps.UnlockExclusiveAndMark();
        break;
      default:
        UnreachableCode();
        break;
      }
    }
  }
}

/**
 * @brief Try to read one or multiple extents from disk, and set necessary
 * run-time variables The caller is responsible for ensuring that large_pages
 * should all refer to existing extents
 */
void BufferManager::ReadExtents(const storage::LargePageList &large_pages) {
  u64 total_page_cnt = 0;
  storage::LargePageList to_read_lp;
  to_read_lp.reserve(large_pages.size());

  // 1. Acquire necessary lock & space for the large pages before reading them
  for (const auto &lp : large_pages) {
    auto require_read = FixShareImpl(lp.start_pid);
    if (require_read) {
      to_read_lp.emplace_back(lp.start_pid, lp.page_cnt);
      total_page_cnt += lp.page_cnt;
    }
    Ensure(
        (sync::PageState::UNLOCKED < GetPageState(lp.start_pid).LockState()) &&
        (GetPageState(lp.start_pid).LockState() <= sync::PageState::EXCLUSIVE));
  }

  physical_used_cnt_ += total_page_cnt;
  EnsureFreePages();

  // 2. Exmap calloc/prefault these huge pages + Read them with io_uring
  for (size_t idx = 0; idx < to_read_lp.size(); idx++) {
#ifdef ENABLE_EXMAP
    exmap_interface_[worker_thread_id]->iov[idx].page =
        to_read_lp[idx].start_pid;
    exmap_interface_[worker_thread_id]->iov[idx].len = to_read_lp[idx].page_cnt;
#endif
    io_interface_[worker_thread_id].ReadPage(to_read_lp[idx].start_pid, false);
  }
#ifdef ENABLE_EXMAP
  Ensure(ExmapAction(exmapfd_, EXMAP_OP_ALLOC, to_read_lp.size()) >= 0);
// io_interface_[worker_thread_id].ReadPage(to_read_lp);
#endif

  // Mark all huge pages as Blob, and set them to SHARED state
  for (const auto &[start_pid, page_cnt] : to_read_lp) {
    resident_set_.Insert(start_pid);
    EXTENT_FRAME_SET(start_pid, page_cnt);
    GetPageState(start_pid).DowngradeLock();
  }

  // 3. If normal buffer pool, lookup all normal pages
  if (FLAGS_blob_normal_buffer_pool) {
    for (const auto &lp : large_pages) {
      for (auto pid = lp.start_pid; pid < lp.start_pid + lp.page_cnt; pid++) {
        resident_set_.Contain(pid);
      }
    }
  }

  // Update statistics
  statistics::buffer::read_cnt += total_page_cnt;
}

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Either Set or Clear bits in [block_start..block_end] in shalas_lk_
 */
auto BufferManager::ToggleShalasLocks(bool set_op, u64 &block_start,
                                      u64 block_end) -> bool {
  if (block_end > shalas_no_blocks_) {
    return false;
  }
  bool success = true;

  for (; block_start < block_end;) {
    // Detect the necessary bits for locking [pos..next_pos)
    auto next_pos = std::min(block_start + NO_BLOCKS_PER_LOCK -
                                 block_start % NO_BLOCKS_PER_LOCK,
                             block_end);
    auto bits = (next_pos % NO_BLOCKS_PER_LOCK == 0)
                    ? std::numeric_limits<u64>::max()
                    : SET_BITS(next_pos);
    if (block_start % NO_BLOCKS_PER_LOCK != 0) {
      bits -= SET_BITS(block_start);
    }

    // Try to acquire the necessary bits
    u64 old_lock;
    u64 x_lock;
    auto &lock = shalas_lk_[block_start / NO_BLOCKS_PER_LOCK];
    do {
      old_lock = lock.load();

      if (set_op) {
        x_lock = old_lock | bits;
        // If at least one of the bits was acquired by concurrent workers, abort
        if ((old_lock & bits) > 0) {
          success = false;
          break;
        }
      } else {
        x_lock = old_lock & ~bits;
      }
    } while (!lock.compare_exchange_strong(old_lock, x_lock));

    // The last lock acquisition fails, break to undo the prev lock acquisitions
    if (!success) {
      break;
    }

    // Move to next lock block
    block_start = next_pos;
  }

  return success;
}

auto BufferManager::RequestAliasingArea(u64 requested_size) -> pageid_t {
  u64 required_page_cnt = storage::blob::BlobHandler::PageCount(requested_size);

  // Check if the worker-local aliasing area is big enough for this request
  if (ALIAS_AREA_CAPABLE(wl_alias_ptr_[worker_thread_id] + required_page_cnt,
                         worker_thread_id)) {
    auto start_pid = wl_alias_ptr_[worker_thread_id];
    wl_alias_ptr_[worker_thread_id] += required_page_cnt;
    return start_pid;
  }

  // There is no room left in worker-local aliasing area, fallback to shalas,
  // i.e. shared-aliasing area
  u64 required_block_cnt =
      std::ceil(static_cast<float>(required_page_cnt) / alias_pg_cnt_);
  u64 pos;     // Inclusive
  u64 new_pos; // Exclusive

  while (true) {
    // Try to find a batch which is large enough to store `required_page_cnt`,
    //  i.e. consecutive of blocks which is bigger than `required_block_cnt`
    bool found_range;
    do {
      pos = shalas_ptr_.load();
      new_pos = pos + required_block_cnt;
      found_range = new_pos <= shalas_no_blocks_;
      if (new_pos >= shalas_no_blocks_) {
        new_pos = 0;
      }
    } while (!shalas_ptr_.compare_exchange_strong(pos, new_pos));

    // The prev shalas_ptr_ is at the end of the shared-alias area, and there is
    // no room left for the `requested_size`
    if (!found_range) {
      continue;
    }
    auto end_pos = (new_pos == 0) ? shalas_no_blocks_ : new_pos;
    Ensure(pos + required_block_cnt == end_pos);

    // Now try to acquire bits in [pos..pos + required_block_cnt)
    auto start_pos = pos;
    auto acquire_success = ToggleShalasLocks(true, start_pos, end_pos);

    // Lock acquisition success, return the start_pid
    if (acquire_success) {
      shalas_lk_acquired_[worker_thread_id].emplace_back(pos,
                                                         required_block_cnt);
      return ToPID(&shalas_area_[pos * alias_pg_cnt_]);
    }

    // Lock acquisition fail, undo bits [pos..start_pos), and continue trying to
    //  acquire a suitable range
    ToggleShalasLocks(false, pos, start_pos);
  }

  UnreachableCode();
  return 0; // This is purely to silent the compiler/clang-tidy warning
}

void BufferManager::ReleaseAliasingArea() {
#ifdef ENABLE_EXMAP
  ExmapAction(exmapfd_, EXMAP_OP_RM_SD, 0);
#endif
  if (wl_alias_ptr_[worker_thread_id] >
      ALIAS_LOCAL_PTR_START(worker_thread_id)) {
    wl_alias_ptr_[worker_thread_id] = ALIAS_LOCAL_PTR_START(worker_thread_id);
  }
  if (!shalas_lk_acquired_[worker_thread_id].empty()) {
    for (auto &[block_pos, block_cnt] : shalas_lk_acquired_[worker_thread_id]) {
      Ensure(ToggleShalasLocks(false, block_pos, block_pos + block_cnt));
    }
    shalas_lk_acquired_[worker_thread_id].clear();
  }
}

// ------------------------------------------------------------------------------------------------------

/**
 * @brief Acquire necessary Locks on huge page (either X or S) before reading
 * that page
 */

auto BufferManager::FixExclusive(pageid_t pid) -> storage::Page * {
  ValidatePID(pid);
  sync::PageState &ps = GetPageState(pid);
  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    switch (sync::PageState::LockState(v)) {
      Ensure(sync::PageState::EVICTED);
    case sync::PageState::EVICTED: {
      if (ps.TryLockExclusive(v)) {
        HandlePageFault(pid);
        return ToPtr(pid);
      }
      break;
    }
    case sync::PageState::MARKED:
    case sync::PageState::UNLOCKED:
      if (ps.TryLockExclusive(v)) {
        statistics::buffer::buffer_hit_cnt++;
        if (ToPtr(pid)->gc_read) {
          statistics::buffer::gc_read_cnt++;
        }
        return ToPtr(pid);
      }
      break;
    default:
      break;
    }
    AsmYield(cnt);
  }
}

auto BufferManager::FixShare(pageid_t pid) -> storage::Page * {
  ValidatePID(pid);

  auto required_read = FixShareImpl(pid);
  if (required_read) {
    HandlePageFault(pid);
    GetPageState(pid).DowngradeLock();
  } else {
    statistics::buffer::buffer_hit_cnt++;
    if (ToPtr(pid)->gc_read) {
      statistics::buffer::gc_read_cnt++;
    }
  }

  return ToPtr(pid);
}

auto BufferManager::FixShareImpl(pageid_t pid) -> bool {
  auto &ps = GetPageState(pid);
  for (auto cnt = 0;; cnt++) {
    u64 v = ps.StateAndVersion().load();
    switch (sync::PageState::LockState(v)) {
    case sync::PageState::EXCLUSIVE: {
      break;
    }
    case sync::PageState::EVICTED: {
      if (ps.TryLockExclusive(v)) {
        return true;
      }
      break;
    }
    default:
      if (ps.TryLockShared(v)) {
        return false;
      }
      break;
    }
    AsmYield(cnt);
  }
}

auto BufferManager::FixShareCachedPage(pageid_t pid) -> bool {
  auto &ps = GetPageState(pid);
  for (auto cnt = 0;; cnt++) {
    if (!frame_[pid].dirty) {
      return false;
    }
    u64 v = ps.StateAndVersion().load();
    switch (sync::PageState::LockState(v)) {
    case sync::PageState::EVICTED: {
      return false;
      break;
    }
    case sync::PageState::EXCLUSIVE: {
      return false;
    }

    default:
      if (ps.TryLockShared(v)) {
        return true;
      }

      break;
    }
    AsmYield(cnt);
  }
}

void BufferManager::UnfixExclusive(pageid_t pid) {
  ValidatePID(pid);
  GetPageState(pid).UnlockExclusive();
}

void BufferManager::UnfixShare(pageid_t pid) {
  ValidatePID(pid);
  GetPageState(pid).UnlockShared();
}

// ------------------------------------------------------------------------------------------------------

bool BufferManager::FixExclusiveOnDiskGC(pageid_t pid, blockid_t bid, u32 p,
                                         u8 pidx) {
  for (auto cnt = 0;; cnt++) {
    if (!sm_->PageIsValid(bid, p, pidx) || !sm_->PIDOffsetIsValid(pid)) {
      return false;
    }

    sync::PageState &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion().load();

    switch (sync::PageState::LockState(v)) {
    case sync::PageState::EVICTED: {
      if (!sm_->PageIsValid(bid, p, pidx)) {
        return false;
      }

      if (ps.TryLockExclusive(v)) {
        return true;
      }
      for (u32 io = 0; io < io_handler_cnt_; io++) {
        if (io_interface_[io].PageInReadBuffer(pid)) {
          // the current thread already xlocked the page to read
          // sm_->UpdatePID2Offset(pid, 0, 0, 0, 0, true);
          if (!frame_[pid].dirty) {
            MakePageDirty(pid);
            statistics::buffer::buffer_hit_cnt++;
          }
          return false;
        }
      }
      break;
    }
    case sync::PageState::EXCLUSIVE:
      for (u32 iohandler = 0; iohandler < io_handler_cnt_; iohandler++) {
        if (io_interface_[iohandler].PageInReadBuffer(pid)) {
          // the current thread already xlocked the page to read
          sm_->UpdatePID2Offset(pid, 0, 0, 0, 0, true);
          if (!frame_[pid].dirty) {
            MakePageDirty(pid);
            statistics::buffer::buffer_hit_cnt++;
          }
          return false;
        }
      }
      break;
    default:
      break;
    }
    AsmYield(cnt);
  }
}
auto BufferManager::FixShareImplGC(pageid_t pid, blockid_t bid, u32 p, u8 pidx,
                                   u64 *dirtified_cnt, u32 gidx) -> bool {
  // return true if it requires GC
  // otherwise, return false
  for (auto cnt = 0;; cnt++) {
    // pid is no longer valid
    if (!sm_->PageIsValid(bid, p, pidx) || !sm_->PIDOffsetIsValid(pid) ||
        sm_->PIDBlock(pid) != bid) {
      return false;
    }

    auto &ps = GetPageState(pid);
    u64 v = ps.StateAndVersion().load();

    switch (sync::PageState::LockState(v)) {
    case sync::PageState::EXCLUSIVE: {
      for (u32 iohandler = 0; iohandler < FLAGS_worker_count; iohandler++) {
        if (io_interface_[iohandler].PageInReadBuffer(pid)) {
          // the current thread already xlocked the page to read
          // need to be copybacked since it needs to be relocated

          // mark the location to indicate it is going through GC
          sm_->UpdatePID2Offset(pid, 0, 0, 0, 0, true);

          if (!frame_[pid].dirty) {
            // mark dirty so that it will be written to the disk  in the future
            MakePageDirty(pid);
            (*dirtified_cnt)++;
            // hit since it was already inside the buffer pool
            statistics::buffer::buffer_hit_cnt++;
          }
          return false;
        }
      }
      break;
    }
    case sync::PageState::EVICTED: {
      // not inside the buffer pool

      if (ps.TryLockExclusive(v)) {
        // read page to GC from the disk
        if (!sm_->PageIsValid(bid, p, pidx) || !sm_->PIDOffsetIsValid(pid)) {
          ps.UnlockExclusive();
          return false;
        }
        HandlePageFaultGC(pid);
        (*dirtified_cnt)++;

        GetPageState(pid).UnlockExclusive();
        statistics::buffer::buffer_hit_cnt++;
        return true;
      }
      for (u32 iohandler = 0; iohandler < io_handler_cnt_; iohandler++) {
        if (io_interface_[iohandler].PageInReadBuffer(pid)) {
          // the current thread already xlocked the page to read
          // need to be copybacked since it needs to be relocated

          // mark the location to indicate it is going through GC
          sm_->UpdatePID2Offset(pid, 0, 0, 0, 0, true);

          if (!frame_[pid].dirty) {
            // mark dirty so that it will be written to the disk  in the future
            MakePageDirty(pid);
            (*dirtified_cnt)++;
            // hit since it was already inside the buffer pool
            statistics::buffer::buffer_hit_cnt++;
          }
          return true;
        }
      }
      break;
    }
    default:
      // pid already inside the buffer pool

      if (ps.TryLockShared(v)) {
        // slock succeeded
        if (!sm_->PageIsValid(bid, p, pidx)) {
          ps.UnlockShared();
          return false;
        }
        // mark dirty to make sure the page is written to the disk in the future
        if (!frame_[pid].dirty) {
          MakePageDirty(pid);
          (*dirtified_cnt)++;
        }
        statistics::buffer::buffer_hit_cnt++;
        // mark the location to indicate it is going through GC
        sm_->UpdatePID2Offset(pid, 0, 0, 0, 0, true);
        ps.UnlockShared();
        return true;
      }

      if (io_interface_[worker_thread_id].PageInUserWBufferPerGroup(pid,
                                                                    gidx)) {
        // the current thread already slocked the page to write
        // mark the location to indicate it is going through GC
        return false;
      }
      break;
    }

    AsmYield(cnt);
  }
}

// ------------------------------------------------------------------------------------------------------

void BufferManager::UpdatePID(pageid_t pid) {
  FixExclusive(pid);
  ToPtr(pid)->p_gsn = recovery::LogManager::local_cur_gsn[worker_thread_id];
  MakePageDirty(pid);
  UnfixExclusive(pid);
}

void BufferManager::ReadPID(pageid_t pid) {
  FixShare(pid);
  UnfixShare(pid);
}

void BufferManager::ModifyPageContent(storage::Page *page) {
  std::memset(reinterpret_cast<u8 *>(page) + sizeof(storage::PageHeader), 111,
              PAGE_SIZE - sizeof(storage::PageHeader));
  page->p_gsn = recovery::LogManager::local_cur_gsn[worker_thread_id];
  MakePageDirty(ToPID(page));
}

} // namespace leanstore::buffer
