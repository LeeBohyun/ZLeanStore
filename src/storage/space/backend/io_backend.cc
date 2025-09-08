#include "storage/space/backend/io_backend.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "share_headers/logger.h"
#include "storage/space/backend/aio.h"
#include "storage/space/backend/zns.h"
#include "storage/space/space_manager.h"

#include "storage/space/sim/SSD.hpp"

namespace leanstore::storage::space::backend {

IOBackend::IOBackend(int blockfd, SpaceManager *sm,
                     PID2Offset *PID2Offset_table, StorageSpace *storage_space)
    : blockfd_(blockfd), trace_logger_(FLAGS_trace_file), libaio_(blockfd),
      sm_(sm), PID2Offset_table_(PID2Offset_table), sspace_(storage_space),
      zns_(blockfd, sm) {
  if (!FLAGS_trace_file.empty()) {
    if (std::filesystem::exists(FLAGS_trace_file)) {
      // Erase the existing file by truncating it
      std::ofstream erase_file(FLAGS_trace_file, std::ios_base::trunc);
      erase_file.close(); // Ensure the file is cleared
    }
  }
  if (FLAGS_use_ZNS) {
    zns_wal_cur_offset_ = sm_->max_userspace_capacity_;
  }

  if (FLAGS_simulator_mode) {
    assert(sm_->device_ != nullptr && "ssdsim_ not initialized.");
    assert(sm_->device_->ssdsim_ != nullptr && "ssdsim_ not initialized.");
    assert(sm_->device_->gc_ != nullptr &&
           "gc_ not initialized. Make sure NVMeDevice is fully constructed "
           "before passing to SpaceManager.");
  }
}

void IOBackend::WritePagesInPlace(std::vector<pageid_t> &pids,
                                  std::vector<u64> &offsets,
                                  std::vector<void *> &buffers,
                                  std::vector<indexid_t> &idxs,
                                  bool synchronous) {

  if (!FLAGS_use_ZNS && !FLAGS_use_FDP) {
    // use io_uring
    for (u32 idx = 0; idx < pids.size(); idx++) {
      trace_logger_.LogWrite(offsets[idx], pids[idx], idxs[idx]);
      WriteStatsUpdate(PAGE_SIZE, 1, false, false);
      if (FLAGS_simulator_mode) {
        sm_->device_->WriteToSSD(offsets[idx]);
      }
      libaio_.IssueWriteRequest(offsets[idx], PAGE_SIZE, buffers[idx],
                                synchronous);
      libaio_.UringSubmitWrite();
    }
    return;
  }
}

void IOBackend::WriteWAL(u64 offset, u32 write_sz, void *buffer,
                         bool synchronous) {
  if (FLAGS_wal_path.empty()) {
    return;
  }
  u64 cur_sz = 0;
  while (cur_sz < write_sz) {
    trace_logger_.LogWrite(offset + cur_sz, 0, 0);
    if (FLAGS_simulator_mode && !FLAGS_wal_path.empty()) {
      sm_->device_->WriteToSSD(offset + cur_sz);
    }
    cur_sz += PAGE_SIZE;
  }

  if (!FLAGS_use_ZNS && !FLAGS_use_FDP) {
    // use io_uring
    libaio_.IssueWriteRequest(offset, write_sz, buffer, synchronous);
    libaio_.UringSubmitWrite();
    return;
  }
  if (FLAGS_use_ZNS) {
    zns_.IssueZoneAppend(offset, buffer, write_sz, synchronous);
    return;
  }

  if (FLAGS_use_FDP) {
    return;
  }
}

void IOBackend::WritePagesOutOfPlace(u64 offset, u32 write_sz, void *buffer,
                                     bool synchronous, bool gcwrite) {

  u64 cur_sz = 0;
  while (cur_sz < write_sz) {
    trace_logger_.LogWrite(offset + cur_sz, 0, 0);
    WriteStatsUpdate(PAGE_SIZE, 1, false, gcwrite);
    if (FLAGS_simulator_mode) {
      sm_->device_->WriteToSSD(offset + cur_sz);
    }
    cur_sz += PAGE_SIZE;
  }

  if (!FLAGS_use_ZNS && !FLAGS_use_FDP) {
    // use io_uring
    // if idx_id == 4096 -> DWB
    libaio_.IssueWriteRequest(offset, write_sz, buffer, synchronous);
    libaio_.UringSubmitWrite();

    return;
  }
  if (FLAGS_use_ZNS) {
    zns_.IssueZoneAppend(offset, buffer, write_sz, synchronous);
    return;
  }

  if (FLAGS_use_FDP) {
    return;
  }
}

void IOBackend::WritePageSynchronously(pageid_t pid, u64 offset, void *buffer,
                                       indexid_t idx_id) {
  // for inplace write
  if (!FLAGS_use_ZNS && !FLAGS_use_FDP) {
    // use io_uring
    // if idx_id == 4096 -> DWB
    bool dwbwrite = idx_id == 999 ? true : false;
    trace_logger_.LogWrite(offset, pid, idx_id);
    WriteStatsUpdate(PAGE_SIZE, 1, dwbwrite, false);
    if (FLAGS_simulator_mode) {
      assert(sm_->device_ != nullptr && "nvmedevice not initialized.");
      assert(sm_->device_->ssdsim_ != nullptr && "ssdsim_ not initialized.");
      assert(sm_->device_->gc_ != nullptr &&
             "gc_ not initialized. Make sure NVMeDevice is fully constructed "
             "before passing to SpaceManager.");
      sm_->device_->WriteToSSD(offset);
    }

    libaio_.IssueWriteRequest(offset, PAGE_SIZE, buffer, true);

    return;
  }
  if (FLAGS_use_ZNS) {
    return;
  }

  if (FLAGS_use_FDP) {
    return;
  }
}

void IOBackend::ReadPagesOutOfPlace(std::vector<pageid_t> &pids,
                                    std::vector<u64> &offsets,
                                    std::vector<void *> &buffers,
                                    std::vector<u16> &r_sizes,
                                    bool synchronous) {

  for (u32 idx = 0; idx < offsets.size(); idx++) {
    ReadStatsUpdate(FLAGS_read_size, 1, true);
  }

  if (!FLAGS_use_ZNS && !FLAGS_use_FDP) {
    // use io_uring
    for (u32 idx = 0; idx < offsets.size(); idx++) {
      libaio_.IssueReadRequest(offsets[idx], FLAGS_read_size, buffers[idx],
                               synchronous);
    }
    libaio_.UringSubmitRead();
    return;
  }
  if (FLAGS_use_ZNS) {
    for (u32 idx = 0; idx < offsets.size(); idx++) {
      zns_.ZNSRead(offsets[idx], buffers[idx], FLAGS_read_size, synchronous);
    }
    return;
  }

  if (FLAGS_use_FDP) {
    return;
  }
}

void IOBackend::ReadPage(u64 offset, void *buffer, u16 read_size, bool sync) {
  ReadStatsUpdate(read_size, 1, false);
  if (!FLAGS_use_ZNS && !FLAGS_use_FDP) {
    // use io_uring
    // if idx_id == 4096 -> DWB
    libaio_.IssueReadRequest(offset, read_size, buffer, sync);

    return;
  }
  if (FLAGS_use_ZNS) {
    zns_.ZNSRead(offset, buffer, read_size, sync);
    return;
  }

  if (FLAGS_use_FDP) {
    return;
  }
}

void IOBackend::WriteStatsUpdate(u64 write_size, u32 write_cnt, bool is_dwb,
                                 bool gcwrite) {
  if (is_dwb) {
    statistics::storage::space::cum_total_writes += PAGE_SIZE;
    statistics::storage::space::doublewrite_writes += PAGE_SIZE;
    statistics::storage::space::total_writes += PAGE_SIZE;
    statistics::storage::space::total_writes_cnt += 1;
    return;
  }
  if (gcwrite) {
    statistics::storage::space::cum_total_writes += write_size;
    statistics::storage::space::gc_writes += write_size;
    statistics::storage::space::total_writes += write_size;
    statistics::storage::space::total_writes_cnt += write_cnt;
    return;
  }
  if (worker_thread_id < FLAGS_worker_count) {
    statistics::storage::space::evict_writes += write_size;
  }
  if ((worker_thread_id >= FLAGS_worker_count) &&
      (worker_thread_id < FLAGS_worker_count + FLAGS_checkpointer_cnt)) {
    statistics::storage::space::checkpoint_writes += write_size;
  }
  if (worker_thread_id >=
      FLAGS_worker_count + FLAGS_checkpointer_cnt * FLAGS_wal_enable) {
    statistics::storage::space::gc_writes += write_size;
  }

  // total write stats update
  statistics::storage::space::cum_total_writes += write_size;
  statistics::storage::space::total_writes += write_size;
  statistics::storage::space::total_writes_cnt += write_cnt;
}

void IOBackend::ReadStatsUpdate(u64 size, u64 cnt, bool isgcread) {
  statistics::storage::space::total_reads += size;
  statistics::storage::space::total_reads_cnt += cnt;

  if (isgcread) {
    statistics::storage::space::gc_reads += size;
  }
}

} // namespace leanstore::storage::space::backend
