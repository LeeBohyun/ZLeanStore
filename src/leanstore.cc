#include "leanstore/leanstore.h"
#include "fmt/core.h"
#include "leanstore/config.h"
#include "share_headers/mem_usage.h"
#include "storage/btree/tree.h"
#include "storage/space/device.h"
#include "storage/space/garbage_collector.h"
#include <regex>

#include <array>
#include <chrono>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <sys/sysinfo.h>

namespace leanstore {

LeanStore::LeanStore()
    : device(std::make_unique<storage::space::NVMeDevice>()),
      sm(std::make_unique<storage::space::SpaceManager>(
          device->blockfd_, device->max_db_byte_offset_,
          device->zone_alignment_size_, device->zone_size_,
          device->db_zone_cnt_, device->max_open_zone_cnt_,
          device->max_mapped_pid_cnt_, device->sector_size_,
          device.get())), // device->max_open_zone_cnt_
      fp_manager(std::make_unique<storage::FreePageManager>()),
      buffer_pool(std::make_unique<buffer::BufferManager>(
          device->blockfd_, device->max_mapped_pid_cnt_, is_running,
          fp_manager.get(), sm.get(), is_loading)),
      log_manager(std::make_unique<recovery::LogManager>(is_running)),
      transaction_manager(std::make_unique<transaction::TransactionManager>(
          buffer_pool.get(), log_manager.get())),
      blob_manager(
          std::make_unique<storage::blob::BlobManager>(buffer_pool.get())),
      worker_pool(is_running) {

  is_loading = false;
  if (FLAGS_bm_physical_gb * GB <= (FLAGS_block_size_mb * MB)) {
    shard_cnt = 2;
  } else {
    shard_cnt = FLAGS_checkpointer_cnt *
                (FLAGS_bm_physical_gb * GB / (FLAGS_block_size_mb * MB)) * 2;
  }
  if (FLAGS_use_compression) {
    shard_cnt *= (PAGE_SIZE / MIN_COMP_SIZE);
  }
  // Necessary dirty works
  worker_pool.ScheduleSyncJob(0, [&]() { buffer_pool->AllocMetadataPage(); });
  all_buffer_pools.push_back(buffer_pool.get());

  // Group-commit initialization
  gct = make_unique<recovery::GroupCommitExecutor>(
      buffer_pool.get(), log_manager.get(), is_running, shard_cnt,
      device->walfd_, device->wal_start_byte_offset_,
      device->wal_end_byte_offset_, device->sector_size_);
  StartGroupCommitThread(); // Ensure group commit thread starts

  // Page provider threads
  if (FLAGS_page_provider_thread > 0) {
    buffer_pool->RunPageProviderThreads();
  }

  // Checkpointer initialization
  if (FLAGS_checkpointer_cnt) {
    cp.reserve(FLAGS_checkpointer_cnt);
    for (size_t idx = 0; idx < FLAGS_checkpointer_cnt; ++idx) {
      cp.emplace_back(recovery::Checkpointer(buffer_pool.get(), gct.get(),
                                             is_running, shard_cnt));
    }
    StartCheckpointThread();
  }

  // Garbage collector threads
  if (FLAGS_garbage_collector_cnt > 0 && FLAGS_use_out_of_place_write) {
    gc.reserve(FLAGS_garbage_collector_cnt);
    for (size_t idx = 0; idx < FLAGS_garbage_collector_cnt; ++idx) {
      gc.emplace_back(storage::space::GarbageCollector(
          buffer_pool->PID2Offset_table_, buffer_pool->sspace_,
          buffer_pool->blockfd_, idx));
    }
    StartGarbageCollectorThread();
  }
}

LeanStore::~LeanStore() {
  Shutdown();
  start_profiling = false;
}

void LeanStore::Shutdown() {
  worker_pool.Stop();
  Ensure(is_running == false);
#ifdef ENABLE_EXMAP
  for (size_t w_id = 0; w_id <= FLAGS_worker_count; w_id++) {
    struct exmap_action_params params = {
        .interface = static_cast<u16>(w_id),
        .iov_len = 0,
        .opcode = EXMAP_OP_RM_SD,
        .flags = 0,
    };
    ioctl(buffer_pool->exmapfd_, EXMAP_IOCTL_ACTION, &params);
  }
#endif
  /** It's possible that these two special threads are already completed before
   * calling join */
  if (group_committer.joinable()) {
    group_committer.join();
  }

  if (FLAGS_checkpointer_cnt) {
    if (checkpointer.joinable()) {
      checkpointer.join();
    }
  }

  if (FLAGS_garbage_collector_cnt) {
    if (garbage_collector.joinable()) {
      garbage_collector.join();
    }
  }

  if (stat_collector.joinable())
    stat_collector.join();
}

void LeanStore::EndDataLoad() {
  sleep(10);
  // buffer_pool->FlushAll();
}

// -------------------------------------------------------------------------------------
void LeanStore::RegisterTable(const std::type_index &relation) {
  assert(indexes.find(relation) == indexes.end());
  assert(FLAGS_worker_count > 0);
  worker_pool.ScheduleSyncJob(0, [&]() {
    transaction_manager->StartTransaction(
        leanstore::transaction::Transaction::Type::SYSTEM);
    indexes.try_emplace(
        relation, std::make_unique<storage::BTree>(buffer_pool.get(), false));
    CommitTransaction();
  });
}

auto LeanStore::RetrieveIndex(const std::type_index &relation)
    -> KVInterface * {
  assert(indexes.find(relation) != indexes.end());
  return indexes.at(relation).get();
}

void LeanStore::StartTransaction(bool read_only, Transaction::Mode tx_mode,
                                 const std::string &tx_isolation_level) {
  transaction_manager->StartTransaction(
      Transaction::Type::USER,
      transaction::TransactionManager::ParseIsolationLevel(tx_isolation_level),
      tx_mode, read_only);
}

void LeanStore::CommitTransaction() {
  transaction_manager->CommitTransaction();
  blob_manager->UnloadAllBlobs();
}

void LeanStore::AbortTransaction() {
  transaction_manager->AbortTransaction();
  blob_manager->UnloadAllBlobs();
}

// -------------------------------------------------------------------------------------
auto LeanStore::AllocatedPIDsCnt() -> u64 {
  return buffer_pool->alloc_cnt_.load();
}

// -------------------------------------------------------------------------------------
auto LeanStore::AllocatedSize() -> float {
  return static_cast<float>((buffer_pool->alloc_cnt_.load() -
                             buffer_pool->dealloc_cnt_.load()) *
                            PAGE_SIZE) /
         MB; // - buffer_pool->dealloc_cnt_.load()
}

auto LeanStore::WALSize() -> float {
  auto wal_size_in_bytes = gct->w_cur_offset_ - device->wal_start_byte_offset_;
  return static_cast<float>(wal_size_in_bytes) / MB;
}

auto LeanStore::DBSize() -> float {
  // vanilla code
  if (FLAGS_use_out_of_place_write) {
    return sm->GetDBSize();
  } else {
    return AllocatedSize();
  }
}

void LeanStore::DropCache() {
  worker_pool.ScheduleSyncJob(0, [&]() {
    LOG_INFO("Dropping the cache");
    buffer_pool->FlushAll();
    LOG_INFO("Complete cleaning the cache. Buffer size: %lu",
             buffer_pool->physical_used_cnt_.load());
  });
}

void LeanStore::StartGroupCommitThread() {
  group_committer = std::thread([&]() {
    worker_thread_id = FLAGS_worker_count + FLAGS_checkpointer_cnt +
                       FLAGS_garbage_collector_cnt;
    gct->StartExecution();
    fprintf(stderr, "Halt LeanStore's GroupCommit thread\n");
  });
  group_committer.detach();
}

// -------------------------------------------------------------------------------------
void LeanStore::StartCheckpointThread() {
  wid_t min_cp_id = FLAGS_worker_count;
  wid_t max_cp_id = FLAGS_worker_count + FLAGS_checkpointer_cnt;

  for (wid_t t_id = min_cp_id; t_id < max_cp_id; t_id++) {
    checkpointer = std::thread([&, t_id, min_cp_id]() {
      worker_thread_id = t_id;
      u32 cp_idx = worker_thread_id - min_cp_id;
      pthread_setname_np(pthread_self(), "checkpointer");

      while (is_running) {
        for (u32 shard_idx = cp_idx; shard_idx < shard_cnt;
             shard_idx += FLAGS_checkpointer_cnt) {

          transaction_manager->StartTransaction(
              leanstore::transaction::Transaction::Type::VOID);

          if (std::get<0>(gct->cp_target_[shard_idx])) {
            bool completed = cp[cp_idx].RunCheckpointThread(shard_idx, false);
          } else {
            if (FLAGS_use_out_of_place_write) {
              // bool completed = cp[cp_idx].RunCheckpointThread(shard_idx,
              // true);
            }
          }

          transaction_manager->CommitTransaction();
        }
      }
      fprintf(stderr, "Halt LeanStore's Checkpoint thread\n");
    });
    checkpointer.detach();
  }
}

void LeanStore::StartGarbageCollectorThread() {
  wid_t min_gc_thread_id = FLAGS_worker_count + FLAGS_checkpointer_cnt; // [
  wid_t max_gc_thread_id = FLAGS_worker_count + FLAGS_checkpointer_cnt +
                           FLAGS_garbage_collector_cnt; // )

  for (wid_t t_id = min_gc_thread_id; t_id < max_gc_thread_id; t_id++) {
    std::thread garbage_collector([=, t_id, min_gc_thread_id]() {
      worker_thread_id = t_id;
      wid_t gc_id = t_id - min_gc_thread_id;
      pthread_setname_np(pthread_self(), "garbage_collector");
      while (is_running) {
        // transaction_manager->StartTransaction(
        //     leanstore::transaction::Transaction::Type::VOID);
        blockid_t bid = sm->block_cnt_;
        std::vector<blockid_t> wbids;
        if (!is_loading && sm->ShouldTriggerGC(0)) {
          buffer_pool->GCWriteValidPIDs(0);

          sm->SelectFullBlockToGCRead();
          blockid_t readbid = sm->FindFirstGCInUseBlockInActiveList();
          if (readbid < sm->block_cnt_) {
            buffer_pool->GCReadValidPIDs(readbid, false);
          }
        } else {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        //   transaction_manager->CommitTransaction();
      }
      fprintf(stderr, "Halt LeanStore's Garbage Collector thread\n");
    });
    garbage_collector.detach();
  }
}

u32 LeanStore::GetVersion() {
  if (!FLAGS_use_out_of_place_write)
    return 0;

  u32 version = FLAGS_use_out_of_place_write + FLAGS_use_compression +
                FLAGS_use_edt + FLAGS_use_SSDWA1_pattern + FLAGS_use_ZNS;
  return version;
}

void LeanStore::StartProfilingThread() {
  start_profiling = true;
  stat_collector = std::thread([&]() {
    pthread_setname_np(pthread_self(), "stats_collector");
    float r_mb;
    float w_mb;
    float ew_mb;
    float cpw_mb;
    float gcw_mb;
    float gcr_mb;
    float dwbw_mb;
    float running_waf = 0;
    float cumulative_waf = 0;
    float valid_pct = 0;
    float comp_ratio = 0.0;
    float hit_ratio = 0.0;
    float gc_read_ratio = 0.0;
    float db_waf = 0.0;
    float prev_db_waf = 1.0;
    u64 w_cnt = 0;
    u64 r_cnt = 0;
    u64 cnt = 0;
    u64 edt_gap = 0;
    u64 ew, cpw, gcw;
    auto version = GetVersion();

    cum_total_wal_write = 0;
    cum_total_write = 0;
    cum_tps = 0;
    statistics::storage::space::total_writes.exchange(0);
    statistics::buffer::write_cnt.exchange(0);
    statistics::buffer::read_cnt.exchange(0);
    statistics::buffer::buffer_miss_cnt.exchange(0);
    statistics::buffer::buffer_hit_cnt.exchange(0);
    statistics::buffer::gc_read_cnt.exchange(0);
    statistics::buffer::edt_gap.exchange(0);
    statistics::buffer::edt_cnt.exchange(0);
    statistics::storage::space::evict_writes.exchange(0);
    statistics::storage::space::checkpoint_writes.exchange(0);
    statistics::storage::space::gc_reads.exchange(0);
    statistics::storage::space::gc_writes.exchange(0);
    statistics::storage::space::doublewrite_writes.exchange(0);
    statistics::recovery::gct_write_bytes.exchange(0);
    statistics::recovery::log_write_bytes.exchange(0);
    statistics::storage::space::total_size_after_compression.exchange(0);
    statistics::storage::space::valid_page_cnt.exchange(0);
    statistics::storage::space::gced_page_cnt.exchange(0);
    statistics::storage::space::total_writes_cnt.exchange(0);
    statistics::storage::space::total_reads_cnt.exchange(0);

    if (FLAGS_measure_waf) {
      std::printf(
          "ts,tps,cum_tps,r_iops,w_iops,hit_ratio,hit_ratio_gc,edt_gap,rmb_s,"
          "wmb_s,ewmb_s,cpwmb_s,"
          "dwbw_mb,gcwmb_s,gcrmb_s,"
          "valid_ratio,comp_ratio,logmb_s,"
          "wal_sz,db_sz,alloc_sz,mem_mb,"
          "cum_flash_wmb,db_waf,ssd_run_waf,ssd_cum_waf,"
          "version\n");
      first_flash_write = GetFlashWrite();
    } else {
      std::printf(
          "ts,tps,cum_tps,r_iops,w_iops,hit_ratio,hit_ratio_gc,edt_gap,rmb_s,"
          "wmb_s,ewmb_s,cpwmb_s,"
          "dwbw_mb,gcwmb_s,gcrmb_s,"
          "valid_ratio,comp_ratio,logmb_s,"
          "wal_sz,db_sz,db_waf,alloc_sz,mem_mb,"
          "version\n");
    }
    while (is_running) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      // Progress stats
      auto progress = statistics::txn_processed.exchange(0);
      cum_tps += progress;
      statistics::total_txn_completed += progress;
      auto hit_cnt = statistics::buffer::buffer_hit_cnt.exchange(0);
      auto miss_cnt = statistics::buffer::buffer_miss_cnt.exchange(0);
      auto gc_read_cnt = statistics::buffer::gc_read_cnt.exchange(0);

      hit_ratio =
          static_cast<float>(hit_cnt) / static_cast<float>(miss_cnt + hit_cnt);
      gc_read_ratio =
          static_cast<float>(gc_read_cnt) / static_cast<float>(hit_cnt);

      ew_mb = static_cast<float>(
                  statistics::storage::space::evict_writes.exchange(0)) /
              MB;
      cpw_mb = static_cast<float>(
                   statistics::storage::space::checkpoint_writes.exchange(0)) /
               MB;
      dwbw_mb =
          static_cast<float>(
              statistics::storage::space::doublewrite_writes.exchange(0)) /
          MB;
      gcw_mb = static_cast<float>(
                   statistics::storage::space::gc_writes.exchange(0)) /
               MB;
      gcr_mb =
          static_cast<float>(statistics::storage::space::gc_reads.exchange(0)) /
          MB;
      u64 edt_cnt = statistics::buffer::edt_cnt.exchange(0);
      if (edt_cnt != 0) {
        edt_gap = statistics::buffer::edt_gap.exchange(0) / edt_cnt;
      } else {
        edt_gap = 0;
      }
      auto comp_size =
          statistics::storage::space::total_size_after_compression.exchange(0);
      auto total_write_per_sec =
          statistics::storage::space::total_writes.exchange(0);

      w_mb = static_cast<float>(total_write_per_sec) / MB;

      w_cnt = statistics::storage::space::total_writes_cnt.exchange(0);

      auto written_pid_cnt = statistics::buffer::write_cnt.exchange(0);

      float comp_ratio =
          !FLAGS_use_compression
              ? 1.0
              : static_cast<float>(total_write_per_sec) /
                    static_cast<float>(written_pid_cnt * PAGE_SIZE);
      if (!FLAGS_use_compression) {
        comp_ratio = 1.0;
      } else {
        if (w_cnt == 0) {
          comp_ratio = 0.0;
        }
      }

      if (!FLAGS_use_binpacking && FLAGS_use_compression) {
        comp_ratio = static_cast<float>(comp_size) /
                     static_cast<float>(written_pid_cnt * PAGE_SIZE);
      }
      if (!FLAGS_use_out_of_place_write) {
        comp_ratio = 1.0;
        if (FLAGS_enable_doublewrite) {
          db_waf = 2.0;
        } else {
          db_waf = 1.0;
        }
      } else {
        if (statistics::storage::space::valid_page_cnt.load() == 0 ||
            statistics::storage::space::gced_page_cnt.load() == 0) {
          valid_pct = 0.0;
          db_waf = prev_db_waf;
        } else {
          valid_pct =
              static_cast<float>(
                  statistics::storage::space::valid_page_cnt.exchange(0)) /
              statistics::storage::space::gced_page_cnt.exchange(0);
          db_waf = (1.0) / (1.0 - valid_pct) * comp_ratio;
          prev_db_waf = db_waf;
        }
      }

      r_mb = static_cast<float>(
                 statistics::storage::space::total_reads.exchange(0)) /
             MB;
      r_cnt = statistics::storage::space::total_reads_cnt.exchange(0);

      auto db_sz = DBSize();
      auto alloc_sz = AllocatedSize();
      auto log_write = statistics::recovery::log_write_bytes.exchange(0);
      auto gct_write = statistics::recovery::gct_write_bytes.exchange(0);
      cum_total_write += (total_write_per_sec + log_write);
      cum_total_wal_write += log_write;
      auto logw_mb = static_cast<float>(log_write) / MB;
      auto wal_sz = WALSize();
      if (FLAGS_wal_path.empty()) {
        wal_sz = 0;
      }

      auto mem_mb = static_cast<float>(getPeakRSS()) / MB;

      /* Write metrics */
      if (FLAGS_measure_waf) {
        size_t cur_flash_write = GetFlashWrite();
        cum_total_flash_write = cur_flash_write - first_flash_write;

        if (cum_total_flash_write > 0) {
          size_t flash_writes_in_interval =
              cum_total_flash_write - prev_flash_write;

          if (flash_writes_in_interval > 0) {
            running_waf = static_cast<float>(flash_writes_in_interval) /
                          (cum_total_write - prev_total_write);
            cumulative_waf =
                static_cast<float>(cum_total_flash_write) / cum_total_write;
            prev_flash_write = cum_total_flash_write;
            prev_total_write = cum_total_write;
            prev_cum_waf = cumulative_waf;
            statistics::flash_writes.store(cum_total_flash_write);
          } else {
            running_waf = 0;
            cumulative_waf = prev_cum_waf;
          }
        } else {
          running_waf = 0;
          cumulative_waf = 0;
        }
        std::printf(
            "%lu,%lu,%lu,%lu,%lu,%.3f,%.3f,%lu,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%."
            "3f,%.3f,%."
            "3f,%.3f,%.3f,"
            "%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%d\n",
            cnt++, progress, cum_tps, r_cnt, w_cnt, hit_ratio, gc_read_ratio,
            edt_gap, r_mb, w_mb, ew_mb, cpw_mb, dwbw_mb, gcw_mb, gcr_mb,
            valid_pct, comp_ratio, logw_mb, wal_sz, db_sz, alloc_sz, mem_mb,
            static_cast<float>(cum_total_flash_write) / MB, db_waf, running_waf,
            cumulative_waf, version);
      } else {
        std::printf(
            "%lu,%lu,%lu,%lu,%lu,%.3f,%.3f,%lu,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%."
            "3f,%.3f,%."
            "3f,%.3f,%.3f,%.3f,"
            "%.3f,%.3f,%.3f,%d\n",
            cnt++, progress, cum_tps, r_cnt, w_cnt, hit_ratio, gc_read_ratio,
            edt_gap, r_mb, w_mb, ew_mb, cpw_mb, dwbw_mb, gcw_mb, gcr_mb,
            valid_pct, comp_ratio, logw_mb, wal_sz, db_sz, alloc_sz, mem_mb,
            db_waf, version);
      }
    }
    fprintf(stderr, "Halt LeanStore's Profiling thread\n");
  });
}

// -------------------------------------------------------------------------------------
auto LeanStore::CreateNewBlob(std::span<const u8> blob_payload,
                              BlobState *prev_blob, bool likely_grow)
    -> std::span<const u8> {
  if (blob_payload.empty()) {
    throw leanstore::ex::GenericException("Blob payload shouldn't be empty");
  }
  // TODO(Duy): Compress blob_payload before calling AllocateBlob
  auto blob_hd =
      blob_manager->AllocateBlob(blob_payload, prev_blob, likely_grow);
  if (FLAGS_blob_logging_variant == -1) {
    auto &current_txn = transaction::TransactionManager::active_txn;
    current_txn.LogBlob(blob_payload);
  }
  return std::span{reinterpret_cast<u8 *>(blob_hd), blob_hd->MallocSize()};
}

void LeanStore::LoadBlob(const BlobState *blob_t,
                         const storage::blob::BlobCallbackFunc &read_cb,
                         bool partial_load) {
  if (partial_load) {
    blob_manager->LoadBlob(blob_t, PAGE_SIZE, read_cb);
  } else {
    blob_manager->LoadBlob(blob_t, blob_t->blob_size, read_cb);
  }
}

void LeanStore::RemoveBlob(BlobState *blob_t) {
  blob_manager->RemoveBlob(blob_t);
}

auto LeanStore::RetrieveComparisonFunc(ComparisonOperator cmp_op)
    -> ComparisonLambda {
  switch (cmp_op) {
  case ComparisonOperator::BLOB_LOOKUP:
    return {cmp_op, [blob_man = blob_manager.get()](
                        const void *a, const void *b, [[maybe_unused]] size_t) {
              return blob_man->BlobStateCompareWithString(a, b);
            }};
  case ComparisonOperator::BLOB_STATE:
    return {cmp_op, [blob_man = blob_manager.get()](
                        const void *a, const void *b, [[maybe_unused]] size_t) {
              return blob_man->BlobStateComparison(a, b);
            }};
  default:
    return {cmp_op, std::memcmp};
  }
}

size_t LeanStore::GetFlashWrite() { // for smrc
  if (FLAGS_simulator_mode) {
    return device->GetSSDWA();
  }
  try {
    // Extract base device path: /dev/nvmeX from /dev/nvmeXnY
    std::string base_path = FLAGS_db_path;
    std::regex base_regex(R"(/dev/nvme\d+)");
    std::smatch match;
    if (std::regex_search(base_path, match, base_regex)) {
      base_path = match.str();
    } else {
      std::cerr << "Error: Could not extract NVMe base device from "
                << FLAGS_db_path << std::endl;
      return 0;
    }

    // Build and execute shell command
    std::string command = "echo " + FLAGS_user_pwd +
                          " | sudo -S nvme ocp smart-add-log " + base_path +
                          " 2>/dev/null | strings | grep \"Physical media "
                          "units written\" | awk '{print $NF}'";

    // std::cout << "Running command: " << command << std::endl;

    std::string output = ExecCommand(command);

    if (output.empty()) {
      std::cerr << "Error: Command returned empty output.\n";
      return 0;
    }

    // std::cout << "Raw physical writes (512B units): " << output << std::endl;

    return std::stoull(output); // Multiply by 512 if you want bytes
  } catch (const std::runtime_error &e) {
    std::cerr << "Error: " << e.what() << std::endl;
  }

  return 0;
}

// size_t LeanStore::GetFlashWrite() { // for genoa
//   if (FLAGS_simulator_mode) {
//     return device->GetSSDWA();
//   }
//   try {
//     // Extract base device path (remove trailing "nX" from "/dev/nvmeXnY")
//     std::string base_path = FLAGS_db_path;
//     size_t pos = base_path.rfind("n");
//     if (pos != std::string::npos &&
//         pos > 8) { // Ensure "n" is after "/dev/nvme"
//       base_path = base_path.substr(0, pos);
//     }

//     std::string command = "echo " + FLAGS_user_pwd +
//                           " | sudo -S nvme ocp smart-add-log " + base_path +
//                           " 2>/dev/null | grep \"Physical media units "
//                           "written\" | awk '{print $7}'";
//     std::string output = ExecCommand(command);
//     return std::stoul(output);
//   } catch (const std::runtime_error &e) {
//     std::cerr << "Error: " << e.what() << std::endl;
//   }
//   return 0;
// }

// size_t LeanStore::GetFlashWrite() {
//   try {
//     std::string command = "echo " + FLAGS_user_pwd +
//                           " | sudo -S nvme ocp smart-add-log " +
//                           FLAGS_db_path + " 2>/dev/null | grep \"Physical
//                           media units " "written\" | awk '{print $7}'";
//     std::string output = ExecCommand(command);
//     return std::stoul(output);
//   } catch (const std::runtime_error &e) {
//     std::cerr << "Error: " << e.what() << std::endl;
//   }
//   return 0;
// }

std::string LeanStore::ExecCommand(const std::string &cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                pclose);

  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }

  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }

  // Remove only trailing newline
  if (!result.empty() && result.back() == '\n') {
    result.pop_back();
  }

  if (result.empty()) {
    throw std::runtime_error("Command returned empty output.");
  }

  return result;
}

} // namespace leanstore
