#include "benchmark/flashbench/config.h"
#include "benchmark/flashbench/schema.h"
#include "benchmark/flashbench/workload.h"
#include "leanstore/leanstore.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

using FlashBench = flash::FlashWorkload<
    LeanStoreAdapter,
    flash::FlashRelation<BytesPayload<flash::FLASH_NORMAL_PAYLOAD>, 0>>;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore FlashBench");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (std::find(std::begin(flash::SUPPORTED_PAYLOAD_SIZE),
                std::end(flash::SUPPORTED_PAYLOAD_SIZE), FLAGS_payload_size) ==
      std::end(flash::SUPPORTED_PAYLOAD_SIZE)) {
    LOG_WARN(
        "Payload size %lu not supported, check flash::SUPPORTED_PAYLOAD_SIZE",
        FLAGS_payload_size);
    return 0;
  }

  // Setup worker-local payloads before initializing FlashWorkload
  flash::WorkerLocalPayloads payloads(FLAGS_worker_count);

  for (auto &payload : payloads) {
    payload.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE))
                      uint8_t[flash::FLASH_NORMAL_PAYLOAD]{});
    // LOG_INFO("payload offset: %p payload length: %zu", payload.get(),
    // payloads.size());
  }

  // Setup env
  tbb::global_control c(tbb::global_control::max_allowed_parallelism,
                        FLAGS_worker_count);
  std::atomic<bool> keep_running(true);
  leanstore::RegisterSEGFAULTHandler();
  PerfEvent e;
  PerfController ctrl;

  // Initialize LeanStore and FlashWorkload with payloads by reference
  auto db = std::make_unique<leanstore::LeanStore>();
  auto flashbench = std::make_unique<FlashBench>(payloads, *db);

  // flashbench loader
  db->is_loading = true;
  std::atomic<UInteger> w_id_loader = 0;
  std::atomic<bool> stop_loading = false;
  uint64_t alloc_cnt =
      ((FLAGS_initial_dataset_size_gb * leanstore::GB) / GLOBAL_BLOCK_SIZE);
  LOG_INFO("alloc_cnt: %lu", alloc_cnt);

  // Define a smaller grain size
  const size_t grain_size = 1024;

  tbb::parallel_for(
      tbb::blocked_range<Integer>(1, alloc_cnt, grain_size),
      [&](const tbb::blocked_range<Integer> &range) {
        if (stop_loading.load()) {
          return; // Early exit if stop condition is met
        }

        auto w_id = (++w_id_loader) % FLAGS_worker_count;
        db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id, range]() {
          if (stop_loading.load()) {
            return; // Early exit if stop condition is met
          }

          leanstore::worker_thread_id = w_id;
          db->StartTransaction();
          flashbench->LoadInitialData(w_id, range);
          db->CommitTransaction();

          // Check allocated size
          double allocated_size_mb = db->AllocatedSize();
          fprintf(stderr, "wid: %d Space used: %.4f MB\n",
                  leanstore::worker_thread_id, allocated_size_mb);

          // Convert the allocated size to GB and check against the threshold
          if (allocated_size_mb * leanstore::MB >=
              FLAGS_initial_dataset_size_gb * leanstore::GB) {
            stop_loading.store(true); // Signal to stop loading
          }
        });
      });

  db->worker_pool.JoinAll();
  fprintf(stderr, "Space used: %.4f MB\n", db->AllocatedSize());
  LOG_INFO("alloc_cnt: %lu", alloc_cnt);

  // db->DropCache();

  // start profiling
  db->is_loading = false;
  flashbench->pid_count = db->AllocatedPIDsCnt();
  flashbench->InitializePatternGen();
  db->StartProfilingThread();
  ctrl.StartPerfRuntime();
  e.startCounters();

  // YCSB execution
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id]() {
      // LOG_INFO("w_id: %d worker_thread_id: %d", w_id,
      // leanstore::worker_thread_id);
      while (keep_running.load()) {
        db->StartTransaction();
        flashbench->ExecuteTransaction(w_id);
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();

  fprintf(stderr,
          "Total completed txn %lu - Space used: %.4f MB - WAL size: %.4f MB",
          leanstore::statistics::total_txn_completed.load(),
          db->AllocatedSize(), db->WALSize());

  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_txn_completed);
}