#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"
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

using YCSBKeyValue = ycsb::YCSBWorkload<
    LeanStoreAdapter,
    ycsb::Relation<BytesPayload<ycsb::BLOB_NORMAL_PAYLOAD>, 0>>;
using YCSBBlobState =
    ycsb::YCSBWorkload<LeanStoreAdapter, ycsb::BlobStateRelation>;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (std::find(std::begin(ycsb::SUPPORTED_PAYLOAD_SIZE),
                std::end(ycsb::SUPPORTED_PAYLOAD_SIZE),
                FLAGS_ycsb_payload_size) ==
      std::end(ycsb::SUPPORTED_PAYLOAD_SIZE)) {
    LOG_WARN(
        "Payload size %lu not supported, check ycsb::SUPPORTED_PAYLOAD_SIZE",
        FLAGS_ycsb_payload_size);
    return 0;
  }

  // Flags correction
  if (!FLAGS_ycsb_random_payload) {
    FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size;
  }

  // Setup worker-local payload
  ycsb::WorkerLocalPayloads payloads(FLAGS_worker_count);
  for (auto &payload : payloads) {
    payload.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE))
                      uint8_t[FLAGS_ycsb_max_payload_size]());
  }

  // Setup env
  tbb::global_control c(tbb::global_control::max_allowed_parallelism,
                        FLAGS_worker_count);
  std::atomic<bool> keep_running(true);
  leanstore::RegisterSEGFAULTHandler();

  // Initialize LeanStore
  auto db = std::make_unique<leanstore::LeanStore>();
  std::unique_ptr<ycsb::YCSBWorkloadInterface> ycsb;

  if (FLAGS_ycsb_payload_size == ycsb::BLOB_NORMAL_PAYLOAD) {
    ycsb = std::make_unique<YCSBKeyValue>(
        FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta,
        false, payloads, *db);
  } else {
    ycsb = std::make_unique<YCSBBlobState>(
        FLAGS_ycsb_record_count, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta,
        true, payloads, *db);
  }

  // YCSB loader
  // YCSB loader
  db->is_loading = true;
  std::atomic<UInteger> w_id_loader = 0;
  std::atomic<bool> stop_loading = false;

  Integer start_range = 1;
  std::atomic<uint64_t> total_records_loaded = 0;
  const size_t grain_size = 1024;
  Integer end_range = FLAGS_ycsb_record_count > INT_MAX / 2
                          ? INT_MAX / 2
                          : FLAGS_ycsb_record_count; // Upper bound

  // Compute target allocated size
  const double target_allocated_size =
      FLAGS_ycsb_dataset_size_gb * leanstore::GB;
  LOG_INFO("Start loading the dataset");

  while (true) {
    if (stop_loading.load()) {
      break;
    }

    tbb::parallel_for(
        tbb::blocked_range<Integer>(start_range, end_range, grain_size),
        [&](const tbb::blocked_range<Integer> &range) {
          if (stop_loading.load())
            return;
          int w_id = w_id_loader.fetch_add(1, std::memory_order_relaxed) %
                     FLAGS_worker_count;

          db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id, range]() {
            if (stop_loading.load())
              return;

            db->StartTransaction();
            ycsb->LoadInitialData(w_id, range);
            db->CommitTransaction();

            thread_local int local_records = 0;
            int records_loaded_now = range.end() - range.begin() + 1;
            local_records += records_loaded_now;

            if (local_records >= 128) {
              total_records_loaded.fetch_add(local_records,
                                             std::memory_order_relaxed);
              local_records = 0;
            }

            double allocated_size_mb = db->AllocatedSize();
            fprintf(stderr,
                    "Current status: Space used: %.2f GB, loaded %.2f%% record "
                    "cnt: %lu\n",
                    allocated_size_mb / 1024.0,
                    allocated_size_mb * leanstore::MB / target_allocated_size *
                        100.0,
                    total_records_loaded.load());

            if (allocated_size_mb * leanstore::MB >= target_allocated_size ||
                total_records_loaded.load() >= FLAGS_ycsb_record_count) {
              if (local_records > 0) {
                total_records_loaded.fetch_add(local_records,
                                               std::memory_order_relaxed);
                local_records = 0;
              }
              stop_loading.store(true, std::memory_order_relaxed);
              return;
            }
          });
        });

    // Wait for all scheduled jobs to complete
    db->worker_pool.JoinAll();

    // Check again after job completion in case stop flag wasn't caught early
    double allocated_size_mb = db->AllocatedSize();
    if (allocated_size_mb * leanstore::MB >= target_allocated_size ||
        total_records_loaded.load() >= FLAGS_ycsb_record_count) {
      stop_loading.store(true, std::memory_order_relaxed);
      break;
    }

    ycsb->phase_cnt++;
  }

  fprintf(stderr, "Final space used: %.2f GB, total_records_loaded: %lu\n",
          db->AllocatedSize() / 1024.0, total_records_loaded.load());

  // Init zipfian distribution generator
  ycsb->InitZipfGenerator(total_records_loaded.load());
  ycsb->total_records_loaded = total_records_loaded.load();
  db->is_loading = false;
  db->StartProfilingThread();

  // YCSB execution
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id]() {
      while (keep_running.load()) {
        db->StartTransaction();
        ycsb->ExecuteTransaction(w_id);
        db->CommitTransaction();
      }
    });
  }

  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
  keep_running = false;

  db->Shutdown();
  fprintf(stderr,
          "Total completed txn %lu - Space used: %.4f MB - WAL size: %.4f MB",
          leanstore::statistics::total_txn_completed.load(),
          db->AllocatedSize(), db->WALSize());
}