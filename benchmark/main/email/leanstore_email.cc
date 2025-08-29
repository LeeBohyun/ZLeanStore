#include "benchmark/email/config.h"
#include "benchmark/email/workload.h"
#include "leanstore/leanstore.h"

#include "gflags/gflags.h"
#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

// using AdapterClass = email::Email<LeanStoreAdapter, email::Relation<1>>;
using AdapterClass =
    email::Email<LeanStoreAdapter, leanstore::schema::InrowBlobRelation<0>>;

// using AdapterClass = email::Email<LeanStoreAdapter>;
auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore Email");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (!FLAGS_email_random_payload) {
    FLAGS_email_max_payload_size = FLAGS_email_payload_size;
  }

  // Setup worker-local payload
  email::WorkerLocalPayloads payloads(FLAGS_worker_count);
  for (auto &payload : payloads) {
    payload.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE))
                      Varchar<60>[FLAGS_email_max_payload_size]());
  }

  // Setup env
  tbb::global_control c(tbb::global_control::max_allowed_parallelism,
                        FLAGS_worker_count);
  leanstore::RegisterSEGFAULTHandler();
  PerfEvent e;
  PerfController ctrl;

  // LeanStore & Workload initialization
  auto db = std::make_unique<leanstore::LeanStore>();
  auto email = std::make_unique<AdapterClass>(
      FLAGS_email_record_count, FLAGS_email_read_ratio, FLAGS_email_zipf_theta,
      payloads, *db);

  // Drop cache before the experiment
  // if (FLAGS_email_clear_cache_before_expr) { db->DropCache(); }

  // email loader
  LOG_INFO("Start loading initial data");
  auto time_start = std::chrono::high_resolution_clock::now();
  std::atomic<UInteger> w_id_loader = 0;
  tbb::parallel_for(
      tbb::blocked_range<Integer>(1, FLAGS_email_record_count + 1),
      [&](const tbb::blocked_range<Integer> &range) {
        auto w_id = (++w_id_loader) % FLAGS_worker_count;
        db->worker_pool.ScheduleAsyncJob(w_id, [&, w_id, range]() {
          db->StartTransaction();
          email->LoadInitialData(w_id, range);
          db->CommitTransaction();
        });
      });
  db->worker_pool.JoinAll();
  auto time_end = std::chrono::high_resolution_clock::now();
  LOG_INFO("Time: %lu ms - Space used: %.4f GB",
           std::chrono::duration_cast<std::chrono::milliseconds>(time_end -
                                                                 time_start)
               .count(),
           db->AllocatedSize());

  // email Read-only Workload
  std::atomic<bool> keep_running(true);
  ctrl.StartPerfRuntime();
  db->StartProfilingThread();
  e.startCounters();
  time_start = std::chrono::high_resolution_clock::now();

  // Email data execution
  for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
    db->worker_pool.ScheduleAsyncJob(w_id, [&]() {
      while (keep_running.load()) {
        db->StartTransaction();
        email->ExecuteTransaction(w_id);
        db->CommitTransaction();
      }
    });
  }
  time_end = std::chrono::high_resolution_clock::now();
  // Run for a few seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_email_exec_seconds));
  keep_running = false;
  ctrl.StopPerfRuntime();
  db->Shutdown();
  LOG_INFO("Total completed txn %lu - Space used: %.4f GB - WAL size: %.4f GB",
           leanstore::statistics::total_txn_completed.load(),
           db->AllocatedSize(), db->WALSize());
  e.stopCounters();
  e.printReport(std::cout, leanstore::statistics::total_txn_completed);
}