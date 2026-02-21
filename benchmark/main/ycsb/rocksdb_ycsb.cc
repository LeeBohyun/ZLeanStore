#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"

#include "benchmark/adapters/rocksdb_adapter.h"

#include "share_headers/perf_ctrl.h"
#include "share_headers/perf_event.h"

#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <gflags/gflags.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <limits>
#include <string>
#include <thread>
#include <vector>

// Helper to query RocksDB allocated size (in MB)
static double RocksDBAllocatedSizeMB(rocksdb::DB *db) {
  std::string value;
  // total SST size is a good approximation for space usage
  if (db->GetProperty("rocksdb.total-sst-files-size", &value)) {
    long long bytes = std::stoll(value);
    return static_cast<double>(bytes) / (1024.0 * 1024.0);
  }
  return 0.0;
}

// Helper to query RocksDB WAL size (in MB)
static double RocksDBWALSizeMB(rocksdb::DB *db) {
  std::string value;
  if (db->GetProperty("rocksdb.total-log-size", &value)) {
    long long bytes = std::stoll(value);
    return static_cast<double>(bytes) / (1024.0 * 1024.0);
  }
  return 0.0;
}

// Implemented in rocksdb_adapter.cc
rocksdb::DB *OpenRocksDB();

// Type aliases for YCSB workloads using RocksDBAdapter
using YCSBKeyValue = ycsb::YCSBWorkload<
    RocksDBAdapter, ycsb::Relation<BytesPayload<ycsb::BLOB_NORMAL_PAYLOAD>, 0>>;

using YCSBBlobState =
    ycsb::YCSBWorkload<RocksDBAdapter, ycsb::BlobStateRelation>;

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("RocksDB YCSB");
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

  // Behaviour:
  //   exec_seconds == 0  -> LOAD-ONLY (no run phase)
  //   exec_seconds > 0   -> RUN-ONLY (no load in this process)
  const bool load_only = (FLAGS_ycsb_exec_seconds == 0);
  const bool run_only = (FLAGS_ycsb_exec_seconds > 0);

  // Limit parallelism to worker_count (for TBB)
  tbb::global_control c(tbb::global_control::max_allowed_parallelism,
                        FLAGS_worker_count);

  // Profiling control flags
  std::atomic<bool> is_loading{load_only}; // true only in load-only mode
  std::atomic<bool> keep_running{false};   // true during run phase
  std::atomic<uint64_t> completed_txn{0};  // tx/sec counter for profiler

  // ---------------------------------------------------------------------------
  // Initialize RocksDB  (ONE DB instance)
  // ---------------------------------------------------------------------------
  auto db = std::unique_ptr<rocksdb::DB>(OpenRocksDB());

  // ---------------------------------------------------------------------------
  // Create one YCSB workload object per worker.
  // Each workload gets its OWN WorkerLocalPayloads vector.
  // ---------------------------------------------------------------------------
  std::vector<ycsb::WorkerLocalPayloads> all_payloads(FLAGS_worker_count);

  std::vector<std::unique_ptr<ycsb::YCSBWorkloadInterface>> workloads;
  workloads.reserve(FLAGS_worker_count);

  // This is how many records *we intend* to exist in the DB globally.
  const u64 total_record_count = FLAGS_ycsb_record_count;

  // Per-thread record count (logical; workload may interpret it differently)
  const u64 record_count_per_thread =
      (FLAGS_worker_count > 0)
          ? (total_record_count + FLAGS_worker_count - 1) / FLAGS_worker_count
          : total_record_count;

  for (size_t w = 0; w < FLAGS_worker_count; ++w) {
    // Allocate payloads for this specific workload
    auto &payloads = all_payloads[w];
    payloads.resize(FLAGS_worker_count);
    for (auto &p : payloads) {
      p.reset(new (static_cast<std::align_val_t>(GLOBAL_BLOCK_SIZE))
                  uint8_t[FLAGS_ycsb_max_payload_size]());
    }

    if (FLAGS_ycsb_payload_size == ycsb::BLOB_NORMAL_PAYLOAD) {
      workloads.emplace_back(std::make_unique<YCSBKeyValue>(
          record_count_per_thread, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta,
          false,      // is_blob_state
          payloads,   // worker-local payloads for this workload
          db.get())); // same DB*, different RocksDBAdapter inside
    } else {
      workloads.emplace_back(std::make_unique<YCSBBlobState>(
          record_count_per_thread, FLAGS_ycsb_read_ratio, FLAGS_ycsb_zipf_theta,
          true, // is_blob_state
          payloads, db.get()));
    }
  }

  // ===========================================================================
  // PHASE 1: LOAD-ONLY MODE (exec_seconds == 0)
  // This will only run when you call the binary with --ycsb_exec_seconds=0.
  // In run-only invocations (exec_seconds>0) this block is skipped entirely.
  // ===========================================================================
  if (load_only) {

    // Profiling adapter bound to the same DB
    RocksDBAdapter<ycsb::Relation<BytesPayload<ycsb::BLOB_NORMAL_PAYLOAD>, 0>>
        profiling_adapter(db.get());

    // Start profiling thread: runs while (is_loading || keep_running)
    std::thread profiling_thread = profiling_adapter.StartProfilingThread(
        "RocksDB", is_loading, keep_running, completed_txn);

    LOG_INFO("Start loading the dataset (load-only run): %lu records",
             total_record_count);

    std::atomic<UInteger> w_id_loader{0};
    std::atomic<uint64_t> total_records_loaded{0};
    std::atomic<bool> stop_loading{false};

    const size_t grain_size = 1024;

    // Keep the range bounded so it fits safely in Integer (if Integer is
    // 32-bit).
    const Integer start_range = 1;
    const Integer end_range = std::numeric_limits<Integer>::max() /
                              2; // e.g., ~1B if Integer is 32-bit

    std::fprintf(stderr, "[DEBUG] Load range: [%lld, %lld), grain_size=%zu\n",
                 static_cast<long long>(start_range),
                 static_cast<long long>(end_range), grain_size);

    while (!stop_loading.load(std::memory_order_relaxed) &&
           total_records_loaded.load(std::memory_order_relaxed) <
               total_record_count) {

      tbb::parallel_for(
          tbb::blocked_range<Integer>(start_range, end_range, grain_size),
          [&](const tbb::blocked_range<Integer> &range) {
            if (stop_loading.load(std::memory_order_relaxed)) {
              return;
            }

            int w_id = w_id_loader.fetch_add(1, std::memory_order_relaxed) %
                       FLAGS_worker_count;
            auto &wl = workloads[w_id];

            // This uses wl->phase_cnt inside LoadInitialData() to shift key
            // space.
            wl->LoadInitialData(static_cast<UInteger>(w_id), range);

            uint64_t records_loaded_now =
                static_cast<uint64_t>(range.end() - range.begin());

            // Count these as "transactions" for profiling (optional)
            completed_txn.fetch_add(records_loaded_now,
                                    std::memory_order_relaxed);

            uint64_t new_total =
                total_records_loaded.fetch_add(records_loaded_now,
                                               std::memory_order_relaxed) +
                records_loaded_now;

            // If we've logically reached or exceeded the requested count,
            // signal all threads to stop.
            if (new_total >= total_record_count) {
              stop_loading.store(true, std::memory_order_relaxed);
            }

            std::fprintf(stderr,
                         "Load phase Space used: %.2f GB, "
                         "total_records_loaded: %lu pct: %.2f\n",
                         RocksDBAllocatedSizeMB(db.get()) / 1024.0,
                         total_records_loaded.load(),
                         total_records_loaded.load() * 100.0 /
                             static_cast<double>(FLAGS_ycsb_record_count));
          });

      // Move to the next "phase" of the logical keyspace:
      // each workload has its own phase_cnt.
      for (auto &wl : workloads) {
        wl->phase_cnt++;
      }
    }

    std::fprintf(stderr,
                 "Load phase complete (load-only). Space used: %.2f GB, "
                 "total_records_loaded: %lu\n",
                 RocksDBAllocatedSizeMB(db.get()) / 1024.0,
                 total_records_loaded.load());

    // Mark end of load phase, stop profiler, and exit.
    is_loading.store(false, std::memory_order_relaxed);
    keep_running.store(false, std::memory_order_relaxed);

    if (profiling_thread.joinable())
      profiling_thread.join();

    return 0;
  }

  // ===========================================================================
  // PHASE 2: RUN-ONLY MODE (exec_seconds > 0)
  // Here we assume the DB was already loaded by a previous exec_seconds=0 run.
  // No loading happens in this process.
  // ===========================================================================

  if (run_only) {
    LOG_INFO(
        "Execution phase only: duration=%lu s, expecting %lu pre-loaded rows",
        FLAGS_ycsb_exec_seconds, total_record_count);

    // No load here — DB must already contain total_record_count rows.

    // Initialize Zipf generator in each workload based on the per-thread count
    for (auto &wl : workloads) {
      wl->InitZipfGenerator(record_count_per_thread);
      wl->total_records_loaded = record_count_per_thread;
    }

    // Mark that we're in run phase for the profiler
    is_loading.store(false, std::memory_order_relaxed);
    keep_running.store(true, std::memory_order_relaxed);

    // Profiling adapter bound to the same DB
    RocksDBAdapter<ycsb::Relation<BytesPayload<ycsb::BLOB_NORMAL_PAYLOAD>, 0>>
        profiling_adapter(db.get());

    // Start profiling thread: runs while (is_loading || keep_running)
    std::thread profiling_thread = profiling_adapter.StartProfilingThread(
        "RocksDB", is_loading, keep_running, completed_txn);

    // RUN PHASE: YCSB execution (each thread uses its own workload instance)
    std::vector<std::thread> workers;
    workers.reserve(FLAGS_worker_count);

    for (size_t w_id = 0; w_id < FLAGS_worker_count; w_id++) {
      workers.emplace_back([&, w_id]() {
        auto &wl = workloads[w_id]; // per-thread workload ("connection")
        while (keep_running.load(std::memory_order_relaxed)) {
          wl->ExecuteTransaction(static_cast<UInteger>(w_id));
          completed_txn.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));

    // End execution phase
    keep_running.store(false, std::memory_order_relaxed);

    for (auto &t : workers) {
      if (t.joinable())
        t.join();
    }

    // Profiling thread stops when both is_loading == false and keep_running ==
    // false
    if (profiling_thread.joinable())
      profiling_thread.join();

    std::fprintf(stderr, "Done. Space used: %.4f MB - WAL size: %.4f MB\n",
                 RocksDBAllocatedSizeMB(db.get()), RocksDBWALSizeMB(db.get()));

    return 0;
  }
  return 0;
}
