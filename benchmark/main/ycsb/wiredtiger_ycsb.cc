#include "benchmark/adapters/wiredtiger_adapter.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/schema.h"
#include "benchmark/ycsb/workload.h"

#include "fmt/core.h"
#include "share_headers/logger.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// We must only use the YCSB record types that wiredtiger_adapter.cc
// explicitly instantiates, otherwise templates won't be linked.
// ---------------------------------------------------------------------------
using Record120 = ycsb::Relation<BytesPayload<120>, 0>;
using Record4096 = ycsb::Relation<BytesPayload<4096>, 0>;
using Record102400 = ycsb::Relation<BytesPayload<102400>, 0>;
using Record10485760 = ycsb::Relation<BytesPayload<10485760>, 0>;

// Payload sizes supported by this *WiredTiger* YCSB runner
static constexpr uint32_t WT_SUPPORTED_PAYLOAD_SIZE[] = {120, 4096, 102400,
                                                         10485760};

static std::mutex g_stats_mutex;

// ---------------------------------------------------------------------------
// Compute WiredTiger DB directory size in GB (similar to Postgres OSFileSizeGB)
// ---------------------------------------------------------------------------
static double WiredTigerDBSizeGB(const std::string &path) {
  namespace fs = std::filesystem;
  std::uintmax_t total_bytes = 0;

  std::error_code ec;
  if (!fs::exists(path, ec))
    return 0.0;

  for (auto const &entry : fs::recursive_directory_iterator(path, ec)) {
    if (entry.is_regular_file()) {
      total_bytes += entry.file_size(ec);
    }
  }
  return static_cast<double>(total_bytes) / (1024.0 * 1024.0 * 1024.0);
}

// ---------------------------------------------------------------------------
// Template: run the YCSB workload for a given Relation<TablePayload, TypeId>
// ---------------------------------------------------------------------------
template <class Record> int RunWiredTigerYCSBTyped() {
  using KeyType = typename Record::Key;
  bool load_phase = (FLAGS_ycsb_exec_seconds == 0);

  auto db = std::make_shared<WiredTigerDB>(load_phase);

  std::atomic<bool> keep_running(false);
  std::atomic<bool> is_loading{false};

  // Main thread: create the table via adapter constructor
  db->PrepareThread();
  auto table = std::make_shared<WiredTigerAdapter<Record>>(*db);
  // We keep this session; worker threads will call PrepareThread() as well.

  // -------------------------------------------------------------------------
  // LOAD PHASE (if exec_seconds == 0) – mirrors postgres_ycsb.cc
  // -------------------------------------------------------------------------
  if (!FLAGS_ycsb_exec_seconds) {
    LOG_INFO("Start loading initial data (WiredTiger, YCSB loader)");

    keep_running.store(false, std::memory_order_relaxed);
    is_loading.store(true, std::memory_order_relaxed);

    tbb::global_control c(tbb::global_control::max_allowed_parallelism,
                          FLAGS_worker_count);

    static std::atomic<uint64_t> g_loaded_keys{0};
    static std::atomic<uint64_t> garbage{0};
    const uint64_t REPORT_EVERY_KEYS = 100000;

    std::atomic<uint64_t> next_key{1};

    const uint64_t CHUNK_SIZE = 1000;

    const double target_bytes =
        (FLAGS_ycsb_dataset_size_gb > 0)
            ? static_cast<double>(FLAGS_ycsb_dataset_size_gb) * 1024.0 *
                  1024.0 * 1024.0
            : 0.0;

    std::vector<std::thread> loader_threads;
    loader_threads.reserve(FLAGS_worker_count + 2);
    loader_threads.emplace_back(db->StartProfilingThread(
        "wiredtiger", is_loading, keep_running, garbage));
    loader_threads.emplace_back(db->StartCheckpointThread(is_loading));

    for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
      loader_threads.emplace_back([&, t_id]() {
        try {
          // One WT session per loader thread
          db->PrepareThread();

          std::string payload;
          payload.resize(FLAGS_ycsb_max_payload_size);

          while (is_loading.load(std::memory_order_relaxed)) {
            uint64_t start =
                next_key.fetch_add(CHUNK_SIZE, std::memory_order_relaxed);
            if (start > FLAGS_ycsb_record_count)
              break;
            uint64_t end =
                std::min(start + CHUNK_SIZE, FLAGS_ycsb_record_count + 1);

            for (uint64_t key = start; key < end; key++) {
              auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
              RandomGenerator::GetRandRepetitiveString(
                  reinterpret_cast<uint8_t *>(payload.data()), 100UL,
                  payload_sz);

              Record record{};
              KeyType rkey;
              rkey.my_key = static_cast<ycsb::YCSBKey>(key);

              // Fill my_payload with random bytes (opaque blob)
              const uint32_t to_copy = std::min<uint32_t>(
                  payload_sz, static_cast<uint32_t>(sizeof(record.my_payload)));
              std::memcpy(&record.my_payload, payload.data(), to_copy);

              table->Insert(rkey, record);

              uint64_t total =
                  g_loaded_keys.fetch_add(1, std::memory_order_relaxed) + 1;

              if (total % REPORT_EVERY_KEYS == 0) {
                double bytes_per_row =
                    sizeof(ycsb::YCSBKey) +
                    static_cast<double>(FLAGS_ycsb_max_payload_size);
                double approx_bytes =
                    bytes_per_row * static_cast<double>(total);
                double approx_gb = approx_bytes / (1024.0 * 1024.0 * 1024.0);

                double pct = static_cast<double>(total) /
                             static_cast<double>(FLAGS_ycsb_record_count) *
                             100.0;

                double db_gb;
                {
                  std::lock_guard<std::mutex> lg(g_stats_mutex);
                  db_gb = WiredTigerDBSizeGB(FLAGS_db_path);
                }

                LOG_INFO("LoadWiredTiger: logical ~%.2f GB (%lu / %lu rows, "
                         "%.2f%%), "
                         "physical=%.4f GB",
                         approx_gb, total,
                         static_cast<unsigned long>(FLAGS_ycsb_record_count),
                         pct, db_gb);

                if (target_bytes > 0.0 &&
                    db_gb * 1024.0 * 1024.0 * 1024.0 >= target_bytes) {
                  LOG_INFO("Stopping load: WT DB size reached dataset target "
                           "(%.4f GB / %.2f GB)",
                           db_gb,
                           static_cast<double>(FLAGS_ycsb_dataset_size_gb));
                  is_loading.store(false, std::memory_order_relaxed);
                  break;
                }
              }

              if (total >= FLAGS_ycsb_record_count) {
                is_loading.store(false, std::memory_order_relaxed);
                break;
              }
            } // for key
          } // while !is_loading

          db->CloseSession();
        } catch (const std::exception &e) {
          LOG_WARN("WiredTiger loader thread %zu exception: %s", t_id,
                   e.what());
        }
      });
    }

    for (auto &th : loader_threads)
      th.join();

    uint64_t total_loaded = g_loaded_keys.load(std::memory_order_relaxed);
    double final_db_gb;
    {
      std::lock_guard<std::mutex> lg(g_stats_mutex);
      final_db_gb = WiredTigerDBSizeGB(FLAGS_db_path);
    }

    LOG_INFO("Load phase completed (WiredTiger). total_loaded=%lu / %lu, "
             "DB size=%.4f GB",
             total_loaded, static_cast<unsigned long>(FLAGS_ycsb_record_count),
             final_db_gb);

    return 0; // load-only run
  }

  // -------------------------------------------------------------------------
  // EXECUTION PHASE (if exec_seconds > 0) – mirrors postgres_ycsb.cc
  // -------------------------------------------------------------------------
  uint64_t actual_records = FLAGS_ycsb_record_count;
  LOG_INFO("Execution phase will use %lu loaded rows (WiredTiger)",
           actual_records);

  keep_running.store(true, std::memory_order_relaxed);
  is_loading.store(false, std::memory_order_relaxed);

  auto zipf_sampler = std::make_unique<RejectionInversionZipfSampler>(
      static_cast<long>(actual_records), FLAGS_ycsb_zipf_theta);

  std::atomic<uint64_t> completed_txn(0);
  std::vector<std::thread> threads;
  threads.reserve(FLAGS_worker_count + 3);
  std::atomic<bool> compact_request{false};
  std::atomic<bool> compact_running{false};

  PerfEvent e;
  e.startCounters();

  // Start profiling thread: prints WT stats + auto-compact if fs too full
  threads.emplace_back(db->StartProfilingThread("wiredtiger", is_loading,
                                                keep_running, completed_txn));
  threads.emplace_back(db->StartCompactionThread(keep_running));
  threads.emplace_back(db->StartCheckpointThread(keep_running));

  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    threads.emplace_back([&, t_id]() {
      try {
        db->PrepareThread();

        std::string payload;
        payload.resize(FLAGS_ycsb_max_payload_size);

        std::random_device randDevice;
        std::mt19937_64 rng{randDevice()};

        while (keep_running.load(std::memory_order_relaxed)) {
          auto access_key = static_cast<UInteger>(zipf_sampler->sample(rng));
          KeyType rkey;
          rkey.my_key = static_cast<ycsb::YCSBKey>(access_key);

          if (RandomGenerator::GetRandU64(0, 100) <= FLAGS_ycsb_read_ratio) {
            // READ
            (void)table->LookUp(rkey, [](const Record &rec) {
              // Touching the payload is optional; lookup is the key work.
              (void)rec;
            });
          } else {
            // UPDATE
            auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
            RandomGenerator::GetRandRepetitiveString(
                reinterpret_cast<uint8_t *>(payload.data()), 100UL, payload_sz);

            Record tmp{};
            bool found = table->LookUp(rkey, [&](const Record &rec) {
              tmp = rec; // copy existing record
            });

            if (found) {
              const uint32_t to_copy = std::min<uint32_t>(
                  payload_sz, static_cast<uint32_t>(sizeof(tmp.my_payload)));
              std::memcpy(&tmp.my_payload, payload.data(), to_copy);

              table->Update(rkey, tmp);
            }
          }

          completed_txn.fetch_add(1, std::memory_order_relaxed);
        }

        db->CloseSession();
      } catch (const std::exception &e) {
        LOG_WARN("WiredTiger run thread %zu exception: %s", t_id, e.what());
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
  keep_running.store(false, std::memory_order_relaxed);
  e.stopCounters();

  for (auto &t : threads) {
    t.join();
  }

  // Any leftover txns not yet consumed by profiler:
  auto leftover = completed_txn.load(std::memory_order_relaxed);
  auto total_txn =
      db->total_txn_completed.load(std::memory_order_relaxed) + leftover;

  double elapsed_sec = static_cast<double>(FLAGS_ycsb_exec_seconds);
  double avg_ops = elapsed_sec > 0.0 ? total_txn / elapsed_sec : 0.0;

  LOG_INFO("YCSB summary (WiredTiger): total_txn=%lu, elapsed=%.1f s, "
           "avg_ops/sec=%.2f",
           total_txn, elapsed_sec, avg_ops);

  e.printReport(std::cout, total_txn);
  return 0;
}

// ---------------------------------------------------------------------------
// MAIN – mirrors postgres_ycsb.cc, but with WT-supported payload sizes
// ---------------------------------------------------------------------------
auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("WiredTiger_YCSB");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Check against the sizes that *this* runner can actually instantiate
  if (std::find(std::begin(WT_SUPPORTED_PAYLOAD_SIZE),
                std::end(WT_SUPPORTED_PAYLOAD_SIZE), FLAGS_ycsb_payload_size) ==
      std::end(WT_SUPPORTED_PAYLOAD_SIZE)) {
    LOG_WARN("Payload size %lu not supported by wiredtiger_adapter, "
             "supported WT sizes are {120, 4096, 102400, 10485760}",
             FLAGS_ycsb_payload_size);
    return 0;
  }

  // Same flag correction logic as postgres_ycsb.cc
  if (!FLAGS_ycsb_random_payload) {
    FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size;
  }

  switch (FLAGS_ycsb_payload_size) {
  case 120:
    return RunWiredTigerYCSBTyped<Record120>();
  case 4096:
    return RunWiredTigerYCSBTyped<Record4096>();
  case 102400:
    return RunWiredTigerYCSBTyped<Record102400>();
  case 10485760:
    return RunWiredTigerYCSBTyped<Record10485760>();
  default:
    LOG_WARN("Unsupported payload size %lu in wiredtiger_ycsb",
             FLAGS_ycsb_payload_size);
    return 0;
  }
}
