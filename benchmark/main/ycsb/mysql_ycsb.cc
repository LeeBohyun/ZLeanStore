#include "benchmark/adapters/sql_databases.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/workload.h"

#include "fmt/core.h"
#include "share_headers/logger.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <iterator>

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("MySQL YCSB");
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

  // Init MySQL
  auto db = std::make_unique<MySQLDB>();
  db->PrepareThread();

  if (!FLAGS_ycsb_exec_seconds) {
    // ===========================
    // LOAD PHASE ONLY
    // ===========================
    db->StartTransaction();
    std::unique_ptr<sql::Statement> stmt(db->conn->createStatement());
    stmt->execute("DROP TABLE IF EXISTS YCSB_TABLE;");
    if (FLAGS_ycsb_max_payload_size == ycsb::BLOB_NORMAL_PAYLOAD) {
      stmt->execute(
          fmt::format("CREATE TABLE YCSB_TABLE (my_key BIGINT UNSIGNED PRIMARY "
                      "KEY, my_payload VARCHAR({}));",
                      FLAGS_ycsb_max_payload_size));
    } else {
      stmt->execute(
          "CREATE TABLE YCSB_TABLE (my_key BIGINT UNSIGNED PRIMARY KEY, "
          "my_payload MEDIUMBLOB);");
    }
    stmt->execute("ALTER TABLE YCSB_TABLE COMPRESSION='None';");
    db->CommitTransaction();

    LOG_INFO("Start loading initial data");

    tbb::global_control c(tbb::global_control::max_allowed_parallelism,
                          FLAGS_worker_count);

    static std::atomic<uint64_t> g_loaded_keys{0};

    // log every N inserted keys
    const uint64_t REPORT_EVERY_KEYS = 100000;

    std::atomic<uint64_t> next_key{1};
    std::atomic<bool> stop_loading{false};

    const uint64_t CHUNK_SIZE = 1000;

    const double target_bytes =
        (FLAGS_ycsb_dataset_size_gb > 0)
            ? static_cast<double>(FLAGS_ycsb_dataset_size_gb) * 1024.0 *
                  1024.0 * 1024.0
            : 0.0;

    LOG_INFO("Start loading initial data (MySQL, LeanStore-style loader)");

    std::vector<std::thread> loader_threads;
    loader_threads.reserve(FLAGS_worker_count);

    for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
      loader_threads.emplace_back([&, t_id]() {
        db->PrepareThread();

        std::string payload;
        payload.resize(FLAGS_ycsb_max_payload_size);

        std::unique_ptr<sql::PreparedStatement> insert_stmt(
            db->conn->prepareStatement(
                "INSERT INTO YCSB_TABLE (my_key, my_payload) VALUES (?, ?)"));

        while (!stop_loading.load(std::memory_order_relaxed)) {
          uint64_t start =
              next_key.fetch_add(CHUNK_SIZE, std::memory_order_relaxed);
          if (start > FLAGS_ycsb_record_count) {
            break; // nothing left to insert
          }

          uint64_t end =
              std::min(start + CHUNK_SIZE, FLAGS_ycsb_record_count + 1);

          db->StartTransaction();
          for (uint64_t key = start; key < end; key++) {
            auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
            RandomGenerator::GetRandRepetitiveString(
                reinterpret_cast<uint8_t *>(payload.data()), 100UL, payload_sz);

            insert_stmt->setUInt64(1, key);
            insert_stmt->setString(2, payload.substr(0, payload_sz));
            insert_stmt->execute();

            uint64_t total =
                g_loaded_keys.fetch_add(1, std::memory_order_relaxed) + 1;

            if (total % REPORT_EVERY_KEYS == 0) {
              double bytes_per_row =
                  8.0 + static_cast<double>(FLAGS_ycsb_max_payload_size);
              double approx_bytes = bytes_per_row * static_cast<double>(total);
              double approx_gb = approx_bytes / (1024.0 * 1024.0 * 1024.0);

              double pct = static_cast<double>(total) /
                           static_cast<double>(FLAGS_ycsb_record_count) * 100.0;

              double db_gb = db->OSFileSizeGB();

              LOG_INFO("LoadMySQL: logical ~%.2f GB (%lu / %lu rows, %.2f%%), "
                       "physical=%.4f GB",
                       approx_gb, total,
                       static_cast<unsigned long>(FLAGS_ycsb_record_count), pct,
                       db_gb);

              if (target_bytes > 0.0 &&
                  db_gb * 1024.0 * 1024.0 * 1024.0 >= target_bytes) {
                LOG_INFO(
                    "Stopping load: MySQL DatabaseSize() reached dataset "
                    "target (%.4f GB / %lu GB)",
                    db_gb,
                    static_cast<unsigned long>(FLAGS_ycsb_dataset_size_gb));
                stop_loading.store(true, std::memory_order_relaxed);
                break;
              }
            }

            if (total >= FLAGS_ycsb_record_count) {
              stop_loading.store(true, std::memory_order_relaxed);
              break;
            }
          } // for key

          db->CommitTransaction();
        } // while !stop_loading
      });
    }

    for (auto &th : loader_threads) {
      th.join();
    }

    uint64_t total_loaded = g_loaded_keys.load(std::memory_order_relaxed);
    double final_db_gb = db->OSFileSizeGB();

    LOG_INFO("Load phase completed (MySQL). total_loaded=%lu / %lu, "
             "DatabaseSize()=%.4f GB",
             total_loaded, static_cast<unsigned long>(FLAGS_ycsb_record_count),
             final_db_gb);

    // load-only run ends here
    return 0;
  } else {
    // ===========================
    // EXECUTION PHASE
    // ===========================

    // If you want to be precise, uncomment this COUNT(*) query:
    uint64_t actual_records = FLAGS_ycsb_record_count;
    // {
    //   std::unique_ptr<sql::Statement> stmt(db->conn->createStatement());
    //   std::unique_ptr<sql::ResultSet> res(
    //       stmt->executeQuery("SELECT COUNT(*) FROM YCSB_TABLE;"));
    //   if (res->next()) {
    //     actual_records = res->getUInt64(1);
    //   }
    // }
    LOG_INFO("Execution phase will use %lu loaded rows", actual_records);

    auto zipf_sampler = std::make_unique<RejectionInversionZipfSampler>(
        static_cast<long>(actual_records), FLAGS_ycsb_zipf_theta);

    std::atomic<bool> keep_running(true);
    std::atomic<uint64_t> completed_txn(0);
    std::vector<std::thread> threads;
    PerfEvent e;

    threads.emplace_back(
        db->StartProfilingThread("mysql", keep_running, completed_txn));

    e.startCounters();

    for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
      threads.emplace_back([&]() {
        db->PrepareThread();

        std::string payload;
        payload.resize(FLAGS_ycsb_max_payload_size);
        std::random_device rd;
        std::mt19937_64 rng{rd()};

        auto select_stmt =
            std::unique_ptr<sql::PreparedStatement>(db->conn->prepareStatement(
                "SELECT * FROM YCSB_TABLE WHERE my_key = ?;"));

        auto update_stmt =
            std::unique_ptr<sql::PreparedStatement>(db->conn->prepareStatement(
                "UPDATE YCSB_TABLE SET my_payload = ? WHERE my_key = ?;"));

        while (keep_running.load()) {
          auto access_key = static_cast<UInteger>(zipf_sampler->sample(rng));

          db->StartTransaction();
          if (RandomGenerator::GetRandU64(0, 100) <= FLAGS_ycsb_read_ratio) {
            select_stmt->setUInt64(1, access_key);
            auto res = select_stmt->executeQuery();
            res->next();
          } else {
            auto payload_sz = ycsb::YCSBWorkloadInterface::PayloadSize();
            RandomGenerator::GetRandRepetitiveString(
                reinterpret_cast<uint8_t *>(payload.data()), 100UL, payload_sz);

            update_stmt->setString(1, payload.substr(0, payload_sz));
            update_stmt->setUInt64(2, access_key);
            update_stmt->execute();
          }
          db->CommitTransaction();

          completed_txn++;
        }
      });
    }

    std::this_thread::sleep_for(std::chrono::seconds(FLAGS_ycsb_exec_seconds));
    keep_running = false;
    e.stopCounters();

    for (auto &t : threads) {
      t.join();
    }

    auto total_txn = db->total_txn_completed.load();
    double elapsed_sec = static_cast<double>(FLAGS_ycsb_exec_seconds);
    double avg_ops = elapsed_sec > 0.0 ? total_txn / elapsed_sec : 0.0;
    LOG_INFO("YCSB summary: total_txn=%lu, elapsed=%.1f s, avg_ops/sec=%.2f",
             total_txn, elapsed_sec, avg_ops);

    e.printReport(std::cout, total_txn);
  }
}
