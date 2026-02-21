#include "benchmark/adapters/sql_databases.h"
#include <pqxx/pqxx>

#include "mysql_driver.h"

// Thread-local connection & flag for prepared statements
inline thread_local std::unique_ptr<pqxx::connection> tls_conn;
inline thread_local bool tls_prepared = false;

auto BaseDatabase::StartProfilingThread(
    const std::string &system_name, std::atomic<bool> &keep_running,
    std::atomic<uint64_t> &completed_txn) -> std::thread {
  return std::thread([&]() {
    uint64_t cnt = 0;
    std::printf("system,ts,tx\n");
    while (keep_running.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto progress = completed_txn.exchange(0);
      total_txn_completed += progress;
      std::printf("%s,%lu,%lu\n", system_name.c_str(), cnt++, progress);
    }
    std::printf("Halt Profiling thread\n");
  });
}

// -------------------------------------------------------------------------------------
SQLiteDB::SQLiteDB(const std::string &path) : db_path(path), ui(path) {
  ui << "PRAGMA journal_mode = WAL";
  ui << "PRAGMA synchronous = NORMAL";
  ui << "PRAGMA read_uncommitted = true;";
  ui << "PRAGMA page_size = 4096";
  ui << "PRAGMA cache_size = 8388608;";
}

void SQLiteDB::StartTransaction(bool serializable) {
  if (serializable) {
    ui << "BEGIN IMMEDIATE;";
  } else {
    ui << "BEGIN";
  }
}

void SQLiteDB::CommitTransaction() { ui << "COMMIT"; }

auto SQLiteDB::DatabaseSize() -> float {
  uint64_t page_cnt;
  ui << "PRAGMA page_count" >> page_cnt;
  return static_cast<float>(page_cnt * 4096) / (1024 * 1024 * 1024);
}

// -------------------------------------------------------------------------------------
/**
 * Remember to disable peer authentication for Postgres user
 */
PostgresDB::PostgresDB() {
  const auto db_conn = "host=/home/lee/pgsock "
                       "dbname=postgres "
                       "user=postgres "
                       "port=5432 "
                       "sslmode=disable";
  conn = new pqxx::connection{db_conn};
}

PostgresDB::~PostgresDB() { delete conn; }
void PostgresDB::PrepareThread() {
  if (!tls_conn) {
    // Use the same connection string as in the ctor
    const auto db_conn = "host=/home/lee/pgsock "
                         "dbname=postgres "
                         "user=postgres "
                         "port=5432 "
                         "sslmode=disable";
    tls_conn = std::make_unique<pqxx::connection>(db_conn);

    // You can set a default isolation level if you want:
    // pqxx::work tx{*tls_conn};
    // if (FLAGS_txn_default_isolation_level == "rc") {
    //   tx.exec("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL
    //   READ COMMITTED");
    // } else if (...) { ... }
    // tx.commit();
  }

  if (!tls_prepared) {
    // Per-thread prepared statements on this thread's connection
    tls_conn->prepare(
        "ycsb_insert",
        "INSERT INTO ycsb_table (my_key, my_payload) VALUES ($1, $2)");
    tls_conn->prepare("ycsb_select",
                      "SELECT my_payload FROM ycsb_table WHERE my_key = $1");
    tls_conn->prepare(
        "ycsb_update",
        "UPDATE ycsb_table SET my_payload = $1 WHERE my_key = $2");
    tls_prepared = true;
  }
}

// Use the thread-local connection for transactions
void PostgresDB::StartTransaction(bool si) {
  assert(txn == nullptr);
  if (!tls_conn) {
    PrepareThread();
  }

  if (si) {
    txn = std::make_unique<
        pqxx::transaction<pqxx::isolation_level::repeatable_read>>(*tls_conn);
  } else {
    txn = std::make_unique<
        pqxx::transaction<pqxx::isolation_level::read_committed>>(*tls_conn);
  }
}

void PostgresDB::CommitTransaction() {
  assert(txn != nullptr);
  txn->commit();
  txn = nullptr;
}

auto PostgresDB::DatabaseSize() -> float {
  pqxx::work tx{*conn};
  auto db_size =
      tx.query_value<uint64_t>("SELECT pg_database_size('postgres');");
  return static_cast<float>(db_size) / (1024 * 1024 * 1024);
}

auto PostgresDB::OSFileSizeGB() -> double {
  // Approximate table size; adjust schema/table names as needed
  pqxx::work tx{*conn};
  auto bytes = tx.query_value<uint64_t>(
      "SELECT COALESCE(pg_total_relation_size('ycsb_table'), 0);");
  tx.commit();
  return static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
}

std::thread
PostgresDB::StartProfilingThread(const std::string &system_name,
                                 std::atomic<bool> &keep_running,
                                 std::atomic<uint64_t> &completed_txn) {
  return std::thread([this, system_name, &keep_running, &completed_txn]() {
    try {
      std::fprintf(stderr, "Postgres profiling thread: starting\n");

      // Dedicated monitoring connection over TCP
      const char *mon_conn_str = "host=127.0.0.1 "
                                 "dbname=postgres "
                                 "user=postgres "
                                 "port=5432 "
                                 "sslmode=disable";

      std::fprintf(stderr, "Postgres profiling thread: connecting to %s\n",
                   mon_conn_str);
      auto mon_conn = std::make_unique<pqxx::connection>(mon_conn_str);
      std::fprintf(stderr, "Postgres profiling thread: connected\n");

      // -------------------------------------------------------------------
      // Helpers
      // -------------------------------------------------------------------
      auto get_block_size = [&]() -> std::uint64_t {
        try {
          pqxx::work tx{*mon_conn};
          auto r = tx.exec1("SHOW block_size;");
          tx.commit();
          return r[0].as<std::uint64_t>(8192); // default 8KB
        } catch (const std::exception &e) {
          std::fprintf(stderr,
                       "Postgres profiling: SHOW block_size failed: %s\n",
                       e.what());
          return 8192;
        }
      };

      // -------- I/O stats (now split by backend_type) --------
      struct IoStats {
        std::uint64_t writes_client;
        std::uint64_t writes_bgwriter;
        std::uint64_t writes_checkpointer;
        std::uint64_t fsyncs_total;
      };

      auto get_io_stats = [&]() -> IoStats {
        IoStats s{0, 0, 0, 0};
        try {
          pqxx::work tx{*mon_conn};
          auto res = tx.exec(
              "SELECT backend_type, "
              "       COALESCE(sum(writes),0) AS writes, "
              "       COALESCE(sum(fsyncs),0) AS fsyncs "
              "FROM pg_stat_io "
              "WHERE backend_type IN ('client backend','background "
              "writer','checkpointer') "
              "  AND object IN ('relation','index','toast','temp relation') "
              "  AND context IN ('normal','bulkread','bulkwrite') "
              "GROUP BY backend_type;");
          tx.commit();

          for (auto const &row : res) {
            std::string backend = row[0].as<std::string>();
            std::uint64_t w = row[1].as<std::uint64_t>(0);
            std::uint64_t f = row[2].as<std::uint64_t>(0);

            if (backend == "client backend") {
              s.writes_client = w;
            } else if (backend == "background writer") {
              s.writes_bgwriter = w;
            } else if (backend == "checkpointer") {
              s.writes_checkpointer = w;
            }
            s.fsyncs_total += f;
          }
        } catch (const std::exception &e) {
          std::fprintf(stderr,
                       "Postgres profiling: pg_stat_io unavailable: %s\n",
                       e.what());
        }
        return s;
      };

      struct DbStats {
        std::uint64_t blks_read;
        std::uint64_t blks_hit;
      };

      auto get_db_stats = [&]() -> DbStats {
        DbStats s{0, 0};
        try {
          pqxx::work tx{*mon_conn};
          auto r = tx.exec1("SELECT blks_read, blks_hit "
                            "FROM pg_stat_database "
                            "WHERE datname = current_database();");
          tx.commit();
          s.blks_read = r[0].as<std::uint64_t>(0);
          s.blks_hit = r[1].as<std::uint64_t>(0);
        } catch (const std::exception &e) {
          std::fprintf(stderr,
                       "Postgres profiling: pg_stat_database failed: %s\n",
                       e.what());
        }
        return s;
      };

      // ---- WAL stats (unchanged) ----
      struct WalStats {
        std::uint64_t wal_records;
        std::uint64_t wal_fpi;          // number of full-page images
        std::uint64_t wal_bytes;        // total WAL bytes
        std::uint64_t wal_buffers_full; // how often WAL buffers ran full
      };

      auto get_wal_stats = [&]() -> WalStats {
        WalStats s{0, 0, 0, 0};
        try {
          pqxx::work tx{*mon_conn};
          auto r = tx.exec1("SELECT "
                            "  COALESCE(wal_records,0), "
                            "  COALESCE(wal_fpi,0), "
                            "  COALESCE(wal_bytes,0), "
                            "  COALESCE(wal_buffers_full,0) "
                            "FROM pg_stat_wal;");
          tx.commit();
          s.wal_records = r[0].as<std::uint64_t>(0);
          s.wal_fpi = r[1].as<std::uint64_t>(0);
          s.wal_bytes = r[2].as<std::uint64_t>(0);
          s.wal_buffers_full = r[3].as<std::uint64_t>(0);
        } catch (const std::exception &e) {
          std::fprintf(stderr, "Postgres profiling: pg_stat_wal failed: %s\n",
                       e.what());
        }
        return s;
      };

      auto get_table_size_gb = [&]() -> double {
        try {
          pqxx::work tx{*mon_conn};
          auto r = tx.exec1(
              "SELECT COALESCE(pg_total_relation_size('ycsb_table'), 0);");
          tx.commit();
          std::uint64_t bytes = r[0].as<std::uint64_t>(0);
          return static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
        } catch (const std::exception &e) {
          std::fprintf(
              stderr, "Postgres profiling: pg_total_relation_size failed: %s\n",
              e.what());
          return 0.0;
        }
      };

      // -------------------------------------------------------------------
      // Initial snapshots
      // -------------------------------------------------------------------
      std::uint64_t block_size = get_block_size();
      if (block_size == 0)
        block_size = 8192;

      DbStats last_db = get_db_stats();
      IoStats last_io = get_io_stats();
      WalStats last_wal = get_wal_stats();

      std::uint64_t last_blks_read = last_db.blks_read;
      std::uint64_t last_blks_hit = last_db.blks_hit;

      std::uint64_t tick = 0;
      double last_table_gb = get_table_size_gb();

      std::fprintf(
          stderr,
          "Postgres profiling thread: initial stats ok, printing header\n");

      // CSV header:
      // tx, io_pages_s (total), io_client_pages_s, io_bg_pages_s,
      // io_ckpt_pages_s, ...
      std::printf(
          "system,ts,tx,"
          "io_pages_s,io_client_pages_s,io_bg_pages_s,io_ckpt_pages_s,io_MB_s,"
          "wal_MB_s,wal_fpi_s,wal_rec_s,wal_buf_full_s,"
          "fsyncs_s,bp_hit_ratio,table_GB\n");

      // -------------------------------------------------------------------
      // Main loop
      // -------------------------------------------------------------------
      while (keep_running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // TPS for this second
        auto txc = completed_txn.exchange(0);
        total_txn_completed += txc;

        // Current snapshots
        DbStats cur_db = get_db_stats();
        IoStats cur_io = get_io_stats();
        WalStats cur_wal = get_wal_stats();

        // Buffer cache stats
        std::uint64_t cur_blks_read = cur_db.blks_read;
        std::uint64_t cur_blks_hit = cur_db.blks_hit;
        std::uint64_t d_read = cur_blks_read - last_blks_read;
        std::uint64_t d_hit = cur_blks_hit - last_blks_hit;
        last_blks_read = cur_blks_read;
        last_blks_hit = cur_blks_hit;

        std::uint64_t rr = d_read + d_hit; // total buffer accesses
        std::uint64_t rd = d_read;         // misses

        double bp_hit_ratio = 0.0;
        if (rr > 0) {
          bp_hit_ratio =
              1.0 - static_cast<double>(rd) / static_cast<double>(rr);
          if (bp_hit_ratio < 0.0)
            bp_hit_ratio = 0.0;
          if (bp_hit_ratio > 1.0)
            bp_hit_ratio = 1.0;
        }

        // I/O stats deltas
        std::uint64_t d_client_writes =
            cur_io.writes_client - last_io.writes_client;
        std::uint64_t d_bg_writes =
            cur_io.writes_bgwriter - last_io.writes_bgwriter;
        std::uint64_t d_ckpt_writes =
            cur_io.writes_checkpointer - last_io.writes_checkpointer;
        std::uint64_t d_fsyncs = cur_io.fsyncs_total - last_io.fsyncs_total;

        last_io = cur_io;

        std::uint64_t d_writes_total =
            d_client_writes + d_bg_writes + d_ckpt_writes;

        double io_bytes = static_cast<double>(d_writes_total) *
                          static_cast<double>(block_size);
        double io_MB_s = io_bytes / (1024.0 * 1024.0);

        // WAL stats: deltas per second
        std::uint64_t d_wal_bytes = cur_wal.wal_bytes - last_wal.wal_bytes;
        std::uint64_t d_wal_fpi = cur_wal.wal_fpi - last_wal.wal_fpi;
        std::uint64_t d_wal_records =
            cur_wal.wal_records - last_wal.wal_records;
        std::uint64_t d_wal_buffers_full =
            cur_wal.wal_buffers_full - last_wal.wal_buffers_full;
        last_wal = cur_wal;

        double wal_MB_s = static_cast<double>(d_wal_bytes) / (1024.0 * 1024.0);

        // Table size (refresh every 10 seconds)
        if (tick % 10 == 0) {
          last_table_gb = get_table_size_gb();
        }

        std::printf(
            "%s,%lu,%lu,"
            "%llu,%llu,%llu,%llu,%.3f,"
            "%.3f,%llu,%llu,%llu,"
            "%llu,%.4f,%.4f\n",
            system_name.c_str(), tick++,
            static_cast<unsigned long>(txc), // tx (TPS)
            static_cast<unsigned long long>(
                d_writes_total), // io_pages_s (total)
            static_cast<unsigned long long>(
                d_client_writes),                           // io_client_pages_s
            static_cast<unsigned long long>(d_bg_writes),   // io_bg_pages_s
            static_cast<unsigned long long>(d_ckpt_writes), // io_ckpt_pages_s
            io_MB_s,                                        // io_MB_s
            wal_MB_s,                                       // wal_MB_s
            static_cast<unsigned long long>(d_wal_fpi),     // wal_fpi_s
            static_cast<unsigned long long>(d_wal_records), // wal_rec_s
            static_cast<unsigned long long>(
                d_wal_buffers_full),                   // wal_buf_full_s
            static_cast<unsigned long long>(d_fsyncs), // fsyncs_s
            bp_hit_ratio, last_table_gb);
        std::fflush(stdout);
      }

      std::printf("Halt Profiling thread\n");
    } catch (const std::exception &e) {
      std::printf("profiling thread crashed: %s\n", e.what());
    } catch (...) {
      std::printf("profiling thread crashed: unknown exception\n");
    }
  });
}

// -------------------------------------------------------------------------------------
MySQLDB::MySQLDB() {
  db_conn =
      "leanstore:@unix(/tmp/mysql.sock)/test?charset=utf8"; // empty password
}

void MySQLDB::PrepareThread() {
  conn.reset(get_driver_instance()->connect("unix:///tmp/mysql.sock",
                                            "leanstore", "")); // empty password
  conn->setSchema("test");
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  if (FLAGS_txn_default_isolation_level == "rc") {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
  } else if (FLAGS_txn_default_isolation_level == "si") {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
  } else if (FLAGS_txn_default_isolation_level == "ser") {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
  } else {
    stmt->execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
  }
}

void MySQLDB::StartTransaction() {
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  stmt->execute("START TRANSACTION;");
}

void MySQLDB::CommitTransaction() {
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  stmt->execute("COMMIT;");
}

auto MySQLDB::DatabaseSize() -> float {
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  auto res = stmt->executeQuery(
      "SELECT (DATA_LENGTH + INDEX_LENGTH) AS bytes "
      "FROM INFORMATION_SCHEMA.TABLES "
      "WHERE TABLE_SCHEMA='test' AND TABLE_NAME='YCSB_TABLE';");

  // Expect exactly one row
  assert(res->rowsCount() == 1);
  [[maybe_unused]] auto ok = res->next();
  assert(ok);

  double bytes = res->getDouble("bytes");
  return bytes / (1024.0 * 1024.0 * 1024.0); // GB
}

std::thread
MySQLDB::StartProfilingThread(const std::string &system_name,
                              std::atomic<bool> &keep_running,
                              std::atomic<uint64_t> &completed_txn) {
  return std::thread([this, system_name, &keep_running, &completed_txn]() {
    try {
      // --- Dedicated monitoring connection (root, no password) ---
      std::unique_ptr<sql::Connection> mon_conn(get_driver_instance()->connect(
          "unix:///tmp/mysql.sock", "root", "")); // adjust if needed
      mon_conn->setSchema("mysql");

      auto get_status = [&](const std::string &var_name) -> uint64_t {
        std::unique_ptr<sql::Statement> s(mon_conn->createStatement());
        auto res =
            s->executeQuery("SHOW GLOBAL STATUS LIKE '" + var_name + "';");
        if (res->next())
          return res->getUInt64("Value");
        return 0;
      };

      auto get_variable = [&](const std::string &name) -> uint64_t {
        std::unique_ptr<sql::Statement> s(mon_conn->createStatement());
        auto res =
            s->executeQuery("SHOW GLOBAL VARIABLES LIKE '" + name + "';");
        if (res->next())
          return res->getUInt64("Value");
        return 0;
      };

      auto get_table_size_gb = [&]() -> double {
        std::unique_ptr<sql::Statement> s(mon_conn->createStatement());
        auto res = s->executeQuery(
            "SELECT IFNULL(SUM(DATA_LENGTH + INDEX_LENGTH),0) AS bytes "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'YCSB_TABLE';");
        if (res->next()) {
          uint64_t bytes = res->getUInt64("bytes");
          return static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
        }
        return 0.0;
      };

      uint64_t page_size = get_variable("innodb_page_size");
      if (page_size == 0)
        page_size = 16384; // fallback

      // Initial samples
      uint64_t last_dblwr_pages = get_status("Innodb_dblwr_pages_written");
      uint64_t last_dblwr_writes = get_status("Innodb_dblwr_writes");
      uint64_t last_bp_flushed = get_status("Innodb_buffer_pool_pages_flushed");
      uint64_t last_data_written = get_status("Innodb_data_written");
      uint64_t last_data_writes = get_status("Innodb_data_writes");
      uint64_t last_fsyncs = get_status("Innodb_data_fsyncs");
      uint64_t last_bp_read_req =
          get_status("Innodb_buffer_pool_read_requests");
      uint64_t last_bp_reads = get_status("Innodb_buffer_pool_reads");

      uint64_t tick = 0;
      double last_table_gb = get_table_size_gb();

      std::printf("system,ts,tx,"
                  "dblwr_pages_s,dblwr_writes_s,dblwr_MB_s,"
                  "bp_flushed_s,data_writes_s,data_written_MB_s,fsyncs_s,"
                  "bp_hit_ratio,table_GB\n");

      while (keep_running.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto tx = completed_txn.exchange(0);
        total_txn_completed += tx;

        // Current counters
        uint64_t cur_dblwr_pages = get_status("Innodb_dblwr_pages_written");
        uint64_t cur_dblwr_writes = get_status("Innodb_dblwr_writes");
        uint64_t cur_bp_flushed =
            get_status("Innodb_buffer_pool_pages_flushed");
        uint64_t cur_data_written = get_status("Innodb_data_written");
        uint64_t cur_data_writes = get_status("Innodb_data_writes");
        uint64_t cur_fsyncs = get_status("Innodb_data_fsyncs");
        uint64_t cur_bp_read_req =
            get_status("Innodb_buffer_pool_read_requests");
        uint64_t cur_bp_reads = get_status("Innodb_buffer_pool_reads");

        // Deltas (per second)
        uint64_t dp = cur_dblwr_pages - last_dblwr_pages;
        uint64_t dw = cur_dblwr_writes - last_dblwr_writes;
        uint64_t bp = cur_bp_flushed - last_bp_flushed;
        uint64_t dwr = cur_data_writes - last_data_writes;
        uint64_t dr = cur_data_written - last_data_written;
        uint64_t fs = cur_fsyncs - last_fsyncs;
        uint64_t rr = cur_bp_read_req - last_bp_read_req;
        uint64_t rd = cur_bp_reads - last_bp_reads;

        last_dblwr_pages = cur_dblwr_pages;
        last_dblwr_writes = cur_dblwr_writes;
        last_bp_flushed = cur_bp_flushed;
        last_data_written = cur_data_written;
        last_data_writes = cur_data_writes;
        last_fsyncs = cur_fsyncs;
        last_bp_read_req = cur_bp_read_req;
        last_bp_reads = cur_bp_reads;

        double dblwr_MB_s = dp * (double)page_size / (1024.0 * 1024.0);
        double data_MB_s = dr / (1024.0 * 1024.0);

        double bp_hit_ratio = 0.0;
        if (rr > 0) {
          bp_hit_ratio = 1.0 - (double)rd / (double)rr;
          if (bp_hit_ratio < 0.0)
            bp_hit_ratio = 0.0;
          if (bp_hit_ratio > 1.0)
            bp_hit_ratio = 1.0;
        }

        // Update table size every 10 seconds to avoid heavy IS scans
        if (tick % 10 == 0) {
          last_table_gb = get_table_size_gb();
        }

        std::printf("%s,%lu,%lu,"
                    "%llu,%llu,%.3f,"
                    "%llu,%llu,%.3f,%llu,"
                    "%.4f,%.4f\n",
                    system_name.c_str(), tick++, (unsigned long)tx,
                    (unsigned long long)dp, (unsigned long long)dw, dblwr_MB_s,
                    (unsigned long long)bp, (unsigned long long)dwr, data_MB_s,
                    (unsigned long long)fs, bp_hit_ratio, last_table_gb);

        std::fflush(stdout);
      }

      std::printf("Halt Profiling thread\n");
    } catch (...) {
      std::printf("crashed\n");
    }
  });
}

auto MySQLDB::OSFileSizeGB() -> double {
  std::unique_ptr<sql::Statement> stmt(conn->createStatement());
  // DATA_LENGTH + INDEX_LENGTH is the table size in bytes (approx, but smooth)
  auto res = stmt->executeQuery(
      "SELECT (DATA_LENGTH + INDEX_LENGTH) AS FILE_SIZE "
      "FROM information_schema.tables "
      "WHERE table_schema='test' AND table_name='YCSB_TABLE';");

  if (!res->next()) {
    return 0.0; // table not found yet, or just created
  }

  std::uint64_t bytes = res->getUInt64("FILE_SIZE");
  return static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
}
