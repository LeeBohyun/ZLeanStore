#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/tpcc/workload.h"
#include "leanstore/leanstore.h"

#include "gflags/gflags.h"
#include "share_headers/perf_event.h"
#include "tbb/global_control.h"
#include "tbb/parallel_for.h"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

// TPC-C config
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_uint32(tpcc_warehouse_count, 50, "");
DEFINE_uint64(tpcc_exec_seconds, 1800, "");
DEFINE_uint64(tpcc_batch_delete_window, 0,
              "threshold for (max_o_id - max_deleted_o_id) per district, 2100 "
              "means dataset size remains the same");

auto main(int argc, char **argv) -> int {
  gflags::SetUsageMessage("Leanstore TPC-C");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  tbb::global_control c(tbb::global_control::max_allowed_parallelism,
                        FLAGS_worker_count);

  // Statistics
  PerfEvent e;
  std::atomic<bool> keep_running(true);
  leanstore::RegisterSEGFAULTHandler();

  // Initialize LeanStore
  auto db = std::make_unique<leanstore::LeanStore>();
  auto tpcc = std::make_unique<tpcc::TPCCWorkload<LeanStoreAdapter>>(
      FLAGS_tpcc_warehouse_count, true, true, true,
      FLAGS_tpcc_batch_delete_window, *db);
  tpcc->customer.ToggleAppendBiasMode(true);
  tpcc->history.ToggleAppendBiasMode(true);
  tpcc->order.ToggleAppendBiasMode(true);
  tpcc->orderline.ToggleAppendBiasMode(true);
  tpcc->item.ToggleAppendBiasMode(true);
  tpcc->stock.ToggleAppendBiasMode(true); // lia: true

  // TPC-C loader
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    tpcc->LoadItem();
    tpcc->LoadWarehouse();
    db->CommitTransaction();
  });

  db->is_loading = true;
  for (Integer w_id = 1;
       w_id <= static_cast<Integer>(FLAGS_tpcc_warehouse_count); w_id++) {
    fprintf(stderr, "Prepare for warehouse %d\n", w_id);
    db->worker_pool.ScheduleAsyncJob(w_id % FLAGS_worker_count, [&, w_id]() {
      tpcc->InitializeThread();
      db->StartTransaction();
      tpcc->LoadStock(w_id);
      tpcc->LoadDistrinct(w_id);
      for (Integer d_id = 1; d_id <= tpcc->D_PER_WH; d_id++) {
        tpcc->LoadCustomer(w_id, d_id);
        tpcc->LoadOrder(w_id, d_id);
      }
      db->CommitTransaction();
    });
  }
  db->worker_pool.JoinAll();

  // db->DropCache();
  fprintf(stderr, "Space used: %.4f MB\n", db->AllocatedSize());

  if (FLAGS_tpcc_batch_delete_window) {
    for (Integer t_id = 0; t_id <= FLAGS_worker_count; t_id++) {
      tpcc->history_metadata_[t_id].last_deleted_history_pk =
          tpcc->history_metadata_[t_id].history_pk_counter -
          FLAGS_tpcc_batch_delete_window * FLAGS_tpcc_warehouse_count;
    }
    fprintf(stderr, "Run Steady TPC-C window_size :%lu\n",
            FLAGS_tpcc_batch_delete_window);
  }

  // #ifdef DEBUG
  //  db->worker_pool.ScheduleSyncJob(0, [&]() {
  //    db->StartTransaction();
  //    auto w_cnt = tpcc->warehouse.Count();
  //    auto d_cnt = tpcc->district.Count();
  //    LOG_DEBUG("Warehouse count: %lu - District count: %lu", w_cnt, d_cnt);
  //    assert(w_cnt == FLAGS_tpcc_warehouse_count);
  //    assert(d_cnt == FLAGS_tpcc_warehouse_count * tpcc->D_PER_WH);

  //   auto o_cnt = tpcc->order.Count();
  //   auto o_index_cnt = tpcc->order_wdc.Count();
  //   LOG_DEBUG("Order count: %lu - Order's index count: %lu", o_cnt,
  //             o_index_cnt);
  //   assert(o_cnt == o_index_cnt);

  //   auto c_cnt = tpcc->customer.Count();
  //   auto c_index_cnt = tpcc->customer_wdc.Count();
  //   LOG_DEBUG("Customer count: %lu - Customer's index count: %lu", c_cnt,
  //             c_index_cnt);
  //   assert(c_cnt == c_index_cnt);

  //   db->CommitTransaction();
  // });
  // #endif

  // TPC-C execution
  db->StartProfilingThread();
  db->is_loading = false;

  for (size_t t_id = 0; t_id < FLAGS_worker_count; t_id++) {
    db->worker_pool.ScheduleAsyncJob(t_id, [&]() {
      tpcc->InitializeThread();

      while (keep_running.load()) {
        int w_id = (FLAGS_tpcc_warehouse_affinity)
                       ? (t_id % FLAGS_tpcc_warehouse_count) + 1
                       : UniformRand(1, FLAGS_tpcc_warehouse_count);

        db->StartTransaction();
        tpcc->ExecuteTransaction(w_id);
        db->CommitTransaction();
      }
    });
  }

  // // Run for designated seconds, then quit
  std::this_thread::sleep_for(std::chrono::seconds(FLAGS_tpcc_exec_seconds));
  keep_running = false;
  db->Shutdown();
}
