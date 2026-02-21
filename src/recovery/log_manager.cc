#include "recovery/log_manager.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"

namespace leanstore::recovery {

LogManager::LogManager(std::atomic<bool> &is_running) {
  total_logger_cnt = FLAGS_worker_count +
                     FLAGS_checkpointer_cnt *
                         FLAGS_wal_enable; // + FLAGS_garbage_collector_cnt
  logger_ = static_cast<LogWorker *>(
      aligned_alloc(BLK_BLOCK_SIZE, (total_logger_cnt) * sizeof(LogWorker)));
  local_cur_gsn = new logid_t[total_logger_cnt];
  for (size_t idx = 0; idx < (total_logger_cnt); idx++) {
    local_cur_gsn[idx] = 0;
    logger_[idx].Init(is_running);

    Ensure((u64)(&logger_[idx]) % BLK_BLOCK_SIZE == 0);
  }
  assert(logger_ != nullptr);
}

LogManager::~LogManager() {
  for (size_t idx = 0; idx < (total_logger_cnt); idx++) {
    logger_[idx].~LogWorker();
  }
  free(logger_);
}

auto LogManager::LocalLogWorker() -> LogWorker & {
  assert(worker_thread_id < (total_logger_cnt));

  return logger_[worker_thread_id];
}

void LogManager::LocalLogWorkerCurGSN() {
  local_cur_gsn[worker_thread_id] = logger_[worker_thread_id].GetCurrentGSN();
}

} // namespace leanstore::recovery