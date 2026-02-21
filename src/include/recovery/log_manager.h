#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "recovery/log_entry.h"
#include "recovery/log_worker.h"

namespace leanstore::recovery {

class GroupCommitExecutor;

class LogManager {
public:
  // The GSN up to which all logs from all workers have been flushed
  inline static std::atomic<logid_t> global_min_gsn_flushed = 0;
  // Increment the workers' GSN to this value periodically
  //  to prevent local GSN from skewing and undermining RFA
  inline static std::atomic<logid_t> global_sync_to_this_gsn = 0;

  inline static std::atomic<logid_t> checkpoint_gsn = 0;

  inline static logid_t *local_cur_gsn;

  u32 total_logger_cnt = 0;

  explicit LogManager(std::atomic<bool> &is_running);
  ~LogManager();

  auto LocalLogWorker() -> LogWorker &;
  void LocalLogWorkerCurGSN();

private:
  friend class GroupCommitExecutor;
  LogWorker *logger_;
};

} // namespace leanstore::recovery
