#pragma once

#include "common/constants.h"
#include "common/typedefs.h"
#include "leanstore/config.h"
#include "storage/extent/large_page.h"
#include "storage/page.h"

#include "aio.h"
#include "storage/space/backend/zns.h"
#include "storage/space/space_manager.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace leanstore::storage::space::backend {
class TraceLogger {
public:
  explicit TraceLogger(const std::string &trace_file)
      : trace_file_(trace_file) {
    if (!trace_file_.empty() && std::filesystem::exists(trace_file_)) {
      // Erase the existing file by truncating it
      std::ofstream erase_file(trace_file_, std::ios_base::trunc);
      erase_file.close();
    }
  }

  void Log(const std::string &entry) {
    if (trace_file_.empty()) {
      return;
    }
    std::ofstream trace_file(trace_file_, std::ios_base::app);
    if (!trace_file.is_open()) {
      throw std::runtime_error("Could not open trace file");
    }
    trace_file << entry;
    trace_file.close();
  }

  void LogRead(u64 offset, pageid_t pid, u64 idx_id) {
    Log("R," + std::to_string(pid) + "," + std::to_string(idx_id) + "," +
        std::to_string(offset) + "\n");
  }

  void LogWrite(u64 offset, pageid_t pid, u64 idx_id) {
    Log("W," + std::to_string(idx_id) + "," + std::to_string(offset) + "\n");
  }

  void LogAlloc(pageid_t pid, u64 idx_id, u64 offset) {
    Log("A," + std::to_string(pid) + "," + std::to_string(idx_id) + "," +
        std::to_string(offset) + "\n");
  }

private:
  std::string trace_file_;
};

class IOBackend {
  static constexpr u32 MAX_IOS = 4096;

public:
  IOBackend(int blockfd, SpaceManager *sm, PID2Offset *PID2Offset_table,
            StorageSpace *storage_space);
  ~IOBackend() = default;
  void WritePageSynchronously(pageid_t pid, u64 offset, void *buffer,
                              indexid_t idx_id);
  void WritePagesInPlace(std::vector<pageid_t> &pids, std::vector<u64> &offsets,
                         std::vector<void *> &buffers,
                         std::vector<indexid_t> &idxs, bool synchronous);
  void WritePagesOutOfPlace(u64 offset, u32 write_sz, void *buffer,
                            bool synchronous, bool gcwrite);
  void WriteWAL(u64 offset, u32 write_sz, void *buffer, bool synchronous);
  void ReadPage(u64 offset, void *buffer, u16 read_size, bool sync);
  void ReadPagesOutOfPlace(std::vector<pageid_t> &pids,
                           std::vector<u64> &offsets,
                           std::vector<void *> &buffers,
                           std::vector<u16> &r_sizes, bool synchronous);

private:
  int blockfd_;
  storage::space::backend::LibaioInterface libaio_;
  storage::space::backend::ZNSBackend zns_;
  storage::space::SpaceManager *sm_;
  storage::space::PID2Offset *PID2Offset_table_;
  storage::space::StorageSpace *sspace_;

  void WriteStatsUpdate(u64 write_size, u32 write_cnt, bool is_dwb,
                        bool gcwrite);
  void ReadStatsUpdate(u64 size, u64 cnt, bool isgcread);

  u64 zns_wal_cur_offset_;

  // Trace logger for logging read/write/alloc traces
  TraceLogger trace_logger_;
};

} // namespace leanstore::storage::space::backend