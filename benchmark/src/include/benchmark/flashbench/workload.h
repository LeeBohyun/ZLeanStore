#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/flashbench/config.h"
#include "benchmark/flashbench/pattern_gen.h"
#include "benchmark/flashbench/schema.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"

#include "share_headers/config.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include "storage/page.h"

#include <algorithm>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <linux/fs.h>
#include <numeric>
#include <random>
#include <span>
#include <sstream>
#include <sys/ioctl.h>
#include <unistd.h>
#include <unordered_set>
#include <variant>

namespace flash {

using WorkerLocalPayloads = std::vector<std::unique_ptr<uint8_t[]>>;

class FlashWorkloadInterface {
public:
  virtual ~FlashWorkloadInterface() = default;
  virtual auto CountEntries() -> uint64_t = 0;
  virtual void LoadInitialData(UInteger w_id,
                               const tbb::blocked_range<Integer> &range) = 0;
  virtual void ExecuteTransaction(UInteger w_id) = 0;
  static auto PayloadSize() -> uint64_t { return flash::FLASH_NORMAL_PAYLOAD; }
};

template <template <typename> class AdapterType, class FlashRelation>
struct FlashWorkload : public FlashWorkloadInterface {
  AdapterType<FlashRelation> relation;
  // workload settings
  std::string workload_pattern;
  std::string zone_pattern;
  // Benchmark settings
  WorkerLocalPayloads payloads;
  // Workload settings

  const UInteger read_ratio; // Read ratio
  const UInteger op_ratio;   // overprovisioning ratio
  UInteger total_write_cnt;
  UInteger pid_count;             // Number of total allocated pages
  std::unique_ptr<PatternGen> pg; // Change pg to a pointer

  void InitializePatternGen() {
    if (pid_count == 0) {
      LOG_ERROR("Error: pid_count is zero, cannot initialize PatternGen");
      return;
    }

    // std::string pattern_str = FLAGS_workload_pattern; // Initialize your
    // pattern string
    //     unsigned int logical_pages = pid_count * FLAGS_exec_writes_pct / 100
    //     * (FLAGS_read_ratio / 100.0); // Set your logical pages value double
    //     skew_factor = FLAGS_zipf_factor; // Set your skew factor double size
    //     = GLOBAL_BLOCK_SIZE; // Set your additional parameter

    //     // Use make_unique to create an instance of PatternGen
    //     auto pattern_gen = std::make_unique<PatternGen>(pattern_str,
    //     FLAGS_zone_pattern, static_cast<uint64_t>(logical_pages),
    //     skew_factor, size);

    pg = std::make_unique<PatternGen>(
        workload_pattern, pid_count, zone_pattern,
        static_cast<uint64_t>(pid_count * FLAGS_exec_writes_pct / 100 *
                              (100.0 / FLAGS_read_ratio)),
        static_cast<uint64_t>(FLAGS_zipf_factor), GLOBAL_BLOCK_SIZE);

    LOG_INFO("PatternGen initialized with pattern: %s and zones: %s",
             workload_pattern.c_str(), FLAGS_zone_pattern.c_str());
  }

  template <typename... Params>
  FlashWorkload(WorkerLocalPayloads &payloads, Params &&...params)
      : relation(AdapterType<FlashRelation>(std::forward<Params>(params)...)),
        payloads(std::move(payloads)),
        read_ratio(std::min(FLAGS_read_ratio, static_cast<UInteger>(99))),
        workload_pattern(FLAGS_workload_pattern),
        zone_pattern(FLAGS_zone_pattern), op_ratio(FLAGS_op_pct),
        pid_count((FLAGS_initial_dataset_size_gb * leanstore::GB) /
                  GLOBAL_BLOCK_SIZE) {
    assert(read_ratio <= 99);
    LOG_INFO("pid count :%lu", pid_count);
    size_t ssd_capacity = GetSSDCapacity(FLAGS_db_path);
    total_write_cnt =
        ssd_capacity / GLOBAL_BLOCK_SIZE * FLAGS_exec_writes_pct / 100;
  }

  auto CountEntries() -> uint64_t override { return relation.Count(); }

  void LoadInitialData(UInteger w_id,
                       const tbb::blocked_range<Integer> &range) override {
    auto payload = payloads[w_id].get();
    for (auto key = range.begin(); key < range.end(); key++) {
      auto r_key = FlashKey{static_cast<UInteger>(key)}; // pid

      // Allocate memory for the payload with unique_ptr for automatic
      // deallocation
      auto payload = std::make_unique<u8[]>(
          leanstore::PAGE_SIZE - sizeof(leanstore::storage::PageHeader));

      // Fill the payload with a random string
      RandomGenerator::GetRandRepetitiveString(
          payload.get(), 400UL,
          leanstore::PAGE_SIZE - sizeof(leanstore::storage::PageHeader));

      // Pass the payload to AllocPID
      relation.AllocPID(r_key, payload.get());
    }
  }

  void ExecuteTransaction(UInteger w_id) override {
    auto is_read_txn = RandomGenerator::GetRandU64(0, 100) <= read_ratio;

    std::random_device randDevice;
    std::mt19937_64 rng{randDevice()};

    pageid_t pid = pg->AccessPatternGenerator(rng);

    if (is_read_txn) {
      relation.ReadPID(pid);
    } else {
      // Allocate memory for the payload with unique_ptr for automatic
      // deallocation
      auto payload = std::make_unique<u8[]>(
          leanstore::PAGE_SIZE - sizeof(leanstore::storage::PageHeader));

      // Fill the payload with a random string
      RandomGenerator::GetRandRepetitiveString(
          payload.get(), 400UL,
          leanstore::PAGE_SIZE - sizeof(leanstore::storage::PageHeader));

      relation.UpdatePID(pid, payload.get());
    }
  }

  size_t GetSSDCapacity(const std::string &devicePath) {
    int fd = open(devicePath.c_str(), O_RDONLY);
    if (fd == -1) {
      std::cerr << "Failed to open device: " << devicePath << std::endl;
      return 0;
    }

    uint64_t sizeInBytes = 0;
    if (ioctl(fd, BLKGETSIZE64, &sizeInBytes) == -1) {
      std::cerr << "Failed to get device size for: " << devicePath << std::endl;
      close(fd);
      return 0;
    }

    close(fd);
    return static_cast<size_t>(sizeInBytes);
  }
};

} // namespace flash
