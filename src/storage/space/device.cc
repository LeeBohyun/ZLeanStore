#include "storage/space/device.h"

#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"

#include "nvme/ioctl.h"
#include "share_headers/logger.h"
#include "storage/space/sim/GCBase.hpp"
#include "storage/space/sim/Greedy.hpp"
#include "storage/space/sim/SSD.hpp"
#include "storage/space/sim/TwoR.hpp"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <liburing.h>
#include <linux/types.h>
#include <stdlib.h>
#include <sys/queue.h>
#include <unistd.h>

namespace leanstore::storage::space {

NVMeDevice::NVMeDevice() {
  blockfd_ = open(FLAGS_db_path.c_str(), O_RDWR | O_DIRECT, S_IRWXU);
  if (blockfd_ == -1 && !FLAGS_simulator_mode) {
    LOG_ERROR("Cannot open BLOCK data device %d", blockfd_);
    exit(EXIT_FAILURE);
  }

  if (FLAGS_db_path == FLAGS_wal_path && !FLAGS_simulator_mode) {
    walfd_ = blockfd_;
  } else {
    if (!FLAGS_wal_path.empty()) {
      walfd_ = open(FLAGS_wal_path.c_str(), O_RDWR | O_DIRECT, S_IRWXU);
      if (walfd_ == -1) {
        LOG_ERROR("Cannot open BLOCK WAL device %d", walfd_);
        exit(EXIT_FAILURE);
      }
    }
  }

  // setup data device
  nvmeGetInfo();
  // setup data device
  nvmeGetWALInfo();
  GetMaxLogicalPID();
  LOG_INFO(
      "max page cnt: %lu zone_cnt: %lu db_zone_cnt: %lu max_byte_offset_ %lu",
      max_mapped_pid_cnt_, zone_cnt_, db_zone_cnt_, max_byte_offset_);
  LOG_INFO("wal offset: %lu  wal end offset: %lu", wal_start_byte_offset_,
           wal_end_byte_offset_);

  if (FLAGS_simulator_mode) {
    double ssdop = FLAGS_SSD_OP;
    u64 zone_sz = FLAGS_simulator_SSD_gc_unit_mb * MB;

    // Compute physical size by applying overprovisioning
    uint64_t raw_physical_size =
        static_cast<uint64_t>(max_byte_offset_ * (1.0 + ssdop));

    // Round down to the nearest multiple of zone size
    uint64_t scaled_size = (raw_physical_size / zone_sz) * zone_sz;

    // Recompute ssdFill based on the rounded physical size
    double ssdFill = static_cast<double>(max_byte_offset_) / scaled_size;

    ssdsim_ = std::make_unique<SSD>(scaled_size, zone_sz, PAGE_SIZE, ssdFill);
    InitSSDSimulator();
  }
}

NVMeDevice::~NVMeDevice() {}

void NVMeDevice::InitSSDSimulator() {
  std::string gcAlgorithm = FLAGS_simulator_SSD_gc;

  if (gcAlgorithm == "greedy") {
    gc_ = std::unique_ptr<GCBase>(new GreedyGC(*ssdsim_));
  } else if (gcAlgorithm.find("greedy-k") != std::string::npos) {
    int k = std::stoi(gcAlgorithm.substr(8));
    gc_ = std::unique_ptr<GCBase>(new GreedyGC(*ssdsim_, k));
  } else if (gcAlgorithm.find("greedy-s2r") != std::string::npos) {
    gc_ = std::unique_ptr<GCBase>(new GreedyGC(*ssdsim_, 0, true));
  } else if (gcAlgorithm.find("2r") != std::string::npos) {
    gc_ = std::unique_ptr<GCBase>(new TwoR(*ssdsim_, gcAlgorithm));

  } else {
    throw std::runtime_error("Unknown GC algorithm: " + gcAlgorithm);
  }

  ssdsim_->gcalgo = gcAlgorithm;
  ssdsim_->printInfo();
  std::cout << gc_->name() << std::endl;
}

void NVMeDevice::WriteToSSD(u64 offset) {
  if (FLAGS_simulator_mode) {
    assert(ssdsim_ != nullptr);
    assert(gc_ != nullptr);
    u64 pid = offset / PAGE_SIZE;
    std::lock_guard<std::mutex> lock(ssdim_mutex);
    gc_->writePage(pid); // simulate GC write
  }
}

u64 NVMeDevice::GetSSDWA() {
  if (FLAGS_simulator_mode) {
    return (ssdsim_->physWrites()) * PAGE_SIZE;
  }
  return 0;
}

int NVMeDevice::nvmeGetNamespaceID(u32 nsid, enum nvme_identify_cns cns,
                                   enum nvme_csi csi, void *data) {
  if (FLAGS_simulator_mode)
    return 0;
  struct nvme_passthru_cmd cmd = {
      .opcode = nvme_admin_identify,
      .nsid = nsid,
      .addr = (u64)(uintptr_t)data,
      .data_len = NVME_IDENTIFY_DATA_SIZE,
      .cdw10 = cns,
      .cdw11 = (u32)csi << 24,
      .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
  };

  return ioctl(blockfd_, NVME_IOCTL_ADMIN_CMD, &cmd);
}

void NVMeDevice::nvmeGetInfo() {
  if (FLAGS_simulator_mode) {
    if (FLAGS_use_ZNS) {
      zone_size_ = FLAGS_block_size_mb * MB;
      zone_alignment_size_ = zone_size_;
      zone_cnt_ = FLAGS_max_ssd_capacity_gb * GB / zone_alignment_size_;
      max_open_zone_cnt_ =
          FLAGS_max_open_block_cnt > 0 ? FLAGS_max_open_block_cnt : zone_cnt_;
      max_byte_offset_ = zone_cnt_ * zone_alignment_size_;
    } else {
      zone_size_ = FLAGS_block_size_mb * MB;
      zone_alignment_size_ = zone_size_;
      zone_cnt_ = FLAGS_max_ssd_capacity_gb * GB / zone_size_;
      max_open_zone_cnt_ =
          FLAGS_max_open_block_cnt > 0 ? FLAGS_max_open_block_cnt : zone_cnt_;
      max_byte_offset_ = FLAGS_max_ssd_capacity_gb * GB;
    }
    sector_size_ = 4096;
    return;
  }

  struct nvme_id_ns ns;
  int err = -1;

  // Fetch the namespace ID
  nsid_ = ioctl(blockfd_, NVME_IOCTL_ID);
  if (nsid_ < 0) {
    fprintf(stderr, "Error: failed to fetch namespace ID\n");
    close(blockfd_);
    return;
  }

  // Identify namespace to get namespace size, LBA data size, etc.
  err = nvmeGetNamespaceID(nsid_, NVME_IDENTIFY_CNS_NS, NVME_CSI_NVM, &ns);
  if (err) {
    fprintf(stderr, "Error: failed to identify namespace\n");
    close(blockfd_);
    return;
  }

  // Extract block size
  u8 default_lbaf = ns.flbas & 0x0F;            // Get default LBA format index
  sector_size_ = 1 << ns.lbaf[default_lbaf].ds; // Compute block size

  fprintf(stderr, "Namespace ID: %d, Sector Size: %u bytes\n", nsid_,
          sector_size_);

  u64 total_blk_cnt_ = nvmeGetCap();

  if (FLAGS_use_ZNS) {
    Ensure(sector_size_ == 4096ULL);
    ZNSGetAlignmentSize();
    ZNSGetTotalZoneCnt();
    ZNSGetMaxOpenZoneCnt();
    max_byte_offset_ = zone_cnt_ * zone_alignment_size_;
    Ensure(zone_size_ != zone_alignment_size_);
  } else {
    if (FLAGS_simulator_mode) {
      max_byte_offset_ = FLAGS_max_ssd_capacity_gb * GB;
    } else {
      if (FLAGS_max_ssd_capacity_gb) {
        max_byte_offset_ = std::max(FLAGS_max_ssd_capacity_gb * GB,
                                    total_blk_cnt_ * sector_size_);
      } else {
        max_byte_offset_ = total_blk_cnt_ * sector_size_;
      }
    }

    if (FLAGS_use_out_of_place_write) {
      Ensure(FLAGS_block_size_mb * MB *
             (FLAGS_worker_count + FLAGS_wal_enable * FLAGS_checkpointer_cnt +
                  FLAGS_garbage_collector_cnt <=
              FLAGS_bm_physical_gb * GB));
      zone_size_ = FLAGS_block_size_mb * MB;
      zone_alignment_size_ = zone_size_;
      zone_cnt_ = max_byte_offset_ / zone_alignment_size_;

      max_open_zone_cnt_ =
          FLAGS_max_open_block_cnt > 0 ? FLAGS_max_open_block_cnt : zone_cnt_;
      fprintf(stderr, "max open zone cnt: %lu, zone_cnt: %lu\n",
              max_open_zone_cnt_, zone_cnt_);
      // u32 comp_ratio = FLAGS_use_compression ? PAGE_SIZE / MIN_COMP_SIZE : 1;
      // if (max_open_zone_cnt_ * zone_size_ * comp_ratio >
      //     FLAGS_bm_physical_gb * GB) {
      //   max_open_zone_cnt_ =
      //       FLAGS_bm_physical_gb * GB / (zone_size_ * comp_ratio) * 2 / 3;
      //   if (!max_open_zone_cnt_) {
      //     max_open_zone_cnt_ = 1;
      //   }
      // }
    } else {
      zone_size_ = 0;
      zone_alignment_size_ = 0;
      zone_cnt_ = 0;
      max_open_zone_cnt_ = 0;
    }
  }
  return;
}

u64 NVMeDevice::nvmeGetCap() {
  if (FLAGS_simulator_mode)
    return FLAGS_max_ssd_capacity_gb * GB;
  struct nvme_id_ns ns;
  memset(&ns, 0, sizeof(ns));

  // NVMe Identify Namespace command
  struct nvme_passthru_cmd cmd = {
      .opcode = nvme_admin_identify, // NVMe Identify command
      .nsid = nsid_,
      .addr = (uintptr_t)(&ns), // Cast pointer properly
      .data_len = 4096,         // Namespace data structure size
      .cdw10 = NVME_IDENTIFY_CNS_NS,
      .cdw11 = NVME_CSI_NVM << 24,
      .timeout_ms = 1000,
  };

  // Send NVMe ioctl command
  int rc = ioctl(blockfd_, NVME_IOCTL_ADMIN_CMD, &cmd);
  if (rc < 0) {
    LOG_ERROR("Failed to execute NVMe passthrough command: %s (errno: %d)",
              strerror(errno), errno);
    return 0;
  }

  // Extract namespace capacity (`ncap`) in logical blocks
  u64 ncap = le64toh(ns.ncap);
  if (ncap == 0) {
    LOG_ERROR(
        "Namespace capacity (ncap) is zero, which could indicate a problem.");
    return 0;
  }

  fprintf(stderr, "Namespace Capacity (ncap): %lu (GB)\n",
          ncap * sector_size_ / GB);

  return ncap;
}

u64 NVMeDevice::nvmeGetInfoZNS() {
  struct nvme_id_ns ns;
  memset(&ns, 0, sizeof(ns));

  // NVMe Identify Namespace command
  struct nvme_passthru_cmd cmd = {
      .opcode = nvme_admin_identify, // NVMe Identify command
      .nsid = nsid_,
      .addr = (uintptr_t)(&ns), // Cast pointer properly
      .data_len = 4096,         // Namespace data structure size
      .cdw10 = NVME_IDENTIFY_CNS_NS,
      .cdw11 = NVME_CSI_NVM << 24,
      .timeout_ms = 1000,
  };

  // Send NVMe ioctl command
  int rc = ioctl(blockfd_, NVME_IOCTL_ADMIN_CMD, &cmd);
  if (rc < 0) {
    LOG_ERROR("Failed to execute NVMe passthrough command: %s (errno: %d)",
              strerror(errno), errno);
    return 0;
  }

  // Extract namespace capacity (`ncap`) in logical blocks
  u64 ncap = le64toh(ns.ncap);
  if (ncap == 0) {
    LOG_ERROR(
        "Namespace capacity (ncap) is zero, which could indicate a problem.");
    return 0;
  }

  fprintf(
      stderr,
      "Namespace Capacity (ncap): %lu blocks, Logical Block Size: %lu bytes\n",
      ncap, sector_size_);

  return ncap;
}

u64 NVMeDevice::ZNSGetAlignmentSize() {
  struct nvme_zone_report zr;
  struct nvme_zns_id_ns zns_ns;
  memset(&zns_ns, 0, sizeof(zns_ns));
  memset(&zr, 0, sizeof(zr));

  struct nvme_passthru_cmd cmd = {};
  u64 slba = 0;
  // Prepare the command to query the zones
  cmd.opcode = 0x7A;                // NVMe Report Zones (opcode 0x7A)
  cmd.nsid = nsid_;                 // Namespace ID (from your setup)
  cmd.addr = (u64)(uintptr_t)(&zr); // Buffer to store the report zones data
  cmd.data_len = sector_size_; // Set the data length to 4KB for the response
  cmd.cdw10 = slba & 0xffffffff;
  cmd.cdw11 = slba >> 32;
  cmd.cdw12 = (sector_size_ >> 2) - 1;
  cmd.cdw13 = NVME_ZNS_ZRA_REPORT_ZONES | NVME_ZNS_ZRAS_FEAT_ERZ;
  cmd.timeout_ms = 1000;

  // Execute the command using ioctl
  int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
  if (rc < 0) {
    std::cerr << "ReportZones failed: " << strerror(errno) << std::endl;
    return 0;
  }
  // Now cmd.addr points to the raw response data
  u8 *buffer = (u8 *)cmd.addr;
  // Parse the output buffer for zone count and capacity
  if (!buffer) {
    std::cerr << "Invalid buffer pointer" << std::endl;
    return 0;
  }

  // Parse each Zone Descriptor (starting from byte 64)
  u64 first_slba = 0;
  u64 second_slba = 0;

  for (u64 i = 0; i < 2; ++i) {
    u8 *zone_desc = &buffer[64 + (i * 64)]; // Each descriptor is 64 bytes

    u64 slba_hex =
        le64toh(*reinterpret_cast<u64 *>(&zone_desc[16])); // Read raw value
    u64 slba_decimal =
        slba_hex; // It's already a number, now correctly interpreted
    u64 wp = le64toh(*reinterpret_cast<u64 *>(
        &zone_desc[24])); // Write Pointer (bytes 24-31)
    u64 cap_hex =
        le64toh(*reinterpret_cast<u64 *>(&zone_desc[8])); // Read as number
    u64 cap_decimal =
        cap_hex; // It’s already a number, but now correctly interpreted
    zone_size_ = cap_decimal * sector_size_; // Now correct
    u8 state = zone_desc[32];                // Zone State (byte 32)
    u8 type = zone_desc[33];                 // Zone Type (byte 33)
    u8 za = zone_desc[2];                    // Zone Attributes (byte 2)
    u8 zai = zone_desc[3];                   // Zone Attributes Info (byte 3)

    std::cout << "Zone " << i << " => "
              << " SLBA: " << std::dec << slba_decimal << " WP: " << wp
              << " Cap: " << cap_decimal
              << " State: " << static_cast<int>(state)
              << " Type: " << static_cast<int>(type)
              << " ZA: " << static_cast<int>(za)
              << " ZAI: " << static_cast<int>(zai) << std::endl;

    if (i == 0) {
      first_slba = slba_decimal; // Store first zone's SLBA
    } else if (i == 1) {
      second_slba = slba_decimal; // Store second zone's SLBA
    }
  }

  zone_alignment_size_ = (second_slba - first_slba) * sector_size_;

  std::cout << "Zone Size (MB): " << zone_size_ / (1024 * 1024) << std::endl;
  std::cout << "Zone Alignment (MB): " << zone_alignment_size_ / (1024 * 1024)
            << std::endl;

  return zone_alignment_size_;
}

u64 NVMeDevice::ZNSGetTotalZoneCnt() {
  struct nvme_id_ns ns;
  memset(&ns, 0, sizeof(ns));

  // NVMe Identify Namespace command
  struct nvme_passthru_cmd cmd = {
      .opcode = 0x06, // NVMe Identify command
      .nsid = nsid_,
      .addr = (uintptr_t)(&ns), // Cast pointer properly
      .data_len = sector_size_, // Namespace data structure size
      .cdw10 = NVME_IDENTIFY_CNS_NS,
      .cdw11 = NVME_CSI_NVM << 24,
      .timeout_ms = 1000,
  };

  // Send NVMe ioctl command
  int rc = ioctl(blockfd_, NVME_IOCTL_ADMIN_CMD, &cmd);
  if (rc < 0) {
    LOG_ERROR("Failed to execute NVMe passthrough command: %s (errno: %d)",
              strerror(errno), errno);
    return 0;
  }

  // Extract namespace capacity (`ncap`) in logical blocks
  u64 ncap = le64toh(ns.ncap);
  if (ncap == 0) {
    LOG_ERROR(
        "Namespace capacity (ncap) is zero, which could indicate a problem.");
    return 0;
  }

  // Get logical block size from the first LBA format entry
  u64 lba_size = sector_size_; // Convert LBA format descriptor to bytes

  // Compute total number of zones
  u64 nr_zones = ncap / (zone_size_ / lba_size);

  //   LOG_INFO("Namespace Capacity (ncap): %lu blocks, Logical Block Size: %lu
  //   bytes", ncap, lba_size);
  LOG_INFO("Total Number of Zones: %lu", nr_zones);

  zone_cnt_ = nr_zones;

  return nr_zones;
}

u32 NVMeDevice::ZNSGetMaxOpenZoneCnt() {
  struct nvme_zns_id_ns zns_ns = {
      0,
  };
  memset(&zns_ns, 0, sizeof(zns_ns));
  // Allocate and initialize the NVMe passthrough command
  struct nvme_passthru_cmd cmd = {
      .opcode = 0x06,
      .nsid = nsid_,
      .addr = (u64)(uintptr_t)(&zns_ns),
      .data_len = sector_size_,
      .cdw10 = NVME_IDENTIFY_CNS_CSI_NS,
      .cdw11 = NVME_NSTYPE_ZNS << 24,
      .timeout_ms = 1000,
  };

  // Check if memory allocation for the response failed
  if (cmd.addr == 0) {
    LOG_ERROR("Memory allocation for cmd.addr failed");
    return 0;
  }

  // Send the ioctl command to get the namespace information
  int rc = ioctl(blockfd_, NVME_IOCTL_ADMIN_CMD, &cmd); // Correct ioctl command
  if (rc < 0) {
    LOG_ERROR("Failed to execute NVMe passthrough command: %s (errno: %d)",
              strerror(errno), errno);
    return 0;
  }
  // Extract MOR (Maximum Open Resources) correctly
  u32 mor = le32toh(zns_ns.mor) + 1;
  if (mor == 0) {
    LOG_ERROR("Maximum Open Zones Count (MOR) is zero. This could indicate a "
              "problem or unsupported feature.");
  } else {
    LOG_INFO("Maximum Open Zones Count (MOR): %lu", mor); // Log the MOR value
  }

  max_open_zone_cnt_ = std::min(mor, (u32)(FLAGS_max_open_block_cnt));

  return mor; // Return the value of MOR (Maximum Open Zones Count), or MAR if
              // preferred
}

std::string NVMeDevice::ExecCommand(const std::string &cmd) {
  std::array<char, 128> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"),
                                                pclose);

  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }

  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }

  // Remove only trailing newline
  if (!result.empty() && result.back() == '\n') {
    result.pop_back();
  }

  if (result.empty()) {
    throw std::runtime_error("Command returned empty output.");
  }

  return result;
}

void NVMeDevice::nvmeGetWALInfo() {

  // Compute how many zones are used for DB vs WAL
  if (FLAGS_db_path == FLAGS_wal_path) {
    // db and wal stored in the same device
    walfd_ = blockfd_;
    if (FLAGS_use_ZNS) {
      u32 required_zone_cnt =
          (FLAGS_max_wal_capacity_gb * GB + zone_alignment_size_ - 1) /
          zone_alignment_size_;
      fprintf(stderr, "required zone cnt for WAL log: %lu\n",
              required_zone_cnt);
      wal_start_byte_offset_ =
          (zone_cnt_ - required_zone_cnt) * zone_alignment_size_;
      wal_end_byte_offset_ =
          wal_start_byte_offset_ + zone_alignment_size_ * required_zone_cnt;
      db_zone_cnt_ = zone_cnt_ - required_zone_cnt;
      max_open_zone_cnt_--;
      max_db_byte_offset_ = db_zone_cnt_ * zone_alignment_size_;
    } else {
      if (FLAGS_use_out_of_place_write) {
        u32 required_zone_cnt =
            (FLAGS_max_wal_capacity_gb * GB + zone_size_ - 1) / zone_size_;
        db_zone_cnt_ = zone_cnt_ - required_zone_cnt;
        wal_start_byte_offset_ =
            max_byte_offset_ - FLAGS_max_wal_capacity_gb * GB;
        wal_end_byte_offset_ = max_byte_offset_;
        max_db_byte_offset_ = db_zone_cnt_ * zone_alignment_size_;

        if (FLAGS_max_open_block_cnt) {
          max_open_zone_cnt_--;
        }
      } else {
        if (FLAGS_enable_doublewrite) {
          wal_start_byte_offset_ =
              max_byte_offset_ - FLAGS_max_wal_capacity_gb * GB -
              (FLAGS_worker_count + FLAGS_checkpointer_cnt * FLAGS_wal_enable) *
                  384 * MB;
          wal_end_byte_offset_ =
              max_byte_offset_ -
              (FLAGS_worker_count + FLAGS_checkpointer_cnt * FLAGS_wal_enable) *
                  384 * MB;
          db_zone_cnt_ = zone_cnt_;
          max_db_byte_offset_ = wal_start_byte_offset_;
        } else {
          wal_start_byte_offset_ =
              max_byte_offset_ - FLAGS_max_wal_capacity_gb * GB;
          wal_end_byte_offset_ = max_byte_offset_;
          db_zone_cnt_ = zone_cnt_;
          max_db_byte_offset_ = wal_start_byte_offset_;
        }
      }
    }
  } else {
    // use a separate wal device or not use wal at all
    wal_start_byte_offset_ = 0;

    if (FLAGS_use_ZNS) {
      u32 required_zone_cnt =
          (FLAGS_max_wal_capacity_gb * GB + zone_alignment_size_ - 1) /
          zone_alignment_size_;
      fprintf(stderr, "required_zone_cnt: %lu zone_alignment_size: %lu\n",
              required_zone_cnt, zone_alignment_size_);
      wal_end_byte_offset_ =
          wal_start_byte_offset_ + zone_alignment_size_ * required_zone_cnt;
      max_db_byte_offset_ = zone_cnt_ * zone_alignment_size_;
    } else {
      if (FLAGS_use_out_of_place_write) {
        u32 required_zone_cnt =
            (FLAGS_max_wal_capacity_gb * GB + zone_size_ - 1) / zone_size_;
        wal_end_byte_offset_ = zone_size_ * required_zone_cnt;
        max_db_byte_offset_ = zone_alignment_size_ * zone_cnt_;
      } else {

        if (FLAGS_enable_doublewrite) {
          wal_end_byte_offset_ = FLAGS_max_wal_capacity_gb * GB;
          max_db_byte_offset_ =
              max_byte_offset_ -
              (FLAGS_worker_count + FLAGS_checkpointer_cnt * FLAGS_wal_enable) *
                  384 * MB;
        } else {
          wal_end_byte_offset_ = FLAGS_max_wal_capacity_gb * GB;
          max_db_byte_offset_ = max_byte_offset_;
        }
      }
    }
    db_zone_cnt_ = zone_cnt_;
  }

  Ensure(max_db_byte_offset_ % PAGE_SIZE == 0);
}

void NVMeDevice::GetMaxLogicalPID() {
  u64 max_db_size = FLAGS_bm_virtual_gb * GB; // logical db size

  if (!FLAGS_use_out_of_place_write) {
    Ensure(!FLAGS_use_ZNS);
    max_mapped_pid_cnt_ = max_db_byte_offset_ / PAGE_SIZE;
    fprintf(stderr, "zone_cnt: %lu zone_size_: %lu max_db_size: %lu\n",
            zone_cnt_, zone_size_, max_db_size);
  } else {
    if (FLAGS_use_ZNS) {
      if (!FLAGS_use_compression) {
        fprintf(stderr, "zone_cnt: %lu zone_size_: %lu max_db_size: %lu\n",
                zone_cnt_, zone_size_, max_db_size);
        Ensure(zone_alignment_size_ * zone_cnt_ >= max_db_size);
        max_mapped_pid_cnt_ = zone_size_ * db_zone_cnt_ / PAGE_SIZE;
      } else {
        // enable compression
        // maximum 4 PIDs per 4096 bytes (since MIN_COMP_SIZE == 1024)
        // TODO: if you want to store more pids than storage space, use this
        // line max_mapped_pid_cnt_ =
        //     zone_size_ * db_zone_cnt_ / PAGE_SIZE * (PAGE_SIZE /
        //     MIN_COMP_SIZE);
        // only for virtual memory smaller than the SSD capacity
        max_mapped_pid_cnt_ = zone_size_ * db_zone_cnt_ / PAGE_SIZE;
      }
      return;
    }
    if (!FLAGS_use_compression) {
      fprintf(stderr,
              "max db byte offset: %lu max_db_size: %lu zone_cnt: %lu "
              "db_zone_cnt: %lu\n",
              max_db_byte_offset_ / GB, max_db_size / GB, zone_cnt_,
              db_zone_cnt_);
      if (max_db_byte_offset_ < max_db_size) {
        max_db_size = max_db_byte_offset_;
      }
      max_mapped_pid_cnt_ = max_db_byte_offset_ / PAGE_SIZE;
    } else {
      // enable compression
      // maximum 4 PIDs per 4096 bytes (since MIN_COMP_SIZE == 1024)
      Ensure(max_db_byte_offset_ >= max_db_size / (PAGE_SIZE / MIN_COMP_SIZE));
      // TODO:
      max_mapped_pid_cnt_ =
          max_db_byte_offset_ / PAGE_SIZE * (PAGE_SIZE / MIN_COMP_SIZE);
      max_mapped_pid_cnt_ = max_db_byte_offset_ / PAGE_SIZE;
    }
  }
  return;
}

}; // namespace leanstore::storage::space