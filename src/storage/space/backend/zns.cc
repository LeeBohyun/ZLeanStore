#include "storage/space/backend/zns.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "share_headers/logger.h"
#include "storage/space/space_manager.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <liburing.h>
#include <linux/nvme_ioctl.h>
#include <sstream>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <sys/ioctl.h>
#include <unistd.h>

namespace leanstore::storage::space::backend {

ZNSBackend::ZNSBackend(int blockfd, SpaceManager *sm)
    : blockfd_(blockfd), sm_(sm) {
  if (blockfd_ < 0) {
    throw std::runtime_error("Failed to open NVMe block device");
  }
  if (FLAGS_use_ZNS) {
    if (io_uring_queue_init(FLAGS_bm_aio_qd, &ring_, 0) < 0) {
      throw std::runtime_error("Failed to initialize io_uring");
    }
    // uring submit cnt
    submit_cnt_ = 0;

    request_stack.resize(FLAGS_bm_aio_qd);

    // check whether the identified nsid is ZNS
    // check zone nsid
    zone_nsid_ = GetZoneNamespaceID();
    max_open_zone_cnt_ = GetMaxOpenZoneCnt();
    GetZoneReport();
    zone_cnt_ = GetTotalZoneCnt();

    if (sm_->sspace_->blocks[0].pages[0].cnt == 0 &&
        !sm_->sspace_->GetBlockListSize(State::FULL)) {
      for (u32 zid = 0; zid < zone_cnt_; zid++) {
        IssueZoneReset(zid * zone_alignment_unit_, true);
      }
    }

    // reset space manager with zns info
    Ensure(sm_->block_size_ == zone_alignment_unit_);
    Ensure(sm_->max_w_ptr_ == zone_size_);
    Ensure(sm_->max_w_ptr_ < sm_->block_size_);

    zns_wal_cur_offset = sm_->max_userspace_capacity_;
  }
}

ZNSBackend::~ZNSBackend() {
  // io_uring_queue_exit(&ring_);
}

u32 ZNSBackend::GetZoneNamespaceID() {
  // Fetch the namespace ID
  struct nvme_id_ns ns;
  int err = -1;
  u32 nsid_ = ioctl(blockfd_, NVME_IOCTL_ID);
  if (nsid_ < 0) {
    fprintf(stderr, "Error: failed to fetch namespace ID\n");
    close(blockfd_);
    return 0;
  }

  return nsid_;
}

u32 ZNSBackend::GetZoneReport() {
  struct nvme_zone_report zr;
  struct nvme_zns_id_ns zns_ns;
  memset(&zns_ns, 0, sizeof(zns_ns));
  memset(&zr, 0, sizeof(zr));

  struct nvme_passthru_cmd cmd = {};
  u64 slba = 0;
  // Prepare the command to query the zones
  cmd.opcode = 0x7A;                // NVMe Report Zones (opcode 0x7A)
  cmd.nsid = zone_nsid_;            // Namespace ID (from your setup)
  cmd.addr = (u64)(uintptr_t)(&zr); // Buffer to store the report zones data
  cmd.data_len = ZNS_ALIGMENT; // Set the data length to 4KB for the response
  cmd.cdw10 = slba & 0xffffffff;
  cmd.cdw11 = slba >> 32;
  cmd.cdw12 = (ZNS_ALIGMENT >> 2) - 1;
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
  ParseReportZones(buffer);
  return 1;
}

u64 ZNSBackend::GetZoneWPtr(u32 zid) {
  // Compute the SLBA of the zone we want
  u64 zone_slba = (u64)zid * (zone_alignment_unit_ / ZNS_ALIGMENT);

  alignas(4096) u8 buffer[128]; // 64B report header + 64B one zone descriptor

  struct nvme_passthru_cmd cmd = {};
  cmd.opcode = 0x7A; // NVMe Report Zones
  cmd.nsid = zone_nsid_;
  cmd.addr = (uintptr_t)buffer;
  cmd.data_len = sizeof(buffer);
  cmd.cdw10 = (u32)(zone_slba & 0xFFFFFFFF);
  cmd.cdw11 = (u32)(zone_slba >> 32);
  cmd.cdw12 = ((sizeof(buffer)) >> 2) - 1; // dword count
  cmd.cdw13 = NVME_ZNS_ZRA_REPORT_ZONES | NVME_ZNS_ZRAS_FEAT_ERZ;
  cmd.timeout_ms = 1000;

  if (ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd) < 0) {
    std::cerr << "ReportZones (zid=" << zid << ") failed: " << strerror(errno)
              << std::endl;
    return 0;
  }

  u8 *zone_desc = &buffer[64]; // Skip 64B report header

  u64 wp_lba =
      le64toh(*reinterpret_cast<u64 *>(&zone_desc[24])); // Write pointer
  return wp_lba * ZNS_ALIGMENT;                          // Convert to bytes
}

void ZNSBackend::ParseReportZones(u8 *buffer) {
  if (!buffer) {
    std::cerr << "Invalid buffer pointer" << std::endl;
    return;
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
    zone_size_ = cap_decimal * ZNS_ALIGMENT; // Now correct
    u8 state = zone_desc[32];                // Zone State (byte 32)
    u8 type = zone_desc[33];                 // Zone Type (byte 33)
    u8 za = zone_desc[2];                    // Zone Attributes (byte 2)
    u8 zai = zone_desc[3];                   // Zone Attributes Info (byte 3)

    if (i == 0) {
      first_slba = slba_decimal; // Store first zone's SLBA
    } else if (i == 1) {
      second_slba = slba_decimal; // Store second zone's SLBA
    }
  }

  zone_alignment_unit_ = (second_slba - first_slba) * ZNS_ALIGMENT;

  // std::cout  << "Zone Size (MB): " << zone_size_ / (1024 * 1024) <<
  // std::endl; std::cout  << "Zone Alignment (MB): " << zone_alignment_unit_ /
  // (1024 * 1024) << std::endl;
}

u64 ZNSBackend::GetTotalZoneCnt() {
  struct nvme_id_ns ns;
  memset(&ns, 0, sizeof(ns));

  // NVMe Identify Namespace command
  struct nvme_passthru_cmd cmd = {
      .opcode = 0x06, // NVMe Identify command
      .nsid = zone_nsid_,
      .addr = (uintptr_t)(&ns), // Cast pointer properly
      .data_len = ZNS_ALIGMENT, // Namespace data structure size
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
  u64 lba_size = ZNS_ALIGMENT; // Convert LBA format descriptor to bytes

  // Compute total number of zones
  u64 nr_zones = ncap / (zone_size_ / lba_size);

  return nr_zones;
}

u32 ZNSBackend::GetMaxOpenZoneCnt() {
  struct nvme_zns_id_ns zns_ns;
  memset(&zns_ns, 0, sizeof(zns_ns));
  // Allocate and initialize the NVMe passthrough command
  struct nvme_passthru_cmd cmd = {
      .opcode = 0x06,
      .nsid = zone_nsid_,
      .addr = (u64)(uintptr_t)(&zns_ns),
      .data_len = ZNS_ALIGMENT,
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
  }
  return mor; // Return the value of MOR (Maximum Open Zones Count), or MAR if
              // preferred
}

void ZNSBackend::IssueZoneReset(u64 zone_offset, bool sync) {
  const u64 zone_lba = zone_offset / ZNS_ALIGMENT;

  struct nvme_passthru_cmd cmd = {};
  memset(&cmd, 0, sizeof(cmd));
  cmd.opcode = 0x79;
  cmd.nsid = zone_nsid_;
  cmd.cdw10 = (u32)(zone_lba & 0xFFFFFFFF);
  cmd.cdw11 = (u32)(zone_lba >> 32);
  cmd.cdw13 = 0x04;

  // Zone Reset doesn't need a data buffer
  cmd.addr = 0;
  cmd.data_len = 0;

  int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
  if (rc < 0) {
    LOG_ERROR("Zone ID: %lu Reset failed", zone_lba);
    perror("ioctl failed");
  }

  if (zone_offset == 0) {
    LOG_INFO("Zone Reset 0x%llx, zone id=%d, successful (ioctl returned %d)",
             zone_lba, (zone_offset / zone_alignment_unit_), rc);
  }
}

void ZNSBackend::IssueZoneAppend(u64 zone_offset, void *buffer, u64 size,
                                 bool sync) {
  if (size == 0) {
    return;
  }
  if (zone_offset < sm_->max_userspace_capacity_) {
    IssueZoneAppendStorage(zone_offset, buffer, size, sync);
    return;
  } else {
    IssueZoneAppendWAL(zone_offset, buffer, size, sync);
  }
}

void ZNSBackend::IssueZoneAppendStorage(u64 zone_offset, void *buffer, u64 size,
                                        bool sync) {
  blockid_t bid = zone_offset / zone_alignment_unit_;

  Ensure(sm_->sspace_->block_metadata[bid].cur_size + size <=
         sm_->sspace_->max_w_ptr_);

  if (sm_->sspace_->block_metadata[bid].cur_size == 0 &&
      sm_->sspace_->block_metadata[bid].block_invalidation_cnt > 0) {
    Ensure(zone_offset % zone_alignment_unit_ == 0);
    zone_state state = GetZoneState(zone_offset);
    Ensure(state == ZONE_STATE_FULL);
    IssueZoneReset(zone_offset, true);
    Ensure(GetZoneState(zone_offset) == ZONE_STATE_EMPTY);
  }

  constexpr u64 kMaxAppendSize = 128 * 1024;

  u64 zone_id = zone_offset / zone_alignment_unit_;
  u64 zone_start_lba = zone_id * zone_alignment_unit_ / ZNS_ALIGMENT;
  u64 offset_in_zone = zone_offset % zone_alignment_unit_;

  u64 remaining = size;
  u8 *buf_ptr = reinterpret_cast<u8 *>(buffer);
  u64 current_offset = zone_offset;

  while (remaining > 0) {
    u64 chunk_size = std::min(kMaxAppendSize, remaining);
    Ensure(chunk_size % ZNS_ALIGMENT == 0);
    u64 chunk_zone_start_lba = zone_id * zone_alignment_unit_ / ZNS_ALIGMENT;
    u64 offset_in_zone = current_offset % zone_alignment_unit_;

    Ensure(zone_id < sm_->block_cnt_);
    Ensure(offset_in_zone + chunk_size <= zone_size_);
    Ensure((zone_offset + size) % zone_alignment_unit_ <=
           sm_->sspace_->max_w_ptr_);

    struct nvme_passthru_cmd cmd = {};
    memset(&cmd, 0, sizeof(cmd));

    cmd.opcode = 0x7D;
    cmd.nsid = zone_nsid_;
    cmd.addr = (uintptr_t)buf_ptr;
    cmd.data_len = (u32)(chunk_size);

    cmd.cdw10 = (u32)(chunk_zone_start_lba & 0xFFFFFFFF);
    cmd.cdw11 = (u32)(chunk_zone_start_lba >> 32);

    u32 nlb = chunk_size / ZNS_ALIGMENT;
    cmd.cdw12 = (nlb - 1) & 0xFFFF;

    cmd.cdw13 = (1 << 14);
    cmd.timeout_ms = 0;
    cmd.flags = 0;

    int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
    if (rc < 0) {
      perror("Synchronous write failed");
      return;
    }

    Ensure(chunk_size <= size);
    remaining -= chunk_size;
    buf_ptr += chunk_size;
    current_offset += chunk_size;
    Ensure(current_offset % zone_alignment_unit_ <= sm_->sspace_->max_w_ptr_);
  }

  return;
}

void ZNSBackend::IssueZoneAppendWAL(u64 zone_offset, void *buffer, u64 size,
                                    bool sync) {
  // wal write
  Ensure(false);
  if (zns_wal_cur_offset + size > zone_cnt_ * zone_alignment_unit_) {
    zns_wal_cur_offset = sm_->max_userspace_capacity_;
  }

  u64 zone_id = zns_wal_cur_offset / zone_alignment_unit_;
  u64 leftover = zns_wal_cur_offset % zone_alignment_unit_;

  if (leftover + size >= sm_->max_w_ptr_) {
    // --- SPLIT WRITE ACROSS TWO ZONES ---
    u64 first_chunk_size = sm_->max_w_ptr_ - leftover;
    u64 second_chunk_size = size - first_chunk_size;

    // First append: from zns_wal_cur_offset up to max_w_ptr_
    {
      u64 zone_start_lba = zone_id * zone_alignment_unit_ / ZNS_ALIGMENT;
      u64 offset_in_zone = zns_wal_cur_offset % zone_alignment_unit_;
      if (offset_in_zone == 0) {
        IssueZoneReset(zns_wal_cur_offset, true);
        zone_state state = GetZoneState(zns_wal_cur_offset);
        Ensure(state == ZONE_STATE_EMPTY);
      }

      if (offset_in_zone + first_chunk_size >= zone_size_) {
        LOG_ERROR("Zone Append would exceed zone size in first chunk");
        return;
      }

      struct nvme_passthru_cmd cmd = {};
      memset(&cmd, 0, sizeof(cmd));
      cmd.opcode = 0x7D;
      cmd.nsid = zone_nsid_;
      cmd.addr = (uintptr_t)buffer;
      cmd.data_len = first_chunk_size;
      cmd.cdw10 = (u32)(zone_start_lba & 0xFFFFFFFF);
      cmd.cdw11 = (u32)(zone_start_lba >> 32);

      if (first_chunk_size % ZNS_ALIGMENT != 0) {
        LOG_ERROR("First zone append size must be aligned");
        return;
      }

      u32 nlb = first_chunk_size / ZNS_ALIGMENT;
      cmd.cdw12 = (nlb - 1) & 0xFFFF;

      int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
      if (rc < 0) {
        LOG_ERROR("Zone Append failed (first chunk), zone_id=%llu", zone_id);
        perror("ioctl failed");
        return;
      }

      zns_wal_cur_offset += first_chunk_size;
    }

    // Move to the next zone and reset it
    if (zone_id + 1 >= zone_cnt_) {
      zns_wal_cur_offset = zone_id * zone_alignment_unit_;
    } else {
      zone_id += 1;
      zns_wal_cur_offset = zone_id * zone_alignment_unit_;
    }

    IssueZoneReset(zns_wal_cur_offset, true);
    {
      zone_state state = GetZoneState(zns_wal_cur_offset);
      Ensure(state == ZONE_STATE_EMPTY);
    }

    // Second append: remaining part of the buffer
    {
      u64 zone_start_lba = zone_id * zone_alignment_unit_ / ZNS_ALIGMENT;
      u64 offset_in_zone = zns_wal_cur_offset % zone_alignment_unit_;

      if (offset_in_zone + second_chunk_size >= zone_size_) {
        LOG_ERROR("Zone Append would exceed zone size in second chunk");
        return;
      }

      struct nvme_passthru_cmd cmd = {};
      memset(&cmd, 0, sizeof(cmd));
      cmd.opcode = 0x7D;
      cmd.nsid = zone_nsid_;
      cmd.addr = (uintptr_t)((char *)buffer + first_chunk_size);
      cmd.data_len = second_chunk_size;
      cmd.cdw10 = (u32)(zone_start_lba & 0xFFFFFFFF);
      cmd.cdw11 = (u32)(zone_start_lba >> 32);

      if (second_chunk_size % ZNS_ALIGMENT != 0) {
        LOG_ERROR("Second zone append size must be aligned");
        return;
      }

      u32 nlb = second_chunk_size / ZNS_ALIGMENT;
      cmd.cdw12 = (nlb - 1) & 0xFFFF;

      int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
      if (rc < 0) {
        LOG_ERROR("Zone Append failed (second chunk), zone_id=%llu", zone_id);
        perror("ioctl failed");
        return;
      }

      zns_wal_cur_offset += second_chunk_size;
    }

    // if(zone_id * zone_alignment_unit_ + zone_size_ - zns_wal_cur_offset <=
    // 128 * KB  && zone_id +1 == zone_cnt_){
    //   struct nvme_passthru_cmd cmd = {};
    //   memset(&cmd, 0, sizeof(cmd));
    //   cmd.opcode = 0x7D;
    //   cmd.nsid = zone_nsid_;
    //   cmd.addr = (uintptr_t)((char*)buffer + first_chunk_size);
    //   cmd.data_len = (zone_id * zone_alignment_unit_ + zone_size_ -
    //   zns_wal_cur_offset); cmd.cdw10 = (u32)(zns_wal_cur_offset &
    //   0xFFFFFFFF); cmd.cdw11 = (u32)(zns_wal_cur_offset >> 32); u32 nlb =
    //   (zone_id * zone_alignment_unit_ + zone_size_ - zns_wal_cur_offset) /
    //   ZNS_ALIGMENT; cmd.cdw12 = (nlb - 1) & 0xFFFF;

    //   int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
    //   if (rc < 0) {
    //     LOG_ERROR("Zone Append failed (second chunk), zone_id=%llu",
    //     zone_id); perror("ioctl failed"); return;
    //   }
    //   zns_wal_cur_offset += (zone_id * zone_alignment_unit_ + zone_size_ -
    //   zns_wal_cur_offset);
    // }

    return;
  }
}

u32 ZNSBackend::GetZoneCap() {
  struct nvme_passthru_cmd cmd = {};
  memset(&cmd, 0, sizeof(cmd));
  cmd.opcode = 0x06; // NVMe Identify
  cmd.nsid = 2;      // Assuming NSID 2
  cmd.addr = 0;
  cmd.data_len = 0;

  int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
  if (rc < 0) {
    LOG_ERROR("GetZoneCap failed: %s", strerror(errno));
    return 0;
  }

  return static_cast<u32>(zone_size_);
}

void ZNSBackend::IssueZoneClose(u64 zone_offset) {
  const u64 zone_lba = zone_offset / ZNS_ALIGMENT;

  struct nvme_passthru_cmd cmd = {};
  memset(&cmd, 0, sizeof(cmd));
  cmd.opcode = 0x17; // Zone Close
  cmd.nsid = zone_nsid_;

  cmd.cdw10 = (u32)(zone_lba & 0xFFFFFFFF);
  cmd.cdw11 = (u32)(zone_lba >> 32);
  cmd.cdw13 = 0x0; // Usually 0, unless force flag is required by your device

  cmd.addr = 0;
  cmd.data_len = 0;

  int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
  if (rc < 0) {
    fprintf(stderr, "Zone LBA: %lu Close failed\n", zone_lba);
    perror("ioctl failed");
  }

  Ensure(GetZoneState(zone_offset) == ZONE_STATE_CLOSED);
}

void ZNSBackend::IssueZoneReset(u64 zone_offset) {
  const u64 zone_lba =
      zone_offset / ZNS_ALIGMENT; // Align the zone offset as required

  struct nvme_passthru_cmd cmd = {};
  memset(&cmd, 0, sizeof(cmd));

  // Correct opcode for Zone Reset
  cmd.opcode = 0x19;     // Zone Reset opcode
  cmd.nsid = zone_nsid_; // The Namespace ID for the ZNS device

  // Set the LBA for the zone in the cmd
  cmd.cdw10 = (u32)(zone_lba & 0xFFFFFFFF);
  cmd.cdw11 = (u32)(zone_lba >> 32);

  // Optional command parameters depending on your device, can include other
  // flags
  cmd.cdw13 = 0x01; // This might be specific to your NVMe device and usage

  cmd.addr = 0;     // No data buffer required for this command
  cmd.data_len = 0; // No data length for reset operation

  int rc =
      ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd); // Send command through IOCTL
  if (rc < 0) {
    fprintf(stderr, "Zone ID: %lu Reset failed\n",
            zone_offset / zone_alignment_unit_);
    perror("ioctl failed");
  }

  // Ensure the zone state is reset to closed or correct state after reset
  Ensure(GetZoneState(zone_offset) ==
         ZONE_STATE_EMPTY); // Adjust state based on your system’s expected
                            // behavior
}

void ZNSBackend::IssueZoneOpen(u64 zone_offset) {

  zone_state state = GetZoneState(zone_offset);
  if (state == ZONE_STATE_CLOSED || state == ZONE_STATE_FULL) {
    IssueZoneReset(zone_offset, true);
  }

  const u64 zone_lba = zone_offset / ZNS_ALIGMENT;
  struct nvme_passthru_cmd cmd = {};
  memset(&cmd, 0, sizeof(cmd));

  cmd.opcode = 0x18; // Zone Open command
  cmd.nsid = zone_nsid_;
  cmd.cdw10 = (u32)(zone_lba & 0xFFFFFFFF);
  cmd.cdw11 = (u32)(zone_lba >> 32);
  cmd.cdw13 = 0x00; // Change from 0x01 → 0x00 for compatibility
  cmd.addr = 0;
  cmd.data_len = 0;

  int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
  if (rc < 0) {
    fprintf(stderr, "Zone ID: %lu Open failed\n", zone_lba);
    perror("ioctl failed");
    return;
  }

  usleep(50); // Short delay to allow state transition

  state = GetZoneState(zone_offset);

  Ensure(state == ZONE_STATE_OPENED_EXPL || state == ZONE_STATE_OPENED_IMPL ||
         state == ZONE_STATE_EMPTY);
}

zone_state ZNSBackend::GetZoneState(u64 zone_offset) {
  const u64 zone_lba = zone_offset / ZNS_ALIGMENT;
  const size_t buffer_size = ZNS_ALIGMENT; // Buffer size for multiple zones

  struct nvme_zone_report *zr =
      (struct nvme_zone_report *)calloc(1, buffer_size);
  if (!zr) {
    perror("Failed to allocate memory for zone report");
    return ZONE_STATE_OFFLINE;
  }

  struct nvme_passthru_cmd cmd = {};
  memset(&cmd, 0, sizeof(cmd));
  cmd.opcode = 0x7A; // NVMe Report Zones command
  cmd.nsid = zone_nsid_;
  cmd.addr = (__u64)(uintptr_t)zr;
  cmd.data_len = buffer_size;
  cmd.cdw10 = (u32)(zone_lba & 0xFFFFFFFF);
  cmd.cdw11 = (u32)(zone_lba >> 32);
  cmd.cdw12 =
      (buffer_size / sizeof(struct nvme_zns_desc)) - 1; // Max zones returned
  cmd.cdw13 = NVME_ZNS_ZRA_REPORT_ZONES | NVME_ZNS_ZRAS_FEAT_ERZ;

  int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
  if (rc < 0) {
    perror("Failed to retrieve zone report");
    free(zr);
    return ZONE_STATE_OFFLINE;
  }

  // Validate that at least one zone is reported
  if (zr->nr_zones == 0) {
    std::cerr << "No zones reported for LBA " << zone_lba << std::endl;
    free(zr);
    return ZONE_STATE_OFFLINE;
  }

  struct nvme_zns_desc *zone_desc = (struct nvme_zns_desc *)&zr->entries[0];
  u8 state = (zone_desc->zs) >> 4; // Extract only upper 4 bits for state

  free(zr); // Free allocated memory

  switch (state) {
  case 0x1:
    return ZONE_STATE_EMPTY;
  case 0x2:
    return ZONE_STATE_OPENED_IMPL;
  case 0x3:
    return ZONE_STATE_OPENED_EXPL;
  case 0x4:
    return ZONE_STATE_CLOSED;
  case 0xD:
    return ZONE_STATE_READ_ONLY;
  case 0xE:
    return ZONE_STATE_FULL;
  case 0xF:
    return ZONE_STATE_OFFLINE;
  default:
    std::cerr << "Unknown zone state: " << static_cast<int>(state) << std::endl;
    return ZONE_STATE_OFFLINE; // Default to offline if unrecognized
  }
}

void ZNSBackend::ZNSRead(u64 zone_offset, void *buffer, u32 size, bool sync) {
  if (size % ZNS_ALIGMENT != 0) {
    LOG_ERROR("ZNSRead failed: size (%u) is not a multiple of ZNS_ALIGMENT",
              size);
    return;
  }

  // Ensure size is a multiple of ZNS_ALIGMENT
  Ensure(zone_offset % FLAGS_read_size == 0);

  u64 start_lba = zone_offset / ZNS_ALIGMENT;
  void *current_buffer =
      buffer; // This will be the buffer used for each read slice

  // Loop through and issue multiple read requests if size is larger than
  // ZNS_ALIGMENT
  while (size > 0) {
    u32 chunk_size = std::min(static_cast<u32>(ZNS_ALIGMENT),
                              size); // Ensure both arguments are u32

    size -= chunk_size; // Reduce the remaining size

    struct nvme_passthru_cmd cmd = {};
    memset(&cmd, 0, sizeof(cmd));
    cmd.opcode = 0x02;
    cmd.nsid = zone_nsid_;
    cmd.addr = (uintptr_t)current_buffer; // Set buffer to current read buffer
    cmd.data_len = chunk_size;
    cmd.cdw10 = (u32)start_lba & 0xFFFFFFFF;
    cmd.cdw11 = (u32)start_lba >> 32;
    cmd.metadata = 0;
    cmd.metadata_len = 0;
    cmd.timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT;

    // Perform the IO command
    int rc = ioctl(blockfd_, NVME_IOCTL_IO_CMD, &cmd);
    if (rc < 0) {
      fprintf(stderr, "ZNSRead failed: %s", strerror(errno));
      return;
    }

    // Move the buffer pointer for the next slice
    current_buffer = (void *)((uintptr_t)current_buffer + chunk_size);

    // Move to the next LBA
    start_lba += chunk_size / ZNS_ALIGMENT;
  }
  // printf("ZNSRead succeeded at LBA 0x%llx, total read %u bytes", (unsigned
  // long long)zone_offset, size);
}

void ZNSBackend::prep_uring_cmd(u8 opcode, struct io_uring_sqe *sqe, int fd,
                                struct iovec *iov, u64 slba, u64 nlb) {
  sqe->opcode = IORING_OP_URING_CMD;
  sqe->fd = fd;
  sqe->flags = 0;
  sqe->user_data = 0;
  sqe->cmd_op = NVME_URING_CMD_IO;

  struct nvme_uring_cmd *cmd = (struct nvme_uring_cmd *)sqe->cmd;
  memset(cmd, 0, sizeof(*cmd)); // Optional: zero it first for safety

  cmd->cdw10 = slba & 0xFFFFFFFF;
  cmd->cdw11 = slba >> 32;
  cmd->cdw12 = nlb; // NVMe spec requires this as (NLB - 1)

  cmd->addr = (uintptr_t)(iov->iov_base);
  cmd->data_len = iov->iov_len;

  cmd->nsid = zone_nsid_;
  cmd->opcode = opcode;
  cmd->metadata = 0;
  cmd->metadata_len = 0;
  cmd->timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT;
}

void ZNSBackend::UringSubmit(size_t submit_cnt, io_uring *ring) {
  if (submit_cnt == 0)
    return;

  int ret = io_uring_submit(ring); // Only submit, don’t wait here
  Ensure(ret >= 0);                // Ensure the submission succeeded

  u32 completed = 0;
  struct io_uring_cqe *cqe;

  // Wait for at least `submit_cnt` completions
  while (completed < submit_cnt) {
    int wait_ret = io_uring_wait_cqe(ring, &cqe);
    Ensure(wait_ret == 0);
    Ensure(cqe != nullptr);

    struct io_req_t *req = static_cast<io_req_t *>(io_uring_cqe_get_data(cqe));
    if (cqe->res < 0) {
      LOG_ERROR("IO request failed with error: %d", cqe->res);
    }
    Ensure(cqe->res >= 0); // Check if the IO operation succeeded

    io_uring_cqe_seen(ring, cqe);
    completed++;
  }

  // Clean up iovec fields (optional, depends on reuse logic)
  for (u32 i = 0; i < submit_cnt; i++) {
    request_stack[i].iov.iov_base = nullptr;
    request_stack[i].iov.iov_len = 0;
    request_stack[i].req = {}; // Optional: clear req fields too
  }

  submit_cnt_ = 0;
}

} // namespace leanstore::storage::space::backend
