#pragma once
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "storage/space/space_manager.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <cerrno>
#include <fcntl.h>
#include <liburing.h>
#include <sys/ioctl.h>
#include <unistd.h>

namespace leanstore::storage::space {
struct SpaceManager; // fwd decl
} // namespace leanstore::storage::space

namespace leanstore::storage::space::backend {

// ---------- UAPI-lite ----------

// Kernel/uapi-compatible passthru. Keep local to avoid header conflicts.
struct nvme_passthru_cmd {
  __u8 opcode;
  __u8 flags;
  __u16 rsvd1;
  __u32 nsid;
  __u32 cdw2;
  __u32 cdw3;
  __u64 metadata;
  __u64 addr;
  __u32 metadata_len;
  __u32 data_len;
  __u32 cdw10;
  __u32 cdw11;
  __u32 cdw12;
  __u32 cdw13;
  __u32 cdw14;
  __u32 cdw15;
  __u32 timeout_ms;
  __u32 result;
};

// ioctl numbers (match <linux/nvme_ioctl.h>)
#ifndef NVME_IOCTL_ADMIN_CMD
#define NVME_IOCTL_ADMIN_CMD _IOWR('N', 0x41, struct nvme_passthru_cmd)
#endif
#ifndef NVME_IOCTL_IO_CMD
#define NVME_IOCTL_IO_CMD _IOWR('N', 0x43, struct nvme_passthru_cmd)
#endif

// NVMe opcodes (NVM cmd set)
constexpr __u8 NVME_OPC_FLUSH = 0x00;
constexpr __u8 NVME_OPC_WRITE = 0x01;
constexpr __u8 NVME_OPC_READ = 0x02;

// Admin GET LOG and Identify
constexpr __u8 NVME_ADMIN_GET_LOG = 0x02;
constexpr __u8 NVME_ADMIN_IDENTIFY = 0x06;

// I/O Mgmt Receive (RUHS)
constexpr __u8 NVME_OPC_IO_MGMT_RECV = 0x12;

// FDP / Directive constants
constexpr __u8 NVME_LOG_LID_FDP_CONFIGS = 0x20;
constexpr __u16 NVME_DIRECTIVE_DTYPE_DATA_PLACEMENT = 0x2; // DTYPE for FDP

// NVMe NSID “all”
constexpr __u32 NVME_NSID_ALL = 0xFFFFFFFFu;

// Some liburing versions expose sqe->cmd_op; when absent, 0 == IO anyway.
#ifndef NVME_URING_CMD_IO
#define NVME_URING_CMD_IO 0
#endif

// ---------- FDP structs (packed like the spec) ----------

struct nvme_fdp_ruh_desc {
  __u8 ruht;
  __u8 rsvd[3];
} __attribute__((packed));

enum nvme_io_opcode {
  nvme_cmd_write = 0x01,
  nvme_cmd_read = 0x02,
  nvme_cmd_io_mgmt_recv = 0x12,
  nvme_cmd_io_mgmt_send = 0x1d,
};

struct nvme_fdp_config_desc {
  __le16 dsze;
  __u8 fdpa;
  __u8 vss;
  __le32 nrg;
  __le16 nruh;
  __le16 maxpids;
  __le32 nns;
  __le64 runs;
  __le32 erutl;
  __u8 rsvd28[36];
  // followed by nvme_fdp_ruh_desc[nruh]
} __attribute__((packed));

struct nvme_fdp_config_log {
  __le16 n; // number of config descriptors
  __u8 version;
  __u8 rsvd3;
  __le32 size; // total bytes of this log
  __u8 rsvd8[8];
  // followed by nvme_fdp_config_desc[]
} __attribute__((packed));

struct nvme_fdp_ruh_status_desc {
  __le16 pid; // Placement ID
  __le16 ruhid;
  __le32 earutr;
  __le64 ruamw;
  __u8 rsvd[16];
} __attribute__((packed));

struct nvme_fdp_ruh_status {
  __u8 rsvd0[14];
  __le16 nruhsd; // number of descriptors
  // followed by nvme_fdp_ruh_status_desc[nruhsd]
} __attribute__((packed));

// C-style flex-array info holder (like fio’s ruhs_info)
struct ruhs_info {
  u32 nr_ruhs;
  u32 pli_loc;
  u16 plis[]; // size = nr_ruhs
};

// -------- io_uring payload for NVMe passthrough --------

struct nvme_uring_cmd_compat {
  __u8 opcode;
  __u8 flags;
  __u16 rsvd1;
  __u32 nsid;
  __u32 cdw2;
  __u32 cdw3;
  __u64 metadata;
  __u64 addr;
  __u32 metadata_len;
  __u32 data_len;
  __u32 cdw10;
  __u32 cdw11;
  __u32 cdw12;
  __u32 cdw13;
  __u32 cdw14;
  __u32 cdw15;
  __u32 timeout_ms;
  __u32 rsvd2;
  __u32 result;
};

// ---------- FDPBackend ----------

class FDPBackend {
public:
  FDPBackend(int ng_fd, int ctrl_fd, int ns_fd, u32 nsid, u32 lba_size,
             leanstore::storage::space::SpaceManager *sm = nullptr);
  ~FDPBackend();

  // Map from a db_path (e.g., "/dev/nvme4n1") to ctrl/ns/ng fds.
  int InitFromDbPath(const std::string &db_path);

  // Init io_uring with SQE/CQE 128B (required for uring_cmd)
  int InitUring(unsigned qd);

  // Admin: FDP Configs (EGID fixed to 1 in your setup)
  int LoadFDPConfig();

  // RUHS helpers
  int RecvRUHS(void *buf, u32 buflen) const; // raw receive (fio layout)
  int ReadRUHS(std::vector<u8> &out) const;  // grow to fit
  int GetRUHInfo(struct ruhs_info **out);    // dedup PIDs

  // Data I/O (READ/WRITE)
  int WriteWithPLID(const void *buf, u32 len, u64 slba,
                    u16 plid); // picks uring/ioctl
  int Read(void *dst, u32 len, u64 slba);

  // Batch submit/complete for uring commands
  int UringFlushWrites();

  // Accessors
  int fd_ng() const { return ng_fd_; }
  int fd_ctrl() const { return ctrl_fd_; }
  int fd_ns() const { return ns_fd_; }
  u32 nsid() const { return nsid_; }
  u32 lba_size() const { return lba_size_; }
  u32 ru_cnt() const { return ru_cnt_; }
  u64 ru_size() const { return ru_size_; }
  u16 maxpids() const { return maxpids_; }

  // Expose pending count if you need it outside
  u32 pending_submit_cnt() const { return submit_cnt_; }

private:
  // uring path (enqueue only)
  int WriteWithPLID_Uring(const void *buf, u32 len, u64 slba, u16 plid);
  int Read_Uring(void *dst, u32 len, u64 slba);     // enqueue
  int Read_UringSync(void *dst, u32 len, u64 slba); // submit+wait 1

  void PrepFdpUringCmdSqe(struct io_uring_sqe &sqe, void *buf, size_t size,
                          u64 start, uint8_t opcode, uint8_t dtype,
                          uint16_t dspec);

  // ioctl fallbacks
  int WriteWithPLID_IOCTL(const void *buf, u32 len, u64 slba, u16 plid);
  int WriteWithPLID_IOCTL_One(const void *buf, u32 len, u64 slba,
                              u16 plid) const;
  int Read_IOCTL(void *dst, u32 len, u64 slba) const;
  bool ProbeUringCmd();

  // Internals
  static inline u32 bytes_to_numd(u32 bytes) {
    return bytes ? ((bytes >> 2) ? ((bytes >> 2) - 1) : 0) : 0;
  }

private:
  // not owned
  SpaceManager *sm_{nullptr};
  // If you have helpers in your tree, include them instead.
  // Here we assume you already have: realpath_strict(...) and extract_xy(...)
  // std::string realpath_strict(const std::string& p); // provided elsewhere
  // bool extract_xy(const std::string& p, int& X, int& Y, bool& is_ns, bool&
  // is_ctrl); // elsewhere

  // FDs
  int ng_fd_{-1};   // /dev/ngXnY (uring_cmd)
  int ctrl_fd_{-1}; // /dev/nvmeX  (Admin)
  int ns_fd_{-1};   // /dev/nvmeXnY (ioctl)

  // Namespace identity / geometry
  u32 nsid_{0};
  u32 lba_size_{4096};

  // FDP properties
  u64 ru_size_{0};
  u32 ru_cnt_{0};
  u16 maxpids_{0};

  // io_uring
  io_uring ring_{};
  bool ring_inited_{false};
  u32 submit_cnt_{0};       // queued uring cmds (read+write)
  bool uring_cmd_ok_{true}; // <— add this
};

} // namespace leanstore::storage::space::backend
