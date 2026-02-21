#include "storage/space/backend/fdp.h"
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "storage/space/space_manager.h"
#include <cerrno>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstring> // std::memcpy
#include <cstring>
#include <endian.h> // le16toh, le32toh, le64toh
#include <iomanip>
#include <iostream>
#include <liburing.h>
#include <limits.h>
#include <linux/nvme_ioctl.h>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <sys/ioctl.h>
#include <system_error>
#include <unistd.h>
#include <vector>

#include "storage/space/backend/fdp.h"
#include "storage/space/space_manager.h"

#include <algorithm>
#include <cstdlib>
#include <endian.h>

// ---------- ctor / dtor ----------

using namespace leanstore::storage::space::backend;

namespace leanstore::storage::space::backend {

FDPBackend::FDPBackend(int ng_fd, int ctrl_fd, int ns_fd, u32 nsid,
                       u32 lba_size,
                       leanstore::storage::space::SpaceManager *sm)
    : sm_(sm), ng_fd_(ng_fd), ctrl_fd_(ctrl_fd), ns_fd_(ns_fd), nsid_(nsid),
      lba_size_(lba_size) {
  // InitUring(FLAGS_bm_aio_qd);
  // uring_cmd_ok_ = ProbeUringCmd();
}

FDPBackend::~FDPBackend() {
  // Best-effort flush
  if (ring_inited_) {
    io_uring_queue_exit(&ring_);
    ring_inited_ = false;
  }
  // We do NOT close fds here; owner controls lifecycle.
}

// Returns canonical path if possible; otherwise returns the input unchanged.
std::string realpath_strict(const std::string &p) {
  char buf[PATH_MAX];
  if (::realpath(p.c_str(), buf))
    return std::string(buf);
  // If realpath fails (e.g., missing symlink target), just return the original.
  return p;
}

// Accepts:
//   /dev/nvmeX          -> controller (is_ctrl=true, is_ns=false, X set, Y=-1)
//   /dev/nvmeXnY        -> namespace  (is_ns=true,  is_ctrl=false, X,Y set)
//   /dev/nvmeXnYpZ      -> namespace partition (treated as namespace)
//   /dev/ngXnY          -> namespace char device (treated as namespace)
bool extract_xy(const std::string &p, int &X, int &Y, bool &is_ns,
                bool &is_ctrl) {
  is_ns = false;
  is_ctrl = false;
  X = -1;
  Y = -1;

  // /dev/nvmeXnY or /dev/nvmeXnYpZ
  {
    static const std::regex re_ns_nvme(R"(^/dev/nvme(\d+)n(\d+)(?:p\d+)?$)");
    std::smatch m;
    if (std::regex_match(p, m, re_ns_nvme)) {
      X = std::stoi(m[1].str());
      Y = std::stoi(m[2].str());
      is_ns = true;
      return true;
    }
  }
  // /dev/ngXnY (NVMe generic char dev for uring_cmd)
  {
    static const std::regex re_ns_ng(R"(^/dev/ng(\d+)n(\d+)$)");
    std::smatch m;
    if (std::regex_match(p, m, re_ns_ng)) {
      X = std::stoi(m[1].str());
      Y = std::stoi(m[2].str());
      is_ns = true;
      return true;
    }
  }
  // /dev/nvmeX (controller char device)
  {
    static const std::regex re_ctrl(R"(^/dev/nvme(\d+)$)");
    std::smatch m;
    if (std::regex_match(p, m, re_ctrl)) {
      X = std::stoi(m[1].str());
      Y = -1;
      is_ctrl = true;
      return true;
    }
  }
  return false; // unrecognized
}
// ---------- init mapping ----------

int FDPBackend::InitFromDbPath(const std::string &db_path) {
  const std::string p = realpath_strict(db_path);

  int X = -1, Y = -1;
  bool is_ns = false, is_ctrl = false;
  if (!extract_xy(p, X, Y, is_ns, is_ctrl)) {
    std::cerr << "[FDPBackend][ERROR] Unrecognized NVMe path: " << p << "\n";
    errno = EINVAL;
    return -1;
  }

  std::string ctrl_path, ns_block_path, ns_char_path;
  if (is_ns) {
    ctrl_path = "/dev/nvme" + std::to_string(X);
    ns_block_path = "/dev/nvme" + std::to_string(X) + "n" + std::to_string(Y);
    ns_char_path = "/dev/ng" + std::to_string(X) + "n" + std::to_string(Y);
  } else {
    ctrl_path = "/dev/nvme" + std::to_string(X);
    Y = 1;
    ns_block_path = "/dev/nvme" + std::to_string(X) + "n1";
    ns_char_path = "/dev/ng" + std::to_string(X) + "n1";
    std::cerr << "[FDPBackend][WARN] Only controller path provided; defaulting "
                 "to n1\n";
  }

  // if (ctrl_fd_ < 0) {
  //   ctrl_fd_ = ::open(ctrl_path.c_str(), O_RDONLY | O_CLOEXEC);
  //   if (ctrl_fd_ < 0) {
  //     std::cerr << "[FDPBackend][ERROR] open(" << ctrl_path << "): " <<
  //     strerror(errno) << "\n"; return -1;
  //   }
  // }

  // if (ns_fd_ < 0) {
  //   const int flags = (rw_ns ? O_RDWR : O_RDONLY) | O_CLOEXEC;
  //   ns_fd_ = ::open(ns_block_path.c_str(), flags);
  //   if (ns_fd_ < 0) {
  //     std::cerr << "[FDPBackend][ERROR] open(" << ns_block_path << "): " <<
  //     strerror(errno) << "\n";
  //     // continue; ng_fd_ may still provide I/O
  //   }
  // }

  // if (ng_fd_ < 0) {
  //   ng_fd_ = ::open(ns_char_path.c_str(), O_RDWR | O_CLOEXEC);
  //   if (ng_fd_ < 0) {
  //     if (errno == ENOENT) {
  //       std::cerr << "[FDPBackend][WARN] " << ns_char_path
  //                 << " not present; uring_cmd disabled (ioctl fallback)\n";
  //     } else {
  //       std::cerr << "[FDPBackend][ERROR] open(" << ns_char_path << "): "
  //                 << strerror(errno) << "\n";
  //     }
  //   }
  // }

  nsid_ = static_cast<u32>(Y);
  if (sm_->sspace_->GetBlockListSize(State::FULL) == 0) {
    std::cout << "[FDPBackend] device mapping\n"
              << "  db_path     : " << db_path << "\n"
              << "  resolved    : " << p << "\n"
              << "  ctrl_path   : " << ctrl_path << " (fd=" << ctrl_fd_ << ")\n"
              << "  ns_block    : " << ns_block_path << " (fd=" << ns_fd_
              << ")\n"
              << "  ns_char(ng) : " << ns_char_path << " (fd=" << ng_fd_
              << ")\n"
              << "  NSID        : " << nsid_ << "\n"
              << " ns_fd_      : " << ns_fd_ << "\n";
  }

  // Optional: prime RUHS to infer available PLIDs
  // ru_cnt_ = 8;
  ruhs_info *ri = nullptr;
  if (GetRUHInfo(&ri) == 0 && ri) {
    ru_cnt_ = ri->nr_ruhs;
    maxpids_ = ru_cnt_;
    if (sm_->sspace_->GetBlockListSize(State::FULL) == 0)
      std::cerr << "[FDPBackend] RUHS probe: " << ru_cnt_
                << " PLIDs discovered\n";
    std::free(ri);
  } else {
    std::cerr << "[FDPBackend][WARN] GetRUHInfo failed: " << strerror(errno)
              << "\n";
  }

  return 0;
}

bool FDPBackend::ProbeUringCmd() {
  if (!ring_inited_ || ng_fd_ < 0)
    return false;
  alignas(4096) uint8_t buf[4096]{};

  if (io_uring_sqe *sqe = io_uring_get_sqe(&ring_)) {
    try {
      PrepFdpUringCmdSqe(*sqe, buf, lba_size_, /*start*/ 0, NVME_OPC_READ, 0,
                         0);
    } catch (...) {
      return false;
    }

    if (int s = io_uring_submit_and_wait(&ring_, 1); s < 0)
      return false;
    io_uring_cqe *cqe = nullptr;
    int q = io_uring_wait_cqe(&ring_, &cqe);
    if (q < 0 || !cqe)
      return false;
    int res = cqe->res;
    io_uring_cqe_seen(&ring_, cqe);
    return res == 0; // any error (incl. -EOPNOTSUPP) => false
  }
  return false;
}

// ---------- io_uring init/flush ----------

int FDPBackend::InitUring(unsigned qd) {
  if (ng_fd_ < 0) {
    std::cerr << "[FDPBackend][WARN] ng_fd_ < 0; cannot init io_uring_cmd\n";
    errno = ENODEV;
    return -1;
  }

  io_uring_params p{};
  p.flags |= IORING_SETUP_SQE128 | IORING_SETUP_SQE128;

  int rc = io_uring_queue_init_params(static_cast<unsigned>(qd ? qd : 64),
                                      &ring_, &p);
  if (rc != 0) {
    std::cerr << "[FDPBackend][ERROR] io_uring_queue_init_params: "
              << strerror(-rc) << "\n";
    errno = -rc;
    return -1;
  }

  ring_inited_ = true;
  std::cout << "[FDPBackend] io_uring initialized (qd=" << (qd ? qd : 64)
            << ", ng_fd_=" << ng_fd_ << ")\n";
  return 0;
}

int FDPBackend::UringFlushWrites() {

  // LOG_INFO("try to submit requests, wid: %lu submit_cnt: %lu",
  // worker_thread_id, submit_cnt_);
  if (submit_cnt_ == 0)
    return 0;

  const unsigned need = submit_cnt_;
  io_uring_submit_and_wait(&ring_, need);

  // Process the completion queue for the submitted requests
  for (size_t i = 0; i < need; ++i) {
    io_uring_cqe *cqe;
    int wait_ret =
        io_uring_wait_cqe(&ring_, &cqe); // Wait for a completion entry
    Ensure(wait_ret == 0); // Ensure a CQE was retrieved successfully

    // Check the result of the completed request
    if (cqe->res < 0) {
      LOG_ERROR("IO request failed with error: %d wid: %u", cqe->res,
                worker_thread_id);
    }

    // Mark the completion entry as seen
    io_uring_cqe_seen(&ring_, cqe);
  }

  return 0;
}

// ---------- RUHS (matches fio layout) ----------

int FDPBackend::RecvRUHS(void *buf, u32 buflen) const {
  const int fd = (ns_fd_ >= 0) ? ns_fd_ : ng_fd_;
  if (fd < 0) {
    errno = ENODEV;
    return -1;
  }
  if (!buf || (buflen & 3)) {
    errno = EINVAL;
    return -1;
  }

  nvme_passthru_cmd c{};
  c.opcode = NVME_OPC_IO_MGMT_RECV; // 0x12
  c.nsid = nsid_ ? nsid_ : 1;
  c.addr = reinterpret_cast<u64>(buf);
  c.data_len = buflen;

  c.cdw10 = 1;                     // MO = RUHS
  c.cdw11 = bytes_to_numd(buflen); // NUMD = dwords - 1

  int rc = ioctl(fd, NVME_IOCTL_IO_CMD, &c);
  if (rc < 0) {
    std::cerr << "[FDPBackend][ERROR] RUHS recv failed: " << strerror(errno)
              << "\n";
    return -1;
  } else if (rc > 0) {
    std::cerr << "[FDPBackend][ERROR] RUHS NVMe: rc=" << rc << "\n";
    errno = EIO;
    return -1;
  }
  return 0;
}

int FDPBackend::ReadRUHS(std::vector<u8> &out) const {
  out.assign(4096, 0);
  for (int tries = 0; tries < 4; ++tries) {
    if (RecvRUHS(out.data(), static_cast<u32>(out.size())) != 0)
      return -1;

    if (out.size() < sizeof(nvme_fdp_ruh_status)) {
      errno = EIO;
      return -1;
    }
    const auto *hdr = reinterpret_cast<const nvme_fdp_ruh_status *>(out.data());
    const u16 n = le16toh(hdr->nruhsd);
    const size_t need = sizeof(nvme_fdp_ruh_status) +
                        size_t(n) * sizeof(nvme_fdp_ruh_status_desc);
    if (out.size() >= need)
      return 0;
    out.resize(need);
  }
  errno = EOVERFLOW;
  return -1;
}

int FDPBackend::GetRUHInfo(struct ruhs_info **out) {
  if (!out) {
    errno = EINVAL;
    return -1;
  }
  *out = nullptr;

  std::vector<u8> buf;
  if (ReadRUHS(buf) != 0) {
    std::cerr << "[FDPBackend][ERROR] GetRUHInfo: ReadRUHS failed: "
              << strerror(errno) << "\n";
    return -1;
  }

  if (buf.size() < sizeof(nvme_fdp_ruh_status)) {
    errno = EIO;
    return -1;
  }
  const auto *hdr = reinterpret_cast<const nvme_fdp_ruh_status *>(buf.data());
  const u16 n = le16toh(hdr->nruhsd);
  const size_t hdr_sz = sizeof(nvme_fdp_ruh_status);
  const size_t desc_sz = sizeof(nvme_fdp_ruh_status_desc);
  const size_t need = hdr_sz + size_t(n) * desc_sz;
  if (buf.size() < need) {
    errno = EIO;
    return -1;
  }

  const auto *d = reinterpret_cast<const nvme_fdp_ruh_status_desc *>(hdr + 1);

  std::vector<u16> pids;
  pids.reserve(n);
  for (u16 i = 0; i < n; ++i) {
    const u16 pid = le16toh(d[i].pid);
    if (maxpids_ && pid >= maxpids_)
      continue;
    pids.push_back(pid);
  }
  if (pids.empty()) {
    errno = EIO;
    return -1;
  }
  std::sort(pids.begin(), pids.end());
  pids.erase(std::unique(pids.begin(), pids.end()), pids.end());

  const size_t bytes = sizeof(ruhs_info) + pids.size() * sizeof(u16);
  auto *ri = reinterpret_cast<ruhs_info *>(std::malloc(bytes));
  if (!ri) {
    errno = ENOMEM;
    return -1;
  }

  ri->nr_ruhs = static_cast<u32>(pids.size());
  ri->pli_loc = 0;
  std::memcpy(ri->plis, pids.data(), pids.size() * sizeof(u16));
  *out = ri;
  return 0;
}

// ---------- FDP Configs (two-try for vendor quirks) ----------

static int nvme_fdp_cfg_try_cdw14(int ctrl_fd, __u16 egid, __u32 nbytes,
                                  void *data) {
  nvme_passthru_cmd c{};
  c.opcode = NVME_ADMIN_GET_LOG;
  c.nsid = NVME_NSID_ALL; // controller scope
  c.addr = reinterpret_cast<__u64>(data);
  c.data_len = nbytes;

  const __u32 numd = (nbytes >> 2) ? ((nbytes >> 2) - 1) : 0;
  const __u32 numdl = numd & 0xFFFF, numdu = (numd >> 16) & 0xFFFF;

  const __u32 LID = NVME_LOG_LID_FDP_CONFIGS, LSP = 0, RAE = 1, CSI = 0;
  c.cdw10 = (numdl << 16) | (RAE << 15) | (LSP << 8) | (LID & 0xFF);
  c.cdw11 = numdu;
  c.cdw12 = 0;
  c.cdw13 = 0;
  c.cdw14 = (static_cast<__u32>(egid) << 16) | CSI; // LSI=EGID, CSI=NVM
  c.cdw15 = 0;

  return ioctl(ctrl_fd, NVME_IOCTL_ADMIN_CMD, &c);
}

static int nvme_fdp_cfg_try_cdw11(int ctrl_fd, __u16 egid, __u32 nbytes,
                                  void *data) {
  nvme_passthru_cmd c{};
  c.opcode = NVME_ADMIN_GET_LOG;
  c.nsid = NVME_NSID_ALL;
  c.addr = reinterpret_cast<__u64>(data);
  c.data_len = nbytes;

  const __u32 numd = (nbytes >> 2) ? ((nbytes >> 2) - 1) : 0;
  const __u32 numdl = numd & 0xFFFF, numdu = (numd >> 16) & 0xFFFF;

  const __u32 LID = NVME_LOG_LID_FDP_CONFIGS, LSP = 0, RAE = 1;
  c.cdw10 = (numdl << 16) | (RAE << 15) | (LSP << 8) | (LID & 0xFF);
  c.cdw11 =
      (static_cast<__u32>(egid) << 16) | numdu; // legacy: EGID in upper 16
  c.cdw12 = 0;
  c.cdw13 = 0;
  c.cdw14 = 0;
  c.cdw15 = 0;

  return ioctl(ctrl_fd, NVME_IOCTL_ADMIN_CMD, &c);
}

int FDPBackend::LoadFDPConfig() {
  if (ctrl_fd_ < 0) {
    errno = ENODEV;
    return -1;
  }

  const __u16 egid = 1; // your setup

  auto read_hdr = [&](auto try_fn, std::vector<u8> &buf) -> int {
    int rc = try_fn(ctrl_fd_, egid, static_cast<__u32>(buf.size()), buf.data());
    if (rc < 0) {
      std::cerr << "[FDPBackend][ERROR] FDP Config ioctl: " << strerror(errno)
                << "\n";
      return -1;
    }
    if (rc > 0) {
      std::cerr << "[FDPBackend][ERROR] FDP Config NVMe: rc=" << rc << "\n";
      errno = EIO;
      return -1;
    }
    if (buf.size() < sizeof(nvme_fdp_config_log)) {
      errno = EIO;
      return -1;
    }
    return 0;
  };

  std::vector<u8> buf(4096, 0);
  if (read_hdr(nvme_fdp_cfg_try_cdw11, buf) != 0)
    return -1;
  // if (read_hdr(nvme_fdp_cfg_try_cdw14, buf) != 0) {
  //   std::cerr << "[FDPBackend][WARN] FDP Config cdw14 path failed; trying
  //   cdw11 fallback\n"; buf.assign(4096, 0);
  // }

  nvme_fdp_config_log hdr{};
  std::memcpy(&hdr, buf.data(), sizeof(hdr));
  u32 total = le32toh(hdr.size);
  u16 ncfg = le16toh(hdr.n);
  std::cout << "[FDPBackend] FDP Config hdr: size=" << total << " n=" << ncfg
            << " (EGID=" << egid << ")\n";

  if (total > buf.size() && total < (1u << 20)) {
    buf.assign(total, 0);
    if (read_hdr(nvme_fdp_cfg_try_cdw14, buf) != 0) {
      if (read_hdr(nvme_fdp_cfg_try_cdw11, buf) != 0)
        return -1;
    }
    std::memcpy(&hdr, buf.data(), sizeof(hdr));
    total = le32toh(hdr.size);
    ncfg = le16toh(hdr.n);
  }

  const size_t off0 = sizeof(nvme_fdp_config_log);
  if (ncfg == 0 || buf.size() < off0 + sizeof(nvme_fdp_config_desc)) {
    std::cerr << "[FDPBackend][ERROR] FDP Config: no descriptors for EGID="
              << egid << "\n";
    errno = EIO;
    return -1;
  }

  nvme_fdp_config_desc d{};
  std::memcpy(&d, buf.data() + off0, sizeof(d));

  ru_size_ = le64toh(d.runs);
  ru_cnt_ = le16toh(d.nruh);
  maxpids_ = le16toh(d.maxpids);

  std::cout << "[FDPBackend] FDP Config(egid=" << egid << "): RUNS=" << ru_size_
            << " NRUH=" << ru_cnt_ << " MAXPIDS=" << maxpids_ << "\n";
  return 0;
}

// Always submit to ng_fd_ (NVMe generic char dev)
void FDPBackend::PrepFdpUringCmdSqe(io_uring_sqe &sqe, void *buf, size_t size,
                                    u64 start, uint8_t opcode, uint8_t dtype,
                                    uint16_t dspec) {
  if (!ring_inited_ || ng_fd_ < 0)
    throw std::invalid_argument("uring-cmd not ready");
  if (!buf || !size)
    throw std::invalid_argument("null buf/size");

  const u32 lba = lba_size_;
  if ((start % lba) || (size % lba))
    throw std::invalid_argument("unaligned start/size");

  // --- define these before use ---
  const u64 sLba = start / lba;
  const u32 nLbBlocks = static_cast<u32>(size / lba);
  if (nLbBlocks == 0 || nLbBlocks > 65536u)
    throw std::invalid_argument("NLB out of range");
  const u32 nLb = nLbBlocks - 1;
  // -------------------------------

  memset(&sqe, 0, sizeof(sqe));
  sqe.opcode = IORING_OP_URING_CMD;
  sqe.fd = ng_fd_;
#if defined(NVME_URING_CMD_IO)
  sqe.cmd_op = NVME_URING_CMD_IO;
#endif

  auto *cmd = reinterpret_cast<nvme_uring_cmd_compat *>(&sqe.cmd);
  if (!cmd)
    throw std::invalid_argument("uring cmd null");
  memset(cmd, 0, sizeof(*cmd));

  cmd->opcode = opcode; // 0x01 write, 0x02 read
  cmd->nsid = nsid_ ? nsid_ : 1;
  cmd->addr = reinterpret_cast<u64>(buf);
  cmd->data_len = static_cast<u32>(size);

  cmd->cdw10 = static_cast<u32>(sLba & 0xFFFFFFFFu);
  cmd->cdw11 = static_cast<u32>(sLba >> 32);

  if (opcode == NVME_OPC_WRITE) {
    const u8 dtype4 = (dtype ? (dtype & 0x0F) : 2u); // FDP dtype=2
    cmd->cdw12 = (nLb & 0xFFFFu) | (static_cast<u32>(dtype4) << 16);
    cmd->cdw13 = static_cast<u32>(dspec & 0xFFFFu); // PLID
  } else {
    cmd->cdw12 = (nLb & 0xFFFFu);
    cmd->cdw13 = 0;
  }

  sqe.len = sizeof(nvme_uring_cmd_compat);
}

int FDPBackend::WriteWithPLID_Uring(const void *buf, u32 len, u64 slba,
                                    u16 plid) {
  if (ng_fd_ < 0) {
    errno = ENODEV;
    return -1;
  }
  if (!buf || !len || (len % lba_size_)) {
    errno = EINVAL;
    return -1;
  }
  if (maxpids_ && plid >= maxpids_) {
    errno = EINVAL;
    return -1;
  }

  io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  if (!sqe) {
    int ret = io_uring_submit_and_wait(&ring_, submit_cnt_);
    if (ret < 0) {
      errno = -ret;
      return -1;
    }
    io_uring_cq_advance(&ring_, submit_cnt_);
    submit_cnt_ = 0;
    sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      errno = EAGAIN;
      return -1;
    }
  }

  // Convert SLBA -> byte offset and prep SQE with FDP fields.
  const u64 start_bytes = slba * (u64)lba_size_;
  PrepFdpUringCmdSqe(*sqe, const_cast<void *>(buf), len, start_bytes,
                     NVME_OPC_WRITE,
                     NVME_DIRECTIVE_DTYPE_DATA_PLACEMENT, // dtype = FDP
                     plid);                               // dspec = PLID

  ++submit_cnt_;
  return 0; // enqueued; caller may flush via UringFlushWrites()
}

static inline void DumpNvmeCmd(const nvme_passthru_cmd &c) {
  fprintf(stderr,
          "[FDP][ioctl] cmd dump:\n"
          "  opcode=0x%02x nsid=%u addr=0x%016" PRIx64
          " data_len=%u timeout=%u flags=0x%x\n"
          "  cdw10=0x%08x cdw11=0x%08x cdw12=0x%08x cdw13=0x%08x cdw14=0x%08x "
          "cdw15=0x%08x\n"
          "  result(raw)=0x%08x\n",
          c.opcode, c.nsid, (uint64_t)c.addr, c.data_len, c.timeout_ms, c.flags,
          c.cdw10, c.cdw11, c.cdw12, c.cdw13, c.cdw14, c.cdw15, c.result);
}

int FDPBackend::WriteWithPLID_IOCTL_One(const void *buf, u32 len, u64 slba,
                                        u16 plid) const {
  if (ns_fd_ < 0) {
    errno = ENODEV;
    return -1;
  }
  if (!buf || !len || (len % lba_size_) != 0) {
    errno = EINVAL;
    return -1;
  }
  if (maxpids_ && plid >= maxpids_) {
    errno = EINVAL;
    return -1;
  }
  // static constexpr u8 kPlacementMode = 2;
  nvme_passthru_cmd c{};
  c.opcode = NVME_OPC_WRITE;
  c.nsid = nsid_ ? nsid_ : 1;
  c.addr = reinterpret_cast<u64>(buf);
  c.data_len = len;

  c.cdw10 = static_cast<u32>(slba) & 0xffffffff;
  c.cdw11 = static_cast<u32>(slba >> 32);

  const u32 DTYPE_DATA_PLACEMENT = 0x02;
  const u32 nlb = (len / lba_size_) - 1;

  // c.cdw12 = (nlb & 0xFFFF) | (DTYPE_DATA_PLACEMENT << 20);
  c.cdw12 = (DTYPE_DATA_PLACEMENT & 0xFF) << 20 | nlb;
  c.cdw13 = ((u32)plid) << 16; // PLID carried in DSPEC (CDW13[31:16])

  int rc = ioctl(ns_fd_, NVME_IOCTL_IO_CMD, &c);
  if (rc < 0)
    return -1;
  if (rc > 0) {
    errno = EIO;
    return -1;
  }
  return 0;
}

int FDPBackend::WriteWithPLID_IOCTL(const void *buf, u32 len, u64 slba,
                                    u16 plid) {
  // Choose a safe cap. Ideally compute from sysfs/Identify.
  const u32 max_chunk_bytes = 128 * 1024; // 1 MiB is common; lower if needed
  const u32 max_chunk_nlb = max_chunk_bytes / lba_size_;
  if (max_chunk_nlb == 0) {
    errno = EINVAL;
    throw std::system_error(errno, std::generic_category(), "bad lba_size_");
  }

  u64 cur_lba = slba;
  const u8 *p = static_cast<const u8 *>(buf);
  u32 remaining = len;

  while (remaining) {
    u32 this_bytes = remaining;
    // round down to an LBA multiple and obey the cap
    if (this_bytes > max_chunk_bytes)
      this_bytes = max_chunk_bytes;
    this_bytes = (this_bytes / lba_size_) * lba_size_;
    if (this_bytes == 0) {
      errno = EINVAL;
      throw std::system_error(errno, std::generic_category(),
                              "unaligned chunk");
    }

    if (WriteWithPLID_IOCTL_One(p, this_bytes, cur_lba, plid) != 0) {
      int saved = errno ? errno : EIO;
      throw std::system_error(saved, std::generic_category(),
                              "FDP ioctl(write) failed (chunk)");
    }

    p += this_bytes;
    cur_lba += this_bytes / lba_size_;
    remaining -= this_bytes;
  }
  return 0;
}

int FDPBackend::WriteWithPLID(const void *buf, u32 len, u64 slba, u16 plid) {
  int rc = WriteWithPLID_IOCTL(buf, len, slba, plid);
  if (rc == 0)
    return 0;

  // capture now; WriteWithPLID_IOCTL already logged details
  int saved = errno ? errno : EIO;
  throw std::system_error(saved, std::generic_category(),
                          "FDP ioctl(write) failed");
}

// Submit + wait for exactly one READ completion
int FDPBackend::Read_UringSync(void *dst, u32 len, u64 slba) {
  if (!ring_inited_ || ng_fd_ < 0) {
    errno = ENODEV;
    return -1;
  }
  if (!dst || !len || (len % lba_size_)) {
    errno = EINVAL;
    return -1;
  }
  if ((len / lba_size_) > 65536u) {
    errno = EINVAL;
    return -1;
  }

  io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
  if (!sqe) {
    errno = EAGAIN;
    return -1;
  }

  const u64 start_bytes = slba * (u64)lba_size_;

  try {
    PrepFdpUringCmdSqe(*sqe, dst, len, start_bytes, NVME_OPC_READ, /*dtype*/ 0,
                       /*dspec*/ 0);
  } catch (...) {
    errno = EINVAL;
    return -1;
  }

  int sret = io_uring_submit_and_wait(&ring_, 1);
  if (sret < 0) {
    errno = -sret;
    return -1;
  }

  io_uring_cqe *cqe = nullptr;
  int qret = io_uring_wait_cqe(&ring_, &cqe);
  if (qret < 0 || !cqe) {
    errno = (qret < 0) ? -qret : EIO;
    return -1;
  }

  const int res = cqe->res; // <0: -errno, >0: NVMe status
  io_uring_cqe_seen(&ring_, cqe);

  if (res < 0) {
    errno = -res;
    return -1;
  }
  if (res > 0) {
    errno = EIO;
    return -1;
  }
  return 0;
}

int FDPBackend::Read(void *dst, u32 len, u64 slba) {
  if (ring_inited_ && ng_fd_ >= 0) {
    return Read_UringSync(dst, len, slba);
  }
  return Read_UringSync(dst, len, slba);
}

} // namespace leanstore::storage::space::backend