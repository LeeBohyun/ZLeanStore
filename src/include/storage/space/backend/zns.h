#pragma once
#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "storage/space/space_manager.h"

#include "nvme/ioctl.h"
#include "share_headers/logger.h"
#include <cstdint>
#include <liburing.h>
#include <linux/types.h>
#include <sys/queue.h>
#include <vector>

namespace leanstore::storage::space::backend {

#ifndef NVME_NSTYPE_ZNS
#define NVME_NSTYPE_ZNS 2 // ZNS is indicated by type 2
#endif

/* Number of maximum addresses in a single command vector.
 * 	A single address is needed for zone append. We should
 * 	increase this number in case of possible vectored I/Os. */
#define MAX_MADDR 1
#define XZTL_WIO_MAX_MCMD 1024
#define ZNS_ALIGMENT 4096
#define ZNA_1M_BUF (ZNS_ALIGMENT * 256)
#define ZNS_MAX_M_BUF 32
#define ZTL_IO_SEC_MCMD 8
#define ZNS_MAX_BUF (ZNS_MAX_M_BUF * ZNA_1M_BUF)
#define ZTL_IO_RC_NUM (ZNS_MAX_BUF / (ZTL_IO_SEC_MCMD * ZNS_ALIGMENT))
#define XZTL_READ_RS_NUM 256

#define NVME_ZNS_ZRA_REPORT_ZONES 0
#define NVME_ZNS_ZRAS_FEAT_ERZ (1 << 16)

typedef void(xztl_callback)(void *arg);

enum nvme_identify_cns {
  NVME_IDENTIFY_CNS_NS = 0x00,
  NVME_IDENTIFY_CNS_CTRL = 0x01,
  NVME_IDENTIFY_CNS_CSI_NS = 0x05,
  NVME_IDENTIFY_CNS_CSI_CTRL = 0x06,
};

enum nvme_csi {
  NVME_CSI_NVM = 0,
  NVME_CSI_KV = 1,
  NVME_CSI_ZNS = 2,
};

enum zone_state {
  ZONE_STATE_EMPTY = 0x1,
  ZONE_STATE_OPENED_IMPL = 0x2,
  ZONE_STATE_OPENED_EXPL = 0x3,
  ZONE_STATE_CLOSED = 0x4,
  ZONE_STATE_READ_ONLY = 0xD,
  ZONE_STATE_FULL = 0xE,
  ZONE_STATE_OFFLINE = 0xF
};

struct nvme_zns_lbafe {
  u64 zsze;
  u8 zdes;
  u8 rsvd9[7];
};

struct io_req_t {
  u64 start_lba;
  u32 chunk_size;
  void *buffer;
  // Add any other data you might need here
};

struct nvme_zns_id_ns {
  u16 zoc;
  u16 ozcs;
  u32 mar;
  u32 mor;
  u32 rrl;
  u32 frl;
  u32 rrl1;
  u32 rrl2;
  u32 rrl3;
  u32 frl1;
  u32 frl2;
  u32 frl3;
  u32 numzrwa;
  u16 zrwafg;
  u16 zrwasz;
  u8 zrwacap;
  u8 rsvd53[2763];
  struct nvme_zns_lbafe lbafe[64];
  u8 vs[256];
};

struct nvme_zns_desc {
  u8 zt;
  u8 zs;
  u8 za;
  u8 zai;
  u8 rsvd4[4];
  u64 zcap;
  u64 zslba;
  u64 wp;
  u8 rsvd32[32];
};

struct nvme_lbaf {
  __le16 ms;
  u8 ds;
  u8 rp;
};

struct nvme_id_ns {
  __le64 nsze;
  __le64 ncap;
  __le64 nuse;
  u8 nsfeat;
  u8 nlbaf;
  u8 flbas;
  u8 mc;
  u8 dpc;
  u8 dps;
  u8 nmic;
  u8 rescap;
  u8 fpi;
  u8 dlfeat;
  __le16 nawun;
  __le16 nawupf;
  __le16 nacwu;
  __le16 nabsn;
  __le16 nabo;
  __le16 nabspf;
  __le16 noiob;
  u8 nvmcap[16];
  __le16 npwg;
  __le16 npwa;
  __le16 npdg;
  __le16 npda;
  __le16 nows;
  __le16 mssrl;
  __le32 mcl;
  u8 msrc;
  u8 rsvd81[11];
  __le32 anagrpid;
  u8 rsvd96[3];
  u8 nsattr;
  __le16 nvmsetid;
  __le16 endgid;
  u8 nguid[16];
  u8 eui64[8];
  struct nvme_lbaf lbaf[64];
  u8 vs[3712];
};

struct nvme_zone_report {
  u64 nr_zones;
  u8 rsvd8[56];
  struct nvme_zns_desc entries[];
};

struct LiburingIoRequest {
  struct io_req_t req;
  struct iovec iov; // kind of a hack
};

class ZNSBackend {
public:
  // ZNS-specific Identify Namespace structure (simplified for the MOR field)
  ZNSBackend(int blockfd, SpaceManager *sm);
  ~ZNSBackend();
  void prep_uring_cmd(u8 opcode, struct io_uring_sqe *sqe, int fd,
                      struct iovec *iov, u64 slba, u64 nlb);
  void print_sqe(struct io_uring_sqe *sqe);
  u32 GetZoneNamespaceID();
  u32 GetZoneReport();
  void ParseReportZones(u8 *buffer);
  u32 GetZoneCap();
  zone_state GetZoneState(u64 zone_offset);
  u64 GetTotalZoneCnt();
  u32 GetZoneCount();
  u32 GetMaxOpenZoneCnt();
  void IssueZoneAppend(u64 zone_offset, void *buffer, u64 size, bool sync);
  void IssueZoneAppendWAL(u64 zone_offset, void *buffer, u64 size, bool sync);
  void IssueZoneAppendStorage(u64 zone_offset, void *buffer, u64 size,
                              bool sync);
  void IssueZoneReset(u64 zone_offset, bool sync);
  void ZNSRead(u64 zone_offset, void *buffer, u32 size, bool sync);
  void IssueZoneOpen(u64 zone_offset);
  void IssueZoneClose(u64 zone_offset);
  void IssueZoneReset(u64 zone_offset);
  void UringSubmit(size_t submit_cnt, io_uring *ring);
  u64 GetZoneWPtr(u32 zid);

private:
  SpaceManager *sm_;
  int blockfd_;
  u32 zone_nsid_ = 0;
  u32 zone_cnt_;
  u64 zone_size_;
  u64 zone_alignment_unit_ = 0;
  u32 max_open_zone_cnt_ = 0;
  struct io_uring ring_;
  std::vector<struct LiburingIoRequest> request_stack;

  u32 submit_cnt_;

  u64 zns_wal_cur_offset;
};

} // namespace leanstore::storage::space::backend