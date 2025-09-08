#pragma once

#include "common/exceptions.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "leanstore/statistics.h"
#include "storage/space/sim/GCBase.hpp"
#include "storage/space/sim/SSD.hpp"

#include "nvme/ioctl.h"
#include "share_headers/logger.h"
#include <cstdint>
#include <liburing.h>
#include <linux/types.h>
#include <mutex>
#include <sys/queue.h>

#define NVME_ZNS_ZRAS_FEAT_ERZ (1 << 16)

#ifndef NVME_NSTYPE_ZNS
#define NVME_NSTYPE_ZNS 2 // ZNS is indicated by type 2
#endif

namespace leanstore::storage::space {

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

class NVMeDevice {
public:
  NVMeDevice();
  ~NVMeDevice();
  int nvmeGetNamespaceID(u32 nsid, enum nvme_identify_cns cns,
                         enum nvme_csi csi, void *data);
  void nvmeGetInfo();
  u64 nvmeGetInfoZNS();
  u64 nvmeGetCap();
  void nvmeGetWALInfo();

  u64 ZNSGetAlignmentSize();
  u64 ZNSGetTotalZoneCnt();
  u32 ZNSGetMaxOpenZoneCnt();

  std::string ExecCommand(const std::string &cmd);

  void GetMaxLogicalPID();

  void InitSSDSimulator();
  u64 GetSSDWA();

  int blockfd_;            // device fd
  u32 nsid_;               // device namespace id
  u64 max_db_byte_offset_; // maximum db capacity in byte offset
  u64 max_byte_offset_;    // maximum device capacity in byte offset
  u32 sector_size_;        // block device block size (either 512 or 4096 bytes)
  u64 zone_size_;          // GC granularity for oop
  u64 zone_alignment_size_; //
  u64 zone_cnt_;
  u64 db_zone_cnt_;
  u32 max_open_zone_cnt_;

  int walfd_; // device fd
  u64 wal_start_byte_offset_;
  u64 wal_end_byte_offset_;
  u64 max_mapped_pid_cnt_;

  std::unique_ptr<storage::space::GCBase> gc_;
  std::unique_ptr<SSD> ssdsim_;
  std::mutex ssdim_mutex;

  void WriteToSSD(u64 offset); // wrap simulator write
};

}; // namespace leanstore::storage::space
