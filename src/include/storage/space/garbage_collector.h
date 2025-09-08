#pragma once
#include "common/constants.h"
#include "common/exceptions.h"
#include "common/typedefs.h"
#include "common/utils.h"
#include "leanstore/config.h"
#include "storage/page.h"
#include "storage/space/space_manager.h"

#include <vector>

namespace leanstore::storage::space {
class GarbageCollector {
public:
  GarbageCollector(PID2Offset *PID2Offset_table, StorageSpace *storage_space,
                   int fd, u32 gc_id);
  ~GarbageCollector() = default;

  // Main API
  std::vector<blockid_t>
  SelectVictimBlockToGCRead(int victim_cnt,
                            std::unique_lock<std::shared_mutex> &lock);

  // list operation
  void MoveBlocksFromGCToEmptyBlockList();
  bool MoveBlockFromFullToGCBlockList(blockid_t bid);
  void MoveBlocksFromGCToOpenBlockList();

  // victim selection strategy
  std::vector<blockid_t>
  SelectVictimBlocksToGCWrite(u64 write_sz,
                              std::unique_lock<std::shared_mutex> &lock);
  std::vector<blockid_t> TwoRGreedyGC(bool read);
  blockid_t GreedyGC(bool read);
  std::vector<blockid_t> GreedyGC(bool read, int victim_cnt);
  blockid_t FIFOGC();
  blockid_t EDTBasedGC(bool read);
  blockid_t RandomGC();
  blockid_t KGreedyGC(u32 k);
  std::vector<blockid_t> NoSSDWAGC(bool read);

  // block-level operation
  bool AddBlockAsGCVictim(blockid_t bid);
  void EraseBlock(blockid_t bid);
  void TrimBlock(blockid_t bid);
  void DiscardBlockLBA(blockid_t bid);
  void SendBlkdiscardReq(u64 start_offset, u64 size);
  std::string ExecCommand(const std::string &cmd);
  std::vector<pageid_t> &GetValidPIDsInBlock(blockid_t bid);

  u64 ValidPIDsCntInBlock(blockid_t bid);
  u64 ValidPIDsSizeInBlock(blockid_t bid);
  void AddBlockInvalidationHistory(blockid_t bid);
  bool BlockIsGCVictim(blockid_t bid);
  bool WillTriggerImbalance(blockid_t bid);
  blockid_t SearchForOtherCandidate(blockid_t bid,
                                    std::vector<blockid_t> &cur_group);

  // page level method
  void LogValidPage(pageid_t pid, storage::Page *page);
  bool PageIsValid(blockid_t bid, u32 p, u16 pidx);
  u64 GetPageOffset(blockid_t bid, u32 p);
  void ResetOffset2PIDs(blockid_t bid, u32 pn, u16 pcnt);
  pageid_t ConvertOffset2PID(blockid_t bid, u32 p, u16 pidx);

  bool ReachedEDT(logid_t cur_gsn);
  bool ShouldTrustEDT();

  bool ShouldSelectImbalancedBlock(int victim_cnt);
  int GreedyAmongImbalancedGroups(int victim_cnt);
  u64 AvgGroupValidPages(blockid_t bid, std::vector<blockid_t> &cur_group);
  std::vector<blockid_t> victim_bids_;

private:
  StorageSpace *sspace_;
  PID2Offset *PID2Offset_table_;
  int fd_;
  u32 thread_id_;
  u64 block_size_;
  u64 block_cnt_;
  u64 max_w_ptr_;
  u32 page_cnt_per_block_;
};

} // namespace leanstore::storage::space
