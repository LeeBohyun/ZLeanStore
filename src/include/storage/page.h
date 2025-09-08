#pragma once

#include "common/constants.h"
#include "common/utils.h"
#include "sync/page_state.h"
#include <algorithm>
#include <iterator>
#include <vector>

namespace leanstore::storage {

struct PageHeader {
  indexid_t idx_id{0}; // the id of index of this page
  logid_t p_gsn{0};    // the GSN of this Page
  logid_t prev_gcwrite{0};
  u32 wcnt{0};
  logid_t w_p_gsn[WRITE_HISTORY_LENGTH] = {
      0}; // Write history without index for insertion
  bool dirty{false};
  bool gc_read{false};

  // Initialize write history with zeros
  void WHistoryInit() { std::fill(std::begin(w_p_gsn), std::end(w_p_gsn), 0); }

  // Add a new GSN in a circular buffer style without needing an explicit index
  void AddWriteGSN(logid_t wp_gsn) {
    wcnt++;
    u32 max_idx = 0;
    for (u32 i = 1; i < WRITE_HISTORY_LENGTH; ++i) {
      if (w_p_gsn[i] > w_p_gsn[max_idx]) {
        max_idx = i;
      }
    }
    u32 insert_pos = (max_idx + 1) % WRITE_HISTORY_LENGTH;
    w_p_gsn[insert_pos] = wp_gsn;
  }

  // Count valid (non-zero) GSN entries
  u32 CountValidWriteHistories() const {
    return wcnt;
    u64 count = 0;
    for (int i = 0; i < WRITE_HISTORY_LENGTH; i++) {
      if (w_p_gsn[i] > 0) {
        count++;
      }
    }
    return count;
  }

  // Estimate death time using the write history
  logid_t EstimateDeathTime(logid_t cur_lsn_gap_per_wgroup) const {
    // return logid_t(wcnt + 1);
    std::vector<logid_t> valid_gsns;
    for (auto gsn : w_p_gsn)
      if (gsn != 0)
        valid_gsns.push_back(gsn);

    if (valid_gsns.size() == 1)
      return valid_gsns[0];

    std::sort(valid_gsns.begin(), valid_gsns.end());
    logid_t min_gsn = valid_gsns.front();
    logid_t max_gsn = valid_gsns.back();

    logid_t total_gap = 0;
    for (size_t i = 1; i < valid_gsns.size(); ++i) {
      total_gap += valid_gsns[i] - valid_gsns[i - 1];
    }

    logid_t avg_gap = total_gap / (valid_gsns.size() - 1);
    return max_gsn + avg_gap; // max_gsn +
  }

  logid_t GetIntervalGap() const {
    // Extract valid (non-zero) GSN entries
    std::vector<logid_t> valid_gsns;
    for (auto gsn : w_p_gsn) {
      if (gsn != 0)
        valid_gsns.push_back(gsn);
    }

    // Need at least two entries to compute a gap
    if (valid_gsns.size() < 1)
      return valid_gsns[0];
    if (valid_gsns.size() < 2)
      return 0;
    return valid_gsns[0];

    // Sort to ensure correct ordering
    std::sort(valid_gsns.begin(), valid_gsns.end());

    // Calculate total gap
    logid_t total_gap = 0;
    for (size_t i = 1; i < valid_gsns.size(); ++i) {
      total_gap += (valid_gsns[i] - valid_gsns[i - 1]);
    }

    // Return average gap
    return total_gap / (valid_gsns.size() - 1);
  }

  // Remove the most recent GSN entry
  void RemoveLastWriteGSN() {
    // Remove the most recent (rightmost non-zero) GSN
    for (int i = WRITE_HISTORY_LENGTH - 1; i >= 0; --i) {
      if (w_p_gsn[i] != 0) {
        w_p_gsn[i] = 0;
        break;
      }
    }
  }

  // Update the last GSN value when the page was written during GC
  void UpdateLastGCWrite(logid_t gsn) { prev_gcwrite = gsn; }

  // Estimate when the next GC write might occur based on current patterns
  logid_t EstimateNextGCWrite(logid_t cur_ts) const {
    if (!FLAGS_use_edt)
      return 0;

    std::vector<logid_t> valid_gsns;
    for (auto gsn : w_p_gsn) {
      if (gsn != 0)
        valid_gsns.push_back(gsn);
    }

    if (valid_gsns.empty()) {
      return cur_ts * 2;
    }

    std::sort(valid_gsns.begin(), valid_gsns.end());
    logid_t last_gsn = valid_gsns.back();

    // Case: GC has written before, and pattern shows long lifetime
    if (valid_gsns.size() > 2 &&
        prev_gcwrite<last_gsn &&this->EstimateDeathTime(cur_ts)> cur_ts) {
      return this->EstimateDeathTime(cur_ts);
    }

    return cur_ts * 2;
  }
};

class alignas(PAGE_SIZE) Page : public PageHeader {};

// MetadataPage without next_insert_idx
class alignas(PAGE_SIZE) MetadataPage : public PageHeader {
public:
  // Adjusted roots array size
  pageid_t roots[(PAGE_SIZE - sizeof(PageHeader)) / sizeof(pageid_t)];

  auto GetRoot(leng_t slot) -> pageid_t { return roots[slot]; }
};

static_assert(sizeof(Page) == PAGE_SIZE);
static_assert(sizeof(MetadataPage) == PAGE_SIZE);

} // namespace leanstore::storage
