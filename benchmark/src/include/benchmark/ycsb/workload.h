#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/flashbench/riz.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"
#include "benchmark/ycsb/config.h"
#include "benchmark/ycsb/schema.h"

#include "share_headers/config.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include <algorithm>
#include <functional>
#include <random>
#include <span>
#include <variant>

namespace ycsb {

using WorkerLocalPayloads = std::vector<std::unique_ptr<uint8_t[]>>;
std::unique_ptr<RejectionInversionZipfSampler> zipf_sampler;

class YCSBWorkloadInterface {
public:
  virtual ~YCSBWorkloadInterface() = default;
  uint64_t phase_cnt = 0;
  std::atomic<uint64_t> total_records_loaded = 0;
  virtual auto CountEntries() -> uint64_t = 0;
  virtual void LoadInitialData(UInteger w_id,
                               const tbb::blocked_range<Integer> &range) = 0;
  virtual void ExecuteTransaction(UInteger w_id) = 0;

  static auto PayloadSize() -> uint64_t {
    if (FLAGS_ycsb_random_payload) {
      return RoundUp(FLAGS_ycsb_payload_size_align,
                     RandomGenerator::GetRandU64(FLAGS_ycsb_payload_size,
                                                 FLAGS_ycsb_max_payload_size));
    }
    return FLAGS_ycsb_payload_size;
  }

  // Initializes the Zipfian generator
  void InitZipfGenerator(uint64_t record_count) {
    LOG_INFO("record count: %lu zipf theta: %.2f dataset size (GB): %lu",
             record_count, FLAGS_ycsb_zipf_theta, FLAGS_ycsb_dataset_size_gb);
    zipf_sampler = std::make_unique<RejectionInversionZipfSampler>(
        static_cast<long>(record_count), FLAGS_ycsb_zipf_theta);
    Ensure(zipf_sampler != nullptr);
  }
};

/**
 * For Adapter without BlobRep, use this struct for arbitrary-sized payload
 * workloads
 */
template <template <typename> class AdapterType, class YCSBRelation>
struct YCSBWorkloadNoBlobRep : public YCSBWorkloadInterface {
  // Run-time
  AdapterType<YCSBRelation> relation;
  ZipfGenerator zipf_generator;
  WorkerLocalPayloads payloads;
  static constexpr bool ENABLE_BLOB_REP = false;

  // YCSB settings
  const UInteger record_count; // Number of records
  const UInteger read_ratio;   // Read ratio

  template <typename... Params>
  YCSBWorkloadNoBlobRep(Integer initial_record_cnt,
                        UInteger required_read_ratio, double zipf_theta,
                        bool use_blob_register, WorkerLocalPayloads &payloads,
                        Params &&...params)
      : relation(AdapterType<YCSBRelation>(std::forward<Params>(params)...)),
        zipf_generator(zipf_theta, initial_record_cnt),
        payloads(std::move(payloads)), record_count(initial_record_cnt),
        read_ratio(std::min(required_read_ratio, static_cast<UInteger>(100))) {
    Ensure((FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD) ||
           (!use_blob_register));
    if (FLAGS_ycsb_random_payload) {
      Ensure(FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD);
    } else {
      Ensure(FLAGS_ycsb_payload_size == FLAGS_ycsb_max_payload_size);
    }
    Ensure((0 <= zipf_theta) && (zipf_theta <= 1));
    Ensure(read_ratio <= 100);
  }

  auto CountEntries() -> uint64_t override { return relation.Count(); }

  void LoadInitialData(UInteger w_id,
                       const tbb::blocked_range<Integer> &range) override {
    auto payload = payloads[w_id].get();
    auto record = *reinterpret_cast<YCSBRelation *>(payload);

    for (auto key = range.begin(); key < range.end(); key++) {
      // Generate key-value
      auto r_key =
          YCSBKey{static_cast<UInteger>(key) + UINT_MAX / 2 * phase_cnt};
      auto payload_sz = YCSBWorkloadInterface::PayloadSize();
      record.payload.length = payload_sz;
      RandomGenerator::GetRandRepetitiveString(
          reinterpret_cast<uint8_t *>(record.payload.data), 50UL, payload_sz);
      relation.Insert({r_key}, record);
    }
  }

  void ExecuteTransaction(UInteger w_id) override {
    auto is_read_txn = RandomGenerator::GetRandU64(1, 100) <= read_ratio;
    auto payload = payloads[w_id].get();
    auto record = reinterpret_cast<YCSBRelation *>(payload);
    std::random_device randDevice;
    std::mt19937_64 rng{randDevice()};
    auto access_key = static_cast<UInteger>(zipf_sampler->sample(rng));

    // Transaction without Blob rep
    if (is_read_txn) {
      relation.LookUp({access_key}, [&](const YCSBRelation &rec) {
        std::memcpy(payload, const_cast<YCSBRelation &>(rec).payload.data,
                    rec.payload.length);
      });
    } else {

      auto payload_sz = YCSBWorkloadInterface::PayloadSize();
      record->payload.length = payload_sz;
      RandomGenerator::GetRandRepetitiveString(
          reinterpret_cast<uint8_t *>(record->payload.data), 50UL, payload_sz);
      relation.Update({access_key}, *record);
    }
  }
};

template <template <typename> class AdapterType, class YCSBRelation>
struct YCSBWorkload : public YCSBWorkloadInterface {
  // Run-time
  AdapterType<YCSBRelation> relation;
  ZipfGenerator zipf_generator;
  WorkerLocalPayloads payloads;

  /* lia */
  Integer upperbound_keyrange[4] = {0};

  // YCSB settings
  const UInteger record_count; // Number of records
  const UInteger read_ratio;   // Read ratio
  const bool enable_blob_rep;  // Whether to use custom Blob representative
                               // format or not for Blob workloads This depends
                               // on whether the DB engine requires extern Blob
                               // creation API e.g. It's possible to insert Blob
                               // directly into MySQL using SQL `insert` command

  template <typename... Params>
  YCSBWorkload(uint64_t initial_record_cnt, UInteger required_read_ratio,
               double zipf_theta, bool use_blob_register,
               WorkerLocalPayloads &payloads, Params &&...params)
      : relation(AdapterType<YCSBRelation>(std::forward<Params>(params)...)),
        zipf_generator(zipf_theta, initial_record_cnt),
        payloads(std::move(payloads)), record_count(initial_record_cnt),
        read_ratio(std::min(required_read_ratio, static_cast<UInteger>(100))),
        enable_blob_rep((FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD) &&
                        (use_blob_register)) {
    if (FLAGS_ycsb_random_payload) {
      Ensure(FLAGS_ycsb_payload_size > BLOB_NORMAL_PAYLOAD);
    } else {
      FLAGS_ycsb_max_payload_size = FLAGS_ycsb_payload_size;
    }
    assert((0 <= zipf_theta) && (zipf_theta <= 1));
    assert(read_ratio <= 100);
  }

  auto CountEntries() -> uint64_t override { return relation.Count(); }

  void LoadInitialData(UInteger w_id,
                       const tbb::blocked_range<Integer> &range) override {
    auto payload = payloads[w_id].get();

    for (auto key = range.begin(); key < range.end(); key++) {
      //  for (auto key = range.begin()+phase_cnt * INT_MAX; key <
      //  range.end()+phase_cnt * INT_MAX; key++) {
      // Generate key-value

      auto r_key =
          YCSBKey{static_cast<UInteger>(key) + UINT_MAX / 2 * phase_cnt};
      auto payload_sz = YCSBWorkloadInterface::PayloadSize();
      RandomGenerator::GetRandRepetitiveString(payload, 100UL, payload_sz);
      // If the value is Blob, then we register it first
      if (enable_blob_rep) {
        auto blob_rep = relation.RegisterBlob({payload, payload_sz}, {}, false);
        relation.InsertRawPayload({r_key}, blob_rep);
      } else {
        relation.Insert({r_key}, *reinterpret_cast<YCSBRelation *>(payload));
      }
    }
  }

  void ExecuteTransaction(UInteger w_id) override {

    auto payload = payloads[w_id].get();
    std::random_device randDevice;
    std::mt19937_64 rng{randDevice()};
    auto is_read_txn = RandomGenerator::GetRandU64(1, 100) <= read_ratio;
    auto access_key = static_cast<UInteger>(zipf_sampler->sample(rng));

    // Transaction without Blob rep
    if (is_read_txn) {

      relation.LookUp({access_key}, [&](const YCSBRelation &rec) {
        std::memcpy(payload, const_cast<YCSBRelation &>(rec).my_payload.Data(),
                    FLAGS_ycsb_max_payload_size);
      });
    } else {
      /* write transaction */
      // Ensure(access_key <= record_count);
      auto payload_sz = YCSBWorkloadInterface::PayloadSize();
      relation.UpdateInPlace({access_key}, [&](YCSBRelation &rec) {
        std::memcpy(rec.my_payload.Data(), payload, payload_sz);
      });
      return;
    }
  }
};

} // namespace ycsb
