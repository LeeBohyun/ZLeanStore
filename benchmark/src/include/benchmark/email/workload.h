#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/email/config.h"
#include "benchmark/email/schema.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"
#include "leanstore/leanstore.h"

#include "share_headers/config.h"
#include "share_headers/csv.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include <algorithm>
#include <cstring>
#include <functional>
#include <span>
#include <variant>

namespace email {

using WorkerLocalPayloads = std::vector<std::unique_ptr<Varchar<60>>>;

class EmailWorkloadInterface {
public:
  virtual ~EmailWorkloadInterface() = default;
  virtual auto CountEntries() -> uint64_t = 0;
  virtual void LoadInitialData(UInteger w_id,
                               const tbb::blocked_range<Integer> &range) = 0;
  virtual void ExecuteTransaction(UInteger w_id) = 0;

  static auto PayloadSize() -> uint64_t {
    if (FLAGS_email_random_payload) {
      return RoundUp(FLAGS_email_payload_size_align,
                     RandomGenerator::GetRandU64(FLAGS_email_payload_size,
                                                 FLAGS_email_max_payload_size));
    }
    return FLAGS_email_payload_size;
  }
};

template <template <typename> class AdapterType, class EmailRelation>
struct Email : public EmailWorkloadInterface {
  AdapterType<EmailRelation> relation;
  ZipfGenerator zipf_generator;
  WorkerLocalPayloads payloads;

  const Integer record_count; // Number of records
  const UInteger read_ratio;  // Read ratio

  UInteger max_key = 0;

  // Workload characteristics & Random distribution
  std::vector<std::pair<uint64_t, std::string>> characteristic;
  uint64_t max_dist{0};

  template <typename... Params>
  Email(Integer initial_record_cnt, UInteger required_read_ratio,
        double zipf_theta, WorkerLocalPayloads &payloads, Params &&...params)
      : relation(AdapterType<EmailRelation>(std::forward<Params>(params)...)),
        zipf_generator(zipf_theta, initial_record_cnt),
        payloads(std::move(payloads)), record_count(initial_record_cnt),
        read_ratio(std::min(required_read_ratio, static_cast<UInteger>(99))) {
    LOG_INFO("Email Template");
    io::CSVReader<2> in(FLAGS_email_data_path.c_str());
    uint64_t key = 0;
    std::string email = "";
    while (in.read_row(key, email)) {
      // LOG_INFO("Email dataset key: %lu string: %s", key, email.c_str());
      characteristic.emplace_back(RoundUp(64, key), email);
    }
    max_key = key;
  }

  // void InitializeThread() {
  //   if (tpcc_thread_id > 0) { return; }
  //   tpcc_thread_id = tpcc_thread_id_counter++;
  // }

  auto CountEntries() -> uint64_t override { return relation.Count(); }

  void LoadInitialData(UInteger w_id,
                       const tbb::blocked_range<Integer> &range) override {
    auto payload = payloads[w_id].get();
    auto record = *reinterpret_cast<EmailRelation *>(payload);
    // LOG_INFO("w_id: %d range start: %lu end: %lu", w_id, range.begin(),
    // range.end());
    for (auto key = range.begin(); key < range.end(); key++) {
      // Insert key-value pair
      auto r_key = characteristic[key - 1].first;
      auto value = characteristic[key - 1].second;
      // LOG_INFO("before: r_key: %lu input: %s value: %s\n", r_key,
      // characteristic[key-1].second, value.c_str());
      strcpy(payload->data, value.c_str());
      // LOG_INFO("r_key: %lu input: %s value: %s payload data: %s\n", r_key,
      // characteristic[key-1].second, value.c_str(), payload->data);
      payload->length = value.length();
      relation.Insert({r_key}, record);
    }
    // LOG_INFO("loading completed: w_id: %d", w_id);
  }

  void ExecuteTransaction(UInteger w_id) override {
    auto access_key = static_cast<UInteger>(zipf_generator.Rand());
    auto is_read_txn = RandomGenerator::GetRandU64(0, 100) <= read_ratio;
    auto payload = payloads[w_id].get();
    auto record = *reinterpret_cast<EmailRelation *>(payload);

    if (is_read_txn) {
      relation.LookUp({access_key}, [&](const EmailRelation &rec) {
        memset(payload->data, 0, payload->length);
        std::memcpy(payload->data, &rec, rec.PayloadSize());
        // std::memcpy(payload->data, static_cast<char*>(rec.email.data),
        // FLAGS_email_max_payload_size); LOG_INFO("read only tx: access_key:
        // %lu payload->data: %s", access_key, payload->data);
      });
    } else {
      relation.LookUp({access_key}, [&](const EmailRelation &rec) {
        memset(payload->data, 0, payload->length);
        uint64_t payload_length = RandomGenerator::GetRandU64(0, 60);
        char buf[payload_length] = {0};
        GenerateRandomEmailAddress(buf);
        std::string strFromCharArray(buf);
        strncpy(payload->data, buf, payload_length);
        payload->length = payload_length;
        // relation.UpdateInPlace({access_key},
        //                        [&](EmailRelation &rec) {
        //                        std::memcpy(rec.payload, payload,
        //                        payload_length); });
        uint64_t rand_key = RandomGenerator::GetRandU64(max_key, max_key * 4);
        relation.Insert({rand_key}, record);
        // rec.email.Update(buf);
        // strncpy(static_cast<char*>(rec.email.data), email,
        // FLAGS_email_max_payload_size); LOG_INFO("write tx: access_key: %lu
        // email: %s", access_key, email);
      });
    }
    return;
  }

  void GenerateRandomString(char *buffer, int length) {
    const char characters[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    const int charactersLength =
        sizeof(characters) - 1; // Exclude null terminator
    for (int i = 0; i < length; ++i) {
      buffer[i] = characters[rand() % charactersLength];
    }
  }

  void GenerateRandomEmailAddress(char *buffer) {
    GenerateRandomString(buffer, 15);
    buffer[10] = '@';
    GenerateRandomString(buffer + 16, 5);
    buffer[21] = '.'; // Domain separator
    buffer[22] = 'c';
    buffer[23] = 'o';
    buffer[24] = 'm';
    buffer[25] = '\0';
  }
};

} // namespace email