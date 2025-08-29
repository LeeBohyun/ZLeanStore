#pragma once

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/utils/misc.h"
#include "benchmark/utils/rand.h"
#include "benchmark/wikititle/config.h"
#include "benchmark/wikititle/schema.h"
#include "leanstore/leanstore.h"

#include "share_headers/config.h"
#include "share_headers/csv.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"
#include "tbb/blocked_range.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <span>
#include <variant>

namespace wikititle {

using WorkerLocalPayloads = std::vector<std::unique_ptr<Varchar<64>[]>>;

class WikiTitleWorkloadInterface {
public:
  virtual ~WikiTitleWorkloadInterface() = default;
  virtual auto CountEntries() -> uint64_t = 0;
  virtual void LoadInitialData(UInteger w_id,
                               const tbb::blocked_range<Integer> &range) = 0;
  virtual void ExecuteTransaction(UInteger w_id) = 0;

  static auto PayloadSize() -> uint64_t {
    if (FLAGS_wikititle_random_payload) {
      return RoundUp(
          FLAGS_wikititle_payload_size_align,
          RandomGenerator::GetRandU64(FLAGS_wikititle_payload_size,
                                      FLAGS_wikititle_max_payload_size));
    }
    return FLAGS_wikititle_payload_size;
  }
};

template <template <typename> class AdapterType, class WikiTitleRelation>
struct WikiTitle : public WikiTitleWorkloadInterface {
  AdapterType<WikiTitleRelation> relation;
  ZipfGenerator zipf_generator;
  WorkerLocalPayloads payloads;

  const Integer record_count; // Number of records
  const UInteger read_ratio;  // Read ratio

  UInteger max_key = 0;

  // Workload characteristics & Random distribution
  std::vector<std::pair<uint64_t, std::string>> characteristic;
  uint64_t max_dist{0};

  template <typename... Params>
  WikiTitle(Integer initial_record_cnt, UInteger required_read_ratio,
            double zipf_theta, WorkerLocalPayloads &payloads,
            Params &&...params)
      : relation(
            AdapterType<WikiTitleRelation>(std::forward<Params>(params)...)),
        zipf_generator(zipf_theta, initial_record_cnt),
        payloads(std::move(payloads)), record_count(initial_record_cnt),
        read_ratio(std::min(required_read_ratio, static_cast<UInteger>(99))) {
    LOG_INFO("WikiTitle Template");
    io::CSVReader<2> in(FLAGS_wikititle_data_path.c_str());
    uint64_t key = 0;
    std::string wikititle = "";
    uint max_len = 0;
    while (in.read_row(key, wikititle)) {
      if (strlen(wikititle.c_str()) > 512) {
        if (max_len < strlen(wikititle.c_str())) {
          max_len = strlen(wikititle.c_str());
        }
        // LOG_INFO("WikiTitle dataset key: %lu string length: %d", key,
        // strlen(wikititle.c_str()));
      }

      characteristic.emplace_back(RoundUp(64, key), wikititle);
    }
    max_key = key;
  }

  auto CountEntries() -> uint64_t override { return relation.Count(); }

  void LoadInitialData(UInteger w_id,
                       const tbb::blocked_range<Integer> &range) override {
    auto payload = payloads[w_id].get();

    // LOG_INFO("w_id: %d payload ptr: %p payloads size: %d", w_id, payload,
    // payloads.size());
    auto record = *reinterpret_cast<WikiTitleRelation *>(payload);
    // LOG_INFO("w_id: %d range start: %lu end: %lu", w_id, range.begin(),
    // range.end());
    for (auto key = range.begin(); key < range.end(); key++) {
      // Insert key-value pair
      auto r_key = characteristic[key - 1].first;
      auto value = characteristic[key - 1].second;
      // LOG_INFO("before: r_key: %lu input: %s value: %s\n", r_key,
      // characteristic[key-1].second, value.c_str());
      strcpy(payload->data, value.c_str());
      // LOG_INFO("r_key: %lu payload data: %s data_len: %d\n", r_key,
      // payload->data, payload->length);
      payload->length = value.length();
      relation.Insert({r_key}, record);
    }
  }

  void ExecuteTransaction(UInteger w_id) override {
    std::srand(std::time(0));
    auto access_key =
        RandomGenerator::GetRandU64(0, FLAGS_wikititle_record_count);
    auto is_read_txn = RandomGenerator::GetRandU64(0, 100) <= read_ratio;
    auto payload = payloads[w_id].get();
    auto record = *reinterpret_cast<WikiTitleRelation *>(payload);
    memset(payload, 0, payload->length);

    if (is_read_txn) {
      relation.LookUp({access_key}, [&](const WikiTitleRelation &rec) {
        // memset(payload->data, 0, payload->length);
        std::memcpy(payload->data, &rec, rec.PayloadSize());
        // std::memcpy(payload->data, static_cast<char*>(rec.wikititle.data),
        // FLAGS_wikititle_max_payload_size); LOG_INFO("read only tx:
        // access_key: %lu payload->data: %s", access_key, payload->data);
      });
    } else {
      relation.LookUp({access_key}, [&](const WikiTitleRelation &rec) {
        std::srand(std::time(0));
        uint64_t wikititleLength = RandomGenerator::GetRandU64(0, 512);
        char buf[wikititleLength] = {0};
        GenerateRandomWikiTitle(buf, wikititleLength);
        strncpy(payload->data, buf, wikititleLength);
        payload->length = wikititleLength;
        relation.UpdateInPlace({access_key}, [&](WikiTitleRelation &rec) {
          std::memcpy(rec.payload, payload, wikititleLength);
        });
        // relation.Insert({max_key++}, record);
        // LOG_INFO("write tx: access_key: %lu payload->data: %s", access_key,
        // payload->data);
      });
    }
    return;
  }

  char GetRandomChar() {
    const char charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    const int charsetSize = sizeof(charset) - 1;
    return charset[rand() % charsetSize];
  }

  // Function to generate a random WikiTitle
  void GenerateRandomWikiTitle(char *wikititle, int length) {
    wikititle[0] = 'h';
    wikititle[1] = 't';
    wikititle[2] = 't';
    wikititle[3] = 'p';
    wikititle[4] = 's';
    wikititle[5] = ':';
    wikititle[6] = '/';

    for (int i = 7; i < length - 1; ++i) {
      wikititle[i] = GetRandomChar();
    }
    wikititle[length - 1] = '\0'; // Null-terminate the string
  }
};

} // namespace wikititle