#pragma once
#include "benchmark/utils/shared_schema.h"
#include "leanstore/leanstore.h"
#include "sha256/sha256.h"
#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

namespace url {

// Supported sizes: 120 Bytes, 4KB, 100KB, 1MB, 10MB
static constexpr uint32_t SUPPORTED_PAYLOAD_SIZE[] = {512, 4096, 102400,
                                                      1048576, 10485760};
static constexpr uint32_t BLOB_NORMAL_PAYLOAD = SUPPORTED_PAYLOAD_SIZE[0];
static constexpr uint32_t MAX_BLOB_REPRESENT_SIZE =
    leanstore::BlobState::MAX_MALLOC_SIZE;

using URLKey = uint64_t;

template <typename ValueType, int TypeIdValue> struct Relation {
  static constexpr int TYPE_ID = TypeIdValue;

  struct Key {
    URLKey my_key;

    auto String() const -> std::string { return std::to_string(my_key); }

    void FromString(const std::string &s) { my_key = std::stoi(s); }
  };

  ValueType url;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return sizeof(url.length()); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.my_key);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.my_key);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::my_key); }

  Relation(ValueType value) : url(url) {
    std::memcpy(url.data, value.data, url.length);
  }
};

using URLRelation = benchmark::FileRelation<0, 512>;

} // namespace url