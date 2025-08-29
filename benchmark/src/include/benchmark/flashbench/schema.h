#pragma once

#include "leanstore/leanstore.h"

#include "sha256/sha256.h"
#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

namespace flash {

// Supported sizes: 120 Bytes, 4KB, 100KB, 1MB, 10MB
static constexpr uint32_t SUPPORTED_PAYLOAD_SIZE[] = {1024, 1200};
static constexpr uint32_t FLASH_NORMAL_PAYLOAD = SUPPORTED_PAYLOAD_SIZE[1];

using FlashKey = uint64_t;

template <typename TablePayload, int TypeIdValue> struct FlashRelation {
  static constexpr int TYPE_ID = TypeIdValue;

  struct Key {
    FlashKey my_key;

    auto String() const -> std::string { return std::to_string(my_key); }

    void FromString(const std::string &s) { my_key = std::stoi(s); }
  };

  TablePayload my_payload;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t {
    return sizeof(FlashRelation<TablePayload, TypeIdValue>);
  }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.my_key);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.my_key);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::my_key); }
};

} // namespace flash