#pragma once

#include "common/constants.h"
#include "storage/extent/extent_list.h"

#include "openssl/evp.h"
#include "share_headers/db_types.h"
#include "share_headers/logger.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cmath>
#include <span>

namespace leanstore::storage::blob {

/** Helper deleter for EVP_MD_CTX */
struct EvlDeleter {
  void operator()(EVP_MD_CTX *ptr) const { EVP_MD_CTX_destroy(ptr); }
};

struct BlobState {
  static constexpr uint8_t PREFIX_LENGTH = 32;
  static constexpr uint16_t MIN_MALLOC_SIZE = 84UL; // MallocSize(0)
  static constexpr uint16_t MAX_MALLOC_SIZE =
      1100UL; // MallocSize(ExtentList::EXTENT_CNT_MASK)
  static constexpr uint16_t SHA256_DIGEST_LENGTH = 32;
  static thread_local std::unique_ptr<EVP_MD_CTX, EvlDeleter> sha_context;

  // Having `blob_prefix` as the 1st property eases B-Tree impl
  uint8_t blob_prefix[PREFIX_LENGTH] = {
      0};             // Prefix of the BLOB for ordered/range operators
  uint64_t blob_size; // Size of this blob in bytes
  uint8_t sha2_val[SHA256_DIGEST_LENGTH] = {
      0}; // SHA-2 value, used for hash operators

  // Information about Blob physical content
  ExtentList extents;

  // -------------------------------------------------------------------------------------
  auto operator!=(const BlobState &other) -> bool {
    return BlobID() != other.BlobID();
  }

  auto BlobID() const -> UniqueID {
    assert(extents.NumberOfExtents() > 0);
    return extents.extent_pid[0];
  }

  // Utility to access the content directly in LeanStoreAdapter
  auto Data() -> u8 * { return reinterpret_cast<u8 *>(this); }

  // -------------------------------------------------------------------------------------
  /**
   * @brief Size of a BlobState with specified number of extents
   */
  static constexpr auto MallocSize(uint8_t no_extents) -> uint16_t {
    return sizeof(BlobState) + sizeof(pageid_t) * no_extents;
  }

  static auto PageCount(uint64_t req_size) -> uint64_t {
    return std::ceil(static_cast<float>(req_size) / PAGE_SIZE);
  }

  static auto MoveToTempStorage(u8 *tmp, BlobState *tmp_btup) -> BlobState * {
    std::memcpy(tmp, tmp_btup, tmp_btup->MallocSize());
    return reinterpret_cast<BlobState *>(tmp);
  }

  static void CalculateSHA256(uint8_t *sha2_digest,
                              std::span<const uint8_t> payload) {
    EVP_DigestInit_ex(sha_context.get(), EVP_sha256(), nullptr);
    EVP_DigestUpdate(sha_context.get(), payload.data(), payload.size());
    EVP_DigestFinal_ex(sha_context.get(), sha2_digest, nullptr);
  }

  // -------------------------------------------------------------------------------------
  auto MallocSize() const -> uint16_t {
    return MallocSize(extents.NumberOfExtents());
  }

  auto PageCount() const -> uint64_t { return PageCount(blob_size); }

  auto RemainBytesInLastExtent() const -> uint64_t {
    return ExtentList::TotalSizeExtents(ExtentList::NoSpanExtents(PageCount()) -
                                        1) *
               PAGE_SIZE -
           blob_size;
  }

  void CalculateSHA256(std::span<const uint8_t> payload) {
    EVP_DigestInit_ex(sha_context.get(), EVP_sha256(), nullptr);
    EVP_DigestUpdate(sha_context.get(), payload.data(), payload.size());
    EVP_DigestFinal_ex(sha_context.get(), sha2_val, nullptr);
  }
} __attribute__((packed));

struct BlobLookupKey {
  std::span<const u8> blob;
  uint8_t sha2_digest[BlobState::SHA256_DIGEST_LENGTH] = {
      0}; // SHA-2 value, used for hash operators

  explicit BlobLookupKey(std::span<const u8> blob_data) : blob(blob_data) {
    if (blob.size() > BlobState::PREFIX_LENGTH) {
      BlobState::CalculateSHA256(sha2_digest, blob);
    }
  }
};

// Initial size of BlobState should be fixed
static_assert(sizeof(BlobState) == BlobState::MIN_MALLOC_SIZE);
static_assert(BlobState::MallocSize(0) == BlobState::MIN_MALLOC_SIZE);
static_assert(BlobState::MallocSize(ExtentList::EXTENT_CNT_MASK) ==
              BlobState::MAX_MALLOC_SIZE);

} // namespace leanstore::storage::blob