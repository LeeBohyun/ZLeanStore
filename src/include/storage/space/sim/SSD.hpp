//
// from ssd-iq branch: https://github.com/gabriel-haas/ssdiq
// added additional features by Lia
//
#pragma once

#include "shared/Exceptions.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <csignal>
#include <deque>
#include <functional>
#include <fstream>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace leanstore::storage::space {

class SSD {
public:
   constexpr static uint64_t unused = ~0ull;
   const double ssdFill;       // 1-alpha [0-1]
   const uint64_t capacity;    // bytes
   const uint64_t zoneSize;    // bytes
   const uint64_t pageSize;    // bytes
   const uint64_t zones;
   const uint64_t pagesPerZone;
   const uint64_t logicalPages;
   const uint64_t physicalPages;

   std::string gcalgo;

   /* stats */
   std::vector<uint64_t> writtenPages;

   // Write buffer properties
   const uint64_t writeBufferSize;          // number of pages
   std::list<uint64_t> writeBuffer;         // LRU list
   std::map<uint64_t, std::list<uint64_t>> freqList; // (unused here; kept)
   uint64_t minFreq = 1;
   static constexpr double writeBufferSizePct = 0; // 0.0002;
   std::unordered_map<uint64_t, std::list<uint64_t>::iterator> writeBufferMap;

   class Block {
      uint64_t _writePos = 0;
      uint64_t _validCnt = 0;
      uint64_t _eraseCount = 0;
      inline static uint64_t _eraseAgeCounter = 0;
      std::vector<uint64_t> _ptl; // phy to log

   public:
      const std::vector<uint64_t>& ptl() const { return _ptl; }
      uint64_t validCnt() const { return _validCnt; }
      uint64_t writePos() const { return _writePos; }

      const uint64_t pagesPerZone;
      const uint64_t blockId;

      std::vector<uint64_t> invalidationTimes;
      int64_t gcAge = -1;
      int64_t gcGeneration = 0;
      int64_t group = -1;
      bool writtenByGc = false;

      Block(uint64_t pagesPerBlock, uint64_t blockId)
         : _ptl(pagesPerBlock)
         , pagesPerZone(pagesPerBlock)
         , blockId(blockId)
      {
         std::fill(_ptl.begin(), _ptl.end(), unused);
      }

      uint64_t write(uint64_t logPageId) {
         ensure(canWrite());
         ensure(_ptl[_writePos] == unused);
         _ptl[_writePos] = logPageId;
         _validCnt++;
         return _writePos++;
      }

      void setUnused(uint64_t pos) {
         ensure(_ptl[pos] != unused);
         _ptl[pos] = unused;
         _validCnt--;
      }

      bool fullyWritten() const { return _writePos == pagesPerZone; }
      bool canWrite() const { return _writePos < pagesPerZone; }
      bool allValid() const { return _validCnt == pagesPerZone; }
      bool allInvalid() const { return _validCnt == 0; }
      bool isErased() const { return _writePos == 0; }

      void compactNoMappingUpdate() {
         _writePos = 0;
         for (uint64_t p = 0; p < pagesPerZone; p++) {
            const uint64_t logpagemove = _ptl[p];
            if (logpagemove != unused) {
               _ptl[_writePos] = logpagemove;
               _writePos++;
            }
         }
         _validCnt = _writePos;
         std::fill(_ptl.begin() + _writePos, _ptl.end(), unused);

         _eraseCount++;
         gcAge = _eraseAgeCounter++;
         writtenByGc = true;
      }

      void erase() {
         std::fill(_ptl.begin(), _ptl.end(), unused);
         _writePos = 0;
         _eraseCount++;
         gcAge = _eraseAgeCounter++;
         writtenByGc = false;
         _validCnt = 0;
         group = -1;
      }

      void print() {
         std::cout << "age: " << gcAge
                   << " gcGen: " << gcGeneration
                   << " wbgc: " << writtenByGc
                   << " vc: " << _validCnt;
      }
   };

private:
   // Global coarse lock to make SSD simulator thread-safe (debug/correctness first).
   // Many methods call each other; recursive_mutex avoids self-deadlock.
   mutable std::recursive_mutex ssdMutex;

   std::vector<Block> _blocks;               // phys block -> log pages inside
   std::vector<uint64_t> _ltpMapping;        // logPageId -> physAddr
   std::vector<uint64_t> _mappingUpdatedCnt; // stats
   std::vector<uint64_t> _mappingUpdatedGC;  // stats
   uint64_t _physWrites = 0;

   // stats
   uint64_t gcedNormalBlock = 0;
   uint64_t gcedColdBlock = 0;

   // --- Wear leveling configuration ---
   bool wearLevelingEnabled = false; // set true if you want WL on by default
   uint64_t wlDefaultLunCnt = 64;
   uint64_t wlLunCnt = 0;
   uint64_t wlThresholdT = 1024;

   // Free block pools per LUN (blocks that are erased and ready)
   std::vector<std::deque<uint64_t>> wlFreePools;
   std::vector<bool> wlInFreePool;
   std::vector<uint64_t> wlCurrentBlock;
   std::vector<uint64_t> wlUpdateCounter;

   inline uint64_t blockToLun(uint64_t blockId) const {
      return (wlLunCnt == 0) ? 0 : (blockId % wlLunCnt);
   }

   inline uint64_t logPageToLun(uint64_t logPage) const {
      return (wlLunCnt == 0) ? 0 : (logPage % wlLunCnt);
   }

   // Assumes ssdMutex is held.
   void wlPushFreeBlock(uint64_t blockId) {
      uint64_t lun = blockToLun(blockId);
      if (!wlInFreePool[blockId]) {
         wlFreePools[lun].push_back(blockId);
         wlInFreePool[blockId] = true;
      }
   }

   // Assumes ssdMutex is held.
   uint64_t wlPopFreeBlock(uint64_t lun) {
      auto& q = wlFreePools[lun];
      while (!q.empty()) {
         uint64_t b = q.front();
         q.pop_front();
         wlInFreePool[b] = false;
         if (_blocks[b].isErased()) {
            return b;
         }
      }

      // fallback scan
      for (uint64_t b = lun; b < _blocks.size(); b += wlLunCnt) {
         if (_blocks[b].isErased() && !wlInFreePool[b]) {
            return b;
         }
      }

      raise(SIGINT);
      return 0;
   }

   // Assumes ssdMutex is held.
   void wlRotate(uint64_t lun) {
      if (wlCurrentBlock[lun] == unused) {
         wlCurrentBlock[lun] = wlPopFreeBlock(lun);
         wlUpdateCounter[lun] = 0;
         return;
      }

      uint64_t oldId = wlCurrentBlock[lun];
      Block& oldB = _blocks[oldId];

      uint64_t newId = wlPopFreeBlock(lun);
      Block& newB = _blocks[newId];

      // Copy valid pages
      for (uint64_t p = 0; p < pagesPerZone; p++) {
         uint64_t lba = oldB.ptl()[p];
         if (lba != unused) {
            // This is a "real move" (updates mapping, invalidates old phys page).
            writePageWithoutCaching(lba, newB);
         }
      }

      // Erase old and return to free pool
      oldB.erase();
      oldB.gcGeneration = 0;
      wlPushFreeBlock(oldId);

      wlCurrentBlock[lun] = newId;
      wlUpdateCounter[lun] = 0;
   }

   // Assumes ssdMutex is held.
   Block& wlCurrentWriteBlockFor(uint64_t logPage) {
      uint64_t lun = logPageToLun(logPage);
      if (wlCurrentBlock[lun] == unused) {
         wlRotate(lun);
      }
      if (!_blocks[wlCurrentBlock[lun]].canWrite()) {
         wlRotate(lun);
      }
      return _blocks[wlCurrentBlock[lun]];
   }

public:
   const decltype(_blocks)& blocks() const { return _blocks; }
   const decltype(_ltpMapping)& ltpMapping() const { return _ltpMapping; }
   const decltype(_mappingUpdatedCnt)& mappingUpdatedCnt() const { return _mappingUpdatedCnt; }
   const decltype(_mappingUpdatedGC)& mappingUpdatedGC() const { return _mappingUpdatedGC; }
   uint64_t physWrites() const { return _physWrites; }

   void hackForOptimalWASetPhysWrites(uint64_t phyWrites) { _physWrites = phyWrites; }

   SSD(uint64_t capacity, uint64_t zoneSize, uint64_t pageSize, double ssdFill)
      : ssdFill(ssdFill)
      , capacity(capacity)
      , zoneSize(zoneSize)
      , pageSize(pageSize)
      , zones(capacity / zoneSize)
      , pagesPerZone(zoneSize / pageSize)
      , logicalPages((capacity / pageSize) * ssdFill)
      , physicalPages(zones * pagesPerZone)
      , writeBufferSize(static_cast<uint64_t>(logicalPages * writeBufferSizePct))
   {
      _ltpMapping.resize(logicalPages);
      _mappingUpdatedCnt.resize(logicalPages);
      _mappingUpdatedGC.resize(logicalPages);

      std::fill(_ltpMapping.begin(), _ltpMapping.end(), unused);

      for (uint64_t z = 0; z < zones; z++) {
         _blocks.emplace_back(Block(pagesPerZone, z));
      }

      // --- Wear leveling init ---
      wlLunCnt = std::min<uint64_t>(wlDefaultLunCnt, zones);
      if (wlLunCnt == 0) wlLunCnt = 1;

      wlFreePools.assign(wlLunCnt, {});
      wlInFreePool.assign(zones, false);
      wlCurrentBlock.assign(wlLunCnt, unused);
      wlUpdateCounter.assign(wlLunCnt, 0);

      // Initially, all blocks are erased -> free
      for (uint64_t b = 0; b < zones; b++) {
         wlPushFreeBlock(b);
      }
   }

   // Host write entry point using naive wear leveling
   void writePageWL(uint64_t logPage, int64_t group = -1) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      if (!wearLevelingEnabled) {
         writePage(logPage, _blocks[0], group);
         return;
      }

      uint64_t lun = logPageToLun(logPage);
      wlUpdateCounter[lun]++;

      if (wlUpdateCounter[lun] >= wlThresholdT) {
         wlRotate(lun);
      }

      Block& cur = wlCurrentWriteBlockFor(logPage);
      writePage(logPage, cur, group);
   }

   void setWearLeveling(bool enabled) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);
      wearLevelingEnabled = enabled;
   }

   void setWearLevelingThreshold(uint64_t t) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);
      wlThresholdT = std::max<uint64_t>(1, t);
   }

   uint64_t wearLevelingLunCnt() const { return wlLunCnt; }

   uint64_t getZone(uint64_t physAddr) const { return physAddr / pagesPerZone; }
   uint64_t getPage(uint64_t physAddr) const { return physAddr % pagesPerZone; }
   uint64_t getAddr(uint64_t zone, uint64_t pos) const { return (zone * pagesPerZone) + pos; }

   void writePage(uint64_t logPage, uint64_t block, int64_t group = -1) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);
      writePage(logPage, _blocks[block], group);
   }

   void writePage(uint64_t logPage, Block& block, int64_t group = -1) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      if (writeBufferSize == 0) {
         writePageWithoutCaching(logPage, block, group);
      } else {
         if (writeBufferMap.find(logPage) != writeBufferMap.end()) {
            writeBuffer.erase(writeBufferMap[logPage]);
            writeBuffer.push_front(logPage);
            writeBufferMap[logPage] = writeBuffer.begin();
         } else {
            writeBuffer.push_front(logPage);
            writeBufferMap[logPage] = writeBuffer.begin();
            if (writeBuffer.size() >= writeBufferSize) {
               uint64_t lruPage = writeBuffer.back();
               writeBufferMap.erase(lruPage);
               writeBuffer.pop_back();
               writePageWithoutCaching(lruPage, block, group);
            }
         }
      }
   }

   // only use from GC (or WL internal copies)
   void writePageWithoutCaching(uint64_t logPage, Block& block, int64_t group = -1) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      if (block.group == -1) {
         block.group = group;
      }
      uint64_t addr = _ltpMapping.at(logPage);
      if (addr != unused) {
         uint64_t z = getZone(addr);
         uint64_t p = getPage(addr);
         ensure(z < _blocks.size());
         _blocks.at(z).setUnused(p);
      }
      uint64_t writePos = block.write(logPage);
      _ltpMapping[logPage] = getAddr(block.blockId, writePos);
      _mappingUpdatedCnt[logPage]++;
      _physWrites++;
   }

   void eraseBlock(Block& block) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      uint64_t id = block.blockId;
      block.erase();
      if (wearLevelingEnabled) {
         wlPushFreeBlock(id);
      }
   }

   void eraseBlock(uint64_t blockId) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      ensure(blockId < _blocks.size());
      _blocks[blockId].erase();
      ensure(_blocks[blockId].isErased());
      if (wearLevelingEnabled) {
         wlPushFreeBlock(blockId);
      }
   }

   void compactBlock(uint64_t& block) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);
      compactBlock(_blocks[block]);
   }

   void compactBlock(Block& block) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      block.compactNoMappingUpdate();
      block.gcGeneration++;
      if (block.writtenByGc) {
         gcedColdBlock++;
      } else {
         gcedNormalBlock++;
      }
      block.writtenByGc = true;

      for (uint64_t p = 0; p < block.writePos(); p++) {
         uint64_t logPage = block.ptl()[p];
         ensure(block.ptl()[p] != unused);
         _ltpMapping[logPage] = getAddr(block.blockId, p);
         _mappingUpdatedGC[logPage]++;
         _physWrites++;
      }
   }

   bool moveValidPagesTo(uint64_t sourceId, uint64_t destinationId) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      Block& source = _blocks[sourceId];
      Block& destination = _blocks[destinationId];

      if (source.writtenByGc) {
         gcedColdBlock++;
      } else {
         gcedNormalBlock++;
      }
      destination.writtenByGc = true;

      uint64_t p = 0;
      while (p < pagesPerZone && destination.canWrite()) {
         if (source.ptl()[p] != unused) {
            writePageWithoutCaching(source.ptl()[p], destination);
         }
         p++;
      }
      return !source.allInvalid();
   }

   int64_t moveValidPagesTo(
      uint64_t sourceId,
      std::function<std::tuple<int64_t, int64_t>(uint64_t)> destinationFun)
   {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      Block& source = _blocks[sourceId];
      ensure(!source.allValid());

      if (source.writtenByGc) {
         gcedColdBlock++;
      } else {
         gcedNormalBlock++;
      }

      uint64_t p = 0;
      int64_t firstFullDestinationId = -1;
      while (p < pagesPerZone) {
         uint64_t lba = source.ptl()[p];
         if (lba != unused) {
            auto [destinationId, groupId] = destinationFun(lba);
            Block& destination = _blocks[destinationId];
            if (destination.canWrite()) {
               writePageWithoutCaching(source.ptl()[p], destination, groupId);
            } else if (firstFullDestinationId == -1) {
               firstFullDestinationId = destinationId;
            }
         }
         p++;
      }

      if (source.allInvalid()) {
         return -1;
      } else {
         Block& dest = _blocks[firstFullDestinationId];
         ensure(!dest.canWrite());
         return firstFullDestinationId;
      }
   }

   std::tuple<uint64_t, int64_t> compactUntilFreeBlock(
      int64_t gcBlockId,
      std::function<uint64_t()> nextBlock)
   {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      if (gcBlockId == -1 || _blocks[gcBlockId].allValid()) {
         gcBlockId = nextBlock();
         Block& gcBlock = _blocks[gcBlockId];
         compactBlock(gcBlock);
         ensure(!gcBlock.allValid());
      }

      ensure(!_blocks[gcBlockId].allValid());
      uint64_t victimId = nextBlock();

      while (moveValidPagesTo(victimId, gcBlockId)) {
         Block& victim = _blocks[victimId];
         compactBlock(victim);
         gcBlockId = victimId;
         victimId = nextBlock();
      }

      Block& nowFree = _blocks[victimId];
      nowFree.erase();
      nowFree.gcGeneration = 0;
      if (wearLevelingEnabled) {
         wlPushFreeBlock(victimId);
      }
      return std::make_tuple(victimId, gcBlockId);
   }

   std::tuple<uint64_t, int64_t> compactUntilFreeBlock(
      int64_t groupId,
      std::function<uint64_t(int64_t)> nextBlock,
      std::function<std::tuple<int64_t, int64_t>(int64_t)> gcDestinationFun,
      std::function<void(int64_t, int64_t)> updateGroupFun)
   {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      uint64_t victimId = nextBlock(groupId);
      int64_t fullDest;

      do {
         fullDest = moveValidPagesTo(victimId, gcDestinationFun);
         if (fullDest == -1) {
            break;
         }

         Block& dest = _blocks[fullDest];
         ensure(!dest.canWrite());
         Block& victim = _blocks[victimId];
         ensure(fullDest != static_cast<int64_t>(victimId));

         compactBlock(victim);

         victim.group = dest.group;
         ensure(dest.group != -1);
         ensure(!dest.canWrite());

         updateGroupFun(dest.group, victimId);
         victimId = nextBlock(groupId);
      } while (true);

      Block& nowFree = _blocks[victimId];
      nowFree.erase();
      nowFree.gcGeneration = 0;
      if (wearLevelingEnabled) {
         wlPushFreeBlock(victimId);
      }
      return std::make_tuple(victimId, -1);
   }

   uint64_t compactUntilFreeBlock(std::vector<uint64_t> victimBlockList) {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      if (victimBlockList.empty()) {
         throw std::invalid_argument("victimBlockList must contain at least one block ID.");
      }

      if (victimBlockList.size() == 1) {
         uint64_t singleBlockId = victimBlockList[0];
         ensure(_blocks[singleBlockId].allInvalid());
         eraseBlock(singleBlockId);
         ensure(_blocks[singleBlockId].isErased());
         return singleBlockId;
      }

      for (size_t i = victimBlockList.size() - 1; i > 0; i--) {
         uint64_t destBlockId = victimBlockList[i];
         compactBlock(_blocks[destBlockId]);
         uint64_t sourceBlockId = victimBlockList[i - 1];
         moveValidPagesTo(sourceBlockId, destBlockId);
      }

      uint64_t finalBlockId = victimBlockList[0];
      ensure(_blocks[finalBlockId].allInvalid());
      eraseBlock(finalBlockId);
      ensure(_blocks[finalBlockId].isErased());

      return finalBlockId;
   }

   void resetPhysicalCounters() {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);
      _physWrites = 0;
   }

   void printInfo() {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      std::cout << "capacity: " << capacity
                << " blocksize: " << zoneSize
                << " pageSize: " << pageSize << "\n";
      std::cout << "blockCnt: " << zones
                << " pagesPerBlock: " << pagesPerZone
                << " logicalPages: " << logicalPages
                << " ssdfill: " << ssdFill
                << " physical page cnt: " << physicalPages << "\n";
      Ensure(physicalPages % pagesPerZone == 0);
   }

   void stats() {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      auto writtenByGc = std::count_if(
         _blocks.begin(), _blocks.end(),
         [](Block& b) { return b.writtenByGc; });

      int64_t maxGCAge = 20;
      std::vector<uint64_t> gcGenerations(maxGCAge, 0);
      std::vector<uint64_t> gcGenerationValid(maxGCAge, 0);
      std::vector<uint64_t> gcGenerationValidMin(maxGCAge, std::numeric_limits<uint64_t>::max());

      for (auto& b : _blocks) {
         auto idx = std::min<int64_t>(b.gcGeneration, maxGCAge - 1);
         gcGenerations[idx]++;
         gcGenerationValid[idx] += b.validCnt();
         if (b.fullyWritten()) {
            if (b.validCnt() > pagesPerZone) raise(SIGINT);
            gcGenerationValidMin[idx] = std::min(gcGenerationValidMin[idx], b.validCnt());
         }
      }

      std::cout << "writtenByGC: " << writtenByGc
                << " (" << std::round((float)writtenByGc / zones * 100) << "%)"
                << " gcedNormal: " << gcedNormalBlock
                << " gcedCold: " << gcedColdBlock << "\n";

      std::cout << "gc generations: [";
      for (int i = 0; i < maxGCAge; i++) {
         std::cout << i << ":(c: " << gcGenerations[i] * 100 / zones;
         if (gcGenerations[i] > 0) {
            std::cout << ", f: " << std::round(gcGenerationValid[i] * 100 / gcGenerations[i] / pagesPerZone);
         } else {
            std::cout << ", f: 0";
         }
         if (gcGenerationValidMin[i] != std::numeric_limits<uint64_t>::max()) {
            if (gcGenerationValidMin[i] > pagesPerZone) raise(SIGINT);
            std::cout << ", m: " << gcGenerationValidMin[i] * 100 / pagesPerZone;
         } else {
            std::cout << ", m: x";
         }
         std::cout << "), ";
      }
      std::cout << "]\n";

      gcedNormalBlock = 0;
      gcedColdBlock = 0;
   }

   void printBlocksStats() {
      std::lock_guard<std::recursive_mutex> g(ssdMutex);

      std::cout << "BlockStats:\n";
      std::vector<long> ages;
      ages.reserve(_blocks.size());
      for (auto& b : _blocks) {
         ages.emplace_back(b.gcAge);
      }
      std::sort(ages.begin(), ages.end());
      long minAge = *std::min_element(ages.begin(), ages.end());

      std::cout << "age: ";
      for (auto& a : ages) {
         std::cout << (a - minAge) << " ";
      }
      std::cout << "\n";

      std::cout << "gcGen: ";
      for (auto& b : _blocks) {
         std::cout << b.gcGeneration << " ";
      }
      std::cout << "\n";

      std::cout << "writtenByGC: ";
      for (auto& b : _blocks) {
         std::cout << b.writtenByGc << " ";
      }
      std::cout << "\n";

      std::cout << "ValidCnt: ";
      for (auto& b : _blocks) {
         std::cout << pagesPerZone - b.validCnt() << " ";
      }
      std::cout << "\n";

      std::cout << "Groups: ";
      for (auto& b : _blocks) {
         std::cout << b.group << " ";
      }
      std::cout << "\n";
   }

   void writeStatsFile([[maybe_unused]] std::string prefix) {
      // left as-is
   }
};

} // namespace leanstore::storage::space
