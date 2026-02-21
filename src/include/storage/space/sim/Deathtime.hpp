// lbh 07.07.24

#pragma once

#include "SSD.hpp"
#include <algorithm>
#include <vector>
#include <list>
#include <random>
#include <set>
#include <map>
#include <tuple>
#include <chrono>
#include <numeric>

class DTE {
    uint64_t currentBlock;
    std::list<uint64_t> freeBlocks;
    std::map<uint64_t, std::list<uint64_t>> writeHistory; // logicalPageId -> list of timestamps
    size_t maxHistorySize = 8; // Max number of write history to keep for each logical page
    std::vector<uint64_t> blockEstimatedDeathTime; // Average estimated death time of each block
    std::list<uint64_t> fullBlocks; // List of blocks that are fully written

    SSD& ssd;
    std::string gcAlgorithm;
    uint64_t bufferCount;
    uint64_t groupCount;
    std::vector<uint64_t> cachedPageIds; // List of cached page IDs (write buffer)
    bool updateHistoryUponGC;

public:
    DTE(SSD& ssd, std::string gcAlgorithm)
        : ssd(ssd), gcAlgorithm(gcAlgorithm) {
        // Initialize free block list
        for (unsigned z = 0; z < ssd.zones; z++) {
            freeBlocks.push_back(z);
        }

        currentBlock = freeBlocks.front();
        freeBlocks.pop_front();
        ensure(ssd.blocks()[currentBlock].canWrite());
        bufferCount = ssd.pagesPerZone;
        groupCount = 4;
        blockEstimatedDeathTime.resize(ssd.zones, 0); // Initialize death times to 0
        updateHistoryUponGC = true;
    }

    std::string name() {
        return gcAlgorithm;
    }

    uint64_t currentTime() {
        auto now = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
        return duration.count();
    }

    void writePage(uint64_t pageId) {
        if (!ssd.blocks()[currentBlock].canWrite()) {
            if(freeBlocks.size()<groupCount){
                performGC();
            }
           
            std::cout << "GC completed. Continuing with write operations." << std::endl;
            currentBlock = freeBlocks.front();
            freeBlocks.pop_front();
            ensure(ssd.blocks()[currentBlock].canWrite());
        }

        uint64_t timestamp = currentTime();
        if (writeHistory[pageId].size() >= maxHistorySize) {
            writeHistory[pageId].pop_front();
        }
        writeHistory[pageId].push_back(timestamp);
        cachedPageIds.push_back(pageId);

        //std::cout << "Page " << pageId << " written to block " << currentBlock << ". Timestamp: " << timestamp << std::endl;

        if (cachedPageIds.size() >= bufferCount) {
            std::cout << "Write buffer threshold reached. Flushing pages..." << std::endl;
            flushPages(cachedPageIds);
            cachedPageIds.clear();
        }
    }


    uint64_t estimateDeathTime(const std::list<uint64_t>& history) {
        if (history.size() < 2) {
            return history.back();
        }

        size_t count = std::min(history.size(), maxHistorySize);
        uint64_t totalInterval = 0;
        auto it = history.rbegin();
        for (size_t i = 0; i < count - 1; ++i) {
            auto next_it = std::next(it);
            totalInterval += *it - *next_it;
            ++it;
        }
        uint64_t avgInterval = totalInterval / (count - 1);
        return history.back() + avgInterval;
    }

    void flushPages(const std::vector<uint64_t>& pageIds) {
        std::vector<std::pair<uint64_t, uint64_t>> deathTimes; 
        for (const auto& pageId : pageIds) {
            uint64_t estimatedDeathTime = estimateDeathTime(writeHistory[pageId]);
            deathTimes.push_back({estimatedDeathTime, pageId});
        }

        std::sort(deathTimes.begin(), deathTimes.end()); 
        size_t totalPages = deathTimes.size();
        size_t groupSize = (totalPages + groupCount - 1) / groupCount; 

        for (size_t group = 0; group < groupCount; ++group) {
            std::vector<uint64_t> pagesToWrite;
            for (size_t i = group * groupSize; i < (group + 1) * groupSize && i < totalPages; ++i) {
                pagesToWrite.push_back(deathTimes[i].second);
            }
            writeGroupPages(pagesToWrite, group);
        }
        for (const auto& pageId : pageIds) {
            writeHistory[pageId].clear();
        }
    }

    void writeGroupPages(const std::vector<uint64_t>& pagesToWrite, size_t group) {
        std::vector<uint64_t> selectedBlocks = selectBlocksForGroup(group);
        for (const auto& pageId : pagesToWrite) {
            if (selectedBlocks.empty()) {
                if (freeBlocks.size() < groupCount) {
                    performGC();
                }
                ensure(!freeBlocks.empty());
                selectedBlocks = selectBlocksForGroup(group);
            }
            uint64_t blockId = selectedBlocks.back();
            selectedBlocks.pop_back();
            ssd.writePage(pageId, blockId);
            updateBlockDeathTime(blockId);
        }
    }

    std::vector<uint64_t> selectBlocksForGroup(size_t group) {
        std::vector<uint64_t> availableBlocks;
        for (uint64_t blockId = 0; blockId < ssd.zones; ++blockId) {
            if (ssd.blocks()[blockId].canWrite()) {
                availableBlocks.push_back(blockId);
            }
        }
        std::sort(availableBlocks.begin(), availableBlocks.end(), [&](uint64_t a, uint64_t b) {
            return blockEstimatedDeathTime[a] < blockEstimatedDeathTime[b];
        });
        std::vector<uint64_t> selectedBlocks;
        for (size_t i = 0; i < groupCount && i < availableBlocks.size(); ++i) {
            selectedBlocks.push_back(availableBlocks[i]);
        }
        return selectedBlocks;
    }

    void updateBlockDeathTime(uint64_t blockId) {
        uint64_t totalDeathTime = 0;
        uint64_t validPages = ssd.blocks()[blockId].validCnt();
        for (const auto& [pageId, history] : writeHistory) {
            if (!history.empty()) {
                totalDeathTime += estimateDeathTime(history);
            }
        }
        if (validPages > 0) {
            blockEstimatedDeathTime[blockId] = totalDeathTime / validPages;
        } else {
            blockEstimatedDeathTime[blockId] = 0;
        }
    }
    
    void performGC() {
        if (fullBlocks.empty()) {
            std::cout << "No full blocks available for GC." << std::endl;
            return; // No blocks to clean if fullBlocks is empty
        }

        uint64_t victimBlockId = selectVictimBlock();
        std::cout << "Selected victim block " << victimBlockId << " for GC." << std::endl;

        if (victimBlockId < ssd.zones) {
            // Transfer valid pages to cachedPageIds for deferred writing
            const auto& block = ssd.blocks()[victimBlockId];
            for (uint64_t pageId = 0; pageId < ssd.pagesPerZone; ++pageId) {
                if (block.ptl()[pageId] != SSD::unused) { // Assuming SSD::unused defines an unused page
                    cachedPageIds.push_back(block.ptl()[pageId]); // Queue valid pages for re-writing
                    std::cout << "Page " << block.ptl()[pageId] << " queued for rewrite." << std::endl;
                }
            }

            // After transferring all valid pages, erase the block
            ssd.eraseBlock(victimBlockId);
            freeBlocks.push_back(victimBlockId); // Return the block to the freeBlocks list
            std::cout << "Block " << victimBlockId << " erased and returned to free pool." << std::endl;
        }
    }

    uint64_t selectVictimBlock() {
        if (gcAlgorithm.contains("edt")) {
            return selectVictimBlockEDT();
        } else if (gcAlgorithm.contains("greedy")) {
            return selectVictimBlockGreedy();
        } else {
            throw std::runtime_error("unknown GC algorithm");
        }
    }

    uint64_t selectVictimBlockEDT() {
        uint64_t selectedBlock = ssd.zones; // Invalid block id
        uint64_t smallestEDT = std::numeric_limits<uint64_t>::max();
        for (uint64_t blockId : fullBlocks) {
            if (blockEstimatedDeathTime[blockId] < smallestEDT) {
                smallestEDT = blockEstimatedDeathTime[blockId];
                selectedBlock = blockId;
            }
        }
        fullBlocks.remove(selectedBlock); // Remove the selected block from fullBlocks
        return selectedBlock;
    }

    uint64_t selectVictimBlockGreedy() {
        uint64_t selectedBlock = ssd.zones; // Invalid block id
        uint64_t minValidPages = std::numeric_limits<uint64_t>::max();
        for (uint64_t blockId : fullBlocks) {
            uint64_t validCnt = ssd.blocks()[blockId].validCnt();
            if (validCnt < minValidPages) {
                minValidPages = validCnt;
                selectedBlock = blockId;
            }
        }
        fullBlocks.remove(selectedBlock); // Remove the selected block from fullBlocks
        return selectedBlock;
    }

};
