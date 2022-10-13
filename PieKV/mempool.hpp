#ifndef MEMPOOL_HPP_
#define MEMPOOL_HPP_


#include <cstddef>
#include <cstdint>
#include <cstring>

#include "log.hpp"

#define MAX_BLOCK_NUM 16384



typedef int temp;    // a temp type for all functions with undefined return types, remove it later



struct MemBlock
{
    void *addr;
    uint32_t inUse;
};



class MemPool
{
private:
    size_t blockSize;

public:

    uint32_t blockNum;
    uint32_t blockNumInUse;
    MemBlock memBlocks[MAX_BLOCK_NUM];

    MemPool(/* args */);
    ~MemPool();

    size_t getBlockSize();
    void *getBlockPtr(uint32_t blockNumber);   // Q: Why use uint8_t * here?
    uint32_t getBlockAvailableNum();
    temp getPartitionTail();    // TODO: change its name and implement it
    uint32_t allocBlock();
    void memsetBlock(uint32_t blockNumber); //Q: a function making no sense, consider to remove it later

    LogBlock *locateItem(const uint32_t blockNumber, uint64_t logOffset);  //Q:another function makes nosense, why don't locate item in Log Class? find where blockNumber come from

};





#endif