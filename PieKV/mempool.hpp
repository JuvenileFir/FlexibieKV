#ifndef MEMPOOL_HPP_
#define MEMPOOL_HPP_


#include <cstddef>
#include <cstdint>
#include <cstring>

#include "log.hpp"
#include "util.h"


#define MAX_BLOCK_NUM 16384



typedef int temp;    // a temp type for all functions with undefined return types, remove it later

// TODO: flexible block size

struct MemBlock
{
    void *ptr;
    uint32_t in_use;
};



class MemPool
{
private:
    size_t block_size_;

public:

    uint32_t blocknum_;
    uint32_t blocknum_in_use_;
    MemBlock mem_blocks_[MAX_BLOCK_NUM];

    MemPool(/* args */);
    ~MemPool();

    size_t get_block_size();
    void *get_block_ptr(uint32_t blockNumber);   // Q: Why use uint8_t * here?
    uint32_t get_block_available_num();
    temp get_partition_tail();    // TODO: change its name and implement it
    uint32_t alloc_block();
    void memset_block(uint32_t blockNumber); //Q: a function making no sense, consider to remove it later

    LogBlock *locate_item(const uint32_t blockNumber, uint64_t logOffset);  //Q:another function makes nosense, why don't locate item in Log Class? find where blockNumber come from

};






#endif