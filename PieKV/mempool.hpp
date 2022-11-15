#pragma once

#ifndef MEMPOOL_HPP_
#define MEMPOOL_HPP_


#include <cstddef>
#include <cstdint>     // temp , remove it if you want to define your own uint32_t
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <sys/mman.h>    // TODO: find an alternative later


#define MAX_BLOCK_NUM 16384

#define THREAD_NUM 4


typedef int temp;    // a temp type for all functions with undefined return types, remove it later

// TODO: flexible block size

struct MemBlock
{
    void *ptr;
    uint32_t in_use;
};


typedef struct LogItem {
    uint64_t item_size;
    uint32_t expire_time;
    uint32_t kv_length_vec;
    /* key_length: 8, value_length: 24; kv_length_vec == 0: empty item */

    #define ITEMKEY_MASK (((uint32_t)1 << 8) - 1)
    #define ITEMKEY_LENGTH(kv_length_vec) ((kv_length_vec) >> 24)

    #define ITEMVALUE_MASK (((uint32_t)1 << 24) - 1)
    #define ITEMVALUE_LENGTH(kv_length_vec) ((kv_length_vec)&ITEMVALUE_MASK)

    #define ITEMKV_LENGTH_VEC(key_length, value_length) (((uint32_t)(key_length) << 24) | (uint32_t)(value_length))

    /* the rest is meaningful only when kv_length_vec != 0 */
    uint64_t key_hash;
    uint8_t data[0];//长度为0的数组的主要用途是为了满足需要变长度的结构体
    
}LogItem;  //  ALIGNED(64)


class MemPool
{
private:
    size_t block_size_;

public:

    uint32_t blocknum_;
    uint32_t blocknum_in_use_;
    MemBlock mem_blocks_[MAX_BLOCK_NUM];

    MemPool(size_t block_size, size_t block_num_to_init);
    ~MemPool();

    size_t get_block_size();
    void *get_block_ptr(uint32_t blockNumber);   // Q: Why use uint8_t * here?
    uint32_t get_block_available_num();
    temp get_partition_tail();    // TODO: change its name and implement it
    uint32_t alloc_block();
    void memset_block(uint32_t blockNumber); //Q: a function making no sense, consider to remove it later
    void free_all_blocks();

    LogItem *locate_item(const uint32_t blockNumber, uint64_t logOffset);  //Q:another function makes nosense, why don't locate item in Log Class? find where blockNumber come from

};






#endif