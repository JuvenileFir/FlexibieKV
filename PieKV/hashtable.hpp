#pragma once

#include <cstdint>    // temp , remove it if you want to define your own uint32_t
#include <cstdlib>

#include "roundhash.hpp"
#include "mempool.hpp"
using namespace std;

typedef struct TableStats
{
    size_t count;
    size_t set_nooverwrite;
    size_t set_success;
    size_t set_fail;
    size_t set_inplace;
    size_t set_evicted;
    size_t get_found;
    size_t get_notfound;
    size_t test_found;
    size_t test_notfound;
    size_t delete_found;
    size_t delete_notfound;
    size_t cleanup;
    size_t move_to_head_performed;
    size_t move_to_head_skipped;
    size_t move_to_head_failed;
}TableStats;


typedef struct Bucket
{
    uint32_t version;
    uint8_t occupt_bitmap;
    uint8_t lock;
    uint16_t padding;
    uint64_t item_vec[7];
} Bucket;    // ALIGNED(64), use alignas(64) when imply this struct


typedef struct TableBlock
{
    void *block_ptr;
    uint32_t block_id;
} TableBlock;



class HashTable
{
private:
    /* hash table data */
    TableBlock *table_blocks_[MAX_BLOCK_NUM - 1];//
    TableStats table_stats_;
    RoundHash round_hash_;
    uint32_t table_block_num_;//combine "hash_table.num_partitions" & "PartitionMap.numOfpartitions"
    uint32_t is_setting_;
    uint32_t is_flexibling_;
    uint32_t current_version_;
public:
    HashTable(MemPool* mempool);
    ~HashTable();
    void *get_block_ptr(uint32_t tableIndex);
    uint32_t get_block_id(uint32_t tableIndex);
    void AddBlock(uint8_t *pheader, uint32_t block_id); //former partitionMap_add()
    void RemoveBlock();
    void ShrinkTable(size_t blockNum);//H2L中的hashtable部分
    void ExpandTable(size_t blockNum);//L2H中的hashtable部分

};
