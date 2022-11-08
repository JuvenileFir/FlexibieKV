#pragma once

#include <cstdlib>

#include "roundhash.hpp"
#include "cuckoo.h"

#define MAX_KEY_LENGTH 255
#define MAX_VALUE_LENGTH 1048575

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

 
 // ALIGNED(64), use alignas(64) when imply this struct


typedef struct TableBlock
{
    void *block_ptr;
    uint32_t block_id;
} TableBlock;



class HashTable
{
private:
    /* hash table data */
public:
    TableBlock *table_blocks_[MAX_BLOCK_NUM - 1];//
    // TableStats table_stats_;
    RoundHash *round_hash_;
    RoundHash *round_hash_new_;   // new group map used in resizing
    uint32_t table_block_num_;//combine "hash_table.num_partitions" & "PartitionMap.numOfpartitions"
    uint32_t is_setting_;
    uint32_t is_flexibling_;
    uint32_t is_swapping;
    uint32_t current_version_;
    MemPool *mempool_;
    
    HashTable(MemPool* mempool);
    ~HashTable();
    void *get_block_ptr(uint32_t tableIndex);
    uint32_t get_block_id(uint32_t tableIndex);
    void AddBlock(uint8_t *pheader, uint32_t block_id); //former partitionMap_add()
    void RemoveBlock();
    void ShrinkTable(TableBlock **tableblocksToMove, size_t blocknum_to_move);//H2L中的hashtable部分
    void ExpandTable(TableBlock **tableblocksToMove, size_t blocknum_to_move);//L2H中的hashtable部分
    int64_t get_table(twoSnapshot *ts1, twoBucket *tb, Bucket *bucket,
                             uint64_t key_hash, const uint8_t *key,
                             size_t key_length, const Bucket** located_bucket);
    int64_t set_table(tablePosition *tp, twoBucket *tb, uint64_t key_hash, const uint8_t *key, size_t key_length);

    void remap_new_groups();
    void swap_group_maps();

    void redistribute_last_short_group(size_t *parts, size_t count);
    void redistribute_first_long_group(size_t *parts, size_t count);

};
