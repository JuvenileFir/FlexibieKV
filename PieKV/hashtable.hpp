#ifndef HASHTABLE_HPP_
#define HASHTABLE_HPP_


#include <cstdint>    // temp , remove it if you want to define your own uint32_t
#include <cstdlib>

#include "roundhash.hpp"
#include "mempool.hpp"

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
    void *blockPtr;
    uint32_t blockId;
} TableBlock;



class HashTable
{
private:
    /* hash table data */
    TableBlock *tableBlocks[MAX_BLOCK_NUM - 1];//?
    TableStats tableStats;
    uint32_t tableBlockNum;//combine "hash_table.num_partitions" & "PartitionMap.numOfpartitions"
    uint32_t is_setting;
    uint32_t is_flexibling;
    uint32_t current_version;
public:
    HashTable();
    ~HashTable();
    void *getBlockPtr();
    uint32_t getBlockId();
    void addBlock(); //former partitionMap_add()
    void minusBlock();
    void shrinkTable();//H2L中的hashtable部分
    void expandTable();//L2H中的hashtable部分

};

#endif