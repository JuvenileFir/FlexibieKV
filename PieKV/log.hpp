#pragma once

#ifndef LOG_HPP_
#define LOG_HPP_

#include <cstdint>
#include "hashtable.hpp"

#define BLOCK_MAX_NUM 16383  // a temp max num, remove it later
#define BATCH_SIZE (2097152U)




typedef struct StoreStats
{
    size_t actual_used_mem;
    size_t wasted;
}StoreStats;

struct TableStats;
struct TableBlock;
class MemPool;



typedef struct LogBlock
{

    uint8_t *block_ptr;
    uint32_t block_id;
    uint32_t residue;
}LogBlock;


class LogSegment
{

private:
    /* data */
public:
    LogBlock *log_blocks_[BLOCK_MAX_NUM];   // TODO: use SHM_MAX_PAGES - 1 here later
    StoreStats *store_stats_;
    TableStats *table_stats_;
    uint32_t blocknum_;
    uint32_t usingblock_;
    uint32_t offset_;
    uint32_t round_;
    double avg_item_size;
    MemPool *mempool_;

    LogSegment(MemPool *mempool);
    ~LogSegment();

    
    LogItem *locateItem(const uint32_t block_id, uint64_t log_offset);
    void set_item(struct LogItem *item, uint64_t key_hash, const uint8_t *key, uint32_t key_length, const uint8_t *value,uint32_t value_length, uint32_t expire_time);
    uint32_t get_block_id(uint32_t log_index); // get mempool block id
    uint32_t get_log_block_id(uint32_t mem_block_id);
    int64_t AllocItem(uint64_t item_size);
    int64_t set_log(uint64_t key_hash, const uint8_t *key, uint32_t key_length, const uint8_t *value,uint32_t value_length, uint32_t expire_time);
    void get_log(uint8_t *out_value, uint32_t *in_out_value_length, const uint32_t block_id, uint64_t log_offset);
    uint64_t get_tail();
    void print_table_stats();
    void print_trigger(size_t t_id);
};



class Log
{
private:
    uint16_t resizing_pointer_;

public:

    LogSegment *log_segments_[THREAD_NUM];
    uint32_t total_blocknum_;
    uint16_t total_segmentnum_;
    MemPool *mempool_;
    uint64_t mask_;
    Log(MemPool *mempool, uint64_t init_block_number);
    ~Log();

    void Shrink(TableBlock **tableblocksToMove, uint64_t numBlockToShrink);
    void Expand(TableBlock **tableblocksToMove, uint64_t numBlockToExpand, size_t blockSize);

    uint16_t get_next_resize_segment_id(int expandOrShrink); // expand: 0  shrink: 1
    void set_next_resize_segment_id(int expandOrShrink);
};





#endif