#ifndef LOG_HPP_
#define LOG_HPP_

#include <cstdint>
#include "mempool.hpp"
#include "hashtable.hpp"

#define MAX 100  // a temp max num, remove it later
#define BATCH_SIZE (2097152U)
#define THREAD_NUM 4

typedef struct StoreStats
{
    /* data */
}StoreStats;




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
    LogBlock *log_blocks_[MAX];   // TODO: use SHM_MAX_PAGES - 1 here later
    StoreStats *store_stats_;
    TableStats *table_stats_;
    uint32_t blocknum_;
    uint32_t usingblock_;
    uint32_t offset_;
    uint32_t round_;

    LogSegment(/* args */);
    ~LogSegment();

    int64_t AllocItem(uint64_t item_size);
};



class Log
{
private:
    uint16_t resizing_pointer_;


public:

    LogSegment *log_segments_[THREAD_NUM];
    uint64_t total_blocknum_;
    uint16_t total_segmentnum_;


    Log(/* args */);
    ~Log();

    void Shrink(uint64_t numBlockToShrink);
    void Expand(TableBlock **tableblocksToMove, uint64_t numBlockToExpand, size_t blockSize);

    uint16_t get_next_resize_segment_id(int expandOrShrink); // expand: 0  shrink: 1
    void set_next_resize_segment_id(int expandOrShrink);
};





#endif