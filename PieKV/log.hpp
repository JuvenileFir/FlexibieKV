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

typedef struct TableStats
{
    /* data */
}TableStats;


typedef struct LogBlock
{
    uint8_t *blockPtr;
    uint32_t blockId;
    uint32_t Residue;
}LogBlock;


class LogSegment
{
private:
    /* data */
public:
    LogBlock *logBlocks[MAX];   // TODO: use SHM_MAX_PAGES - 1 here later
    StoreStats *storeStats;
    TableStats *tableStats;
    uint32_t blockNum;
    uint32_t usingBlock;
    uint32_t offset;
    uint32_t round;

    LogSegment(/* args */);
    ~LogSegment();

    int64_t allocItem(uint64_t item_size);
};


class Log
{
private:
    uint16_t resizingPointer;


public:
    LogSegment *logSegments[THREAD_NUM];
    uint64_t totalBlockNum;
    uint16_t totalSegmentNum;

    Log(/* args */);
    ~Log();

    void shrink(uint64_t numBlockToShrink);
    void expand(TableBlock **tableblocksToMove, uint64_t numBlockToExpand, size_t blockSize);

    uint16_t get_next_resize_segment_id(int expandOrShrink); // expand: 0  shrink: 1
    void set_next_resize_segment_id(int expandOrShrink);
};





#endif