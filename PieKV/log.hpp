#ifndef LOG_HPP_
#define LOG_HPP_

#include <cstdint>
#include "mempool.hpp"

#define MAX 100  // a temp max num, remove it later


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
    uint8_t blockPtr;
    uint32_t blockId;
    uint32_t Residue;
}LogBlock;


typedef struct LogSegment
{
    LogBlock logBlocks[MAX];
    StoreStats storeStats;
    TableStats tableStats;
    uint32_t numPages;  // change name?
    uint32_t usingPage;
    uint32_t offset;
}LogSegment;



class Log
{
private:
public:
    LogSegment logSegment[MAX];
    uint16_t totalNumPage; // change name?
    uint16_t resizingPointer;

    Log(/* args */);
    ~Log();
    temp shrink();
    temp expand();
    temp locateItem();
    temp allocItem();
};

Log::Log(/* args */)
{
}

Log::~Log()
{
}



#endif