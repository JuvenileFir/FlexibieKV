#ifndef LOG_HPP_
#define LOG_HPP_

#include <cstdint>
#include "mempool.hpp"

#define MAX 100  // a temp max num, remove it later


typedef struct StoreStats
{
    /* data */
}StoreStats;




typedef struct LogBlock
{
    uint8_t blockPtr;
    uint32_t blockId;
    uint32_t Residue;
}LogBlock;


typedef struct LogSegment
{
    LogBlock logBlocks[MAX];//max log blocks
    StoreStats storeStats;
    
    uint32_t numPages;  // change name?
    uint32_t usingPage;
    uint32_t offset;
    uint8_t round;
}LogSegment;



class Log
{
private:
public:
    LogSegment logSegment[MAX];//max thread
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