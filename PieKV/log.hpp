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
    uint8_t block_ptr;
    uint32_t block_id;
    uint32_t residual_space;
}LogBlock;


typedef struct LogSegment
{
    LogBlock log_blocks[MAX];//max log blocks
    StoreStats store_stats;
    
    uint32_t num_pages;  // change name?
    uint32_t using_page;
    uint32_t offset;
    uint8_t round;
}LogSegment;



class Log
{
private:
public:
    LogSegment log_segment_[MAX];//max thread
    uint16_t total_num_page_; // change name?
    uint16_t resizing_pointer_;

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