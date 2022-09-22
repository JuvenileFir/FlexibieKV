#ifndef MEMPOOL_HPP_
#define MEMPOOL_HPP_


#include <cstddef>



#define MAX_BLOCK_NUM 16384



typedef int temp;    // a temp type for all functions with undefined return types, remove it later



struct MemBlock
{
    void *addr;
    uint32_t in_use;
};



class MemPool
{
private:
    /* data */

public:

    size_t block_size;

    MemPool(/* args */);
    ~MemPool();

    temp getBlockSize();
    temp getBlockPtr();
    temp getAvailableNum();
    temp getPartitionTail();
    temp allocBlock();
    temp cleanBlocks();
};

MemPool::MemPool(/* args */)
{
}

MemPool::~MemPool()
{
}





#endif