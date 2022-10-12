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
    MemBlock memBlocks[MAX_BLOCK_NUM];
    size_t block_size;
    uint32_t numBlocks;
    uint32_t numUsedBlocks;

    MemPool(/* args */);
    ~MemPool();

    temp getBlockSize();
    temp get_block_ptr();
    temp getAvailableNum();
    temp getPartitionTail();
    temp allocBlock();
    temp cleanBlock();
};

MemPool::MemPool(/* args */)
{
    
}

MemPool::~MemPool()
{
    for (size_t page_id = 0; page_id < mempool.num_pages; page_id++) {
        free(mempool.memPages[page_id].addr);
    }
    mempool.num_pages = 0;
}





#endif