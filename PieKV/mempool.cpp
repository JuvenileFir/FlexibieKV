#include "mempool.hpp"


MemPool::MemPool(/* args */)
{

}

MemPool::~MemPool()
{

}


/*  
    find a free block in mempool and return its id
    return -1 to sign full
 */
uint32_t MemPool::alloc_block()
{
    // Q: linear search may be low-efficient here?
    for (uint32_t i = 0; i < blockNum; i++)
    {
        if (memBlocks[i].in_use == 0) {
            memBlocks[i].in_use = 1;
            blockNumInUse++;
            return i;
        }
    }
    return -1;
}


/* 
    get block ptr by block number
 */
void *MemPool::get_block_ptr(uint32_t blockNumber)
{
    return memBlocks[blockNumber].ptr;
}

size_t MemPool::get_block_size()
{
    return block_size_;
}

/* 
    clean the last `num_pages` pages of circular_log before being used by hash table. 
*/
void MemPool::memset_block(uint32_t blockNumber)
{
    memset(get_block_ptr(blockNumber), 0, block_size_);
}


uint32_t MemPool::get_block_available_num()
{
    return (blockNum - blockNumInUse);
}




LogBlock *MemPool::locate_item(const uint32_t blockNumber, uint64_t logOffset)
{
    return (LogBlock *)(get_block_ptr(blockNumber) + logOffset);
}