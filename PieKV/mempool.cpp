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
    for (uint32_t i = 0; i < blocknum_; i++)
    {
        if (mem_blocks_[i].in_use == 0) {
            mem_blocks_[i].in_use = 1;
            blocknum_in_use_++;
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
    return mem_blocks_[blockNumber].ptr;
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
    return (blocknum_ - blocknum_in_use_);
}




LogItem *MemPool::locate_item(const uint32_t blockNumber, uint64_t logOffset)
{
    return (LogItem *)(get_block_ptr(blockNumber) + logOffset);
}
