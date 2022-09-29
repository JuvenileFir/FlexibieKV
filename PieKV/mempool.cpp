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
uint32_t MemPool::allocBlock()
{
    // Q: linear search may be low-efficient here?
    for (uint32_t i = 0; i < blockNum; i++)
    {
        if (memBlocks[i].inUse == 0) {
            memBlocks[i].inUse = 1;
            blockNumInUse++;
            return i;
        }
    }
    return -1;
}


/* 
    get block ptr by block number
 */
void *MemPool::getBlockPtr(uint32_t blockNumber)
{
    return memBlocks[blockNumber].addr;
}

size_t MemPool::getBlockSize()
{
    return blockSize;
}

/* 
    clean the last `num_pages` pages of circular_log before being used by hash table. 
*/
void MemPool::memsetBlock(uint32_t blockNumber)
{
    memset(getBlockPtr(blockNumber), 0, blockSize);
}


uint32_t MemPool::getBlockAvailableNum()
{
    return (blockNum - blockNumInUse);
}