#include "mempool.hpp"


static int mem_blocks_compare_vaddr(const void *a, const void *b) {
  const struct MemBlock *pa = (const struct MemBlock *)a;
  const struct MemBlock *pb = (const struct MemBlock *)b;
  if (pa->ptr < pb->ptr)
    return -1;
  else
    return 1;
}


MemPool::MemPool(size_t block_size, size_t block_num_to_init)
{
    // TODO: add lock here and add a lock to the entire mempool later
    block_size_ = block_size;
    printf("MEM: Initializing pages...\n");

    for (int i = 0; i < block_num_to_init; i++) {
        void *ptr = mmap(NULL, block_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        if (ptr == MAP_FAILED) {
            printf("MEM: map failed.\n");
        }
        mem_blocks_[i].ptr = ptr;
        if (mem_blocks_[i].ptr == NULL) break;   // TODO: what does this mean? and why is it here?
        memset(mem_blocks_[i].ptr, 0, block_size_);   // TODO: is this really necessary?
        blocknum_++;
    }
    blocknum_in_use_ = 0;
    printf("MEM:   initial allocation of %zu blocks\n", blocknum_);
    printf("MEM:   sorting by virtual address\n");
    qsort(mem_blocks_, blocknum_, sizeof(MemBlock), mem_blocks_compare_vaddr);
}

MemPool::~MemPool()
{
    for (int i = 0; i < blocknum_; i++){
        free(mem_blocks_[i].ptr);
    }
    blocknum_ = 0;
    blocknum_in_use_ = 0;
}

void MemPool::free_all_blocks()
{
    for (int i = 0; i < blocknum_; i++){
        free(mem_blocks_[i].ptr);
    }
    blocknum_ = 0;
    blocknum_in_use_ = 0;
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
