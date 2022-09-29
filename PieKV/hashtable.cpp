#include "hashtable.hpp"

HashTable::HashTable(){
  is_setting = 0;
  is_flexibling = 0;
  tableBlockNum = MemPool::getAvailableNum();
  assert(tableBlockNum);

  RoundHash::RoundHash(tableBlockNum);
  
  for (uint32_t i = 0; i < tableBlockNum; i++) {
    int32_t ret = MemPool::allocBlock();
    assert(ret + 1); 
    tableBlocks[i].blockId = (uint32_t)ret;
    tableBlocks[i].blockPtr = MemPool::getBlockPtr(ret);
    MemPool::cleanBlock(ret);
  }
}

HashTable::~HashTable(){
  
}

void *HashTable::getBlockPtr(uint32_t tableIndex){
  return tableBlocks[tableIndex].blockPtr;
}

uint32_t HashTable::getBlockId(uint32_t tableIndex){
  return tableBlocks[tableIndex].blockId;
}

void HashTable::addBlock(){

}

void HashTable::minusBlock(){
    
}
  /* naive transplant */
void HashTable::shrinkTable(size_t blockNum){
  /* once in a cycle*/
  while (blockNum--){
  get_last_short_group_parts(parts, &count, current_version);//current_version???
  DelBucket_v();

  while (1)
    if(__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting), 0U, 1U)) break;
  *(volatile uint32_t *)&(is_flexibling) = 1U;
  __sync_fetch_and_sub((volatile uint32_t *)&(is_setting), 1U);

  redistribute_last_short_group(table, parts, count);
  tableBlockNum--;

  while (1)
    if(__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting), 0U, 1U)) break;
  uint64_t v = (uint64_t)(!current_version) << 32;
  *(volatile uint64_t *)&(is_flexibling) = v;
  /* clean is_flexibling and flip current_version in a single step */
  __sync_fetch_and_sub((volatile uint32_t *)&(is_setting), 1U);
  }
}

void HashTable::expandTable(){
    
}