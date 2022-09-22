#ifndef HASHTABLE_HPP_
#define HASHTABLE_HPP_


#include <cstdint>    // temp , remove it if you want to define your own uint32_t
#include <cstdlib>

#include "roundhash.hpp"
#include "mempool.hpp"

typedef struct Bucket
{
    uint32_t version;
    uint8_t occupt_bitmap;
    uint8_t lock;
    uint16_t padding;
    uint64_t item_vec[7];
} Bucket;    // ALIGNED(64), use alignas(64) when imply this struct


typedef struct TableBlock
{
    void *blockPtr;
    uint32_t blockId;
} TableBlock;



class HashTable
{
private:
    /* hash table data */
    TableBlock tableBlocks[MAX_BLOCK_NUM - 1];
public:
    HashTable(/* args */);
    ~HashTable();
    temp getBlockPtr();
    temp getBlockId();
    temp addBlock();
    temp minusBlock();
};

HashTable::HashTable(/* args */)
{
}

HashTable::~HashTable()
{
}

















#endif