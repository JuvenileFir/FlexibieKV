#pragma once //确保同一个文件不被include多次

#include "table.h"

EXTERN_BEGIN

Cbool fkv_set(uint64_t batchid, hash_table* table, SlabStore* slab_store, uint64_t key_hash, uint8_t* key,
              uint32_t key_len, uint8_t* val, uint32_t val_len, uint32_t expire_time, Cbool overwrite);

EXTERN_END
