#include "fkv.h"

EXTERN_BEGIN
/*
 * In this function, all sub-actions assume itself can succeed directly.
 * If it is not the case, these sub-actions will return false and this
 * function does other sub-actions to make these sub-actions meet there
 * requirements for success.
 * In other words, this function operates optimistically. If it fails,
 * then it operates in a pessimistically.
 * Does it make any sense about adopting an OCC style tech? It evicts first
 * and then set again assuming that there is not other thread will cut
 * operation in and influent its result.
 */
Cbool fkv_set(uint64_t batchid, hash_table* table, SlabStore* slab_store, uint64_t key_hash, uint8_t* key,
              uint32_t key_len, uint8_t* val, uint32_t val_len, uint32_t expire_time, Cbool overwrite) {
  Cbool ret;
  fkvStatus status;
  ret = orphan_chk(table, key_hash, key, key_len);
  if (ret) return false;  // Alreagdy exists.

  status = set(batchid, table, slab_store, key_hash, key, key_len, val, val_len, expire_time, overwrite);
  if (status == success_set) return true;

  // else if (status == failure_hashtable_full)
  //   return false;
  // else if (status == batch_full) {
  //   return false;
  // } else if (status == batch_too_samll) {
  //   return false;
  // }
  printf("error:%d\n",status);
  return false;
}

EXTERN_END
