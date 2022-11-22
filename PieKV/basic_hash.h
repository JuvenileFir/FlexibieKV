#pragma once

#include <stdint.h>

#include "mempool.hpp"
#include "util.h"

#define TAG_MASK (((uint64_t)1 << 8) - 1)
#define TAG(item_vec) ((item_vec) >> 56)
#define ROUND_MASK (0x00ff000000000000UL)
#define ROUND(item_vec) ((item_vec & ROUND_MASK) >> 48)
#define PAGE_MASK (0x0000fffff8000000UL)
#define PAGE(item_vec) ((item_vec & PAGE_MASK) >> 27)
#define SET_PAGE(entry, pageNumber) (((uint64_t)entry) | ((uint64_t)pageNumber) << 27)

#define ITEM_OFFSET_MASK (((uint64_t)1 << 27) - 1)
#define ITEM_OFFSET(item_vec) ((item_vec)&ITEM_OFFSET_MASK)

#define ITEM_VEC(tag, round, pageNumber, item_offset) \
  (((uint64_t)(tag) << 56) | ((uint64_t)(round) << 48) | ((uint64_t)(pageNumber) << 27) | (uint64_t)(item_offset))


typedef enum cuckooStatus {
  ok,
  failure,
  failure_key_not_found,
  failure_key_duplicated,
  failure_table_full,
  failure_under_expansion,
  overwrite,
} cuckooStatus;

typedef enum ITEM_RESULT {
  ITEM_OK = 0,
  ITEM_ERROR,
  ITEM_FULL,
  ITEM_EXIST,
  ITEM_NOT_FOUND,
  ITEM_PARTIAL_VALUE,
  ITEM_NOT_PROCESSED,
} ITEM_RESULT;

typedef struct tablePosition {
  uint32_t bucket;
  uint32_t slot;
  enum cuckooStatus cuckoostatus;
} tablePosition;

/* typedef struct page_bucket {
  uint32_t version;  // XXX: Is uint32_t wide enough?
#ifdef ITEMS_PER_BUCKET_7
#define bitmap uint8_t
  uint8_t occupy_bitmap;
  uint8_t unused1;//用于加锁
  uint16_t unused2;//用于补位
#elif defined ITEMS_PER_BUCKET_15
#define bitmap uint16_t
  uint16_t occupy_bitmap;
  uint16_t unused1;
#else
  uint32_t unused;  // TODO: bitmap for early pruning.
#endif
  uint64_t item_vec[ITEMS_PER_BUCKET];

  // 64: key hash
  // 16: tag (1-base)
  // 48: item offset
  // item == 0: empty item
} page_bucket ALIGNED(64); */

typedef struct Bucket
{
    uint32_t version;
    uint8_t occupy_bitmap;
    uint8_t lock;  // TODO: change to padding
    uint16_t padding;
    uint64_t item_vec[7];
} Bucket;     //  ALIGNED(64)

uint16_t calc_tag(uint64_t key_hash);

uint32_t read_version_begin(const Bucket *bucket UNUSED);

uint32_t read_version_end(const Bucket *bucket UNUSED);

inline void write_lock_bucket(Bucket *bucket UNUSED) {
  while (1) {
    uint32_t v = *(volatile uint32_t *)&bucket->version & ~1U;
    uint32_t new_v = v | 1U;
    if (__sync_bool_compare_and_swap((volatile uint32_t *)&bucket->version, v, new_v)) break;
  }
}

void write_unlock_bucket(Bucket *bucket UNUSED);

Cbool is_entry_expired(uint64_t index_entry);

Cbool key_eq(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len);

Cbool val_eq(const uint8_t *val1, size_t val1_len, const uint8_t *val2, size_t val2_len);

uint16_t try_read_from_bucket(const Bucket *bucket, const uint16_t tag, const uint8_t *key, uint32_t keylength);

uint16_t try_find_slot(const Bucket *bucket, const uint16_t tag, const uint64_t offset);

Cbool try_find_insert_bucket(Bucket *bucket_, uint32_t *slot, const uint16_t tag, const uint8_t *key,
                             uint32_t keylength, uint64_t *rounds);
                             
Cbool try_find_insert_bucket(Bucket *bucket_, uint32_t *slot, const uint16_t tag, const uint8_t *key,
                             uint32_t keylength);


uint32_t calc_segment_id(uint16_t tag);
