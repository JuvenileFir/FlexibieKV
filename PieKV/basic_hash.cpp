#include "basic_hash.h"

extern MemPool *kMemPool;

uint16_t calc_tag(uint64_t key_hash) {
  // uint16_t tag = (uint16_t)(key_hash & TAG_MASK);
  uint16_t tag = (uint16_t)((key_hash >> 16) & TAG_MASK);
  if (tag == 0)
    return 1;
  else
    return tag;
}


uint32_t calc_segment_id(uint16_t tag) {
  return tag % THREAD_NUM;
}

uint32_t read_version_begin(const Bucket *bucket UNUSED) {
#ifdef TABLE_CONCURRENT
  while (true) {
    uint32_t v = *(volatile uint32_t *)&bucket->version;
    memory_barrier();
    if ((v & 1U) != 0U) continue;
    return v;
  }
#else
  return 0U;
#endif
}

uint32_t read_version_end(const Bucket *bucket UNUSED) {
#ifdef TABLE_CONCURRENT
  memory_barrier();
  uint32_t v = *(volatile uint32_t *)&bucket->version;
  return v;
#else
  return 0U;
#endif
}


void write_unlock_bucket(Bucket *bucket UNUSED) {
  memory_barrier();
  assert((*(volatile uint32_t *)&bucket->version & 1U) == 1U);
  // // No need to use atomic add because this thread is the only one writing to version
  // (*(volatile uint32_t *)&bucket->version)++;
	// assert(__sync_bool_compare_and_swap((volatile uint32_t *)&bucket->version, 0U, 1U));
  __sync_fetch_and_add((volatile uint32_t *)&bucket->version, 1U);
}

// inline
Cbool is_entry_expired(uint64_t index_entry) { return !index_entry; }

Cbool key_eq(const uint8_t *key1, size_t key1_len, const uint8_t *key2, size_t key2_len) {
  return key1_len == key2_len && memcmp8(key1, key2, key1_len);
}

Cbool val_eq(const uint8_t *val1, size_t val1_len, const uint8_t *val2, size_t val2_len) {
  return val1_len == val2_len && memcmp8(val1, val2, val1_len);
}

uint16_t try_read_from_bucket(const Bucket *bucket, const uint16_t tag, const uint8_t *key, uint32_t keylength) {
  uint32_t slot;
  for (slot = 0; slot < ITEMS_PER_BUCKET; slot++) {
    if (TAG(bucket->item_vec[slot]) != tag) continue;

    // we may read garbage values, which do not cause any fatal issue
    LogItem *item = (LogItem *)kMemPool->locate_item(PAGE(bucket->item_vec[slot]), ITEM_OFFSET(bucket->item_vec[slot]));
    // a key comparison reads up to min(source key length and destination key length), which is always safe to do
    if (!key_eq(item->data, ITEMKEY_LENGTH(item->kv_length_vec), key, keylength)) continue;

    return slot;
  }
  return ITEMS_PER_BUCKET;
}

uint16_t try_find_slot(const Bucket *bucket, const uint16_t tag, const uint64_t offset) {
  uint32_t slot;
  for (slot = 0; slot < ITEMS_PER_BUCKET; slot++) {
    if (ITEM_OFFSET(bucket->item_vec[slot]) != offset || TAG(bucket->item_vec[slot]) != tag) continue;
    return slot;
  }
  return ITEMS_PER_BUCKET;
}

/*
 * try_find_insert_bucket will search the bucket for the given key, and for
 * an empty slot. If the key is found, we store the slot of the key in
 * `slot` and return false. If we find an empty slot, we store its position
 * in `slot` and return true. If no duplicate key is found and no empty slot
 * is found, we store `ITEMS_PER_BUCKET` in `slot` and return true.
 */
Cbool try_find_insert_bucket(Bucket *bucket_, uint32_t *slot,
                             const uint16_t tag, const uint8_t *key,
                             uint32_t keylength) {
  uint32_t i;
  *slot = ITEMS_PER_BUCKET;
  for (i = 0; i < ITEMS_PER_BUCKET; ++i) {
    if (!bucket_->item_vec[i]) {
      *slot = i;
    } else {
      if (TAG(bucket_->item_vec[i]) != tag) continue;
      LogItem *item = (LogItem *)kMemPool->locate_item(PAGE(bucket_->item_vec[i]),
                                                       ITEM_OFFSET(bucket_->item_vec[i]));
      if (key_eq(item->data, ITEMKEY_LENGTH(item->kv_length_vec), key, keylength)) {
        *slot = i;
        // printf("%ld in hash and %ld come in\n",(uint64_t)(*(uint64_t *)item->data),(uint64_t)(*(uint64_t *)key));
        return false;
      }
    }
  }
  return true;
}

Cbool try_find_insert_bucket(Bucket *bucket_, uint32_t *slot,
                             const uint16_t tag, const uint8_t *key,
                             uint32_t keylength, uint64_t *rounds) {
  uint32_t i;
  *slot = ITEMS_PER_BUCKET;
  for (i = 0; i < ITEMS_PER_BUCKET; ++i) {
    if (!bucket_->item_vec[i]) {
      *slot = i;
    } else {
      uint16_t entry_tag = TAG(bucket_->item_vec[i]);
      if (entry_tag != tag) continue;
      LogItem *item = (LogItem *)kMemPool->locate_item(PAGE(bucket_->item_vec[i]),
                                                       ITEM_OFFSET(bucket_->item_vec[i]));
      // TODO: add an option here to choose if cleanup or not
      uint32_t segmentId = calc_segment_id(entry_tag);
      if (ROUND(bucket_->item_vec[i]) + 1 < rounds[segmentId]) {
        // cleanup this entry for its outdated round
        bucket_->item_vec[i] = 0;
      }
      if (key_eq(item->data, ITEMKEY_LENGTH(item->kv_length_vec), key, keylength)) {
        *slot = i;
        // printf("%ld in hash and %ld come in\n",(uint64_t)(*(uint64_t *)item->data),(uint64_t)(*(uint64_t *)key));
        return false;
      }
    }
  }
  return true;
}

