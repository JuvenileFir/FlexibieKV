/*
 * == Abort ==
 */

#include "../dynamickv.h"
#include "../partition.h"
#include "../table.h"
#include "../xxhash.h"
#include "../zipf.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
// #include <signal.h>

#include <sched.h>
#include <pthread.h>
#include <thread>
#include <vector>

#define MAX_INSTANCES 1

typedef enum _concurrency_mode_t {
  CONCURRENCY_MODE_EREW = 0,
  CONCURRENCY_MODE_CREW,
  CONCURRENCY_MODE_CRCW,  // not supported yet
  CONCURRENCY_MODE_CRCWS,
} concurrency_mode_t;

Cbool basic_set(struct hash_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length,
                const uint8_t *value, size_t value_length, uint32_t expire_time, Cbool overwrite);

void test_basic() {
  const size_t num_items = 16 * 1048576;
  const size_t num_instances = 1;

  const size_t num_threads = 16;
  const size_t num_operations = 16 * 1048576;
  const size_t max_num_operatios_per_thread = num_operations;

  const size_t key_length = ROUNDUP8(8);
  const size_t value_length = ROUNDUP8(8);

  size_t alloc_overhead = sizeof(struct log_item);

  size_t owner_thread_id[MAX_INSTANCES];

  // bool concurrent_table_read = (concurrency_mode >= CONCURRENCY_MODE_CREW);
  // bool concurrent_table_write = (concurrency_mode >=
  // CONCURRENCY_MODE_CRCW); bool concurrent_alloc_write = (concurrency_mode
  // >= CONCURRENCY_MODE_CRCWS);
}

int main(int argc, char *argv[]) {
  const size_t page_size = 1048576 * 2;
  const size_t num_pages_to_try = 16384;

  shm_init(page_size, num_pages_to_try);

  test_basic();

  return EXIT_SUCCESS;
}

Cbool basic_set(struct hash_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length,
                const uint8_t *value, size_t value_length, uint32_t expire_time, Cbool overwrite) {
  assert(key_length <= MAX_KEY_LENGTH);
  assert(value_length <= MAX_VALUE_LENGTH);

  uint32_t partition_index = calc_partition_index(key_hash, table->is_flexibling);
  uint32_t bucket_index = key_hash & BUCKETS_PER_PARTITION;
  uint16_t tag = calc_tag(key_hash);

  struct page_bucket *parition = (struct page_bucket *)get_partiton_head(partition_index);
  struct page_bucket *located_bucket = &(parition[bucket_index]);

  Cbool overwriting;

  write_lock_bucket(table, located_bucket);

  size_t item_index = find_item_index(table, located_bucket, key_hash, tag, key, key_length);

  if (item_index != ITEMS_PER_BUCKET) {
    if (!overwrite) {
      TABLE_STAT_INC(table, set_nooverwrite);

      write_unlock_bucket(table, located_bucket);
      return false;  // already exist but cannot overwrite
    } else {
      overwriting = true;
    }
  } else {
#ifndef NO_EVICTION
    item_index = find_same_tag(table, located_bucket, tag);

    if (item_index == ITEMS_PER_BUCKET) {
      item_index = find_empty_or_oldest(table, located_bucket);
    }
#else
    item_index = find_empty(table, located_bucket);
    if (item_index == ITEMS_PER_BUCKET) {
      // no more space // TODO: add a statistics entry
      write_unlock_bucket(table, located_bucket);
      return false;
    }
#endif

    overwriting = false;
  }

  if (overwriting) {
    // use a random size to simulate item_size
    if (item->item_size >= new_item_size) {
      TABLE_STAT_INC(table, set_inplace);

      write_unlock_bucket(table, located_bucket);
      return true;
    }
  }

  TABLE_STAT_INC(table, set_success);

  if (TAG(located_bucket->item_vec[item_index])) {
    TABLE_STAT_INC(table, set_evicted);
    TABLE_STAT_DEC(table, count);
  }

  located_bucket->item_vec[item_index] = ITEM_VEC(tag, table->alloc.last_global_round, 0UL);

  write_unlock_bucket(table, located_bucket);

  TABLE_STAT_INC(table, count);
  return true;
}