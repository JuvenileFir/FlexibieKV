#include "table.h"

#include <chrono>

EXTERN_BEGIN

void hash_table_init(hash_table *table, DataStore *stores) {
  table->num_partitions = shm_available_numPages();//初始化时为240-0=240
  assert(table->num_partitions);
  table->is_flexibling = (Cbool)0;
  table->current_version = (Cbool)0;
  table->is_running = (Cbool)1;
  table->stop_entry_gc = (Cbool)0;
  table->is_setting = (Cbool)0;
  table->stores = stores;

  hash_init_partition(table->current_version, 8, table->num_partitions);
}

void print_table_stats(const hash_table *table UNUSED) {
#ifdef TABLE_COLLECT_STATS
  const SlabStore *store = &table->stores->slab_store[0];
  printf("count:                  %10zu\n", store->tstats.count);
  printf("set_nooverwrite:        %10zu | ", store->tstats.set_nooverwrite);
  printf("set_new:                %10zu | ", store->tstats.set_new);
  printf("set_fail:                %10zu\n", store->tstats.set_fail);
  printf("set_inplace:            %10zu | ", store->tstats.set_inplace);
  printf("set_evicted:            %10zu\n", store->tstats.set_evicted);
  printf("get_found:              %10zu | ", store->tstats.get_found);
  printf("get_notfound:           %10zu\n", store->tstats.get_notfound);
  printf("test_found:             %10zu | ", store->tstats.test_found);
  printf("test_notfound:          %10zu\n", store->tstats.test_notfound);
  printf("cleanup:                %10zu\n", store->tstats.cleanup);
  printf("move_to_head_performed: %10zu | ", store->tstats.move_to_head_performed);
  printf("move_to_head_skipped:   %10zu | ", store->tstats.move_to_head_skipped);
  printf("move_to_head_failed:    %10zu\n", store->tstats.move_to_head_failed);
#endif
}

void clean_stats(hash_table *table UNUSED) {
#ifdef TABLE_COLLECT_STATS
  for (uint32_t i = 0; i < NUM_THREAD; i++) {
    SlabStore *store = &table->stores->slab_store[i];
    // store->tstats.count = 0;
    store->tstats.set_nooverwrite = 0;
    store->tstats.set_new = 0;
    store->tstats.set_fail = 0;
    store->tstats.set_inplace = 0;
    store->tstats.set_evicted = 0;
    store->tstats.get_found = 0;
    store->tstats.get_notfound = 0;
    // store->tstats.test_found = 0;
    // store->tstats.test_notfound = 0;
    // store->tstats.cleanup = 0;
    // store->tstats.move_to_head_performed = 0;
    // store->tstats.move_to_head_skipped = 0;
    // store->tstats.move_to_head_failed = 0;
  }
#endif
}

// 16 is from TAG_MASK's log length in original MICA.
uint32_t calc_partition_index(uint64_t key_hash, Cbool probe_version) { return RoundHash(key_hash, probe_version); }

void *get_partition_head(uint32_t partition_index) { return partition_header(partition_index); }

fkvStatus set(int32_t batchid, struct hash_table *table, SlabStore *store, uint64_t key_hash, const uint8_t *key,
              size_t key_length, const uint8_t *value, size_t value_length, uint32_t expire_time, Cbool overwrite) {
  assert(key_length <= MAX_KEY_LENGTH);
  assert(value_length <= MAX_VALUE_LENGTH);
#ifdef EXP_LATENCY
  Cbool isTransitionPeriod = table->is_flexibling;
  auto start = std::chrono::steady_clock::now();
#endif

  // Cbool overwriting = false;
  /*
   * Waiting for transition period of fliping is_flexibling and blocking
   * is_flexibling fliping when set is operating.
   * The fliping and sets are organized in FIFO fashion.
   */

  uint16_t tag = calc_tag(key_hash);
  uint32_t partition_index = calc_partition_index(key_hash, table->is_flexibling ^ table->current_version);
  page_bucket *partition = (page_bucket *)get_partition_head(partition_index);

#ifdef _CUCKOO_
  /*
   * XXX: Temporarily, the first bucket's `unused1` of a partiton is used for
   * lock this partition. When we execute a `set` that needs to displace
   * slot(s) for an empty slot, the order of touched bucket(s) cannot be
   * ensured in ascending order. If the KV is executing a rebalance at the
   * same time, moving slot(s) by touching buckets in ascending order, there
   * is the possibility for losing slot(s). So locking the partition with FIFO
   * policy is the simplest way.
   */
  while (1) {
    uint8_t v = *(volatile uint8_t *)&partition->unused1 & ~((uint8_t)1);
    uint8_t new_v = v + (uint8_t)2;
    if (__sync_bool_compare_and_swap((volatile uint8_t *)&partition->unused1, v, new_v)) break;
  }

  twoBucket tb = cal_two_buckets(key_hash);
  tablePosition tp = cuckoo_insert(partition, key_hash, tag, tb, key, key_length);

  // memory_barrier();
  assert((*(volatile uint8_t *)&partition->unused1) > 1);
  __sync_fetch_and_sub((volatile uint8_t *)&(partition->unused1), (uint8_t)2);
#endif
  if (tp.cuckoostatus == failure_table_full) {
    // TODO: support eviction
#ifdef EXP_LATENCY
    auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
    if (isTransitionPeriod) {
      printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }
#else
    printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
    return failure_hashtable_full;
  }
  if (tp.cuckoostatus == failure_key_duplicated) {
    // TODO: support overwrite
    // overwriting = true;
#ifdef _CUCKOO_
    unlock_two_buckets(partition, tb);
#endif
#ifdef EXP_LATENCY
    auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
    if (isTransitionPeriod) {
      printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }
#else
    printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
    return failure_already_exist;
  }
  assert(tp.cuckoostatus == ok);
  struct page_bucket *located_bucket = &partition[tp.bucket];

  uint64_t new_item_size = (uint32_t)(sizeof(struct log_item) + ROUNDUP8(key_length) + ROUNDUP8(value_length));
  int64_t item_offset;

  item_offset = alloc_item(store, &new_item_size);
  if (item_offset == -1) {
    unlock_two_buckets(partition, tb);
#ifdef EXP_LATENCY
    auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
    if (isTransitionPeriod) {
      printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }
#else
    printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
    return batch_full;
  } else if (item_offset == -2) {
    unlock_two_buckets(partition, tb);
#ifdef EXP_LATENCY
    auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
    if (isTransitionPeriod) {
      printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
    }
#else
    printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
    return batch_too_samll;
  }
  uint32_t page_number = store->slabs[store->usingPage].pageNumber;

  log_item *new_item = (log_item *)log_item_locate(page_number, item_offset);
  TABLE_STAT_INC(store, set_new);
#ifdef STORE_COLLECT_STATS
  STORE_STAT_ADD(store, actual_used_mem, new_item_size);
#endif
  new_item->item_size = new_item_size;
  set_item(new_item, key_hash, key, (uint32_t)key_length, value, (uint32_t)value_length, expire_time);

  located_bucket->item_vec[tp.slot] = ITEM_VEC(tag, page_number, item_offset);

#ifdef _CUCKOO_
  unlock_two_buckets(partition, tb);
#endif
  TABLE_STAT_INC(store, count);

#ifdef EXP_LATENCY
  auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
  if (isTransitionPeriod) {
    printf("SET(succ): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
  }
#else
  printf("SET(succ): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
  return success_set;
}

Cbool get(hash_table *table, SlabStore *store, uint64_t key_hash, const uint8_t *key, size_t key_length,
          uint8_t *out_value, uint32_t *in_out_value_length, uint32_t *out_expire_time, Cbool readonly) {
  assert(key_length <= MAX_KEY_LENGTH);
  Cbool snapshot_is_flexibling = table->is_flexibling;
#ifdef EXP_LATENCY
  Cbool isTransitionPeriod = snapshot_is_flexibling;
  auto start = std::chrono::steady_clock::now();
#endif

  uint32_t partition_index = calc_partition_index(key_hash, (Cbool)0 ^ table->current_version);

  page_bucket *partition = (page_bucket *)get_partition_head(partition_index);
  tablePosition tp;
  while (1) {
#ifdef _CUCKOO_
    twoBucket tb = cal_two_buckets(key_hash);
    twoSnapshot ts1;
    while (1) {
      ts1 = read_two_buckets_begin(partition, tb);
      tp = cuckoo_find(partition, key_hash, tb, key, key_length);
      if (is_snapshots_same(ts1, read_two_buckets_end(partition, tb))) break;
    }
#endif
    if (tp.cuckoostatus == failure_key_not_found) {
      if (snapshot_is_flexibling) {
        snapshot_is_flexibling = (Cbool)0;
        partition_index = calc_partition_index(key_hash, (Cbool)1 ^ table->current_version);
        partition = (page_bucket *)get_partition_head(partition_index);
        continue;
      }
      TABLE_STAT_INC(store, get_notfound);
      TABLE_STAT_INC(store, set_fail);
#ifdef EXP_LATENCY
      auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
      if (isTransitionPeriod) {
        printf("GET(false): [time: %lu ns]\n",
               std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
      }
#else
      printf("GET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
      return false;
    }
    assert(tp.cuckoostatus == ok);

    // Cbool partial_value;
    uint32_t expire_time;
    page_bucket *located_bucket = &partition[tp.bucket];

    uint64_t item_vec = located_bucket->item_vec[tp.slot];
    uint64_t item_offset = ITEM_OFFSET(item_vec);

    log_item *item = (log_item *)log_item_locate(PAGE(item_vec), item_offset);

    expire_time = item->expire_time;

    size_t key_length = ITEMKEY_LENGTH(item->kv_length_vec);
    if (key_length > MAX_KEY_LENGTH) key_length = MAX_KEY_LENGTH;  // fix-up for possible garbage read

    size_t value_length = ITEMVALUE_LENGTH(item->kv_length_vec);
    if (value_length > MAX_VALUE_LENGTH) value_length = MAX_VALUE_LENGTH;  // fix-up for possible garbage read

    // adjust value length to use
    // *in_out_value_length = 8;
    // if (value_length > *in_out_value_length) {
    //   // partial_value = true;
    //   value_length = *in_out_value_length;
    // } else {
    //   // partial_value = false;
    //   // TODO: we can set this `false by defalut to emliminate this.
    // }
    memcpy8(out_value, item->data + ROUNDUP8(key_length), value_length);

    if (is_entry_expired(located_bucket->item_vec[tp.slot])) {
      if (!is_snapshots_same(ts1, read_two_buckets_end(partition, tb))) continue;

      TABLE_STAT_INC(store, get_notfound);
#ifdef EXP_LATENCY
      auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
      if (isTransitionPeriod) {
        printf("GET(false): [time: %lu ns]\n",
               std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
      }
#else
      printf("GET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
      return false;
    }

    if (!is_snapshots_same(ts1, read_two_buckets_end(partition, tb))) continue;

    *in_out_value_length = value_length;
    if (out_expire_time != NULL) *out_expire_time = expire_time;

    TABLE_STAT_INC(store, get_found);
    break;
  }
#ifdef EXP_LATENCY
  auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
  if (isTransitionPeriod) {
    printf("GET(succ): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
  }
#else
  printf("GET(succ): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
  return true;
}

/* ************************* Debug Functions ************************************ */

Cbool orphan_chk(hash_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length) {
  // Cbool snapshot_is_flexibling = table->is_flexibling;
  uint64_t snapshot_meta = *(uint64_t *)&table->is_flexibling;
  Cbool snapshot_is_flexibling = ((uint32_t *)&snapshot_meta)[0];
  Cbool snapshot_current_version = ((uint32_t *)&snapshot_meta)[1];

  uint32_t partition_index = calc_partition_index(key_hash, (Cbool)0 ^ snapshot_current_version);

  page_bucket *partition = (page_bucket *)get_partition_head(partition_index);
  tablePosition tp;
  while (1) {
#ifdef _CUCKOO_
    twoBucket tb = cal_two_buckets(key_hash);
    twoSnapshot ts1;
    while (1) {
      ts1 = read_two_buckets_begin(partition, tb);
      tp = cuckoo_find(partition, key_hash, tb, key, key_length);
      if (is_snapshots_same(ts1, read_two_buckets_end(partition, tb))) break;
    }
#endif
    if (tp.cuckoostatus == failure_key_not_found) {
      if (snapshot_is_flexibling) {
        snapshot_is_flexibling = (Cbool)0;
        partition_index = calc_partition_index(key_hash, (Cbool)1 ^ snapshot_current_version);
        partition = (page_bucket *)get_partition_head(partition_index);
        continue;
      }
      return false;
    }
    assert(tp.cuckoostatus == ok);

    if (!is_snapshots_same(ts1, read_two_buckets_end(partition, tb))) continue;
    return false;
  }
  return true;
}

Cbool find_an_offset(const hash_table *table, size_t entry) {
  for (size_t partition_index = 0; partition_index < table->num_partitions; partition_index++) {
    struct page_bucket *partition = (struct page_bucket *)get_partition_head(partition_index);
    for (size_t bucket_index = 0; bucket_index <= BUCKETS_PER_PARTITION; bucket_index++) {
      for (size_t entry_index = 0; entry_index < ITEMS_PER_BUCKET; entry_index++) {
        if (ITEM_OFFSET(entry) == ITEM_OFFSET(partition[bucket_index].item_vec[entry_index])) {
          printf("partition: %zu\t bucket: %zu\t index: %zu\t entry: %zu\n", partition_index, bucket_index, entry_index,
                 partition[bucket_index].item_vec[entry_index]);
          return true;
        }
      }
    }
  }
  return false;
}

void redistribute_last_short_group(hash_table *table, size_t *parts, size_t count) {
  uint64_t bucket_index;
  uint32_t entry_index;
  size_t target_p, target_e;
  int32_t k = count - 2;
  struct page_bucket *partition;
  struct page_bucket *workp;
  struct page_bucket *workb;
  // uint8_t key[MAX_KEY_LENGTH];
  struct log_item *item;
  struct twoBucket tb;
  while (k >= 0) {
    partition = (page_bucket *)partition_header(parts[k]);
    /* Go through the entire partition blindly */
    __sync_fetch_and_add((volatile uint8_t *)&(partition->unused1), (uint8_t)1);
    while (1) {
      if (*(volatile uint8_t *)&partition->unused1 == (uint8_t)1) break;
    }
    for (bucket_index = 0; bucket_index <= BUCKETS_PER_PARTITION; bucket_index++) {
      write_lock_bucket(&partition[bucket_index]);
      for (entry_index = 0; entry_index < ITEMS_PER_BUCKET; entry_index++) {
        if (!is_entry_expired(partition[bucket_index].item_vec[entry_index])) {
          item = log_item_locate(PAGE(partition[bucket_index].item_vec[entry_index]),
                                 ITEM_OFFSET(partition[bucket_index].item_vec[entry_index]));

          if ((target_p = calc_partition_index(item->key_hash, (Cbool)1 ^ table->current_version)) != parts[k]) {
            workp = (page_bucket *)partition_header(target_p);
            tb = cal_two_buckets(item->key_hash);
            tablePosition tp = cuckoo_insert(workp, item->key_hash, TAG(partition[bucket_index].item_vec[entry_index]),
                                             tb, item->data, ITEMKEY_LENGTH(item->kv_length_vec));
            if (tp.cuckoostatus == failure_table_full) {
              // TODO: support overwrite
              // assert(false);
              item->expire_time = EXPIRED;
              partition[bucket_index].item_vec[entry_index] = (uint64_t)0;
              continue;
            }
            if (tp.cuckoostatus == failure_key_duplicated) {
              // TODO: support overwrite
              // assert(false);
              item->expire_time = EXPIRED;
              partition[bucket_index].item_vec[entry_index] = (uint64_t)0;
              unlock_two_buckets(workp, tb);
              continue;
            }
            assert(tp.cuckoostatus == ok);

            workb = &(workp[tp.bucket]);
            workb->item_vec[tp.slot] = partition[bucket_index].item_vec[entry_index];
            unlock_two_buckets(workp, tb);

            /* ToThink:
             * For H2L, do not clean origanl hash table entry is OK.
             * During resizing, if we check both cv(current_version)
             * and !cv, Keeping orignal entry may shorten porbe
             * length.
             */
            partition[bucket_index].item_vec[entry_index] = (uint64_t)0;
          }
        }
      }
      write_unlock_bucket(&partition[bucket_index]);
    }
    // memory_barrier();
    assert((*(volatile uint8_t *)&partition->unused1 & (uint8_t)1) == (uint8_t)1);
    __sync_fetch_and_sub((volatile uint8_t *)&(partition->unused1), (uint8_t)1);
    k--;
  }
}

void redistribute_first_long_group(hash_table *table, size_t *parts, size_t count) {
  int bucket_index, entry_index;
  size_t target_p, target_e;
  size_t k = 0;
  struct page_bucket *partition;
  struct page_bucket *workp;
  struct page_bucket *workb;
  uint8_t key[MAX_KEY_LENGTH];
  struct log_item *item;
  struct twoBucket tb;
  while (k < count) {
    partition = (page_bucket *)partition_header(parts[k]);
    // Go through the entire partition blindly
    __sync_fetch_and_add((volatile uint8_t *)&(partition->unused1), (uint8_t)1);
    while (1) {
      if (*(volatile uint8_t *)&partition->unused1 == (uint8_t)1) break;
    }
    for (bucket_index = 0; bucket_index <= BUCKETS_PER_PARTITION; bucket_index++) {
      write_lock_bucket(&partition[bucket_index]);
      for (entry_index = 0; entry_index < ITEMS_PER_BUCKET; entry_index++) {
        if (!is_entry_expired(partition[bucket_index].item_vec[entry_index])) {
          item = log_item_locate(PAGE(partition[bucket_index].item_vec[entry_index]),
                                 ITEM_OFFSET(partition[bucket_index].item_vec[entry_index]));

          if ((target_p = calc_partition_index(item->key_hash, (Cbool)1 ^ table->current_version)) != parts[k]) {
            workp = (struct page_bucket *)partition_header(target_p);
            tb = cal_two_buckets(item->key_hash);
            tablePosition tp = cuckoo_insert(workp, item->key_hash, TAG(partition[bucket_index].item_vec[entry_index]),
                                             tb, item->data, ITEMKEY_LENGTH(item->kv_length_vec));
            if (tp.cuckoostatus == failure_table_full) {
              // TODO: support overwrite
              item->expire_time = EXPIRED;
              partition[bucket_index].item_vec[entry_index] = (uint64_t)0;
              continue;
            }
            if (tp.cuckoostatus == failure_key_duplicated) {
              // TODO: support overwrite
              item->expire_time = EXPIRED;
              partition[bucket_index].item_vec[entry_index] = (uint64_t)0;
              unlock_two_buckets(workp, tb);
              continue;
            }
            assert(tp.cuckoostatus == ok);

            workb = &workp[tp.bucket];
            workb->item_vec[tp.slot] = partition[bucket_index].item_vec[entry_index];
            unlock_two_buckets(workp, tb);

            partition[bucket_index].item_vec[entry_index] = (uint64_t)0;
          }
        }
      }
      write_unlock_bucket(&partition[bucket_index]);
    }
    // memory_barrier();
    assert((*(volatile uint8_t *)&partition->unused1 & (uint8_t)1) == (uint8_t)1);
    __sync_fetch_and_sub((volatile uint8_t *)&(partition->unused1), (uint8_t)1);
    k++;
  }
}

EXTERN_END
