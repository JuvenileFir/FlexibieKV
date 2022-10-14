#include <cassert>
#include "hashtable.hpp"
#include "basic_hash.h"

HashTable::HashTable(MemPool* mempool) {
	is_setting_ = 0;
	is_flexibling_ = 0;
	current_version_ = 0;
	table_block_num_ = mempool->getAvailableNum();
	assert(table_block_num_);
	round_hash_ = RoundHash(table_block_num_);

	for (uint32_t i = 0; i < table_block_num_; i++) {
		int32_t alloc_id = mempool->allocBlock();
		assert(alloc_id + 1); 
		table_blocks_[i]->block_id = (uint32_t)alloc_id;
		table_blocks_[i]->block_ptr = mempool->get_block_ptr(alloc_id);
		mempool->cleanBlock(alloc_id);
	}
}

HashTable::~HashTable() {
  /* ...... */
}

void *HashTable::get_block_ptr(uint32_t tableIndex) {
	return table_blocks_[tableIndex]->block_ptr;
}

uint32_t HashTable::get_block_id(uint32_t tableIndex) {
	return table_blocks_[tableIndex]->block_id;
}

void HashTable::AddBlock(uint8_t *pheader, uint32_t block_id) {
	table_blocks_[table_block_num_]->block_ptr = pheader;
	table_blocks_[table_block_num_]->block_id = block_id;
	table_block_num_++;
}

void HashTable::RemoveBlock() {
  table_blocks_[table_block_num_-1]->block_ptr = NULL;
  table_blocks_[table_block_num_-1]->block_id = -1;
  table_block_num_--;
}

void HashTable::ShrinkTable(TableBlock **tableblocksToMove, size_t blocknum_to_move) {
	size_t count;
	size_t parts[round_hash_.S_ << 1];
	/* once in a cycle*/
  for (int i = 0; i < blocknum_to_move; i++) {
		round_hash_.get_parts_to_remove(parts, &count);// get all parts to move
		round_hash_.DelBucket();

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 1U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);

		this->redistribute_last_short_group(parts, count);
    memset(parts, 0, sizeof(size_t) * (round_hash_.S_ << 1));
    tableblocksToMove[i]->block_id = table_blocks_[table_block_num_]->block_id;
    tableblocksToMove[i]->block_ptr = table_blocks_[table_block_num_]->block_ptr;
    table_block_num_ -= 1;

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 0U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);
	}
}

void HashTable::ExpandTable(TableBlock **tableblocksToMove, size_t blocknum_to_move) {
	size_t count;
	size_t parts[round_hash_.S_ << 1];
	/* once in a cycle*/
  for (int i = 0; i < blocknum_to_move; i++) {
    AddBlock(tableblocksToMove[i]->block_ptr,tableblocksToMove[i]->block_id);
		round_hash_.get_parts_to_add(parts, &count);// get all parts to move
		round_hash_.NewBucket();

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 1U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);

		this->redistribute_first_long_group(parts, count);
    memset(parts, 0, sizeof(size_t) * (round_hash_.S_ << 1));
		table_block_num_++;

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 0U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);
	}  
}



void HashTable::set_table(uint64_t key_hash, const uint8_t *key, size_t key_length){
  /*
   * XXX: Temporarily, the first bucket's `unused1` of a partiton is used for
   * lock this partition. When we execute a `set` that needs to displace
   * slot(s) for an empty slot, the order of touched bucket(s) cannot be
   * ensured in ascending order. If the KV is executing a rebalance at the
   * same time, moving slot(s) by touching buckets in ascending order, there
   * is the possibility for losing slot(s). So locking the bucket with FIFO
   * policy is the simplest way.
   */
  uint16_t tag = calc_tag(key_hash);
  uint32_t block_index = round_hash_.HashToBucket(key_hash);
  Bucket *bucket = (Bucket *)this->get_block_ptr(block_index);

  while (1) {
    uint8_t v = (*(volatile uint8_t *)&bucket->lock) & ~((uint8_t)1);//8bit向下取偶数
    uint8_t new_v = v + (uint8_t)2;
    if (__sync_bool_compare_and_swap((volatile uint8_t *)&bucket->lock, v, new_v))
      break;
  }

  twoBucket tb = cal_two_buckets(key_hash);
  tablePosition tp = cuckoo_insert(bucket, key_hash, tag, tb, key, key_length);

  // memory_barrier();
  assert((*(volatile uint8_t *)&bucket->lock) > 1);
  __sync_fetch_and_sub((volatile uint8_t *)&bucket->lock, (uint8_t)2);

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
    TABLE_STAT_INC(store, set_fail);
    return failure_hashtable_full;
  }
  if (tp.cuckoostatus == failure_key_duplicated) {
    // TODO: support overwrite
    // overwriting = true;
    unlock_two_buckets(bucket, tb);
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
    TABLE_STAT_INC(store, set_fail);
    return failure_already_exist;
    assert(tp.cuckoostatus == ok); 
    Bucket *located_bucket = &partition[tp.bucket];

  }
 
}


//to modify
void HashTable::redistribute_last_short_group(size_t *parts, size_t count) {
	uint64_t bucket_index;
  uint32_t entry_index;
  size_t target_p, target_e;
  int32_t k = count - 2;
  Bucket *bucket;
  Bucket *workp;
  Bucket *workb;
  // uint8_t key[MAX_KEY_LENGTH];
  struct LogItem *item;
  struct twoBucket tb;
  while (k >= 0) {
    bucket = (Bucket *)(this->get_block_ptr(parts[k]));
    /* Go through the entire bucket blindly */
    while (1) {
			if (__sync_bool_compare_and_swap((volatile uint8_t *)&(bucket->lock),
																			 (uint8_t)0, (uint8_t)1))
				break;
		}
    for (bucket_index = 0; bucket_index <= BUCKETS_PER_PARTITION; bucket_index++) {//要调整的group里面的block
      write_lock_bucket(&bucket[bucket_index]);//此bucket指block
      for (entry_index = 0; entry_index < ITEMS_PER_BUCKET; entry_index++) {//block里面的bucket
        if (!is_entry_expired(bucket[bucket_index].item_vec[entry_index])) {
          item = log_item_locate(PAGE(bucket[bucket_index].item_vec[entry_index]),
                                 ITEM_OFFSET(bucket[bucket_index].item_vec[entry_index]));

          if ((target_p = calc_bucket_index(item->key_hash, (Cbool)1 ^ table->current_version)) != parts[k]) {
            workp = (Bucket *)get_block_ptr(target_p);
            tb = cal_two_buckets(item->key_hash);
            tablePosition tp = cuckoo_insert(workp, item->key_hash, TAG(bucket[bucket_index].item_vec[entry_index]),
                                             tb, item->data, ITEMKEY_LENGTH(item->kv_length_vec));
            if (tp.cuckoostatus == failure_table_full) {
              // TODO: support overwrite
              // assert(false);
              item->expire_time = EXPIRED;
              bucket[bucket_index].item_vec[entry_index] = (uint64_t)0;
              continue;
            }
            if (tp.cuckoostatus == failure_key_duplicated) {
              // TODO: support overwrite
              // assert(false);
              item->expire_time = EXPIRED;
              bucket[bucket_index].item_vec[entry_index] = (uint64_t)0;
              unlock_two_buckets(workp, tb);
              continue;
            }
            assert(tp.cuckoostatus == ok);

            workb = &(workp[tp.bucket]);
            workb->item_vec[tp.slot] = bucket[bucket_index].item_vec[entry_index];
            unlock_two_buckets(workp, tb);

            /* ToThink:
             * For H2L, do not clean origanl hash table entry is OK.
             * During resizing, if we check both cv(current_version)
             * and !cv, Keeping orignal entry may shorten porbe
             * length.
             */
            bucket[bucket_index].item_vec[entry_index] = (uint64_t)0;
          }
        }
      }
      write_unlock_bucket(&bucket[bucket_index]);
    }
    // memory_barrier();
    assert((*(volatile uint8_t *)&bucket->lock & (uint8_t)1) == (uint8_t)1);
    __sync_fetch_and_sub((volatile uint8_t *)&(bucket->lock), (uint8_t)1);
    k--;
  }
}
//to modify
void HashTable::redistribute_first_long_group(size_t *parts, size_t count) {
	int bucket_index, entry_index;
  size_t target_p, target_e;
  size_t k = 0;
  Bucket *bucket;
  Bucket *workp;
  Bucket *workb;
  uint8_t key[MAX_KEY_LENGTH];
  struct LogItem *item;
  struct twoBucket tb;
  while (k < count) {
    bucket = (Bucket *)(this->get_block_ptr(parts[k]));
    // Go through the entire bucket blindly
		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint8_t *)&(bucket->lock),
																			 (uint8_t)0, (uint8_t)1))
				break;
		}
    for (bucket_index = 0; bucket_index <= BUCKETS_PER_PARTITION; bucket_index++) {
      write_lock_bucket(&bucket[bucket_index]);
      for (entry_index = 0; entry_index < ITEMS_PER_BUCKET; entry_index++) {
        if (!is_entry_expired(bucket[bucket_index].item_vec[entry_index])) {
          item = log_item_locate(PAGE(bucket[bucket_index].item_vec[entry_index]),
                                 ITEM_OFFSET(bucket[bucket_index].item_vec[entry_index]));

          if ((target_p = calc_bucket_index(item->key_hash, (Cbool)1 ^ table->current_version)) != parts[k]) {
            workp = (struct Bucket *)get_block_ptr(target_p);
            tb = cal_two_buckets(item->key_hash);
            tablePosition tp = cuckoo_insert(workp, item->key_hash, TAG(bucket[bucket_index].item_vec[entry_index]),
                                             tb, item->data, ITEMKEY_LENGTH(item->kv_length_vec));
            if (tp.cuckoostatus == failure_table_full) {
              // TODO: support overwrite
              item->expire_time = EXPIRED;
              bucket[bucket_index].item_vec[entry_index] = (uint64_t)0;
              continue;
            }
            if (tp.cuckoostatus == failure_key_duplicated) {
              // TODO: support overwrite
              item->expire_time = EXPIRED;
              bucket[bucket_index].item_vec[entry_index] = (uint64_t)0;
              unlock_two_buckets(workp, tb);
              continue;
            }
            assert(tp.cuckoostatus == ok);

            workb = &workp[tp.bucket];
            workb->item_vec[tp.slot] = bucket[bucket_index].item_vec[entry_index];
            unlock_two_buckets(workp, tb);

            bucket[bucket_index].item_vec[entry_index] = (uint64_t)0;
          }
        }
      }
      write_unlock_bucket(&bucket[bucket_index]);
    }
    // memory_barrier();
    assert((*(volatile uint8_t *)&bucket->lock & (uint8_t)1) == (uint8_t)1);
    __sync_fetch_and_sub((volatile uint8_t *)&(bucket->lock), (uint8_t)1);
    k++;
  }
}