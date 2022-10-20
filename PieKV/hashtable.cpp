#include <cassert>
#include <cstdint> 
#include <cstdio>
#include "hashtable.hpp"
#include "basic_hash.h"

HashTable::HashTable(MemPool* mempool) {
	is_setting_ = 0;
	is_flexibling_ = 0;
	current_version_ = 0;
  mempool_ = mempool;
	table_block_num_ = mempool->get_block_available_num();

	assert(table_block_num_);
	round_hash_ = new RoundHash(table_block_num_, 8);
  round_hash_new_ = new RoundHash(table_block_num_, 8);

  for (int i = 0; i < MAX_BLOCK_NUM; i++) {
        // TODO: use max here for temp, create an init block function later
        table_blocks_[i] = new TableBlock;
    }
	for (uint32_t i = 0; i < table_block_num_; i++) {
		int32_t alloc_id = mempool->alloc_block();
		assert(alloc_id + 1); 
		table_blocks_[i]->block_id = (uint32_t)alloc_id;
		table_blocks_[i]->block_ptr = mempool->get_block_ptr(alloc_id);
		mempool->memset_block(alloc_id);
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
	size_t parts[round_hash_->S_ << 1];
	/* once in a cycle*/
  for (int i = 0; i < blocknum_to_move; i++) {
		round_hash_->get_parts_to_remove(parts, &count);// get all parts to move
		// round_hash_->DelBucket();
    remap_new_groups();

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 1U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);

		this->redistribute_last_short_group(parts, count);
    memset(parts, 0, sizeof(size_t) * (round_hash_->S_ << 1));
    tableblocksToMove[i]->block_id = table_blocks_[table_block_num_]->block_id;
    tableblocksToMove[i]->block_ptr = table_blocks_[table_block_num_]->block_ptr;
    table_block_num_ -= 1;

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 0U;
    swap_group_maps();
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);
	}
}

void HashTable::ExpandTable(TableBlock **tableblocksToMove, size_t blocknum_to_move) {
	size_t count;
	size_t parts[round_hash_->S_ << 1];
	/* once in a cycle*/
  for (int i = 0; i < blocknum_to_move; i++) {
    AddBlock((uint8_t *)tableblocksToMove[i]->block_ptr,tableblocksToMove[i]->block_id);
		round_hash_->get_parts_to_add(parts, &count);// get all parts to move
		round_hash_->NewBucket();

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 1U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);

		this->redistribute_first_long_group(parts, count);
    memset(parts, 0, sizeof(size_t) * (round_hash_->S_ << 1));
		table_block_num_++;

		while (1) {
			if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_setting_), 0U, 1U))
				break;
		}
		*(volatile uint32_t *)&(is_flexibling_) = 0U;
		__sync_fetch_and_sub((volatile uint32_t *)&(is_setting_), 1U);
	}  
}

int64_t HashTable::get_table(twoSnapshot *ts1, twoBucket *tb, Bucket *bucket, uint64_t key_hash, const uint8_t *key, size_t key_length){
  Cbool snapshot_is_flexibling = is_flexibling_;
  tablePosition tp;
  *tb = cal_two_buckets(key_hash);

  while (1) {
    *ts1 = read_two_buckets_begin(bucket, *tb);
    tp = cuckoo_find(bucket, key_hash, *tb, key, key_length);
    if (is_snapshots_same(*ts1, read_two_buckets_end(bucket, *tb))) break;
  }
  if (tp.cuckoostatus == failure_key_not_found) {
    if (snapshot_is_flexibling) {
      snapshot_is_flexibling = (Cbool)0;
      uint32_t block_index = round_hash_new_->HashToBucket(key_hash);
      Bucket *bucket = (Bucket *)this->get_block_ptr(block_index);
      return -2;
    }
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
    return -1;
  }
  assert(tp.cuckoostatus == ok);

  // Cbool partial_value;
  Bucket *located_bucket = &bucket[tp.bucket];
  return located_bucket->item_vec[tp.slot];
}

int64_t HashTable::set_table(tablePosition *tp, twoBucket *tb, uint64_t key_hash, const uint8_t *key, size_t key_length){
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
  uint32_t block_index = round_hash_->HashToBucket(key_hash);
  Bucket *bucket = (Bucket *)this->get_block_ptr(block_index);

  while (1) {
    uint8_t v = (*(volatile uint8_t *)&bucket->lock) & ~((uint8_t)1);//8bit向下取偶数
    uint8_t new_v = v + (uint8_t)2;
    if (__sync_bool_compare_and_swap((volatile uint8_t *)&bucket->lock, v, new_v))
      break;
  }

  tablePosition tps = cuckoo_insert(bucket, key_hash, tag, *tb, key, key_length);

  tp = &tps;

  // memory_barrier();
  assert((*(volatile uint8_t *)&bucket->lock) > 1);
  __sync_fetch_and_sub((volatile uint8_t *)&bucket->lock, (uint8_t)2);

  if (tp->cuckoostatus == failure_table_full) {
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
    return -1;//TABLE_STAT_INC(store, set_fail);
  }
  if (tp->cuckoostatus == failure_key_duplicated) {
    // TODO: support overwrite
    // overwriting = true;
    unlock_two_buckets(bucket, *tb);
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
    return -2;//TABLE_STAT_INC(store, set_fail);
  }
  assert(tp->cuckoostatus == ok); 
  
  return (uint64_t)&bucket[tp->bucket];
 
}



//to modify
void HashTable::redistribute_last_short_group(size_t *parts, size_t count) {
	uint64_t bucket_index;
  uint32_t entry_index;
  size_t target_p, target_e;
  int32_t k = count - 2;
  Bucket *buckets;
  Bucket *work_block;
  Bucket *workb;
  // uint8_t key[MAX_KEY_LENGTH];
  struct LogItem *item;
  struct twoBucket tb;
  while (k >= 0) {
    buckets = (Bucket *)(this->get_block_ptr(parts[k]));

    while (1) {
      // wait for the first bucket lock
      // Q: why wait here? why not wait per bucket?
			if (__sync_bool_compare_and_swap((volatile uint8_t *)&(buckets->lock), (uint8_t)0, (uint8_t)1))   break;
		}

    /* Go through the entire bucket blindly */
    for (bucket_index = 0; bucket_index <= BUCKETS_PER_PARTITION; bucket_index++) {

      // lock bucket to write
      write_lock_bucket(&buckets[bucket_index]);
      for (entry_index = 0; entry_index < ITEMS_PER_BUCKET; entry_index++) {
        if (!is_entry_expired(buckets[bucket_index].item_vec[entry_index])) {  // check if this entry exists

          /* 
          here checks if target_p should move or not, which depends on using the new group map or the old one
          we decide to use the new group map to check
          if target_p == parts[k], which means this entry should not move, we will leave it here
          if target_p != parts[k], which means this entry should move, then write it into new position and then delete it here 
          */
          item = mempool_->locate_item(PAGE(buckets[bucket_index].item_vec[entry_index]), ITEM_OFFSET(buckets[bucket_index].item_vec[entry_index]));
          target_p = round_hash_new_->HashToBucket(item->key_hash);

          if (target_p != parts[k])  {
            work_block = (Bucket *)get_block_ptr(target_p);
            tb = cal_two_buckets(item->key_hash);
            tablePosition tp = cuckoo_insert(work_block, item->key_hash, TAG(buckets[bucket_index].item_vec[entry_index]),
                                             tb, item->data, ITEMKEY_LENGTH(item->kv_length_vec));
            if (tp.cuckoostatus == failure_table_full) {
              // TODO: support overwrite
              // assert(false);
              item->expire_time = EXPIRED;
              buckets[bucket_index].item_vec[entry_index] = (uint64_t)0;
              continue;
            }
            if (tp.cuckoostatus == failure_key_duplicated) {
              // this may mean an overwrite comes before resizing, so leave it be
              // assert(false);
              item->expire_time = EXPIRED;
              buckets[bucket_index].item_vec[entry_index] = (uint64_t)0;
              unlock_two_buckets(work_block, tb);
              continue;
            }
            assert(tp.cuckoostatus == ok);

            workb = &(work_block[tp.bucket]);
            workb->item_vec[tp.slot] = buckets[bucket_index].item_vec[entry_index];
            unlock_two_buckets(work_block, tb);

            /* ToThink:
             * For H2L, do not clean origanl hash table entry is OK.
             * During resizing, if we check both cv(current_version)
             * and !cv, Keeping orignal entry may shorten porbe
             * length.
             */
            buckets[bucket_index].item_vec[entry_index] = (uint64_t)0;
          }
        }
      }
      write_unlock_bucket(&buckets[bucket_index]);
    }
    // memory_barrier();
    assert((*(volatile uint8_t *)&buckets->lock & (uint8_t)1) == (uint8_t)1);
    __sync_fetch_and_sub((volatile uint8_t *)&(buckets->lock), (uint8_t)1);
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
          item = mempool_->locate_item(PAGE(bucket[bucket_index].item_vec[entry_index]),
                                 ITEM_OFFSET(bucket[bucket_index].item_vec[entry_index]));
          target_p = round_hash_->HashToBucket(item->key_hash);
          if (target_p != parts[k]) {
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


/*
remap round_hash_new_ with round_hash_
don't exchange them here
*/
void HashTable::remap_new_groups()
{
  assert(round_hash_->get_block_num());

  if (round_hash_->get_block_num() <= round_hash_->S_) {
    round_hash_new_->num_long_arcs_ = round_hash_->num_long_arcs_ - 1;
    round_hash_new_->num_short_arc_groups_ = round_hash_->num_short_arc_groups_;
    round_hash_new_->num_short_arcs_ = round_hash_->num_short_arcs_;
    round_hash_new_->current_s_ = round_hash_->current_s_;
    round_hash_new_->arc_groups_ = round_hash_->arc_groups_;
    return;
  }

  round_hash_new_->current_s_ = round_hash_->current_s_;
  round_hash_new_->arc_groups_ = round_hash_->arc_groups_;
  round_hash_new_->num_short_arc_groups_ = round_hash_->num_short_arc_groups_;
  round_hash_new_->num_long_arcs_ = round_hash_->num_long_arcs_;
  round_hash_new_->num_short_arcs_ = round_hash_->num_short_arcs_;

  // If we completed a doubling.
  if (round_hash_->current_s_ == round_hash_->S_ && !round_hash_->num_short_arcs_) {
    round_hash_new_->current_s_ = (round_hash_->S_ << 1);
    round_hash_new_->arc_groups_ >>= 1;
  }

  if (round_hash_->num_short_arcs_ == 0) {
    round_hash_new_->num_short_arcs_ = round_hash_->num_long_arcs_;
    round_hash_new_->num_short_arc_groups_ = round_hash_new_->arc_groups_;
    round_hash_new_->num_long_arcs_ = 0;
    round_hash_new_->current_s_--;
  }

  round_hash_new_->num_short_arcs_ = round_hash_new_->num_short_arcs_ - (round_hash_new_->current_s_ + 1);
  round_hash_new_->num_short_arc_groups_ = round_hash_new_->num_short_arc_groups_ - 1;
  round_hash_new_->num_long_arcs_ = round_hash_new_->num_long_arcs_ + round_hash_new_->current_s_;

  return;
}

void HashTable::swap_group_maps()
{
  // TODO: since we can't atomicly swap here
  // is it necessary to add lock everytime before get HashToBucket?

  RoundHash *temp;

  while (1) {
    // use flexibling temp, should change it to a new lock
    if (__sync_bool_compare_and_swap((volatile uint32_t *)&(is_flexibling_), 0U, 1U))
      break;
	}
  
  temp = round_hash_;
  round_hash_ = round_hash_new_;
  round_hash_new_ = temp;

  __sync_fetch_and_sub((volatile uint32_t *)&(is_flexibling_), 1U);
}