#include "piekv.hpp"


bool Piekv::get(size_t t_id, uint64_t key_hash, const uint8_t *key, size_t key_length, uint8_t *out_value, uint32_t *in_out_value_length)
{
    LogSegment *segmentToGet = log_->log_segments_[t_id];
    #ifdef EXP_LATENCY
    Cbool isTransitionPeriod = hashtable_->is_flexibling_;
    auto start = std::chrono::steady_clock::now();
    #endif

    uint32_t block_index = hashtable_->round_hash_->HashToBucket(key_hash);
    Bucket *bucket = (Bucket *)hashtable_->get_block_ptr(block_index);  


    uint16_t tag = calc_tag(key_hash);
    
    twoSnapshot *ts1 = (twoSnapshot *)malloc(sizeof(twoBucket));
    twoBucket *tb = (twoBucket *)malloc(sizeof(twoBucket));

    while (1) {

        int64_t ret = hashtable_->get_table(ts1, tb, bucket, key_hash, key, key_length);
        uint64_t item_vec;
        if (ret){
            item_vec = (uint64_t)ret;
        } else if (ret == -2) {
            continue;
        } else {
            segmentToGet->table_stats_->get_notfound += 1;
            return false;
        }

        uint32_t block_id = PAGE(item_vec);
        uint64_t item_offset = ITEM_OFFSET(item_vec);
        
        segmentToGet->get_log(out_value,in_out_value_length,block_id,item_offset);

        if (!is_snapshots_same(*ts1, read_two_buckets_end(bucket, *tb))) continue;


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


bool Piekv::set(size_t t_id, uint64_t key_hash, uint8_t* key,uint32_t key_len, uint8_t* val, uint32_t val_len, bool overwrite)
{
    LogSegment *segmentToSet = log_->log_segments_[t_id];
    static int prt = 0;
    Cbool ret;
    ret = set_check(key_hash, key, key_len);
    uint16_t tag = calc_tag(key_hash);
    if (ret) return false;  // Alreagdy exists.

    uint32_t block_index = hashtable_->round_hash_->HashToBucket(key_hash);
    Bucket *bucket = (Bucket *)hashtable_->get_block_ptr(block_index);

    assert(key_len <= MAX_KEY_LENGTH);
    assert(val_len <= MAX_VALUE_LENGTH);

    tablePosition *tp = (tablePosition *)malloc(sizeof(tablePosition));
    twoBucket *tb = (twoBucket *)malloc(sizeof(twoBucket));

    #ifdef EXP_LATENCY
    Cbool isTransitionPeriod = table->is_flexibling;
    auto start = std::chrono::steady_clock::now();
    #endif
    Bucket *located_bucket;
    // Cbool overwriting = false;
    int64_t ptr = hashtable_->set_table(tp, tb, key_hash, key, key_len);
    if (ptr = -1){
        return FAILURE_HASHTABLE_FULL;
    } else if (ptr = -2){
        return FAILURE_ALREADY_EXIST;
    } else {
        located_bucket =(Bucket *)ptr;
    }

    uint64_t new_item_size = (uint32_t)(sizeof(LogItem) + ROUNDUP8(key_len) + ROUNDUP8(val_len));
    int64_t item_offset;


    int64_t block_id = segmentToSet->set_log(key_hash, key, (uint32_t)key_len, val, (uint32_t)val_len, VALID);

    located_bucket->item_vec[tp->slot] = ITEM_VEC(tag, block_id, item_offset);

    #ifdef _CUCKOO_
    unlock_two_buckets(bucket, *tb);
    #endif
    segmentToSet->table_stats_->count += 1;

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
    return SUCCESS_SET;
}






bool Piekv::set_check(uint64_t key_hash, const uint8_t *key, size_t key_length) {
  // Cbool snapshot_is_flexibling = table->is_flexibling;
  uint64_t snapshot_meta = *(uint64_t *)hashtable_->is_flexibling_;
  Cbool snapshot_is_flexibling = ((uint32_t *)&snapshot_meta)[0];
  Cbool snapshot_current_version = ((uint32_t *)&snapshot_meta)[1];

  uint32_t block_index = hashtable_->round_hash_->HashToBucket(key_hash);

  Bucket *bucket = (Bucket *)hashtable_->get_block_ptr(block_index);
  tablePosition tp;
  while (1) {
    twoBucket tb = cal_two_buckets(key_hash);
    twoSnapshot ts1;
    while (1) {
      ts1 = read_two_buckets_begin(bucket, tb);
      tp = cuckoo_find(bucket, key_hash, tb, key, key_length);
      if (is_snapshots_same(ts1, read_two_buckets_end(bucket, tb))) break;
    }
    if (tp.cuckoostatus == failure_key_not_found) {
      if (snapshot_is_flexibling) {
        snapshot_is_flexibling = (Cbool)0;
        block_index = hashtable_->round_hash_->HashToBucket(key_hash);
        bucket = (Bucket *)hashtable_->get_block_ptr(block_index);
        continue;
      }
      return false;
    }
    assert(tp.cuckoostatus == ok);

    if (!is_snapshots_same(ts1, read_two_buckets_end(bucket, tb))) continue;
    return false;
  }
  return true;
}