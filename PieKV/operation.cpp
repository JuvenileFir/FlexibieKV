#include "piekv.hpp"


bool Piekv::get(LogSegment *segmentToGet, uint64_t key_hash, const uint8_t *key, size_t key_length, uint8_t *out_value, uint32_t *in_out_value_length)
{

    #ifdef EXP_LATENCY
    Cbool isTransitionPeriod = hashtable_.is_flexibling_;
    auto start = std::chrono::steady_clock::now();
    #endif

    uint32_t block_index = hashtable_.round_hash_.HashToBucket(key_hash);
    Bucket *bucket = (Bucket *)hashtable_.get_block_ptr(block_index);  


    uint16_t tag = calc_tag(key_hash);
    
    twoSnapshot *ts1 = (twoSnapshot *)malloc(sizeof(twoBucket));
    twoBucket *tb = (twoBucket *)malloc(sizeof(twoBucket));

    while (1) {

        int64_t ret = hashtable_.get_table(ts1, tb, bucket, key_hash, key, key_length);
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


bool Piekv::set(LogSegment *segmentToSet, uint64_t key_hash, uint8_t* key,uint32_t key_len, uint8_t* val, uint32_t val_len, bool overwrite)
{
    static int prt = 0;
    Cbool ret;
    ret = orphan_chk(key_hash, key, key_len);
    if (ret) return false;  // Alreagdy exists.



    assert(key_length <= MAX_KEY_LENGTH);
    assert(value_length <= MAX_VALUE_LENGTH);
    #ifdef EXP_LATENCY
    Cbool isTransitionPeriod = table->is_flexibling;
    auto start = std::chrono::steady_clock::now();
    #endif

    // Cbool overwriting = false;
    int64_t ptr = hashtable_.set_table(key_hash,key,key_len);
    if (ptr = -1){
        return FAILURE_HASHTABLE_FULL;
    } else if (ptr = -2){
        return FAILURE_ALREADY_EXIST;
    } else {
        Bucket *located_bucket =(Bucket *)ptr;
    }

    uint64_t new_item_size = (uint32_t)(sizeof(struct log_item) + ROUNDUP8(key_length) + ROUNDUP8(value_length));
    int64_t item_offset;


    segmentToSet->set_log(key_hash, key, (uint32_t)key_length, value, (uint32_t)value_length, expire_time);

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
