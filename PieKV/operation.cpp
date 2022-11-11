#include "piekv.hpp"

extern size_t pre_count[4];
extern struct timeval t0;                           

MemPool *kMemPool;
bool allow_mutation = false;

Piekv::Piekv(int init_log_block_number, int init_block_size, int init_mem_block_number){
    is_running_ = 1;
    stop_entry_gc_ = 0;

    kMemPool = new MemPool(init_block_size, init_mem_block_number);  // TODO: move this line to main function later, it shouldn't be here
    mempool_ = kMemPool;
    log_ = new Log(mempool_, init_log_block_number);
    hashtable_ = new HashTable(mempool_);
}

Piekv::~Piekv()
{
    printf("deleting piekv......\n");
    delete hashtable_;
    delete log_;
    delete kMemPool;
}

bool Piekv::get(size_t t_id, uint64_t key_hash, const uint8_t *key,
                size_t key_length, uint8_t *out_value,
                uint32_t *in_out_value_length) {

    LogSegment *segmentToGet = log_->log_segments_[t_id];
   #ifdef EXP_LATENCY
    Cbool isTransitionPeriod = hashtable_->is_flexibling_;
    auto start = std::chrono::steady_clock::now();
    #endif
    uint32_t block_index = hashtable_->round_hash_->HashToBucket(key_hash);
    Bucket *bucket = (Bucket *)hashtable_->get_block_ptr(block_index);  

    twoSnapshot *ts1 = (twoSnapshot *)malloc(sizeof(twoBucket));
    twoBucket *tb = (twoBucket *)malloc(sizeof(twoBucket));
    while (1) {
        const Bucket* located_bucket;
        int64_t ret = hashtable_->get_table(ts1, tb, bucket, key_hash, key,
                                            key_length, &located_bucket);
        uint64_t item_vec;
        if (ret >= 0){
            item_vec = located_bucket->item_vec[ret];
        } else if (ret == -2) {
            continue;
        } else if (ret == -1) {
            segmentToGet->table_stats_->get_notfound += 1;
            return false;
        }
        uint32_t block_id = PAGE(item_vec);
        uint64_t item_offset = ITEM_OFFSET(item_vec);
        // segmentToGet->get_log(out_value,in_out_value_length,block_id,item_offset);

        LogItem *item = segmentToGet->locateItem(block_id, item_offset);
        
        size_t key_length = 
            std::min(ITEMKEY_LENGTH(item->kv_length_vec), (uint32_t)MAX_KEY_LENGTH);
        size_t value_length = 
            std::min(ITEMVALUE_LENGTH(item->kv_length_vec), (uint32_t)MAX_VALUE_LENGTH);

        memcpy8(out_value, item->data + ROUNDUP8(key_length), value_length);
        *in_out_value_length = value_length;
        
        if (!is_snapshots_same(*ts1, read_two_buckets_end(bucket, *tb))) continue;
        segmentToGet->table_stats_->get_found += 1;
        // segmentToGet->table_stats_->count += 1;

        item_offset += block_id * mempool_->get_block_size();
        if (allow_mutation)
            this->move_to_head(bucket, (Bucket*)located_bucket,item, key_length,
                               value_length, ret, item_vec,
                               item_offset,segmentToGet);

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

bool Piekv::set_check(uint64_t key_hash, const uint8_t *key, size_t key_length) {
  // uint32_t snapshot_is_flexibling = table->is_flexibling;
  uint32_t snapshot_meta = hashtable_->is_flexibling_;
  uint32_t snapshot_is_flexibling = snapshot_meta;
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
        snapshot_is_flexibling = (uint32_t)0;
        block_index = hashtable_->round_hash_new_->HashToBucket(key_hash);
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

bool Piekv::set(size_t t_id, uint64_t key_hash, uint8_t *key, uint32_t key_len,
                uint8_t *val, uint32_t val_len, bool overwrite) {
    assert(key_len <= MAX_KEY_LENGTH);
    assert(val_len <= MAX_VALUE_LENGTH);

    LogSegment *segmentToSet = log_->log_segments_[t_id];

#ifdef EXP_LATENCY
    Cbool isTransitionPeriod = hashtable_->is_flexibling_;
    auto start = std::chrono::steady_clock::now();
#endif

    // Cbool overwriting = false;
    /*
     * Waiting for transition period of fliping is_flexibling and blocking
     * is_flexibling fliping when set is operating.
     * The fliping and sets are organized in FIFO fashion.
     */

    uint16_t tag = calc_tag(key_hash);
    uint32_t block_index = hashtable_->round_hash_->HashToBucket(key_hash);
    Bucket *bucket = (Bucket *)hashtable_->get_block_ptr(block_index);

    /*
     * XXX: Temporarily, the first bucket's `lock` of a partiton is used for
     * lock this bucket. When we execute a `set` that needs to displace
     * slot(s) for an empty slot, the order of touched bucket(s) cannot be
     * ensured in ascending order. If the KV is executing a rebalance at the
     * same time, moving slot(s) by touching buckets in ascending order, there
     * is the possibility for losing slot(s). So locking the bucket with FIFO
     * policy is the simplest way.
     */

    twoBucket tb = cal_two_buckets(key_hash);
    lock_two_buckets(bucket, tb);
    tablePosition tp = cuckoo_insert(bucket, key_hash, tag, tb, key, key_len);

    if (tp.cuckoostatus == failure_table_full)
    {
        // TODO: support eviction
#ifdef EXP_LATENCY
        auto end = std::chrono::steady_clock::now();
#ifdef TRANSITION_ONLY
        if (isTransitionPeriod)
        {
            printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
        }
#else
        printf("SET(false): [time: %lu ns]\n", std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
#endif
#endif
        segmentToSet->table_stats_->set_fail += 1;
        return false;//return FAILURE_HASHTABLE_FULL;
    }
    if (tp.cuckoostatus == failure_key_duplicated)
    {
        // TODO: support overwrite
        // overwriting = true;
        Bucket *located_bucket = &bucket[tp.bucket];
        LogItem *item = (LogItem *)kMemPool->locate_item(PAGE(located_bucket->item_vec[tp.slot]),
                                                       ITEM_OFFSET(located_bucket->item_vec[tp.slot]));
        size_t key_length = 
            std::min(ITEMKEY_LENGTH(item->kv_length_vec), (uint32_t)MAX_KEY_LENGTH);
        size_t old_value_length = 
            std::min(ITEMVALUE_LENGTH(item->kv_length_vec), (uint32_t)MAX_VALUE_LENGTH);
        
        if (!val_eq(val,val_len,item->data + ROUNDUP8(key_length),old_value_length)) {
            if (val_len <= old_value_length){
                memcpy8(item->data + ROUNDUP8(key_length), val, val_len);
                unlock_two_buckets(bucket, tb);
                segmentToSet->table_stats_->set_inplace += 1;
                return true;
            }
        }
        else {
            unlock_two_buckets(bucket, tb);
            segmentToSet->table_stats_->set_fail += 1;
            return false;
        }


        // segmentToSet->table_stats_->set_fail += 1;
        // return false;//return FAILURE_ALREADY_EXIST;
    }
    // if (!((tp.cuckoostatus == 0 ) || (tp.cuckoostatus == 6))) assert(0);
    struct Bucket *located_bucket = &bucket[tp.bucket];

    uint64_t new_item_size = (uint32_t)(sizeof(LogItem) + ROUNDUP8(key_len) + ROUNDUP8(val_len));
    int64_t item_offset;
    item_offset = segmentToSet->AllocItem(new_item_size);
    if (item_offset == -1) {
        unlock_two_buckets(bucket, tb);
 
        segmentToSet->table_stats_->set_fail += 1;
        return false;//return BATCH_FULL;
    } else
    if (item_offset == -2)
    {
        unlock_two_buckets(bucket, tb);//???lock two buckets在cuckoo insert中

        segmentToSet->table_stats_->set_fail += 1;
        return false;//return BATCH_TOO_SMALL;
    }
    // uint64_t new_tail = segmentToSet->get_tail();//仿照mica添加
    uint32_t block_id = segmentToSet->get_block_id(segmentToSet->usingblock_);
    LogItem *new_item = (LogItem *)segmentToSet->locateItem(block_id, item_offset);
    segmentToSet->table_stats_->set_success += 1;

#ifdef STORE_COLLECT_STATS
    segmentToSet->store_stats_->actual_used_mem += new_item_size;
#endif
    new_item->item_size = new_item_size;

    segmentToSet->set_item(new_item, key_hash, key, (uint32_t)key_len, val, (uint32_t)val_len, VALID);
    
    located_bucket->item_vec[tp.slot] = ITEM_VEC(tag, block_id, item_offset);

    unlock_two_buckets(bucket, tb);
    if (!(tp.cuckoostatus == 6 || tp.cuckoostatus == 3))
        segmentToSet->table_stats_->count += 1;

    //cleanup_bucket(item_offset,new_tail); TODO!
    return true;
}

void Piekv::move_to_head(Bucket* bucket, Bucket* located_bucket,
                         const LogItem* item, size_t key_length,
                         size_t value_length, size_t item_index,
                         uint64_t item_vec, uint64_t item_offset, 
                         LogSegment *segmentToGet) {
    uint64_t new_item_size = 
        sizeof(LogItem) + ROUNDUP8(key_length) + ROUNDUP8(value_length);
    uint64_t distance_from_tail = 
        (segmentToGet->get_tail() - item_offset) & (log_->mask_);//tail <- set
    if (distance_from_tail > mth_threshold_ +1 ) {

    write_lock_bucket(bucket);
    
    // pool_->lock();//PieKV不锁pool

    // check if the original item is still there
    if (located_bucket->item_vec[item_index] == item_vec) {//仅验证
 
        // uint64_t new_tail = segmentToGet->get_tail();
        uint64_t new_item_offset = (uint64_t)segmentToGet->AllocItem(new_item_size);

        if (item_offset < BLOCK_MAX_NUM * kblock_size) {
            uint32_t block_id = segmentToGet->get_block_id(segmentToGet->usingblock_);
            LogItem *new_item = (LogItem *)segmentToGet->locateItem(block_id, new_item_offset);
            memcpy8((uint8_t*)new_item, (const uint8_t*)item, new_item_size);
    
            located_bucket->item_vec[item_index] = ITEM_VEC(TAG(item_vec), block_id, new_item_offset);

            // success
            segmentToGet->table_stats_->move_to_head_performed++;
        } else {
            // failed -- original data become invalid in the pool
            segmentToGet->table_stats_->move_to_head_failed++;
        }

        // we need to hold the lock until we finish writing

        write_unlock_bucket(bucket);

        // cleanup_bucket(new_item_offset, new_tail);
    } else {
        //pool_->unlock();//PieKV不锁pool
        write_unlock_bucket(bucket);

        // failed -- original data become invalid in the table
        segmentToGet->table_stats_->move_to_head_failed++;
    }
  } else {
    segmentToGet->table_stats_->move_to_head_skipped++;
  }
}

void Piekv::print_trigger() {
    printf(" == [STAT] printer started on CORE 36 == \n");
    
    // bind memflowing thread to core 34
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(36, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
        fprintf(stderr, "[E] set thread affinity failed\n");
    }
    while (is_running_) {
        gettimeofday(&t0, NULL);
        uint64_t res[4];
        double temp = (t0.tv_sec + t0.tv_usec / 1000000.0);
        pre_count[0] = log_->log_segments_[0]->table_stats_->count
                        +log_->log_segments_[1]->table_stats_->count
                        +log_->log_segments_[2]->table_stats_->count
                        +log_->log_segments_[3]->table_stats_->count;
        // printf("former count:%ld\n",pre_count[0]);
        pre_count[1] = log_->log_segments_[0]->table_stats_->get_found
                        +log_->log_segments_[1]->table_stats_->get_found
                        +log_->log_segments_[2]->table_stats_->get_found
                        +log_->log_segments_[3]->table_stats_->get_found;
        pre_count[2] = log_->log_segments_[0]->table_stats_->set_success
                        +log_->log_segments_[1]->table_stats_->set_success
                        +log_->log_segments_[2]->table_stats_->set_success
                        +log_->log_segments_[3]->table_stats_->set_success;
        pre_count[3] = log_->log_segments_[0]->table_stats_->rx_pkt_num
                        +log_->log_segments_[1]->table_stats_->rx_pkt_num
                        +log_->log_segments_[2]->table_stats_->rx_pkt_num
                        +log_->log_segments_[3]->table_stats_->rx_pkt_num;
        sleep(1);
        gettimeofday(&t0, NULL);
        double timediff = (t0.tv_sec + t0.tv_usec / 1000000.0) - temp;
        res[0] = log_->log_segments_[0]->table_stats_->count
                                                +log_->log_segments_[1]->table_stats_->count
                                                +log_->log_segments_[2]->table_stats_->count
                                                +log_->log_segments_[3]->table_stats_->count
                                                - pre_count[0];
        res[1] = log_->log_segments_[0]->table_stats_->get_found
                                                +log_->log_segments_[1]->table_stats_->get_found
                                                +log_->log_segments_[2]->table_stats_->get_found
                                                +log_->log_segments_[3]->table_stats_->get_found
                                                - pre_count[1];
        res[2] = log_->log_segments_[0]->table_stats_->set_success
                                                +log_->log_segments_[1]->table_stats_->set_success
                                                +log_->log_segments_[2]->table_stats_->set_success
                                                +log_->log_segments_[3]->table_stats_->set_success
                                                - pre_count[2];
        res[3] = log_->log_segments_[0]->table_stats_->rx_pkt_num
                                                +log_->log_segments_[1]->table_stats_->rx_pkt_num
                                                +log_->log_segments_[2]->table_stats_->rx_pkt_num
                                                +log_->log_segments_[3]->table_stats_->rx_pkt_num
                                                - pre_count[3];
        if (res[3])  printf("\n[P]rx pkt num:            %7.3lf /s\n", res[3] / timediff );
        if (res[0])  printf("\n[P]count:                 %7.3lf /s\n", res[0] / timediff );
        if (res[1])  printf("\n[P]get found:             %7.3lf /s\n", res[1] / timediff );
        if (res[2])  printf("\n[P]set success:           %7.3lf /s\n", res[2] / timediff );
        if (res[0]||res[1]||res[2]) printf("\n----------------------------------\n"); 

        pre_count[0] = log_->log_segments_[0]->table_stats_->count
                        +log_->log_segments_[1]->table_stats_->count
                        +log_->log_segments_[2]->table_stats_->count
                        +log_->log_segments_[3]->table_stats_->count;
        pre_count[1] = log_->log_segments_[0]->table_stats_->get_found
                        +log_->log_segments_[1]->table_stats_->get_found
                        +log_->log_segments_[2]->table_stats_->get_found
                        +log_->log_segments_[3]->table_stats_->get_found;
        pre_count[2] = log_->log_segments_[0]->table_stats_->set_success
                        +log_->log_segments_[1]->table_stats_->set_success
                        +log_->log_segments_[2]->table_stats_->set_success
                        +log_->log_segments_[3]->table_stats_->set_success;
        pre_count[3] = log_->log_segments_[0]->table_stats_->rx_pkt_num
                        +log_->log_segments_[1]->table_stats_->rx_pkt_num
                        +log_->log_segments_[2]->table_stats_->rx_pkt_num
                        +log_->log_segments_[3]->table_stats_->rx_pkt_num;
        if (res[0]||res[1]||res[2]) this->showUtilization();        
// printf("latter count: %ld\n",pre_count[0]);

    }
}

/*


bool Piekv::set(size_t t_id, uint64_t key_hash, uint8_t* key,uint32_t key_len, uint8_t* val, uint32_t val_len, bool overwrite)
{
    printf("[INFO]set 1\n");
    printf("[INFO] tid: %d\n",t_id);
    LogSegment *segmentToSet = log_->log_segments_[t_id];
    printf("[INFO]set 1.01\n");
    static int prt = 0;
    Cbool ret;
    ret = set_check(key_hash, key, key_len);
    uint16_t tag = calc_tag(key_hash);
    if (ret) return false;  // Alreagdy exists.
    printf("[INFO]set 2\n");
    uint32_t block_index = hashtable_->round_hash_->HashToBucket(key_hash);
    Bucket *bucket = (Bucket *)hashtable_->get_block_ptr(block_index);
    printf("[INFO]set 3\n");
    assert(key_len <= MAX_KEY_LENGTH);
    assert(val_len <= MAX_VALUE_LENGTH);
printf("bucket version %d in set\n",bucket->version);
    tablePosition *tp = (tablePosition *)malloc(sizeof(tablePosition));
    twoBucket tb = cal_two_buckets(key_hash);
    printf("[INFO]set 4\n");
    #ifdef EXP_LATENCY
    Cbool isTransitionPeriod = table->is_flexibling;
    auto start = std::chrono::steady_clock::now();
    #endif
    Bucket *located_bucket;
    // Cbool overwriting = false;
    printf("[INFO]set 5\n");

    int64_t ptr = hashtable_->set_table(tp, tb, key_hash, key, key_len);
    if (ptr == -1){
        return FAILURE_HASHTABLE_FULL;
    } else if (ptr == -2){
        return FAILURE_ALREADY_EXIST;
    } else {
        located_bucket =(Bucket *)ptr;
    }
    printf("bucket version %d in after set table\n",bucket->version);
    printf("[INFO]set 6\n");
    uint64_t new_item_size = (uint32_t)(sizeof(LogItem) + ROUNDUP8(key_len) + ROUNDUP8(val_len));
    int64_t item_offset;

    printf("[INFO]set 7\n");
    int64_t block_id = segmentToSet->set_log(key_hash, key, (uint32_t)key_len, val, (uint32_t)val_len, VALID);

    located_bucket->item_vec[tp->slot] = ITEM_VEC(tag, block_id, item_offset);
    printf("[INFO]set 8\n");
    printf("bucket version %d before unlock\n",bucket->version);
    unlock_two_buckets(bucket, *tb);
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





*/