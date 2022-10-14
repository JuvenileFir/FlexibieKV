#include "piekv.hpp"


void Piekv::memFlowingController()
{

    printf(" == [STAT] Memory flowing controler started on CORE 34 == \n");
    
    // bind memflowing thread to core 34
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(34, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
        fprintf(stderr, "[E] set thread affinity failed\n");
    }

    // factor
    const double factor = 1.0;
    const double threshold_log = 0.9;
    const double threshold_hashtable = 0.85;

    size_t store_load = 0;
    size_t index_load = 0;
    size_t store_capa, index_capa;

    double load_factor;
    double vaild_percentage;


    #ifdef EXP_MEM_EFFICIENCY
    static uint32_t times = 0;
    #endif

    while (table->is_running) {

        // sleep so get/set can run for a while
        sleep(5);
        

        // log class |
        store_load = 0;
        index_load = 0;
        for (uint16_t i = 0; i < table->stores->numStores; i++) {
        store_load += table->stores->slab_store[i].sstats.actual_used_mem + table->stores->slab_store[i].sstats.wasted;
        index_load += table->stores->slab_store[i].tstats.count;
        }
        store_capa = table->stores->totalNumPage * mempool_.get_block_size();
        index_capa = table->num_partitions * BUCKETS_PER_PARTITION * ITEMS_PER_BUCKET;
    
        vaild_percentage = store_load * factor / store_capa;
        load_factor = index_load * factor / index_capa;
        //Index less & Store more
        if (load_factor < threshold_hashtable && vaild_percentage >= threshold_log) {
        PRINT_EXCECUTION_TIME("  === [STAT] H2L is executed by Daemon === ", H2L(1));
    #ifdef MULTIPLE_SHIFT
        int segment_num = table->num_partitions;
        printf("         segment num: %d\n", segment_num);
        for (int i = 1; i < segment_num; i++) {
            PRINT_EXCECUTION_TIME("        ### expenentially shifting pages ### ", page_usage_balance_H2L(table, 1));
        }
    #endif
        /* ######### For macro-benchmarks that tests memory efficiency ######### */
    #ifdef EXP_MEM_EFFICIENCY
        printf("EXP1: %04u\t%lf\n", times++, (index_load + store_load) * factor / (index_capa + store_capa));
        printf("      %lf\t%lf\n", vaild_percentage, load_factor);
        fflush(stdout);
    #endif
        /* --------------------------------------------------------------------- */
        //Index more & Store less
        } else if (load_factor >= threshold_hashtable && vaild_percentage < threshold_log) {
        PRINT_EXCECUTION_TIME("  === [STAT] L2H is executed by Daemon === ", L2H(1));
    #ifdef MULTIPLE_SHIFT
        int segment_num = table->num_partitions;
        printf("         segment num: %d\n", segment_num);
        for (int i = 1; i < segment_num; i++) {
            PRINT_EXCECUTION_TIME("        ### expenentially shifting pages ### ", page_usage_balance_L2H(table, 1));
        }
    #endif
        /* ######### For macro-benchmarks that tests memory efficiency ######### */
    #ifdef EXP_MEM_EFFICIENCY
        printf("EXP1: %04u\t%lf\n", times++, (index_load + store_load) * factor / (index_capa + store_capa));
        printf("      %lf\t%lf\n", vaild_percentage, load_factor);
        fflush(stdout);
    #endif
        /* --------------------------------------------------------------------- */
        }
    }
}


bool Piekv::H2L(size_t blocknum_to_move)
{
    // check if hash blocks are too few to shrink
    // assert(num_pages < NumBuckets_v(current_version));

    if (!(blocknum_to_move < hashtable_.round_hash_.get_block_num())) {
        printf("Too few partitions for expanding Log\n");
        return false;
    }

    printf("[ARGS](H2L) to_shrink = %zu\t log = %u\t partition = %u\n", blocknum_to_move, table->stores->totalNumPage,
            table->num_partitions);


    TableBlock **tableblocksToMove = (TableBlock **)malloc(blocknum_to_move * sizeof(TableBlock));
    hashtable_.ShrinkTable(tableblocksToMove, blocknum_to_move);

    // Append page(s) to SlabStore in round robin.
    log_.Expand(tableblocksToMove,blocknum_to_move,4*64);   //  TODO: flexible log item size here

    return true;
}


bool Piekv::L2H(size_t blocknum_to_move)
{
    // check if log blocks are too few to shrink
    // assert(num_pages < table->stores->totalNumPage);
    if (!(blocknum_to_move < log_.total_blocknum_)) {
        printf("Too few memory hold by log for expanding Hash table\n");
        return false;
    }
    printf("[ARGS](L2H) to_shrink = %zu\t log = %u\t partition = %u\n", blocknum_to_move, table->stores->totalNumPage,
            table->num_partitions);


    // shrink store
    TableBlock **tableblocksToMove = (TableBlock **)malloc(blocknum_to_move * sizeof(TableBlock));
    log_.Shrink(tableblocksToMove, blocknum_to_move);
    hashtable_.ExpandTable(tableblocksToMove, blocknum_to_move);


    return true;
}


bool Piekv::get()
{
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


bool Piekv::set(LogSegment *segmentToSet, uint64_t key_hash, uint8_t* key,uint32_t key_len, uint8_t* val, uint32_t val_len, bool overwrite)
{
    static int prt = 0;
    Cbool ret;
    fkvStatus status;
    ret = orphan_chk(key_hash, key, key_len);
    if (ret) return false;  // Alreagdy exists.



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
    uint32_t block_index = hashtable_.round_hash_.HashToBucket(key_hash);
    page_bucket *partition = (page_bucket *)hashtable_.get_block_ptr(block_index);


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
        TABLE_STAT_INC(store, set_fail);
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
        TABLE_STAT_INC(store, set_fail);
        return failure_already_exist;
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


