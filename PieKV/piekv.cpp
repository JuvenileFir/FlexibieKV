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
   
}


bool Piekv::set(LogSegment *segmentToSet,uint64_t key_hash, uint8_t* key,uint32_t key_len, uint8_t* val, uint32_t val_len, bool overwrite)
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
    int64_t ptr= hashtable_.set_table(key_hash,key,key_len);
    if (ptr = -1){
        return failure_hashtable_full;
    } else if (ptr = -2){
        return failure_already_exist;
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


