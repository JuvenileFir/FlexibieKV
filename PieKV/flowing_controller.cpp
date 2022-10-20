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

    while (is_running_) {

        // sleep so get/set can run for a while
        sleep(5);
        
        // log class |
        store_load = 0;
        index_load = 0;
        for (uint16_t i = 0; i < log_->total_segmentnum_; i++) {
            LogSegment *segment_in_use = log_->log_segments_[i];
            store_load += segment_in_use->store_stats_->actual_used_mem + segment_in_use->store_stats_->wasted;
            index_load += segment_in_use->table_stats_->count;
        }
        store_capa = log_->total_blocknum_ * mempool_->get_block_size();
        index_capa = hashtable_->table_block_num_ * BUCKETS_PER_PARTITION * ITEMS_PER_BUCKET;
    
        vaild_percentage = store_load * factor / store_capa;
        load_factor = index_load * factor / index_capa;
        //Index less & Store more
        if (load_factor < threshold_hashtable && vaild_percentage >= threshold_log) {
            printf("[STATUS] store load: %d  store capa: %d\n",store_load,store_capa);
            printf("[STATUS] index load: %d  index capa: %d\n",index_load,index_capa);
            printf("[STATUS] load_factor: %f  valid_percentage: %f\n",load_factor,vaild_percentage);
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
            printf("[STATUS] store load: %d  store capa: %d\n",store_load,store_capa);
            printf("[STATUS] index load: %d  index capa: %d\n",index_load,index_capa);
            printf("[STATUS] load_factor: %f  valid_percentage: %f\n",load_factor,vaild_percentage);
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

    if (!(blocknum_to_move < hashtable_->round_hash_->get_block_num())) {
        printf("Too few partitions for expanding Log\n");
        return false;
    }

    printf("[ARGS](H2L) to_shrink = %zu\t log = %u\t partition = %u\n", blocknum_to_move, log_->total_blocknum_,
            hashtable_->table_block_num_);


    TableBlock **tableblocksToMove = (TableBlock **)malloc(blocknum_to_move * sizeof(TableBlock));
    hashtable_->ShrinkTable(tableblocksToMove, blocknum_to_move);

    // Append page(s) to SlabStore in round robin.
    log_->Expand(tableblocksToMove,blocknum_to_move,4*64);   //  TODO: flexible log item size here

    return true;
}


bool Piekv::L2H(size_t blocknum_to_move)
{
    // check if log blocks are too few to shrink
    // assert(num_pages < table->stores->totalNumPage);
    if (!(blocknum_to_move < log_->total_blocknum_)) {
        printf("Too few memory hold by log for expanding Hash table\n");
        return false;
    }
    printf("[ARGS](L2H) to_shrink = %zu\t log = %u\t partition = %u\n", blocknum_to_move, log_->total_blocknum_,
            hashtable_->table_block_num_);


    // shrink store
    TableBlock **tableblocksToMove = (TableBlock **)malloc(blocknum_to_move * sizeof(TableBlock));
    log_->Shrink(tableblocksToMove, blocknum_to_move);
    hashtable_->ExpandTable(tableblocksToMove, blocknum_to_move);


    return true;
}