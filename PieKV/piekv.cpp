#include "piekv.hpp"



void Piekv::memFlowingController()
{

}


bool Piekv::H2L(size_t blocknum_to_move)
{
    // compare if there are too few hash blocks 
    // assert(num_pages < NumBuckets_v(current_version));
    if (!(blocknum_to_move < NumBuckets_v(table->current_version))) {
        fprintf(stderr, "Too few partitions for expanding Log\n");
        usleep(500);
        return (Cbool)0;
    }

    printf("[ARGS](H2L) to_shrink = %zu\t log = %u\t partition = %u\n", blocknum_to_move, table->stores->totalNumPage,
            table->num_partitions);


    TableBlock **tableblocksToMove = (TableBlock **)malloc(blocknum_to_move * sizeof(TableBlock));
    for (int i = 0; i < blocknum_to_move; i++) {
        size_t count;
        size_t parts[S_ << 1];

        // get all parts to move
        hashtable_.round_hash_.get_parts_to_remove(parts, &count);
        hashtable_.round_hash_.DelBucket();

        __sync_fetch_and_add((volatile uint32_t *)&(table->is_setting), 1U);
        while (*(volatile uint32_t *)&(table->is_setting) != 1U)
        ;
        *(volatile uint32_t *)&(table->is_flexibling) = 1U;   // new version here
        __sync_fetch_and_sub((volatile uint32_t *)&(table->is_setting), 1U);


        hashtable_.redistribute_last_short_group(parts, count);
        tableblocksToMove[i]->block_id = hashtable_.table_blocks_[hashtable_.table_block_num_]->block_id;
        tableblocksToMove[i]->block_ptr = hashtable_.table_blocks_[hashtable_.table_block_num_]->block_ptr;
        hashtable_.table_block_num_ -= 1;
    }
    
    __sync_fetch_and_add((volatile uint32_t *)&(table->is_setting), 1U);
    while (*(volatile uint32_t *)&(table->is_setting) != 1U)
    ;
    *(volatile uint32_t *)&(table->is_flexibling) = 0;//?????table->is_flexibling此步确定赋0，同时多覆盖了后面4byte，即current_version
    /* clean is_flexibling and flip current_version in a single step */
    __sync_fetch_and_sub((volatile uint32_t *)&(table->is_setting), 1U);

    // Append page(s) to SlabStore in round robin.
    log_.Expand(tableblocksToMove,blocknum_to_move,4*64);   //  TODO: flexible log item size here

    return true;
}


bool Piekv::L2H(size_t num_pages)
{

}


bool Piekv::get()
{

}


bool Piekv::set()
{

}


