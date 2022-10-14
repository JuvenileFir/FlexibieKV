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
    hashtable_.ShrinkTable(tableblocksToMove, blocknum_to_move);

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


