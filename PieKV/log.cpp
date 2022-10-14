#include "log.hpp"


LogSegment::LogSegment(/* args */)
{
}

LogSegment::~LogSegment()
{
}

Log::Log(/* args */)
{
}

Log::~Log()
{
}

/*
expand log segemnt by `numBlockToExpand` blocks:
`tableblocksToMove`: an array for ptrs of tableblocks to move
`numBlockToExpand`: the number of blocks to move
`blocksize`: init logblocks with this blocksize
*/
void Log::Expand(TableBlock **tableblocksToMove, uint64_t numBlockToExpand, size_t blockSize)
{
    for(int i = 0; i < numBlockToExpand; i++) {
        // first get the segment id to resize
        uint16_t segmentId = get_next_resize_segment_id(0);
        LogSegment *segmentToResize = log_segments_[segmentId];
        uint32_t blockNum = segmentToResize->blocknum_;
        segmentToResize->log_blocks_[blockNum]->block_ptr = (uint8_t *)(tableblocksToMove[i]->block_ptr);
        segmentToResize->log_blocks_[blockNum]->block_id = tableblocksToMove[i]->block_id;
        segmentToResize->log_blocks_[blockNum]->residue = blockSize;
        // TODO: sync add here

    }
}

void Log::Shrink(TableBlock **tableblocksToMove, uint64_t numBlockToShrink)
{
    for(int i = 0; i < numBlockToShrink; i++) {
        // first get the segment id to resize
        uint16_t segmentId = get_next_resize_segment_id(1);
        LogSegment *segmentToResize = log_segments_[segmentId];

        // check if there is free block
        if(segmentToResize->blocknum_ > 1 && segmentToResize->usingblock_ < segmentToResize->blocknum_ - 1) {
            __sync_fetch_and_sub((uint32_t *)&(segmentToResize->blocknum_), 1U);
            __sync_fetch_and_sub((uint16_t *)&(total_blocknum_), 1U);

            tableblocksToMove[i]->block_id = segmentToResize->log_blocks_[segmentToResize->blocknum_]->block_id;
            tableblocksToMove[i]->block_ptr = segmentToResize->log_blocks_[segmentToResize->blocknum_]->block_ptr;
        }
        else {
            numBlockToShrink++;         //  try to find next usable segment
        }
        set_next_resize_segment_id(segmentId);
    }



}

 // expand: 0  shrink: 1
uint16_t Log::get_next_resize_segment_id(int expandOrShrink) 
{
    if (expandOrShrink == 0) {
        return resizing_pointer_;
    }
    if (expandOrShrink == 1) {
        return (resizing_pointer_ + total_segmentnum_ - 1) % total_segmentnum_;
    }
}

 // expand: 0  shrink: 1
void Log::set_next_resize_segment_id(int expandOrShrink) 
{
    if (expandOrShrink == 0) {
        resizing_pointer_ = (resizing_pointer_ + 1) % total_segmentnum_;
    }
    if (expandOrShrink == 1) {
        resizing_pointer_ = (resizing_pointer_ + total_segmentnum_ - 1) % total_segmentnum_;
    }
}




int64_t LogSegment::AllocItem(uint64_t item_size) {
    // uint64_t item_size = mem_size;
    //TODO: assert(item_size == ROUNDUP8(item_size));

    int64_t item_offset;
    if (item_size <= BATCH_SIZE) {
        if (log_blocks_[usingblock_]->residue < item_size) {
            // block in use is already filled up 
            // check if there is free block left
            if (usingblock_ < blocknum_ - 1) {
                // use next block
                usingblock_++;
                offset_ = 0;
            }
            else {
                // no free block left
                return -1;
            }
        }
        else {
            // TODO: implement a function `big_set`
            return -2;  // batch_too_small
        }
}