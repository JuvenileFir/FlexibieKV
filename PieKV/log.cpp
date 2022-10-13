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
void Log::expand(TableBlock **tableblocksToMove, uint64_t numBlockToExpand, size_t blockSize)
{
    for(int i = 0; i < numBlockToExpand; i++) {
        // first get the segment id to resize
        uint16_t segmentId = get_next_resize_segment_id(0);
        LogSegment *segmentToResize = logSegments[segmentId];
        uint32_t blockNum = segmentToResize->blockNum;
        segmentToResize->logBlocks[blockNum]->blockPtr = (uint8_t *)(tableblocksToMove[i]->blockPtr);
        segmentToResize->logBlocks[blockNum]->blockId = tableblocksToMove[i]->blockId;
        segmentToResize->logBlocks[blockNum]->Residue = blockSize;


    }
}

void Log::shrink(uint64_t numBlockToShrink)
{
    for(int i = 0; i < numBlockToShrink; i++) {
        // first get the segment id to resize
        uint16_t segmentId = get_next_resize_segment_id(1);
        LogSegment *segmentToResize = logSegments[segmentId];

        // check if there is free block
        if(segmentToResize->blockNum > 1 && segmentToResize->usingBlock < segmentToResize->blockNum - 1) {
            __sync_fetch_and_sub((uint32_t *)&(segmentToResize->blockNum), 1U);
            __sync_fetch_and_sub((uint16_t *)&(totalBlockNum), 1U);

            // TODO: partitionMap_add removed here, should add it in Piekv::L2H
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
        return resizingPointer;
    }
    if (expandOrShrink == 1) {
        return (resizingPointer + totalSegmentNum - 1) % totalSegmentNum;
    }
}

 // expand: 0  shrink: 1
void Log::set_next_resize_segment_id(int expandOrShrink) 
{
    if (expandOrShrink == 0) {
        resizingPointer = (resizingPointer + 1) % totalSegmentNum;
    }
    if (expandOrShrink == 1) {
        resizingPointer = (resizingPointer + totalSegmentNum - 1) % totalSegmentNum;
    }
}




int64_t LogSegment::allocItem(uint64_t item_size) {
    // uint64_t item_size = mem_size;
    //TODO: assert(item_size == ROUNDUP8(item_size));

    int64_t item_offset;
    if (item_size <= BATCH_SIZE) {
        if (logBlocks[usingBlock]->Residue < item_size) {
            // block in use is already filled up 
            // check if there is free block left
            if (usingBlock < blockNum - 1) {
                // use next block
                usingBlock++;
                offset = 0;
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