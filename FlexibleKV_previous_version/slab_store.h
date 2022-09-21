#pragma once

#include "basic_log.h"

#define BATCH_SIZE (2097152U) /* A item cannot exceed one page */
#define NUM_THREAD (4U)       // number of concurrent running `set` workers
EXTERN_BEGIN

typedef struct Slab {
  uint8_t *pheader;
  uint32_t pageNumber;
  uint32_t empty;
} Slab;

typedef struct SlabStore {
  Slab slabs[SHM_MAX_PAGES - 1];
  uint32_t numPages;
  uint32_t usingPage;
  uint32_t offset;
  uint32_t id;
#ifdef STORE_COLLECT_STATS
  struct {
    size_t actual_used_mem;
    size_t wasted;  // counter for fragmentation
  } sstats;
#endif
#ifdef TABLE_COLLECT_STATS
  struct {
    size_t count;
    size_t set_nooverwrite;
    size_t set_success;
    size_t set_fail;
    size_t set_inplace;
    size_t set_evicted;
    size_t get_found;
    size_t get_notfound;
    size_t test_found;
    size_t test_notfound;
    size_t delete_found;
    size_t delete_notfound;
    size_t cleanup;
    size_t move_to_head_performed;
    size_t move_to_head_skipped;
    size_t move_to_head_failed;
  } tstats;
#endif
} SlabStore;

typedef struct DataStore {
  SlabStore slab_store[NUM_THREAD];
  uint32_t global_pageID;
  uint16_t numStores;
  uint16_t totalNumPage;
  uint16_t expandPointer;
} DataStore;

void slabStore_init(DataStore *data_store, uint64_t num_pages);

int64_t alloc_item(SlabStore *slab_store, uint64_t *mem_size);

EXTERN_END
