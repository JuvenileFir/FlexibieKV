#include "slab_store.h"

EXTERN_BEGIN

void slabStore_init(DataStore *data_store, uint64_t num_pages) {
  // size = shm_adjust_size(size);
  // assert(size == shm_adjust_size(size));

  memset(data_store, 0, sizeof(DataStore));
  data_store->numStores = NUM_THREAD;
  data_store->expandPointer = 0;

  SlabStore *slabstore = NULL;
  for (uint32_t i = 0; i < num_pages; i++) {
    int ret = shm_alloc_page();//ret为可用page ID
    if (ret != -1) {
      data_store->totalNumPage++;

      slabstore = &data_store->slab_store[i % NUM_THREAD];
      slabstore->slabs[slabstore->numPages].pheader = shm_get_page_addr(ret);
      slabstore->slabs[slabstore->numPages].pageNumber = ret;
      slabstore->slabs[slabstore->numPages].empty = shm_get_page_size();
      slabstore->numPages++;
    } else {
      break;
    }
  }
  assert(data_store->totalNumPage >= NUM_THREAD);
#ifdef COLLECT_STATS
  for (uint32_t i = 0; i < NUM_THREAD; i++) {
#ifdef TABLE_COLLECT_STATS
    memset(&(data_store->slab_store[i].tstats), 0, sizeof(data_store->slab_store[i].tstats));
#endif
#ifdef STORE_COLLECT_STATS
    memset(&(data_store->slab_store[i].sstats), 0, sizeof(data_store->slab_store[i].sstats));
#endif
  }
#endif
}

int64_t alloc_item(SlabStore *slab_store, uint64_t *mem_size) {
  uint64_t item_size = *mem_size;
  assert(item_size == ROUNDUP8(item_size));

  int64_t item_offset;
  if (item_size <= BATCH_SIZE) {
    if (slab_store->slabs[slab_store->usingPage].empty < item_size) {
      if (slab_store->usingPage < slab_store->numPages - 1) {
        slab_store->usingPage++;
        slab_store->offset = 0;
      } else {
        return -1;  // No available free memory space
      }
    }
    item_offset = slab_store->offset;
    slab_store->offset += item_size;
    slab_store->slabs[slab_store->usingPage].empty -= item_size;

    return item_offset;  // % balloc->size;
  } else {
    // TODO: implement a function `big_set`
    return -2;  // batch_too_samll
  }
}

EXTERN_END
