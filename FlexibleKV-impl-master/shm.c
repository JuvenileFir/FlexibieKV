#include "shm.h"
#include <sys/mman.h>

EXTERN_BEGIN

static size_t shm_page_size;
static uint64_t shm_state_lock;
static struct MemPool mempool;
// static size_t shm_page_number;
// static size_t num_pages_for_items = 0;

static inline void shm_lock() UNUSED;
static inline void shm_lock() {
  while (1) {
    if (__sync_bool_compare_and_swap((volatile uint64_t *)&shm_state_lock, 0UL, 1UL)) break;
  }
}

static inline void shm_unlock() UNUSED;
static inline void shm_unlock() {
  memory_barrier();
  *(volatile uint64_t *)&shm_state_lock = 0UL;
}

size_t shm_get_page_size() { return shm_page_size; }

uint8_t *shm_get_page_addr(uint32_t pageNumber) { return mempool.memPages[pageNumber].addr; }

log_item *log_item_locate(const uint32_t pageNumber, uint64_t log_offset) {
  return (log_item *)(shm_get_page_addr(pageNumber) + log_offset);
}

static int shm_compare_vaddr(const void *a, const void *b) {
  const struct MemPage *pa = (const struct MemPage *)a;
  const struct MemPage *pb = (const struct MemPage *)b;
  if (pa->addr < pb->addr)
    return -1;
  else
    return 1;
}

void shm_init(size_t page_size, size_t num_pages_to_try) {
  assert(next_power_of_two(page_size) == page_size);//验证page_size是2的幂次(mem_flowing中为2MiB)
  assert(num_pages_to_try <= SHM_MAX_PAGES);

  size_t page_id;
  shm_state_lock = 0;
  shm_page_size = page_size;
  memset(&mempool, 0, sizeof(MemPool));
  /* initialize pages */
  printf("MEM: Initializing pages...\n");
  /* NOTE: One more page for circular log like this:
   *  | circular_log | page | partition |
   * Using mmap() with hugepages need load two pages when changing size.
   * Appeding one more page at the end of the circular log to avoid
   * overwriting.
   */
  for (page_id = 0; page_id < num_pages_to_try; page_id++) {
    // mempool.memPages[page_id].addr = aligned_alloc(shm_page_size, shm_page_size);
    void *ptr = mmap(NULL, shm_page_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    if (ptr == MAP_FAILED) assert(0);
    mempool.memPages[page_id].addr = ptr;
    memset(mempool.memPages[page_id].addr, 0, shm_page_size);
    if (mempool.memPages[page_id].addr == NULL) break;
  }
  mempool.numPages = page_id;//初始化为240
  mempool.usedPages = 0;
  printf("MEM:   initial allocation of %zu pages\n", page_id);
  // sort by virtual address
  printf("MEM:   sorting by virtual address\n");
  qsort(mempool.memPages, mempool.numPages, sizeof(struct MemPage), shm_compare_vaddr);
}

void shm_free_all() {
  for (size_t page_id = 0; page_id < mempool.numPages; page_id++) {
    free(mempool.memPages[page_id].addr);
  }
  mempool.numPages = 0;
}

void shm_clean_pages(size_t page_number) {
  /* clean the last `num_pages` pages of circular_log before being used by hash table. */
  memset(mempool.memPages[page_number].addr, 0, shm_page_size);
}
//一次给出一个可用page，并返回编号
int32_t shm_alloc_page() {
  for (uint32_t i = 0; i < mempool.numPages; i++) {
    if (mempool.memPages[i].in_use == 0) {
      mempool.memPages[i].in_use = 1;
      mempool.usedPages++;
      return i;
    }
  }
  return -1;
}

uint32_t shm_available_numPages() { return (mempool.numPages - mempool.usedPages); }

EXTERN_END
