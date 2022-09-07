#pragma once

#include "util.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define SHM_MAX_PAGES (16384)
//共享内存
EXTERN_BEGIN

typedef struct MemPage {
  void *addr;
  uint32_t numa_node;
  uint32_t in_use;
} MemPage;

typedef struct MemPool {
  MemPage memPages[SHM_MAX_PAGES];
  uint32_t numPages;
  uint32_t usedPages;
} MemPool;

size_t shm_get_page_size();

int32_t shm_alloc_page();

uint8_t *shm_get_page_addr(uint32_t pageNumber);

log_item *log_item_locate(const uint32_t pageNumber, uint64_t log_offset);

void shm_init(size_t page_size, size_t num_pages_to_try);

void shm_free_all();

void shm_clean_pages(size_t num_pages);

uint32_t shm_available_numPages();

void *shm_get_partition_tail(uint32_t index);

EXTERN_END
