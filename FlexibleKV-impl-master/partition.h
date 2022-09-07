/* Partition keys basing on its hash value */

#pragma once

#include "shm.h"
// #include "util.h"

#include <stdint.h>
#include <stddef.h>

#define CLK CLOCK_PROCESS_CPUTIME_ID

EXTERN_BEGIN

uint64_t rotl(uint64_t x, int k);

extern uint64_t num_long_arcs_[2];
extern uint64_t num_short_arc_groups_[2];
extern uint64_t num_short_arcs_[2];
extern uint64_t current_s_[2];
extern uint64_t S_;
extern uint64_t S_minus_one;
extern uint64_t S_log_;
extern uint64_t arc_groups_[2];
extern uint64_t num_buckets_;
extern uint64_t a_, b_;

extern uint64_t lh_n, lh_n0, lh_l, lh_p, lh_x;

typedef struct Partition {
  void *pheader;
  uint32_t pageNumber;
} Partition;

typedef struct PartitionMap {
  Partition partitions[SHM_MAX_PAGES - 1];
  size_t numOfpartitions;
} PartitionMap;

void *partition_header(uint32_t partitionNumber);

uint32_t partition_pageNumber(uint32_t partitionNumber);

size_t ArcNum(uint64_t divs, uint64_t hash);

size_t RoundHash(uint64_t value, Cbool version);

void print_partiton_stats(Cbool version);

/* For two version info */
uint64_t NumBuckets_v(Cbool version);

void NewBucket(Cbool version);

void DelBucket(Cbool version);

void NewBucket_v(Cbool version);

void DelBucket_v(Cbool version);

/* ******************** */

void hash_init_partition(const Cbool version, uint64_t S, uint64_t num_buckets);

void partitionMap_add(uint8_t *pheader, uint32_t pageNumber);

void get_last_short_group_parts(size_t *parts, size_t *count, Cbool version);

void get_first_long_group_parts(size_t *parts, size_t *count, Cbool version);

EXTERN_END
