#pragma once

/* for using `SET_ZERO` and `CPU_SET` */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "table.h"
#include "partition.h"

EXTERN_BEGIN

void mem_flowing_controler(hash_table *table);

Cbool page_usage_balance_H2L(hash_table *table, size_t num_pages);

Cbool page_usage_balance_L2H(hash_table *table, size_t num_pages);

void store_expansion(DataStore *data_store, uint64_t num_partitions, uint64_t num_pages);

void store_shrink(DataStore *data_store, uint64_t num_pages);

EXTERN_END
