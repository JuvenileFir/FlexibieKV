#pragma once
/* In order to be effective, a feature test macro must
 * be defined before including any header files. This can
 * be done either in the compilation command (cc -DMACRO=value)
 * or by defining the macro within the source code
 * before including any headers.
 */

/* for using sched_getcpu() */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "slab_store.h"
#include "cuckoo.h"
#include "partition.h"

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <stdbool.h>
#include <sched.h>
#include <pthread.h>

#define MAX_KEY_LENGTH (255)
#define MAX_VALUE_LENGTH (1048575)

// TODO:
// do move-to-head if when (item's distance from tail) >= (pool size) *
// mth_threshold 0.0: full LRU; 1.0: full FIFO
#define MTH_THRESHOLD_FIFO (1.0)
#define MTH_THRESHOLD_LRU (0.0)

EXTERN_BEGIN

typedef enum fkvStatus {
  success_set,
  batch_full,
  batch_too_samll,
  failure_already_exist,
  failure_hashtable_full,
} fkvStatus;

typedef struct hash_table {
  uint8_t *pointer_base;
  uint32_t num_partitions;

  uint32_t is_setting;  // `set` and `flexible` adjcent for atomic update
                        // state. Oh! Brilliant!
  Cbool is_flexibling;  // used for indicating `H2L`/`L2H` is running.
  Cbool current_version;
  // `is_flexibeling` bit over set `is_setting`
  Cbool is_running;     // used for control `clean_expired_entry` to exit
  Cbool stop_entry_gc;  // used for stopping index entry gc when `move_h2t`

  DataStore *stores;

} hash_table ALIGNED(64);

void hash_table_init(hash_table *table, DataStore *stores);

void print_table_stats(const hash_table *table);

void clean_stats(hash_table *table);

uint32_t calc_partition_index(uint64_t key_hash, Cbool probe_version);

void *get_partition_head(uint32_t partition_index);

fkvStatus set(int32_t batchid, hash_table *table, SlabStore *store, uint64_t key_hash, const uint8_t *key,
              size_t key_length, const uint8_t *value, size_t value_length, uint32_t expire_time, Cbool overwrite);

Cbool get(hash_table *table, SlabStore *store, uint64_t key_hash, const uint8_t *key, size_t key_length,
          uint8_t *out_value, uint32_t *in_out_value_length, uint32_t *out_expire_time, Cbool readonly);

void redistribute_first_long_group(hash_table *table, size_t *parts, size_t count);

void redistribute_last_short_group(hash_table *table, size_t *parts, size_t count);

/* ************************* Debug Functions ************************************ */

Cbool orphan_chk(hash_table *table, uint64_t key_hash, const uint8_t *key, size_t key_length);

Cbool find_an_offset(const hash_table *table, size_t entry);

EXTERN_END
