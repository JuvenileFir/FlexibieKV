#pragma once

#define _CUCKOO_

#include "basic_hash.h"
#include "util.h"

#include <math.h>


#define MAX_CUCKOO_COUNT 5602U
// 2 * ((ITEMS_PER_BUCKET == 1)
//     ? MAX_BFS_PATH_LEN
//     : (const_pow_slot_per_bucket_MAX_BFS_PATH_LEN - 1) /
//     (ITEMS_PER_BUCKET - 1));

typedef struct twoBucket {
  uint32_t b1;
  uint32_t b2;
} twoBucket;

typedef struct twoSnapshot {
  uint32_t v1;
  uint32_t v2;
} twoSnapshot;

typedef struct cuckooRecord {
  uint32_t bucket;
  uint32_t slot;
  uint16_t tag;
} cuckooRecord;

typedef struct bSlot {
  // The bucket of the last item in the path.
  uint32_t bucket;
  // a compressed representation of the slots for each of the buckets in
  // the path. pathcode is sort of like a base-slot_per_bucket number, and
  // we need to hold at most MAX_BFS_PATH_LEN slots. Thus we need the
  // maximum pathcode to be at least slot_per_bucket()^(MAX_BFS_PATH_LEN).
  uint16_t pathcode;
  // static_assm const_pow_slot_per_bucket_MAX_BFS_PATH_LEN");
  // const_pow_slot_per_bucket_MAX_BFS_PATH_LEN = (uint16_t)16807
  //                                            =
  //                                            slot_per_bucket()^(MAX_BFS_PATH_LEN)
  // static_assert(const_pow_slot_per_bucket_MAX_BFS_PATH_LEN < UINT16_MAX,
  //                 "pathcode may not be large enough to encode a cuckoo "
  //                 "path");
  // The 0-indexed position in the cuckoo path this slot occupies. It must
  // be less than MAX_BFS_PATH_LEN, and also able to hold negative values.
  int8_t depth;
  // static_assert(MAX_BFS_PATH_LEN - 1 <= INT8_MAX,
  //                 "The depth type must able to hold a value of"
  //                 " MAX_BFS_PATH_LEN - 1");
  // static_assert(-1 >= INT8_MIN,
  //                 "The depth type must be able to hold a value of -1");
} bSlot;

// bQueue is the queue used to store bSlots for BFS cuckoo hashing.
typedef struct bQueue {
  // The size of the BFS queue. It holds just enough elements to fulfill a
  // MAX_BFS_PATH_LEN search for two starting buckets, with no circular
  // wrapping-around. For one bucket, this is the geometric sum
  // sum_{k=0}^{MAX_BFS_PATH_LEN-1} slot_per_bucket()^k
  // = (1 - slot_per_bucket()^MAX_BFS_PATH_LEN) / (1 - slot_per_bucket())

  // Note that if slot_per_bucket() == 1, then this simply equals
  // MAX_BFS_PATH_LEN.
  static_assert(ITEMS_PER_BUCKET > 0, "SLOT_PER_BUCKET must be greater than 0.");

  // An array of b_slots. Since we allocate just enough space to complete a
  // full search, we should never exceed the end of the array.
  bSlot slots_[MAX_CUCKOO_COUNT];
  // The index of the head of the queue in the array
  uint32_t first_;
  // One past the index of the last_ item of the queue in the array.
  uint32_t last_;
} bQueue;

struct twoBucket cal_two_buckets(uint64_t keyhash);

uint32_t alt_bucket(uint32_t b1, uint16_t tag);

void swap_uint(uint32_t *i1, uint32_t *i2);

void lock_two_buckets(Bucket *partition, struct twoBucket twobuckets);

void unlock_two_buckets(Bucket *partition, struct twoBucket twobuckets);

void lock_three_buckets(Bucket *partition, uint32_t b1, uint32_t b2, uint32_t extrab);

twoSnapshot read_two_buckets_begin(Bucket *partition, struct twoBucket tb);

twoSnapshot read_two_buckets_end(Bucket *partition, struct twoBucket tb);

Cbool is_snapshots_same(struct twoSnapshot ts1, struct twoSnapshot ts2);

tablePosition cuckoo_find(Bucket *partition, uint64_t keyhash, struct twoBucket tb, const uint8_t *key,
                          uint32_t keylength);

tablePosition cuckoo_find_shallow(Bucket *partition, struct twoBucket tb, uint64_t offset, uint16_t tag);

tablePosition cuckoo_insert(Bucket *partition, uint64_t keyhash, uint16_t tag, struct twoBucket tb,
                            const uint8_t *key, size_t keylength);

cuckooStatus run_cuckoo(Bucket *partition, struct twoBucket tb, uint32_t *insertbucket, uint32_t *insertslot);

int cuckoopath_search(Bucket *partition, cuckooRecord *cuckoopath, const uint32_t b1, const uint32_t b2);

struct bSlot slot_search(Bucket *partition, const uint32_t b1, const uint32_t b2);

Cbool cuckoopath_move(Bucket *partition, cuckooRecord *cuckoopath, int depth, twoBucket *tb);

