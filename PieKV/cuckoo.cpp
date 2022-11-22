#include <assert.h>
#include "cuckoo.h"

static const uint8_t MAX_BFS_PATH_LEN = 5;
// static const uint16_t const_pow_slot_per_bucket_MAX_BFS_PATH_LEN =
//     (uint16_t)16807;

/* static uint32_t const_pow(uint32_t a, uint32_t b) {
    return (b == 0) ? 1 : a * const_pow(a, b - 1);
} */

static Cbool empty(bQueue que) { return que.first_ == que.last_; }

static Cbool full(bQueue que) { return que.last_ == MAX_CUCKOO_COUNT; }

static void enqueue(bQueue *que, bSlot x) {
  assert(!full(*que));
  que->slots_[que->last_++] = x;
}

static bSlot dequeue(bQueue *que) {
  assert(!empty(*que));
  assert(que->first_ < que->last_);
  bSlot x = que->slots_[que->first_++];
  return x;
}

struct twoBucket cal_two_buckets(uint64_t keyhash) {
  struct twoBucket tb;
  tb.b1 = keyhash & BUCKETS_PER_PARTITION;
  tb.b2 = alt_bucket(tb.b1, calc_tag(keyhash));
  tb.b2 += (tb.b1 == tb.b2);
  assert(tb.b1 != tb.b2);

  if (tb.b1 > tb.b2) swap_uint(&tb.b1, &tb.b2);
  assert(tb.b2 <= BUCKETS_PER_PARTITION);
  return tb;
}

uint32_t alt_bucket(uint32_t b1, uint16_t tag) {
  uint32_t b2 = (b1 ^ (tag * 0xc6a4a7935bd1e995)) & BUCKETS_PER_PARTITION;
  assert(b2 != b1);
  return b2 != b1 ? b2 : (b2 + 1) & BUCKETS_PER_PARTITION;
}

void swap_uint(uint32_t *i1, uint32_t *i2) {
  assert(i1 != i2);
  uint32_t tmp = *i1;
  *i1 = *i2;
  *i2 = tmp;
}

struct twoSnapshot read_two_buckets_begin(Bucket *partition, twoBucket tb) {
  struct twoSnapshot ts;
  ts.v1 = read_version_begin(&partition[tb.b1]);
  ts.v2 = read_version_begin(&partition[tb.b2]);
  return ts;
}

struct twoSnapshot read_two_buckets_end(Bucket *partition, twoBucket tb) {
  struct twoSnapshot ts;
  ts.v1 = read_version_end(&partition[tb.b1]);
  ts.v2 = read_version_end(&partition[tb.b2]);
  return ts;
}

Cbool is_snapshots_same(twoSnapshot ts1, twoSnapshot ts2) { return (ts1.v1 == ts2.v1) && (ts1.v2 == ts2.v2); }

void lock_two_buckets(Bucket *partition, twoBucket twobuckets) {
  // sort_two_bucket(&twobuckets);
  // assert(twobuckets.b1 < twobuckets.b2);
  if (twobuckets.b1 > twobuckets.b2) swap_uint(&twobuckets.b1, &twobuckets.b2);
  write_lock_bucket(&partition[twobuckets.b1]);
  if (twobuckets.b1 != twobuckets.b2) write_lock_bucket(&partition[twobuckets.b2]);
}

void unlock_two_buckets(Bucket *partition, twoBucket twobuckets) {
  // sort_two_bucket(&twobuckets);
  write_unlock_bucket(&partition[twobuckets.b1]);
  write_unlock_bucket(&partition[twobuckets.b2]);
}

void lock_three_buckets(Bucket *partition, uint32_t b1, uint32_t b2, uint32_t extrab) {
  assert(b1 < b2);
  if (extrab < b2) swap_uint(&extrab, &b2);
  if (b2 < b1) swap_uint(&b1, &b2);

  write_lock_bucket(&partition[b1]);
  if (b1 != b2) write_lock_bucket(&partition[b2]);
  if (b2 != extrab) write_lock_bucket(&partition[extrab]);
}

tablePosition cuckoo_find(Bucket *partition, uint64_t keyhash, twoBucket tb, const uint8_t *key,
                          uint32_t keylength) {
  uint16_t tag = calc_tag(keyhash);

  struct tablePosition tpos = {tb.b1, 0, ok};
  tpos.slot = try_read_from_bucket(&partition[tb.b1], tag, key, keylength);
  if (tpos.slot != ITEMS_PER_BUCKET) {
    return tpos;
  }
  tpos.slot = try_read_from_bucket(&partition[tb.b2], tag, key, keylength);
  if (tpos.slot != ITEMS_PER_BUCKET) {
    tpos.bucket = tb.b2;
    return tpos;
  }
  tpos.cuckoostatus = failure_key_not_found;
  return tpos;
}

tablePosition cuckoo_find_shallow(Bucket *partition, twoBucket tb, uint64_t offset, uint16_t tag) {
  struct tablePosition tpos = {tb.b1, 0, ok};
  tpos.slot = try_find_slot(&partition[tb.b1], tag, offset);
  if (tpos.slot != ITEMS_PER_BUCKET) {
    return tpos;
  }
  tpos.slot = try_find_slot(&partition[tb.b2], tag, offset);
  if (tpos.slot != ITEMS_PER_BUCKET) {
    tpos.bucket = tb.b2;
    return tpos;
  }
  tpos.cuckoostatus = failure_key_not_found;
  return tpos;
}

struct tablePosition cuckoo_insert(Bucket *partition, uint64_t keyhash, uint16_t tag, struct twoBucket tb,
                                   const uint8_t *key, size_t keylength) {
  // tablePosition tpos;
  uint32_t res1, res2;
  // lock_two_buckets(partition, tb);
  if (!try_find_insert_bucket(&partition[tb.b1], &res1, tag, key, keylength)) {
    return (tablePosition){tb.b1, res1, failure_key_duplicated};
  }
  if (!try_find_insert_bucket(&partition[tb.b2], &res2, tag, key, keylength)) {
    return (tablePosition){tb.b1, res2, failure_key_duplicated};
  }
  if (res1 != ITEMS_PER_BUCKET) {
    return (tablePosition){tb.b1, res1, ok};
  }
  if (res2 != ITEMS_PER_BUCKET) {
    return (tablePosition){tb.b2, res2, ok};
  }
  res2 = random() % 7;
  return (tablePosition){tb.b2, res2, overwrite};

  uint32_t insertbucket, insertslot;
  cuckooStatus st = run_cuckoo(partition, tb, &insertbucket, &insertslot);
  if (st == ok) {
    assert(is_entry_expired(partition[insertbucket].item_vec[insertslot]));
    /*
     * Since we unlocked the buckets during run_cuckoo, another insert
     * could have inserted the same key into either tb.b1 or
     * tb.b2, so we check for that before doing the insert.
     */
    tablePosition pos = cuckoo_find(partition, keyhash, tb, key, keylength);
    if (pos.cuckoostatus == ok) {
      pos.cuckoostatus = failure_key_duplicated;
      return pos;
    }
    return (tablePosition){insertbucket, insertslot, ok};
  }
  assert(st == failure);
  return (tablePosition){0, 0, failure_table_full};
}

struct tablePosition cuckoo_insert(Bucket *partition, uint64_t keyhash, uint16_t tag, struct twoBucket tb,
                                   const uint8_t *key, size_t keylength, uint64_t *rounds) {
  // tablePosition tpos;
  uint32_t res1, res2;
  // lock_two_buckets(partition, tb);
  if (!try_find_insert_bucket(&partition[tb.b1], &res1, tag, key, keylength, rounds)) {
    return (tablePosition){tb.b1, res1, failure_key_duplicated};
  }
  if (!try_find_insert_bucket(&partition[tb.b2], &res2, tag, key, keylength, rounds)) {
    return (tablePosition){tb.b1, res2, failure_key_duplicated};
  }
  if (res1 != ITEMS_PER_BUCKET) {
    return (tablePosition){tb.b1, res1, ok};
  }
  if (res2 != ITEMS_PER_BUCKET) {
    return (tablePosition){tb.b2, res2, ok};
  }
  res2 = random() % 7;
  return (tablePosition){tb.b2, res2, overwrite};
}

/*
 * run_cuckoo performs cuckoo hashing on the table in an attempt to free up
 * a slot on either of the insert buckets, which are assumed to be locked
 * before the start. On success, the bucket and slot that was freed up is
 * stored in insert_bucket and insert_slot. In order to perform the search
 * and the swaps, it has to release the locks, which can lead to certain
 * concurrency issues, the details of which are explained in the function.
 * If run_cuckoo returns ok (success), then `tb` will be active, otherwise it
 * will not.
 */
enum cuckooStatus run_cuckoo(Bucket *partition, struct twoBucket tb, uint32_t *insertbucket,
                             uint32_t *insertslot) {
  /*
   * We must unlock the buckets here, so that cuckoopath_search and
   * cuckoopath_move can lock buckets as desired without deadlock.
   * cuckoopath_move has to move something out of one of the original
   * buckets as its last operation, and it will lock both buckets and
   * leave them locked after finishing. This way, we know that if
   * cuckoopath_move succeeds, then the buckets needed for insertion are
   * still locked. If cuckoopath_move fails, the buckets are unlocked and
   * we try again. This unlocking does present two problems. The first is
   * that another insert on the same key runs and, finding that the key
   * isn't in the table, inserts the key into the table. Then we insert
   * the key into the table, causing a duplication. To check for this, we
   * search the buckets for the key we are trying to insert before doing
   * so (this is done in cuckoo_insert, and requires that both buckets are
   * locked).
   */
  unlock_two_buckets(partition, tb);
  struct cuckooRecord cuckoopath[MAX_BFS_PATH_LEN];

  Cbool done = false;
  while (!done) {
    int depth = cuckoopath_search(partition, cuckoopath, tb.b1, tb.b2);
    if (depth < 0) break;

    if (cuckoopath_move(partition, cuckoopath, depth, &tb)) {
      *insertbucket = cuckoopath[0].bucket;
      *insertslot = cuckoopath[0].slot;
      assert(*insertbucket == tb.b1 || *insertbucket == tb.b2);
      assert(is_entry_expired(partition[*insertbucket].item_vec[*insertslot]));
      done = true;
      break;
    }
  }
  return done ? ok : failure;
}

/*
 * cuckoopath_search finds a cuckoo path from one of the starting buckets to
 * an empty slot in another bucket. It returns the depth of the discovered
 * cuckoo path on success, and -1 on failure. Since it doesn't take locks on
 * the buckets it searches, the data can change between this function and
 * cuckoopath_move. Thus cuckoopath_move checks that the data matches the
 * cuckoo path before changing it.
 */
int cuckoopath_search(Bucket *partition, cuckooRecord *cuckoopath, const uint32_t b1, const uint32_t b2) {
  bSlot x = slot_search(partition, b1, b2);
  if (x.depth == -1) {
    return -1;
  }
  /* Fill in the cuckoo path slots from the end to the beginning. */
  for (int i = x.depth; i >= 0; i--) {
    cuckoopath[i].slot = x.pathcode % ITEMS_PER_BUCKET;
    x.pathcode /= ITEMS_PER_BUCKET;
  }
  // Fill in the cuckoo_path buckets and keys from the beginning to the
  // end, using the final pathcode to figure out which bucket the path
  // starts on. Since data could have been modified between slot_search
  // and the computation of the cuckoo path, this could be an invalid
  // cuckoo_path.
  cuckooRecord *first = &cuckoopath[0];
  if (x.pathcode == 0) {
    first->bucket = b1;
  } else {
    assert(x.pathcode == 1);
    first->bucket = b2;
  }
  {
    Bucket *b = &partition[first->bucket];
    write_lock_bucket(b);
    if (is_entry_expired(b->item_vec[first->slot])) {
      // We can terminate here
      write_unlock_bucket(b);
      return 0;
    }
    first->tag = TAG(b->item_vec[first->slot]);
    write_unlock_bucket(b);
  }
  for (int i = 1; i <= x.depth; ++i) {
    cuckooRecord *curr = &cuckoopath[i];
    const struct cuckooRecord *prev = &cuckoopath[i - 1];

    // We get the bucket that this slot is on by computing the alternate
    // index of the previous bucket
    curr->bucket = alt_bucket(prev->bucket, prev->tag);
    struct Bucket *b = &partition[curr->bucket];
    write_lock_bucket(b);
    if (is_entry_expired(b->item_vec[curr->slot])) {
      // We can terminate here
      write_unlock_bucket(b);
      return i;
    }
    curr->tag = TAG(b->item_vec[curr->slot]);
    write_unlock_bucket(b);
  }
  return x.depth;
}

/*
 * slot_search searches for a cuckoo path using breadth-first search. It
 * starts with the b1 and b2 buckets, and, until it finds a bucket with an
 * empty slot, adds each slot of the bucket in the bSlot. If the queue runs
 * out of space, it fails.
 */
struct bSlot slot_search(Bucket *partition, const uint32_t b1, const uint32_t b2) {
  bQueue que;
  que.first_ = 0;
  que.last_ = 0;
  // The initial pathcode informs cuckoopath_search which bucket the path
  // starts on
  enqueue(&que, (bSlot){b1, 0, 0});
  enqueue(&que, (bSlot){b2, 1, 0});
  while (!empty(que)) {
    bSlot x = dequeue(&que);
    struct Bucket *b = &partition[x.bucket];
    write_lock_bucket(b);
    // Picks a (sort-of) random slot to start from
    uint32_t startingslot = x.pathcode % ITEMS_PER_BUCKET;
    for (uint32_t i = 0; i < ITEMS_PER_BUCKET; ++i) {
      uint32_t slot = (startingslot + i) % ITEMS_PER_BUCKET;
      if (is_entry_expired(b->item_vec[slot])) {
        // We can terminate the search here
        x.pathcode = x.pathcode * ITEMS_PER_BUCKET + slot;
        write_unlock_bucket(b);
        return x;
      }

      // If x has less than the maximum number of path components,
      // create a new b_slot item, that represents the bucket we would
      // have come from if we kicked out the item at this slot.
      if (x.depth < MAX_BFS_PATH_LEN - 1) {
        assert(!full(que));
        uint32_t alt_bucket_index = alt_bucket(x.bucket, TAG(b->item_vec[slot]));
        if (alt_bucket_index != b1 && alt_bucket_index != b2) {
          bSlot y = {alt_bucket(x.bucket, TAG(b->item_vec[slot])),
                     (uint16_t)(x.pathcode * ITEMS_PER_BUCKET + slot), (int8_t)(x.depth + 1)};
          enqueue(&que, y);
        }
      }
    }
    write_unlock_bucket(b);
  }
  // We didn't find a short-enough cuckoo path, so the search terminated.
  // Return a failure value.
  return (bSlot){0, 0, -1};
}

/*
 * cuckoopath_move moves keys along the given cuckoo path in order to make
 * an empty slot in one of the buckets in cuckoo_insert. Before the start of
 * this function, the two insert-locked buckets were unlocked in run_cuckoo.
 * At the end of the function, if the function returns true (success), then
 * both insert-locked buckets remain locked. If the function is
 * unsuccessful, then both insert-locked buckets will be unlocked.
 */
Cbool cuckoopath_move(Bucket *partition, cuckooRecord *cuckoopath, int depth, twoBucket *tb) {
  if (depth == 0) {
    // There is a chance that depth == 0, when try_add_to_bucket sees
    // both buckets as full and cuckoopath_search finds one empty. In
    // this case, we lock both buckets. If the slot that
    // cuckoopath_search found empty isn't empty anymore, we unlock them
    // and return false. Otherwise, the bucket is empty and insertable,
    // so we hold the locks and return true.
    const uint32_t bucket_i = cuckoopath[0].bucket;
    assert(bucket_i == tb->b1 || bucket_i == tb->b2);
    lock_two_buckets(partition, *tb);
    if (is_entry_expired(partition[bucket_i].item_vec[cuckoopath[0].slot])) {
      return true;
    } else {
      unlock_two_buckets(partition, *tb);
      return false;
    }
  }

  while (depth > 0) {
    cuckooRecord *from = &cuckoopath[depth - 1];
    cuckooRecord *to = &cuckoopath[depth];
    uint32_t fs = from->slot;
    uint32_t ts = to->slot;
    twoBucket twob;
    uint32_t extra_bucket = BUCKETS_PER_PARTITION + 1;
    if (depth == 1) {
      // Even though we are only swapping out of one of the original
      // buckets, we have to lock both of them along with the slot we
      // are swapping to, since at the end of this function, they both
      // must be locked. We store tb inside the extrab container so it
      // is unlocked at the end of the loop.
      if (to->bucket != tb->b1 && to->bucket != tb->b2) {
        extra_bucket = to->bucket;
        twob = *tb;
        lock_three_buckets(partition, tb->b1, tb->b2, to->bucket);
      } else {
        twob = (twoBucket){from->bucket, to->bucket};
        lock_two_buckets(partition, twob);
      }
    } else {
      twob = (twoBucket){from->bucket, to->bucket};
      lock_two_buckets(partition, twob);
    }

    struct Bucket *from_b = &partition[from->bucket];
    struct Bucket *to_b = &partition[to->bucket];

    /*
     * We plan to kick out fs, but let's check if it is still there;
     * there's a small chance we've gotten scooped by a later cuckoo. If
     * that happened, just... try again. Also the slot we are filling in
     * may have already been filled in by another thread, or the slot we
     * are moving from may be empty, both of which invalidate the swap.
     * We only need to check that the hash value is the same, because,
     * even if the keys are different and have the same hash value, then
     * the cuckoopath is still valid.
     */
    // ToThink: Why should it returns false when the slot we are moving
    // from is empty. Why not just step forward to next move?
    if (!is_entry_expired(to_b->item_vec[ts]) || is_entry_expired(from_b->item_vec[fs]) ||
        (TAG(from_b->item_vec[fs]) != from->tag)) {
      if (extra_bucket != BUCKETS_PER_PARTITION + 1) write_unlock_bucket(&partition[extra_bucket]);
      unlock_two_buckets(partition, twob);
      return false;
    }

    to_b->item_vec[ts] = from_b->item_vec[fs];
#ifdef IMPORT_LOG
    printf("[CUK] from(%u, %u) to(%u, %u) entry: %lu\n", from->bucket, from->slot, to->bucket, to->slot,
           from_b->item_vec[fs]);
#endif
    from_b->item_vec[fs] = 0UL;

    if (extra_bucket != BUCKETS_PER_PARTITION + 1) write_unlock_bucket(&partition[extra_bucket]);

    if (depth != 1) {
      // Release the locks contained in twob
      unlock_two_buckets(partition, twob);
    }
    depth--;
  }
  return true;
}

