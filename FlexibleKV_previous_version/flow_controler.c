#include "flow_controler.h"

EXTERN_BEGIN

void mem_flowing_controler(hash_table *table) {
  printf(" == [STAT] Memory flowing controler started on CORE 34 == \n");
  cpu_set_t mask;
  CPU_ZERO(&mask);
  CPU_SET(34, &mask);
  if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
    fprintf(stderr, "[E] set thread affinity failed\n");
  }

  const double factor = 1.0;
  const double threshold_log = 0.9;
  const double threshold_hashtable = 0.85;

  size_t store_load = 0;
  size_t index_load = 0;
  size_t store_capa, index_capa;

  double load_factor;
  double vaild_percentage;

#ifdef EXP_MEM_EFFICIENCY
  static uint32_t times = 0;
#endif

  while (table->is_running) {
    sleep(5);
    

    store_load = 0;
    index_load = 0;
    for (uint16_t i = 0; i < table->stores->numStores; i++) {
      store_load += table->stores->slab_store[i].sstats.actual_used_mem + table->stores->slab_store[i].sstats.wasted;
      index_load += table->stores->slab_store[i].tstats.count;
    }
    store_capa = table->stores->totalNumPage * shm_get_page_size();
    index_capa = table->num_partitions * BUCKETS_PER_PARTITION * ITEMS_PER_BUCKET;
  
    vaild_percentage = store_load * factor / store_capa;
    load_factor = index_load * factor / index_capa;
    //Index less & Store more
    if (load_factor < threshold_hashtable && vaild_percentage >= threshold_log) {
      PRINT_EXCECUTION_TIME("  === [STAT] H2L is executed by Daemon === ", page_usage_balance_H2L(table, 1));
#ifdef MULTIPLE_SHIFT
      int segment_num = table->num_partitions;
      printf("         segment num: %d\n", segment_num);
      for (int i = 1; i < segment_num; i++) {
        PRINT_EXCECUTION_TIME("        ### expenentially shifting pages ### ", page_usage_balance_H2L(table, 1));
      }
#endif
      /* ######### For macro-benchmarks that tests memory efficiency ######### */
#ifdef EXP_MEM_EFFICIENCY
      printf("EXP1: %04u\t%lf\n", times++, (index_load + store_load) * factor / (index_capa + store_capa));
      printf("      %lf\t%lf\n", vaild_percentage, load_factor);
      fflush(stdout);
#endif
      /* --------------------------------------------------------------------- */
    //Index more & Store less
    } else if (load_factor >= threshold_hashtable && vaild_percentage < threshold_log) {
      PRINT_EXCECUTION_TIME("  === [STAT] L2H is executed by Daemon === ", page_usage_balance_L2H(table, 1));
#ifdef MULTIPLE_SHIFT
      int segment_num = table->num_partitions;
      printf("         segment num: %d\n", segment_num);
      for (int i = 1; i < segment_num; i++) {
        PRINT_EXCECUTION_TIME("        ### expenentially shifting pages ### ", page_usage_balance_L2H(table, 1));
      }
#endif
      /* ######### For macro-benchmarks that tests memory efficiency ######### */
#ifdef EXP_MEM_EFFICIENCY
      printf("EXP1: %04u\t%lf\n", times++, (index_load + store_load) * factor / (index_capa + store_capa));
      printf("      %lf\t%lf\n", vaild_percentage, load_factor);
      fflush(stdout);
#endif
      /* --------------------------------------------------------------------- */
    }
  }
}

Cbool page_usage_balance_H2L(struct hash_table *table, size_t num_pages) {
  // assert(num_pages < NumBuckets_v(current_version));
  if (!(num_pages < NumBuckets_v(table->current_version))) {
    fprintf(stderr, "Too few partitions for expanding Log\n");
    usleep(500);
    return (Cbool)0;
  }
  printf("[ARGS](H2L) to_shrink = %zu\t log = %u\t partition = %u\n", num_pages, table->stores->totalNumPage,
         table->num_partitions);

  size_t count;
  size_t parts[S_ << 1];
  size_t delta_pages = num_pages;

  get_last_short_group_parts(parts, &count, table->current_version);
  DelBucket_v(table->current_version);

  __sync_fetch_and_add((volatile uint32_t *)&(table->is_setting), 1U);
  while (*(volatile uint32_t *)&(table->is_setting) != 1U)
    ;
  *(volatile uint32_t *)&(table->is_flexibling) = 1U;
  __sync_fetch_and_sub((volatile uint32_t *)&(table->is_setting), 1U);

#ifdef IMPORT_LOG
  printf(
      "[DIS](%zu) %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t "
      "%zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t",
      count, parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], parts[8], parts[9],
      parts[10], parts[11], parts[12], parts[13], parts[14], parts[15]);
#endif

  redistribute_last_short_group(table, parts, count);
  num_pages--;

  table->num_partitions -= delta_pages;
  // ToThink: Dose `table->num_partitions` order important?
  __sync_fetch_and_add((volatile uint32_t *)&(table->is_setting), 1U);
  while (*(volatile uint32_t *)&(table->is_setting) != 1U)
    ;
  uint64_t v = (uint64_t)(!table->current_version) << 32;//?????
  *(volatile uint64_t *)&(table->is_flexibling) = v;//?????table->is_flexibling此步确定赋0，同时多覆盖了后面4byte，即current_version
  /* clean is_flexibling and flip current_version in a single step */
  __sync_fetch_and_sub((volatile uint32_t *)&(table->is_setting), 1U);

  // Append page(s) to SlabStore in round robin.
  store_expansion(table->stores, table->num_partitions, delta_pages);

  return true;
}

Cbool page_usage_balance_L2H(hash_table *table, size_t num_pages) {
  // assert(num_pages < table->stores->totalNumPage);
  if (!(num_pages < table->stores->totalNumPage)) {
    fprintf(stderr, "Too few memory hold by log for expanding Hash table\n");
    usleep(500);
    return (Cbool)0;
  }
  printf("[ARGS](L2H) to_shrink = %zu\t log = %u\t partition = %u\n", num_pages, table->stores->totalNumPage,
         table->num_partitions);

  store_shrink(table->stores, num_pages);

  size_t count;
  size_t delta_pages = num_pages;
  size_t parts[S_ << 1];

  get_first_long_group_parts(parts, &count, table->current_version);
  NewBucket_v(table->current_version);

  __sync_fetch_and_add((volatile uint32_t *)&(table->is_setting), 1U);
  while (*(volatile uint32_t *)&(table->is_setting) != 1U)
    ;
  *(volatile uint32_t *)&(table->is_flexibling) = 1U;
  __sync_fetch_and_sub((volatile uint32_t *)&(table->is_setting), 1U);

#ifdef IMPORT_LOG
  printf(
      "[DIS](%zu) %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\t "
      "%zu\t %zu\t %zu\t %zu\t %zu\t %zu\t %zu\n",
      count, parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], parts[8], parts[9],
      parts[10], parts[11], parts[12], parts[13], parts[14], parts[15]);
#endif

  redistribute_first_long_group(table, parts, count);
  num_pages--;

  table->num_partitions += delta_pages;
  // ToThink: Dose `table->num_partitions` order important?
  __sync_fetch_and_add((volatile uint32_t *)&(table->is_setting), 1U);
  while (*(volatile uint32_t *)&(table->is_setting) != 1U)
    ;
  uint64_t v = (uint64_t)(!table->current_version) << 32;
  *(volatile uint64_t *)&(table->is_flexibling) = v;
  /* clean is_flexibling and flip current_version in a single step */
  __sync_fetch_and_sub((volatile uint32_t *)&(table->is_setting), 1U);

  return true;
}

void store_expansion(DataStore *data_store, uint64_t num_partitons, uint64_t num_pages) {
  for (uint16_t i = 0; i < num_pages; i++) {
    uint16_t id = data_store->expandPointer;
    SlabStore *pstore = &data_store->slab_store[id];
    pstore->slabs[pstore->numPages].pheader = (uint8_t *)partition_header(num_partitons + i);
    pstore->slabs[pstore->numPages].pageNumber = partition_pageNumber(num_partitons + i);
    pstore->slabs[pstore->numPages].empty = shm_get_page_size();
    __sync_fetch_and_add((uint32_t *)&(pstore->numPages), 1U);
    __sync_fetch_and_add((uint16_t *)&(data_store->totalNumPage), 1U);

    uint16_t next = (data_store->expandPointer + 1) % data_store->numStores;
    __sync_bool_compare_and_swap((uint16_t *)&data_store->expandPointer, id, next);
  }
}

void store_shrink(DataStore *data_store, uint64_t num_pages) {
  for (uint16_t i = 0; i < num_pages; i++) {
    // -1 <==> (+3) % 4 // For numStores = 4
    uint16_t id = (data_store->expandPointer + data_store->numStores - 1) % data_store->numStores;
    SlabStore *pstore = &data_store->slab_store[id];
    if (pstore->numPages > 1 && pstore->usingPage < pstore->numPages - 1) {
      __sync_fetch_and_sub((uint32_t *)&(pstore->numPages), 1U);
      __sync_fetch_and_sub((uint16_t *)&(data_store->totalNumPage), 1U);

      partitionMap_add(pstore->slabs[pstore->numPages].pheader, pstore->slabs[pstore->numPages].pageNumber);
    } else {
      num_pages++;
    }
    data_store->expandPointer = id;
  }
}

EXTERN_END