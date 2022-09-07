#include "fkv.h"
#include "tools/xxhash.h"
// #include "flow_controler.h"

#include "zipf.h"
#include "perf_count/perf_count.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

#include <sched.h>
#include <pthread.h>
#include <thread>
#include <vector>

#define MAX_THREAD_NUM 18
#define MAX_INSTANCES 1

enum PERF_COUNT_TYPE pct[4];
size_t pct_size = sizeof(pct) / sizeof(pct[0]);

size_t set_core_affinity = 1;

perf_count_t benchmark_perf_count_init() {
#ifdef USE_PERF_COUNT
  assert(pct_size == 4);
  pct[0] = perf_count_type_by_name("BranchInstructions");
  pct[1] = perf_count_type_by_name("BranchMisses");
  pct[2] = perf_count_type_by_name("CacheReferences");
  pct[3] = perf_count_type_by_name("CacheMisses");
  perf_count_t pc = perf_count_init(pct, pct_size, 0);
  return pc;
#endif
  return NULL;
}

void benchmark_perf_count_free(perf_count_t pc) {
#ifdef USE_PERF_COUNT
  perf_count_free(pc);
#else
  (void)pc;
#endif
}

void benchmark_perf_count_start(perf_count_t pc) {
#ifdef USE_PERF_COUNT
  perf_count_start(pc);
#else
  (void)pc;
#endif
}

void benchmark_perf_count_stop(perf_count_t pc) {
#ifdef USE_PERF_COUNT
  perf_count_stop(pc);
  size_t i;
  for (i = 0; i < pct_size; i++)
    printf("%-20s: %10lu\n", perf_count_name_by_type(pct[i]), perf_count_get_by_index(pc, i));
  perf_count_reset(pc);
#else
  (void)pc;
#endif
}

static size_t running_threads;

typedef enum _benchmark_mode_t {
  BENCHMARK_MODE_ADD = 0,
  // BENCHMARK_MODE_SET,
  BENCHMARK_MODE_GET_HIT,
  BENCHMARK_MODE_GET_MISS,
  BENCHMARK_MODE_GET_SET_95,
  BENCHMARK_MODE_GET_SET_50,
  // BENCHMARK_MODE_DELETE,
  BENCHMARK_MODE_SET_1,
  BENCHMARK_MODE_GET_1,
  BENCHMARK_MODE_MAX,
} benchmark_mode_t;

typedef enum _concurrency_mode_t {
  CONCURRENCY_MODE_EREW = 0,
  CONCURRENCY_MODE_CREW,
  CONCURRENCY_MODE_CRCW,  // not supported yet
  CONCURRENCY_MODE_CRCWS,
} concurrency_mode_t;

struct proc_arg {
  size_t num_threads;

  size_t key_length;
  size_t value_length;

  size_t op_count;
  uint8_t *op_types;
  uint8_t *op_keys;
  uint64_t *op_key_hashes;
  uint16_t *op_key_parts;
  uint8_t *op_values;

  hash_table *table;
  SlabStore *slabstore;
  DataStore *datastore;

  benchmark_mode_t benchmark_mode;
  concurrency_mode_t concurrency_mode;

  uint64_t success_count;
} __attribute__((aligned(128)));  // To prevent false sharing caused by adjacent cacheline prefetching.

static uint16_t mehcached_get_instance_id(uint64_t key_hash, uint16_t num_instances) {
  return 0;
  // return (uint16_t)(key_hash >> 48) & (uint16_t)(num_instances - 1);
}

void benchmark_proc(struct proc_arg *arg, size_t thread_id) {
  if (set_core_affinity) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(thread_id, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
      fprintf(stderr, "[E] set thread affinity failed\n");
    }
  }

  struct proc_arg *p_arg = (struct proc_arg *)arg;

  const size_t num_threads = p_arg->num_threads;

  size_t key_length = p_arg->key_length;
  uint32_t value_length = p_arg->value_length;

  const int64_t op_count = (int64_t)p_arg->op_count;
  const uint8_t *op_types = p_arg->op_types;
  uint8_t *op_keys = p_arg->op_keys;
  uint64_t *op_key_hashes = p_arg->op_key_hashes;
  // const uint16_t *op_key_parts = p_arg->op_key_parts;
  uint8_t *op_values = p_arg->op_values;

  hash_table *table = p_arg->table;
  SlabStore *slab_store = p_arg->slabstore;

  benchmark_mode_t benchmark_mode = p_arg->benchmark_mode;

  uint64_t success_count = 0;

  int64_t i;
  uint8_t value[value_length] __attribute__((aligned(8)));  // For 8-byte aligned access.

  __sync_add_and_fetch((volatile size_t *)&running_threads, 1);
  while (*(volatile size_t *)&running_threads < num_threads)
    ;

  switch (benchmark_mode) {
    case BENCHMARK_MODE_ADD:
      for (i = 0; i < op_count; i++) {
        // hash_table *table = tables[op_key_parts[i]];
        if (fkv_set(thread_id, table, slab_store, op_key_hashes[i], op_keys + (size_t)i * key_length, key_length,
                    op_values + (size_t)i * value_length, value_length, VALID, false))
          success_count++;
      }
      break;
    // case BENCHMARK_MODE_SET:
    case BENCHMARK_MODE_GET_HIT:
    case BENCHMARK_MODE_GET_MISS:
    case BENCHMARK_MODE_GET_SET_95:
    case BENCHMARK_MODE_GET_SET_50:
    case BENCHMARK_MODE_SET_1:
    case BENCHMARK_MODE_GET_1:
      for (i = 0; i < op_count; i++) {
        // hash_table *table = tables[op_key_parts[i]];
        bool is_get = op_types[i] == 0;
        if (is_get) {
          // uint8_t value[value_length];
          if (get(table, slab_store, op_key_hashes[i], op_keys + (size_t)i * key_length, key_length, value,
                  &value_length, NULL, false))
            success_count++;
        } else {
          if (fkv_set(thread_id, table, slab_store, op_key_hashes[i], op_keys + (size_t)i * key_length, key_length,
                      op_values + (size_t)i * value_length, value_length, VALID, true))
            success_count++;
        }
      }
      break;
    // case BENCHMARK_MODE_DELETE:
    //     for (i = -PREFETCH_GAP * 2; i < op_count; i++)
    //     {
    //         if (i >= 0)
    //         {
    //             struct mehcached_table *table = tables[op_key_parts[i]];
    //             if (mehcached_delete(alloc_id, table, op_key_hashes[i],
    //             op_keys + (size_t)i * key_length, key_length))
    //                 success_count++;
    //         }
    //     }
    //     break;
    default:
      assert(false);
  }

  p_arg->success_count = success_count;
}

void benchmark(double zipf_theta, size_t pagesOfSotre, size_t flowMode, size_t flowUnitPages) {
  printf("benchmark\n");

  printf("zipf_theta = %lf\n", zipf_theta);

  const size_t num_items = 255987;  // 16 * 1048576;
  const size_t num_instances = 1;

  const size_t num_threads = NUM_THREAD;
  const size_t num_operations = 16 * 1048576;
  const size_t max_num_operatios_per_thread = num_operations;

  const size_t key_length = ROUNDUP8(8);
  const size_t value_length = ROUNDUP8(8);

  // printf("initializing shm\n");
  // const size_t page_size = 1048576 * 2;
  // const size_t num_numa_nodes = 2;
  // const size_t num_pages_to_try = 16384;
  // const size_t num_pages_to_reserve = 16384 - 2048;  // give 2048 pages to dpdk

  printf("allocating memory\n");
  uint8_t *keys = (uint8_t *)malloc(key_length * num_items * 2);
  assert(keys);
  uint64_t *key_hashes = (uint64_t *)malloc(sizeof(uint64_t) * num_items * 2);
  assert(key_hashes);
  uint16_t *key_parts = (uint16_t *)malloc(sizeof(uint16_t) * num_items * 2);
  assert(key_parts);
  uint8_t *values = (uint8_t *)malloc(value_length * num_items * 2);
  assert(values);

  uint64_t *op_count = (uint64_t *)malloc(sizeof(uint64_t) * num_threads);
  assert(op_count);
  uint8_t **op_types = (uint8_t **)malloc(sizeof(uint8_t *) * num_threads);
  assert(op_types);
  uint8_t **op_keys = (uint8_t **)malloc(sizeof(uint8_t *) * num_threads);
  assert(op_keys);
  uint64_t **op_key_hashes = (uint64_t **)malloc(sizeof(uint64_t *) * num_threads);
  assert(op_key_hashes);
  uint16_t **op_key_parts = (uint16_t **)malloc(sizeof(uint16_t *) * num_threads);
  assert(op_key_parts);
  uint8_t **op_values = (uint8_t **)malloc(sizeof(uint8_t *) * num_threads);
  assert(op_values);

  size_t thread_id;
  for (thread_id = 0; thread_id < num_threads; thread_id++) {
    op_types[thread_id] = (uint8_t *)malloc(num_operations);
    assert(op_types[thread_id]);
    op_keys[thread_id] = (uint8_t *)malloc(key_length * num_operations);
    assert(op_keys[thread_id]);
    op_key_hashes[thread_id] = (uint64_t *)malloc(sizeof(uint64_t) * num_operations);
    assert(op_key_hashes[thread_id]);
    op_key_parts[thread_id] = (uint16_t *)malloc(sizeof(uint16_t) * num_operations);
    assert(op_key_parts[thread_id]);
    op_values[thread_id] = (uint8_t *)malloc(value_length * num_operations);
    assert(op_values[thread_id]);
  }

  hash_table table;
  DataStore data_store;

  struct proc_arg args[num_threads];

  // size_t mem_diff = (size_t)-1;
  double add_ops = -1.;
  double set_ops = -1.;
  double get_hit_ops = -1.;
  double get_miss_ops = -1.;
  double get_set_95_ops = -1.;
  double get_set_50_ops = -1.;
  double delete_ops = -1.;
  double set_1_ops = -1.;
  double get_1_ops = -1.;

  size_t i;
  struct timeval tv_start;
  struct timeval tv_end;
  double diff;

  printf("generating %zu items (including %zu miss items)\n", num_items, num_items);
  for (i = 0; i < num_items * 2; i++) {
    *(size_t *)(keys + i * key_length) = i;
    *(key_hashes + i) = XXH64(keys + i * key_length, key_length, 1);
    *(key_parts + i) = mehcached_get_instance_id(*(key_hashes + i), (uint16_t)num_instances);
    *(size_t *)(values + i * value_length) = i + 1;
  }
  printf("\n");

  perf_count_t pc = benchmark_perf_count_init();

  slabStore_init(&data_store, pagesOfSotre);
  hash_table_init(&table, &data_store);

  // size_t instance_id;
  // for (instance_id = 0; instance_id < num_instances; instance_id++) {
  //   tables[instance_id] = (hash_table *)malloc(sizeof(hash_table));
  //   assert(tables[instance_id]);
  //   hash_table_init(tables[instance_id]);
  // }

  for (thread_id = 0; thread_id < num_threads; thread_id++) {
    args[thread_id].num_threads = num_threads;
    args[thread_id].key_length = key_length;
    args[thread_id].value_length = value_length;
    args[thread_id].op_types = op_types[thread_id];
    args[thread_id].op_keys = op_keys[thread_id];
    args[thread_id].op_key_hashes = op_key_hashes[thread_id];
    args[thread_id].op_key_parts = op_key_parts[thread_id];
    args[thread_id].op_values = op_values[thread_id];
    args[thread_id].slabstore = &data_store.slab_store[thread_id];
    args[thread_id].datastore = &data_store;
    args[thread_id].table = &table;
  }

  benchmark_mode_t benchmark_mode;
  for (benchmark_mode = BENCHMARK_MODE_ADD; benchmark_mode < BENCHMARK_MODE_MAX;
       benchmark_mode = (benchmark_mode_t)(benchmark_mode + 1)) {
    switch (benchmark_mode) {
      case BENCHMARK_MODE_ADD:
        printf("adding %zu items\n", num_items);
        break;
      // case BENCHMARK_MODE_SET:
      //   printf("setting %zu items\n", num_items);
      //   break;
      case BENCHMARK_MODE_GET_HIT:
        printf("getting %zu items (hit)\n", num_items);
        break;
      case BENCHMARK_MODE_GET_MISS:
        printf("getting %zu items (miss)\n", num_items);
        break;
      case BENCHMARK_MODE_GET_SET_95:
        printf("getting/setting %zu items (95%% get)\n", num_items);
        break;
      case BENCHMARK_MODE_GET_SET_50:
        printf("getting/setting %zu items (50%% get)\n", num_items);
        break;
      case BENCHMARK_MODE_SET_1:
        printf("setting 1 item\n");
        break;
      case BENCHMARK_MODE_GET_1:
        printf("getting 1 item\n");
        break;
      default:
        assert(false);
    }

    printf("generating workload\n");
    // uint64_t thread_rand_state = 1;
    // uint64_t key_rand_state = 2;
    uint64_t op_type_rand_state = 3;

    uint32_t get_threshold = 0;
    if (benchmark_mode == BENCHMARK_MODE_ADD  // || benchmark_mode == BENCHMARK_MODE_SET
        || benchmark_mode == BENCHMARK_MODE_SET_1)
      get_threshold = (uint32_t)(0.0 * (double)((uint32_t)-1));
    else if (benchmark_mode == BENCHMARK_MODE_GET_HIT || benchmark_mode == BENCHMARK_MODE_GET_MISS ||
             benchmark_mode == BENCHMARK_MODE_GET_1)
      get_threshold = (uint32_t)(1.0 * (double)((uint32_t)-1));
    else if (benchmark_mode == BENCHMARK_MODE_GET_SET_95)
      get_threshold = (uint32_t)(0.95 * (double)((uint32_t)-1));
    else if (benchmark_mode == BENCHMARK_MODE_GET_SET_50)
      get_threshold = (uint32_t)(0.5 * (double)((uint32_t)-1));
    else
      assert(false);

    struct zipf_gen_state zipf_state;
    mehcached_zipf_init(&zipf_state, num_items, zipf_theta, (uint64_t)benchmark_mode);

    memset(op_count, 0, sizeof(uint64_t) * num_threads);
    size_t j;
    for (j = 0; j < num_operations; j++) {
      size_t i;
      // if (benchmark_mode == BENCHMARK_MODE_ADD || benchmark_mode ==
      // BENCHMARK_MODE_DELETE)
      if (benchmark_mode == BENCHMARK_MODE_ADD) {
        if (j >= num_items) break;
        i = j;
      } else if (benchmark_mode == BENCHMARK_MODE_GET_1 || benchmark_mode == BENCHMARK_MODE_SET_1)
        i = 0;
      else {
        // i = mehcached_rand(&key_rand_state) % num_items;
        i = mehcached_zipf_next(&zipf_state);
        if (benchmark_mode == BENCHMARK_MODE_GET_MISS) i += num_items;
      }

      uint32_t op_r = mehcached_rand(&op_type_rand_state);
      bool is_get = op_r <= get_threshold;

      size_t thread_id;
      thread_id = calc_partition_index(key_hashes[i], (Cbool)0 ^ table.current_version) % NUM_THREAD;
      // if (is_get) {
      //   if (concurrency_mode <= CONCURRENCY_MODE_EREW)
      //     thread_id = owner_thread_id[instance_id];
      //   else
      //     thread_id = (owner_thread_id[instance_id] % 2) + (mehcached_rand(&thread_rand_state) % (num_threads /
      //     2)) * 2;
      // } else {
      //   if (concurrency_mode <= CONCURRENCY_MODE_CREW)
      //     thread_id = owner_thread_id[instance_id];
      //   else
      //     thread_id = (owner_thread_id[instance_id] % 2) + (mehcached_rand(&thread_rand_state) % (num_threads /
      //     2)) * 2;
      // }

      if (op_count[thread_id] < max_num_operatios_per_thread) {
        op_types[thread_id][op_count[thread_id]] = is_get ? 0 : 1;
        memcpy(op_keys[thread_id] + key_length * op_count[thread_id], keys + key_length * i, key_length);
        op_key_hashes[thread_id][op_count[thread_id]] = key_hashes[i];
        op_key_parts[thread_id][op_count[thread_id]] = key_parts[i];
        memcpy(op_values[thread_id] + value_length * op_count[thread_id], values + value_length * i, value_length);
        op_count[thread_id]++;
      } else
        break;
    }

    printf("executing workload\n");

    for (thread_id = 0; thread_id < num_threads; thread_id++) {
      args[thread_id].op_count = op_count[thread_id];
      args[thread_id].benchmark_mode = benchmark_mode;
      args[thread_id].success_count = 0;
    }

    benchmark_perf_count_start(pc);
    gettimeofday(&tv_start, NULL);

    running_threads = 0;
    std::vector<std::thread> threads;
    memory_barrier();

    for (thread_id = 0; thread_id < num_threads; thread_id++) {
      threads.push_back(std::thread(benchmark_proc, &args[thread_id], thread_id));
    }
    for (auto &t : threads) {
      t.join();
    }

    gettimeofday(&tv_end, NULL);
    benchmark_perf_count_stop(pc);
    diff = (double)(tv_end.tv_sec - tv_start.tv_sec) * 1. + (double)(tv_end.tv_usec - tv_start.tv_usec) * 0.000001;

    size_t success_count = 0;
    size_t operations = 0;
    for (thread_id = 0; thread_id < num_threads; thread_id++) {
      success_count += args[thread_id].success_count;
      operations += args[thread_id].op_count;
    }

    printf("operations: %zu\n", operations);
    printf("success_count: %zu\n", success_count);

    switch (benchmark_mode) {
      case BENCHMARK_MODE_ADD:
        add_ops = (double)operations / diff;
        break;
      // case BENCHMARK_MODE_SET:
      //   set_ops = (double)operations / diff;
      //   break;
      case BENCHMARK_MODE_GET_HIT:
        get_hit_ops = (double)operations / diff;
        break;
      case BENCHMARK_MODE_GET_MISS:
        get_miss_ops = (double)operations / diff;
        break;
      case BENCHMARK_MODE_GET_SET_95:
        get_set_95_ops = (double)operations / diff;
        break;
      case BENCHMARK_MODE_GET_SET_50:
        get_set_50_ops = (double)operations / diff;
        break;
      // case BENCHMARK_MODE_DELETE:
      //     delete_ops = (double)operations / diff;
      //     break;
      case BENCHMARK_MODE_SET_1:
        set_1_ops = (double)operations / diff;
        break;
      case BENCHMARK_MODE_GET_1:
        get_1_ops = (double)operations / diff;
        break;
      default:
        assert(false);
    }

    for (size_t instance_id = 0; instance_id < num_instances; instance_id++) {
      print_table_stats(&table);
      clean_stats(&table);
    }

    printf("\n");
  }

  benchmark_perf_count_free(pc);

  printf("add:        %10.2lf Mops\n", add_ops * 0.000001);
  printf("set:        %10.2lf Mops\n", set_ops * 0.000001);
  printf("get_hit:    %10.2lf Mops\n", get_hit_ops * 0.000001);
  printf("get_miss:   %10.2lf Mops\n", get_miss_ops * 0.000001);
  printf("get_set_95: %10.2lf Mops\n", get_set_95_ops * 0.000001);
  printf("get_set_50: %10.2lf Mops\n", get_set_50_ops * 0.000001);
  printf("delete:     %10.2lf Mops\n", delete_ops * 0.000001);
  printf("set_1:      %10.2lf Mops\n", set_1_ops * 0.000001);
  printf("get_1:      %10.2lf Mops\n", get_1_ops * 0.000001);
}

int main(int argc, char *argv[]) {
  // std::signal(SIGINT, sigint_handler);

  const size_t page_size = 1048576 * 2;
  const size_t num_pages_to_try = 60;
  shm_init(page_size, num_pages_to_try);

  int ch;
  size_t pages = 20;
  size_t flow_mode = 0, flow_pages = 0;
  double zipf_theta = 0.0;
  while ((ch = getopt(argc, argv, "hcs:p:f:n:d:")) != -1) {
    switch (ch) {
      case 'h':
        printf(
            " Usage: %s [-s <workload>] [-p <pages for log>] "
            "[-f <flow "
            "mode>] [-n <flow pages>]\n",
            argv[0]);
        exit(0);
      case 's':
        // multiple = atoi(optarg);
        break;
      case 'p':
        pages = atoi(optarg);
        break;
      case 'f':
        flow_mode = atoi(optarg);
        break;
      case 'n':
        flow_pages = atoi(optarg);
        break;
      case 'c':
        set_core_affinity = 0;
        break;
      case 'd':
        zipf_theta = atof(optarg);
        break;
      default:
        printf("Error: unknown option: %c\n", (char)optopt);
        break;
    }
  }

  benchmark(zipf_theta, pages, flow_mode, flow_pages);

  return EXIT_SUCCESS;
}
