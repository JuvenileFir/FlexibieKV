/********** Code for Test & Debug **********/

#include "debug.h"

EXTERN_BEGIN

void print_item(const char *str, log_item *item) {
  static uint64_t key;
  static Cbool is_odd = 0;
  if (!is_odd) {
    is_odd = !is_odd;
    key = *(uint64_t *)item->data;
  } else {
    is_odd = !is_odd;
    assert(key == *(uint64_t *)item->data);
  }
  printf("[%s] item_size: %lu\t ", str, item->item_size);
  printf("kv_vec: %u\t ", item->kv_length_vec);
  printf("expire_time: %u\t ", item->expire_time);
  printf("key_hash: %lu\t ", item->key_hash);
  printf("key: %lu\t value: %lu\n", *(uint64_t *)item->data, *(((uint64_t *)item->data) + 1));
  fflush(stdout);
  assert(*(uint64_t *)item->data == *(((uint64_t *)item->data) + 1) - 1);
}

EXTERN_END
