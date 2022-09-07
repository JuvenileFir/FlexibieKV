#include "basic_log.h"

EXTERN_BEGIN

void set_item(struct log_item *item, uint64_t key_hash, const uint8_t *key, uint32_t key_length, const uint8_t *value,
              uint32_t value_length, uint32_t expire_time) {
  assert(key_length <= ITEMKEY_MASK);
  assert(value_length <= ITEMVALUE_MASK);

  item->kv_length_vec = ITEMKV_LENGTH_VEC(key_length, value_length);
  item->key_hash = key_hash;
  item->expire_time = expire_time;
  memcpy8(item->data, key, key_length);
  memcpy8(item->data + ROUNDUP8(key_length), value, value_length);
}

void set_item_value(struct log_item *item, const uint8_t *value, uint32_t value_length, uint32_t expire_time) {
  assert(value_length <= ITEMVALUE_MASK);

  uint32_t key_length = ITEMKEY_LENGTH(item->kv_length_vec);
  item->kv_length_vec = ITEMKV_LENGTH_VEC(key_length, value_length);
  item->expire_time = expire_time;
  memcpy8(item->data + ROUNDUP8(key_length), value, value_length);
}

EXTERN_END
