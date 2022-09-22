#pragma once

#include "shm.h"
// #include "util.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

// the minimum pool size that will prevent any invalid read with garbage item
// metadata this must be at least as large as the rounded sum of an item header,
// key, and value, and must also be a multiple of shm_get_page_size()
#define MINIMUM_LOG_SIZE (2097152)

EXTERN_BEGIN

void set_item_value(struct log_item *item, const uint8_t *value, uint32_t value_length, uint32_t expire_time);

void set_item(struct log_item *item, uint64_t key_hash, const uint8_t *key, uint32_t key_length, const uint8_t *value,
              uint32_t value_length, uint32_t expire_time);

EXTERN_END
