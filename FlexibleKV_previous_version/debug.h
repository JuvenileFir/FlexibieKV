/********** Code for Test & Debug **********/

#pragma once

#include "shm.h"
#include "slab_store.h"
#include "basic_hash.h"
#include "cuckoo.h"
#include "table.h"
#include "partition.h"

#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>

EXTERN_BEGIN

void print_item(const char *, log_item *item);

EXTERN_END
