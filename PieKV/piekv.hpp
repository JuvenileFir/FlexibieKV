#pragma once

#ifndef PIEKV_HPP_
#define PIEKV_HPP_

#include "hashtable.hpp"
#include "log.hpp"
#include "basic_hash.h"
#include "cuckoo.h"
#include <thread>

#include <vector>
#include <csignal>


MemPool *kMemPool;

class Piekv
{
private:
    HashTable *hashtable_;
    Log *log_;
    uint32_t stop_entry_gc_;//used for stopping index entry gc when `move_h2t`???
    MemPool *mempool_;


public:
    uint32_t is_running_;
    
    Piekv(int init_log_block_number, int init_block_size, int init_mem_block_number){
        is_running_ = 1;
        stop_entry_gc_ = 0;

        kMemPool = new MemPool(init_block_size, init_mem_block_number);
        mempool_ = kMemPool;
        log_ = new Log(mempool_, init_log_block_number);
        hashtable_ = new HashTable(mempool_);
    }

    ~Piekv(){

    }
    bool H2L(size_t num_pages);   // Q: is num_pages still needed?
    bool L2H(size_t num_pages);   // Q: is num_pages still needed?
    void memFlowingController();


    bool get(size_t t_id, uint64_t key_hash, const uint8_t *key, size_t key_length, uint8_t *out_value, uint32_t *in_out_value_length);
    bool set(size_t t_id, uint64_t key_hash, uint8_t* key, uint32_t key_len, uint8_t* val, uint32_t val_len, bool overwrite);

    bool set_check(uint64_t key_hash, const uint8_t *key, size_t key_length);  // TODO: implement , change name

};


#endif