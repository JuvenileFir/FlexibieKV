#ifndef PIEKV_HPP_
#define PIEKV_HPP_

#include "hashtable.hpp"
#include "log.hpp"
#include "basic_hash.h"
#include "cuckoo.h"

class Piekv
{
private:
    HashTable hashtable_;
    Log log_;
    MemPool mempool_;
    uint32_t is_running_;
    uint32_t stop_entry_gc_;//used for stopping index entry gc when `move_h2t`???

public:
    Piekv(/* args */);
    ~Piekv();
    bool H2L(size_t num_pages);   // Q: is num_pages still needed?
    bool L2H(size_t num_pages);   // Q: is num_pages still needed?
    void memFlowingController();
    bool get(LogSegment *segmentToGet, uint64_t key_hash, const uint8_t *key, size_t key_length, uint8_t *out_value, uint32_t *in_out_value_length);
    bool set(LogSegment *segmentToSet, uint64_t key_hash, uint8_t* key, uint32_t key_len, uint8_t* val, uint32_t val_len, bool overwrite);

    bool orphan_chk();  // TODO: implement , change name
};

Piekv::Piekv(/* args */){
    
    is_running_ = 1;
    stop_entry_gc_ = 0;

}

Piekv::~Piekv(){
}



#endif