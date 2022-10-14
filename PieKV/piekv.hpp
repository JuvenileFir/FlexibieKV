#ifndef PIEKV_HPP_
#define PIEKV_HPP_

#include "hashtable.hpp"
#include "log.hpp"

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
    bool get();
    bool set();
};

Piekv::Piekv(/* args */){
    
    is_running_ = 1;
    stop_entry_gc_ = 0;

}

Piekv::~Piekv(){
}



#endif