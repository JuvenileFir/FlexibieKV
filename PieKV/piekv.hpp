#ifndef PIEKV_HPP_
#define PIEKV_HPP_

#include "hashtable.hpp"
#include "log.hpp"

class Piekv
{
private:
    HashTable hashTable;
    Log log;
    MemPool memPool;
    uint32_t is_running;
    uint32_t stop_entry_gc;//used for stopping index entry gc when `move_h2t`???

public:
    Piekv(/* args */);
    ~Piekv();
    temp H2L();
    temp L2H();
    temp memFlowingController();
    temp get();
    temp set();
};

Piekv::Piekv(/* args */){
    
    is_running = 1;
    stop_entry_gc = 0;

}

Piekv::~Piekv(){
}



#endif