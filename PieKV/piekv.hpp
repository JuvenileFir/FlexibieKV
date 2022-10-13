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

public:
    Piekv(/* args */);
    ~Piekv();
    bool H2L(size_t num_pages);   // Q: is num_pages still needed?
    bool L2H(size_t num_pages);   // Q: is num_pages still needed?
    void memFlowingController();
    bool get();
    bool set();
};

Piekv::Piekv(/* args */)
{
}

Piekv::~Piekv()
{
}



#endif