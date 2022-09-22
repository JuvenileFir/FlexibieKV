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
    temp H2L();
    temp L2H();
    temp memFlowingController();
    temp get();
    temp set();
};

Piekv::Piekv(/* args */)
{
}

Piekv::~Piekv()
{
}



#endif