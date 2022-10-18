#pragma once

#include <cstdint>
using namespace std;



static inline uint64_t rotl(uint64_t x, int k) { return (x << k) | (x >> (64 - k)); }



class RoundHash
{
private:
    
public:
    static uint64_t num_long_arcs_;
    static uint64_t num_short_arc_groups_;
    static uint64_t num_short_arcs_;
    static uint64_t current_s_;
    static uint64_t S_;
    static uint64_t S_log_;
    static uint64_t arc_groups_;
    uint64_t lh_n, lh_l, lh_p;

    
    RoundHash(uint32_t num, uint64_t S);
    ~RoundHash();
    static uint64_t get_block_num();//原 NumBuckets_v()
    static size_t ArcNum(uint64_t divs, uint64_t hash);
    static size_t HashToArc(uint64_t hash);
    static size_t ArcToBucket(size_t arc_num);
    size_t HashToBucket(uint64_t value);//原 RoundHash(uint64_t value)
    void NewBucket();
    void DelBucket();
    void get_parts_to_remove(size_t *parts, size_t *count);
    void get_parts_to_add(size_t *parts, size_t *count);
};