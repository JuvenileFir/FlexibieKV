#ifndef ROUNDHASH_HPP_
#define ROUNDHASH_HPP_

class RoundHash
{
private:
    /* data */
public:
    RoundHash(/* args */);
    ~RoundHash();
    temp getblockNum();
    temp ArcNum();
    temp HashToArc();
    temp ArcToBucket();
    temp NewBucket();
    temp DelBucket();
    temp get_last_short_group_parts();
    temp get_first_long_group_parts();
};

RoundHash::RoundHash(/* args */)
{
}

RoundHash::~RoundHash()
{
}




#endif