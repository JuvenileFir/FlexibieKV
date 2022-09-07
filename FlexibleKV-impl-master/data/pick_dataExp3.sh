#!/bin/bash

. /home/hewen/FlexibleKV-impl-newArc/stats.sh

get_lantency_ave latency_4_dynamic_50_Uni.txt SET succ
get_lantency_ave latency_4_static_50_Uni.txt SET succ
get_lantency_ave latency_4_dynamic_50_Zip.txt SET succ
get_lantency_ave latency_4_static_50_Zip.txt SET succ

get_lantency_ave latency_4_dynamic_50_Uni.txt SET false
get_lantency_ave latency_4_static_50_Uni.txt SET false
get_lantency_ave latency_4_dynamic_50_Zip.txt SET false
get_lantency_ave latency_4_static_50_Zip.txt SET false

get_lantency_ave latency_4_dynamic_50_Uni.txt GET succ
get_lantency_ave latency_4_static_50_Uni.txt GET succ
get_lantency_ave latency_4_dynamic_50_Zip.txt GET succ
get_lantency_ave latency_4_static_50_Zip.txt GET succ

get_lantency_ave latency_4_dynamic_50_Uni.txt GET false
get_lantency_ave latency_4_static_50_Uni.txt GET false
get_lantency_ave latency_4_dynamic_50_Zip.txt GET false
get_lantency_ave latency_4_static_50_Zip.txt GET false