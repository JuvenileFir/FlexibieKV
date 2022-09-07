#!/bin/bash


sudo ./mem_flowing -p 120 -f 3 > ../data/exp3/latency_4_dynamic_50_Zip.txt
sudo ./mem_flowing -p 12000 -f 3 > ../data/exp3/latency_4_static_50_Zip.txt
sudo ./mem_flowing -p 120 -f 3 > ../data/exp3/latency_4_dynamic_95_Zip.txt
sudo ./mem_flowing -p 12000 -f 3 > ../data/exp3/latency_4_static_95_Zip.txt

sudo ./mem_flowing -p 120 -f 3 > ../data/exp3/latency_4_dynamic_50_Uni.txt
sudo ./mem_flowing -p 12000 -f 3 > ../data/exp3/latency_4_static_50_Uni.txt
sudo ./mem_flowing -p 120 -f 3 > ../data/exp3/latency_4_dynamic_95_Uni.txt
sudo ./mem_flowing -p 12000 -f 3 > ../data/exp3/latency_4_static_95_Uni.txt

