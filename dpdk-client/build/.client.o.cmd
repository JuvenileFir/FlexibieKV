cmd_client.o = gcc -Wp,-MD,./.client.o.d.tmp  -m64 -pthread -I/home/bwb/dpdk-stable-19.11.5/lib/librte_eal/linux/eal/include  -march=native -DRTE_MACHINE_CPUFLAG_SSE -DRTE_MACHINE_CPUFLAG_SSE2 -DRTE_MACHINE_CPUFLAG_SSE3 -DRTE_MACHINE_CPUFLAG_SSSE3 -DRTE_MACHINE_CPUFLAG_SSE4_1 -DRTE_MACHINE_CPUFLAG_SSE4_2 -DRTE_MACHINE_CPUFLAG_AES -DRTE_MACHINE_CPUFLAG_PCLMULQDQ -DRTE_MACHINE_CPUFLAG_AVX -DRTE_MACHINE_CPUFLAG_RDRAND -DRTE_MACHINE_CPUFLAG_RDSEED -DRTE_MACHINE_CPUFLAG_FSGSBASE -DRTE_MACHINE_CPUFLAG_F16C -DRTE_MACHINE_CPUFLAG_AVX2  -I/home/bwb/dpdk-client/build/include -DRTE_USE_FUNCTION_VERSIONING -I/home/bwb/dpdk-stable-19.11.5/x86_64-native-linuxapp-gcc/include -include /home/bwb/dpdk-stable-19.11.5/x86_64-native-linuxapp-gcc/include/rte_config.h -D_GNU_SOURCE -g -O0     -o client.o -c /home/bwb/dpdk-client/client.c 
