CXXFLAGS = $(shell pkg-config --cflags ) -std=c++11 -O2 -pthread -mavx2 -march=native -fopenmp -DD_GLIBCXX_PARALLEL

LDFLAGS = $(shell pkg-config --libs ) -O2 -pthread -mavx2 -march=native -fopenmp -DD_GLIBCXX_PARALLEL

include $(RTE_SDK)/mk/rte.vars.mk
include $(RTE_SDK)/mk/rte.extapp.mk

CXX = g++

# all:$(subst .c,.o,$(wildcard *.c))
all:log.o hashtable.o roundhash.o cuckoo.o basic_hash.o mempool.o operation.o communication.o flowing_controller.cpp record.o main.o piekv

%.o: %.cpp %.c
	$(CXX) -c $^ -o $@ $(CXXFLAGS) -p -g



piekv: main.o log.o hashtable.o roundhash.o cuckoo.o basic_hash.o mempool.o operation.o communication.o flowing_controller.cpp record.o 
	$(CXX) -o $@ $^ $(LDFLAGS) -pg

clean:
	rm -rf piekv main.o log.o hashtable.o roundhash.o cuckoo.o basic_hash.o mempool.o operation.o communication.o flowing_controller.cpp record.o 

backup:
	tar cvf CodeBackup.tar ./*.cpp