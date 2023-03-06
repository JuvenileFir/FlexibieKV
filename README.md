## Start
- Server运行(目前dpdk环境配置10.176.64.41参数)：
	- PieKV/build/exp.sh(运行并记录log)
	- PieKV/build/go.sh(仅运行)
- Client运行(目前dpdk环境配置10.176.64.36参数)：
	- Client/test.sh（运行2秒）
	- 也可使用Client/build/client -h自行查看参数使用方法
## Class Structure

```mermaid
flowchart LR
	

	main -->PK
	PK --> Hashtable	
	PK --> Log
	PK --> Mempool
	PK ---> methods
	PK --> mfc
	PK ---> pt

	methods --> mth
	methods --> H2L
	methods --> L2H
	methods --> get
	methods --> set

	get -.->mth
	Hashtable --> RoundHash
	Hashtable --> tb
	tb --> bucket
	Log -->ls
	ls --> logblock

	Mempool --> block

	mfc -.-> H2L
	mfc -.-> L2H

	main ----> rtw
	rtw -.-> get
	rtw -.-> set

	

	classDef header fill:#a8d5eb,font-family:arias,font-size:14px,font-weight:300;
	classDef func fill:#8ec95d,font-family:arias,font-size:14px;
	classDef subclass fill:#fae000,font-family:arias,font-size:14px,font-weight:300;
	classDef common fill:#eeeeee,font-family:arias,font-size:14px,font-weight:300;
	classDef main fill:#e04d6d,font-family:arias,font-size:15px;
		main:::main
		methods:::subclass
		get("get()"):::func
		set("set()"):::func
		H2L("H2L()"):::func
		L2H("L2H()"):::func
		mth("move to head()"):::func
		PK[PieKV]:::main
		RoundHash["RoundHash × 2"]:::common
		logblock:::common
		tb[Tableblock]:::common
		bucket:::common
		block:::common
		Log:::subclass
		Mempool:::subclass
		Hashtable:::subclass
		mfc["memflowingController × 1 线程"]:::header
		pt["print_performance × 1 线程"]:::header
		rtw["RTWorker × 4 线程"]:::header
		ls["LogSegment × 4"]:::common


	

```

## headers‘ dependencies

```mermaid
flowchart BT
roundhash.hpp:::header --> rp(roundhash.cpp):::source
piekv.hpp --> fp(flowing_controller.cpp):::source
subgraph Headers
basic_hash.h:::header --> cuckoo.h
cuckoo.h:::header --> hashtable.hpp
hashtable.hpp:::header --> log.hpp
timer.h:::header --> piekv.hpp
log.hpp:::header --> piekv.hpp
mempool.hpp --> basic_hash.h
piekv.hpp:::header --> communication.hpp
roundhash.hpp --> hashtable.hpp

util.h:::header --> basic_hash.h
end
basic_hash.h --> bp(basic_hash.cpp):::source


communication.hpp --> main(main.cpp):::source
communication.hpp:::header --> cp(communication.cpp):::source

cuckoo.h --> cc(cuckoo.cpp):::source

hashtable.hpp --> hp(hashtable.cpp):::source



mempool.hpp:::header --> mp(mempool.cpp):::source

piekv.hpp --> op(operation.cpp):::source




%%util.h --> zipf.h


%%zipf.h --> NO_INCLUDE
classDef header fill:#eef,font-family:arias,font-size:14.5px,font-weight:300;
classDef source fill:#bcf,font-family:arias,font-size:13px;

```
