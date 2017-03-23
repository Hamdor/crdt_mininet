all:
	g++ -std=c++11 src/main.cpp -lcaf_core -lcaf_io -lcaf_crdt -o crdt_bench
