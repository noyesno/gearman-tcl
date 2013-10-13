


all: libtclgearman.so libtclgearman.d.so 

libtclgearman.so: src/tclgearman.cpp
	g++ -o $@ -shared -fPIC -DNDEBUG $< -ltcl8.5 -lgearman

libtclgearman.d.so: src/tclgearman.cpp
	g++ -o $@ -shared -fPIC $< -ltcl8.5 -lgearman

tclgearman: src/tclgearman.cpp
	g++ -o $@ -shared -fPIC -D__MAIN__ -NDEBUG $< -ltcl8.5 -lgearman


test-worker:
	tclsh8.5 test/worker.tcl

test-client:
	tclsh8.5 test/client.tcl


clean:
	rm -rf *.so *.o *.tmp

