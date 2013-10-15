
LDFLAGS  =  -ltcl8.5 -lgearman
CXXFLAGS = -I/usr/local/include/gearman

TCLSH = tclsh8.5

all: libtclgearman.so libtclgearman.d.so 

libtclgearman.so: src/tclgearman.cpp
	g++ -o $@ -shared -fPIC -DNDEBUG $(CXXFLAGS) $< $(LDFLAGS)

libtclgearman.d.so: src/tclgearman.cpp
	g++ -o $@ -shared -fPIC $(CXXFLAGS) $< $(LDFLAGS)

tclgearman: src/tclgearman.cpp
	g++ -o $@ -shared -fPIC -D__MAIN__ -NDEBUG $(CXXFLAGS) $< $(LDFLAGS)


test-worker: test/worker.tcl
	$(TCLSH) $<

test-client: test/client.tcl
	$(TCLSH) $<

test-admin: test/admin.tcl
	$(TCLSH) $<

clean:
	rm -rf *.so *.o *.tmp

