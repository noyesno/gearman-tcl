
LDFLAGS  =  -ltcl8.5 -lgearman
CXXFLAGS = -I/usr/local/include/gearman

TCLSH = tclsh8.5

all: lib/libtclgearman.so lib/libtclgearman.d.so

lib/libtclgearman.so: src/tclgearman.cpp
	g++ -o $@ -shared -fPIC -DNDEBUG $(CXXFLAGS) $< $(LDFLAGS)

lib/libtclgearman.d.so: src/tclgearman.cpp
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

