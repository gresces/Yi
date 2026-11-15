CXX ?= c++
PKG_CONFIG ?= pkg-config
BASE_URL ?= http://127.0.0.1:8081

CXXFLAGS ?= -O2 -g -Wall -Wextra -std=c++17
CPPFLAGS += $(shell $(PKG_CONFIG) --cflags libcurl)
LDLIBS += $(shell $(PKG_CONFIG) --libs libcurl)

.PHONY: all test test-unit test-e2e clean

all:
	@echo "yi.hpp is header-only; include it and link with -lcurl."

tests/test_unit: tests/test_unit.cpp yi.hpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -I. -o $@ tests/test_unit.cpp $(LDLIBS)

tests/test_e2e: tests/test_e2e.cpp yi.hpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -I. -o $@ tests/test_e2e.cpp $(LDLIBS)

test: tests/test_unit tests/test_e2e
	./tests/test_unit
	./tests/test_e2e $(BASE_URL)

test-unit: tests/test_unit
	./tests/test_unit

test-e2e: tests/test_e2e
	./tests/test_e2e $(BASE_URL)

clean:
	rm -f tests/test_unit tests/test_e2e
