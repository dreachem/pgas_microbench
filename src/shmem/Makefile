ARMCI_ROOT ?=
ARMCI_NETWORK_LIBS ?=
MPICC = mpicc
ARMCI_CFLAGS = -I$(ARMCI_ROOT)/include
ARMCI_LDFLAGS = $(ARMCI_CFLAGS)
COMM_LIBS = -L$(ARMCI_ROOT)/lib -Wl,-rpath,$(ARMCI_ROOT)/lib -larmci $(ARMCI_NETWORK_LIBS) -lm

DEBUG = -D_DEBUG

armci_tests: armci_tests.o
	$(MPICC) $(ARMCI_LDFLAGS) -o $@ $^ $(COMM_LIBS)

armci_tests.o: armci_tests.c
	$(MPICC) -c $(ARMCI_CFLAGS) $(DEBUG) $< -o $@

.PHONY:clean
clean:
	rm -rf *.o armci_tests
