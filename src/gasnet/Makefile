# the following should be set:
#    GASNET_ROOT: point to GASNet installation
#    CONDUIT: to specify a specific conduit you wish to test

GASNET_ROOT ?=
CONDUIT ?= mpi
MPICC = mpicc

MPI_CFLAGS = -I/usr/lib/openmpi/include -pthread -I/usr/lib/openmpi/lib

include $(GASNET_ROOT)/include/$(CONDUIT)-conduit/$(CONDUIT)-par.mak

gasnet_tests: gasnet_tests.o
	$(MPICC) $(GASNET_LDFLAGS) -o $@ $^ $(GASNET_LIBS)

gasnet_tests.o: gasnet_tests.c
	$(GASNET_CC) -c $(GASNET_CPPFLAGS) $(GASNET_CFLAGS) $(MPI_CFLAGS) $< -o $@

.PHONY:clean
clean:
	rm -rf *.o gasnet_tests
