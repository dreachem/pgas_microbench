# the following should be set:
#    GASNET_ROOT: point to GASNet installation
#    CONDUIT: to specify a specific conduit you wish to test

GASNET_ROOT ?=
CONDUIT ?= mpi
MPICC = mpicc

MPI_CFLAGS = -I/usr/lib/openmpi/include -pthread -I/usr/lib/openmpi/lib

include $(GASNET_ROOT)/include/$(CONDUIT)-conduit/$(CONDUIT)-par.mak

gasnet_tests_$(CONDUIT): gasnet_tests.o
	$(MPICC) $(GASNET_LDFLAGS) -o $@ $^ $(GASNET_LIBS)

gasnet_tests.o: gasnet_tests.c
	$(GASNET_CC) -c $(GASNET_CPPFLAGS) $(GASNET_CFLAGS) $(MPI_CFLAGS) $< -o $@

rtc.o: rtc.c
	$(CC) -O3 -DARCH_X86_64 -c $< -o $@

.PHONY:clean
clean:
	rm -rf *.o gasnet_tests_*
