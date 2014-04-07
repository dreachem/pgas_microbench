MPI_CC = mpicc
MPI_CFLAGS =

DEBUG = -D_DEBUG

mpi_tests: mpi_tests.o
	$(MPI_CC) -o $@ $^

mpi_tests.o: mpi_tests.c
	$(MPI_CC) -c $(mpi_CFLAGS) $(DEBUG) $< -o $@

.PHONY:clean
clean:
	rm -rf *.o mpi_tests
