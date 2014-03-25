SHMEM_CC = oshcc
SHMEM_CFLAGS =

DEBUG = -D_DEBUG

shmem_tests: shmem_tests.o
	$(SHMEM_CC) -o $@ $^

shmem_tests.o: shmem_tests.c
	$(SHMEM_CC) -c $(SHMEM_CFLAGS) $(DEBUG) $< -o $@

.PHONY:clean
clean:
	rm -rf *.o shmem_tests
