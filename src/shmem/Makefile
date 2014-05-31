SHMEM_CC = oshcc
SHMEM_CFLAGS =
SHMEM_LDFLAGS =

DEBUG = -D_DEBUG -DUSE_SHMEM_PTR

shmem_tests: shmem_tests.o
	$(SHMEM_CC) -g -o $@ $(SHMEM_LDFLAGS) $^

shmem_tests.o: shmem_tests.c
	$(SHMEM_CC) -g -c $(SHMEM_CFLAGS) $(DEBUG) $< -o $@

.PHONY:clean
clean:
	rm -rf *.o shmem_tests
