FC = uhcaf
LD = $(FC)
FFLAGS = --mpi
LDFLAGS = $(FFLAGS)

caf_tests: caf_tests.o
	$(LD) $(LDFLAGS) -o  $@ $^

caf_tests.o: caf_tests.F90
	$(FC) -c $(FFLAGS) $< -o $@

.PHONY:clean
clean:
	rm -rf *.o caf_tests *.mod
