/*
 * Microbenchmarks for GASNet
 *
 * (C) HPCTools Groups University of Houston
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include "gasnet.h"
#include "gasnet_tools.h"
#include "gasnet_vis.h"
#include "gasnet_coll.h"

const size_t SEGMENT_SIZE = 30*1024*1024;
const long long NITER = 1000000;

int my_node;
int num_nodes;
int partner;
gasnet_seginfo_t *seginfo;

void run_latency_test();
void run_put_bw_test();
void run_get_bw_test();
void run_put_duplex_bw_test();
void run_get_duplex_bw_test();

int main(int argc, char **argv)
{
    int ret;
    double t1, t2, t3;

    /* start up gasnet */
    gasnet_init(&argc, &argv);
    my_node   = gasnet_mynode();
    num_nodes = gasnet_nodes();

    if (num_nodes < 2) {
        fprintf(stderr, "not enough nodes running\n");
        gasnet_exit(1);
    }

    if (num_nodes % 2 == 0) {
        partner = (my_node + num_nodes/2) % num_nodes;
    } else {
        partner = (my_node + (num_nodes-1)/2) % (num_nodes-1);
    }

    /* attach segment */
    ret = gasnet_attach(NULL, 0, (SEGMENT_SIZE/GASNET_PAGESIZE+1)*GASNET_PAGESIZE, 0);
    if (ret != GASNET_OK) {
        fprintf(stderr, "error in gasnet_attach\n");
        gasnet_exit(1);
    }

    /* get segment info */
    seginfo = malloc( sizeof(*seginfo) * num_nodes);
    ret = gasnet_getSegmentInfo(seginfo, num_nodes);
    if (ret != GASNET_OK) {
        fprintf(stderr, "error in gasnet_getSegmentInfo\n");
        gasnet_exit(1);
    }

    run_latency_test();
    run_put_bw_test();
    run_get_bw_test();
    run_put_duplex_bw_test();
    run_get_duplex_bw_test();

    return 0;
}



void run_latency_test()
{
    int *origin, *target;
    int i;
    double t1, t2;

    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    if (my_node == 0) {
        t1 = MPI_Wtime();
        for (i = 0; i < NITER; i++) {
            gasnet_put(partner, target, origin, sizeof(int));
            gasnet_get(origin, partner, target, sizeof(int));
        }
        t2 = MPI_Wtime();

        printf("Measured Latency = %8.8lf us\n",
                1000000*(t2 - t1) / (2*NITER));
    }
}

void run_put_bw_test()
{
}

void run_get_bw_test()
{
}

void run_put_duplex_bw_test()
{
}

void run_get_duplex_bw_test()
{
}
