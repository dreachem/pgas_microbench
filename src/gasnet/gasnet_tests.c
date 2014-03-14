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

enum {
    GASNET_HANDLER_SYNC_NOTIFY_REQUEST = 128
};

typedef enum {
  BARRIER,
  P2P
} sync_type_t;

void handler_sync_notify(gasnet_token_t token, gasnet_handlerarg_t partner);
void do_sync_notify(int partner);
void do_sync_wait(int partner);
void do_sync(sync_type_t sync);

void run_putget_latency_test();
void run_getput_latency_test(sync_type_t sync);
void run_putput_latency_test(sync_type_t sync);
void run_getget_latency_test(sync_type_t sync);
void run_put_bw_test(int do_bulk);
void run_get_bw_test(int do_bulk);
void run_put_duplex_bw_test(int do_bulk);
void run_get_duplex_bw_test(int do_bulk);


const size_t SEGMENT_SIZE = 30*1024*1024;
const size_t MAX_MSG_SIZE = 4*1024*1024;
const long long LAT_NITER = 1000000;
const long long BW_NITER = 10000;

int my_node;
int num_nodes;
int partner;
gasnet_seginfo_t *seginfo;
int *sync_notify_buffer;


static gasnet_handlerentry_t handlers[] = {
    {GASNET_HANDLER_SYNC_NOTIFY_REQUEST, handler_sync_notify}
};

static const int nhandlers = sizeof(handlers) / sizeof(handlers[0]);


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
    ret = gasnet_attach(handlers, nhandlers,
                        (SEGMENT_SIZE/GASNET_PAGESIZE+1)*GASNET_PAGESIZE,
                        0);

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

    /* initialize address of sync notify buffer */
    sync_notify_buffer = (int *)((char *)seginfo[my_node].addr +
                        MAX_MSG_SIZE + sizeof(double)*num_nodes);

    /* run tests */
    run_putget_latency_test();
    run_putput_latency_test(BARRIER);
    run_putput_latency_test(P2P);
    run_getget_latency_test(BARRIER);
    run_getget_latency_test(P2P);
    run_put_bw_test(0);
    run_get_bw_test(0);
    run_put_duplex_bw_test(0);
    run_get_duplex_bw_test(0);

    return 0;
}



void run_putget_latency_test()
{
    int *origin, *target;
    int i;
    double t1, t2;
    double *stats;

    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    stats = (double*)((char *)seginfo[my_node].addr + MAX_MSG_SIZE);

    if (my_node == 0) {
        printf("\n\nPut-Get Latency: (%d active pairs)\n", (num_nodes/2));
    }

    if (my_node < partner) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            gasnet_put(partner, target, origin, sizeof(*target));
            gasnet_get(origin, partner, target, sizeof(*target));
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    gasnet_barrier_notify(0,0);
    gasnet_barrier_wait(0,0);

    if (my_node == 0) {
        /* collect stats from other nodes */
        for (i = 1; i < num_nodes/2; i++) {
            double latency_other;
            double *stats_other =
                (double *)((char *)seginfo[i].addr + MAX_MSG_SIZE);
            gasnet_get(&latency_other, i, stats_other, sizeof(latency_other));
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n",
                stats[0]/(num_nodes/2));
    }
}

void run_putput_latency_test(sync_type_t sync)
{
    int *origin, *target;
    int i;
    int num_active_nodes;
    double t1, t2;
    double *stats;

    num_active_nodes = (num_nodes % 2) ? num_nodes - 1 : num_nodes;
    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    stats = (double*)((char *)seginfo[my_node].addr + MAX_MSG_SIZE);

    if (my_node == 0) {
        printf("\n\nPut-Put Latency: (%d active pairs, %s)\n",
                (num_nodes/2), (sync == BARRIER) ? "barrier" : "p2p");
    }

    if (my_node < partner) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            gasnet_put(partner, target, origin, sizeof(*target));
            do_sync(sync);
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    } else if (my_node < num_active_nodes) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            do_sync(sync);
            gasnet_put(partner, target, origin, sizeof(*target));
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    gasnet_barrier_notify(0,0);
    gasnet_barrier_wait(0,0);

    if (my_node == 0) {
        /* collect stats from other nodes */
        for (i = 1; i < num_nodes; i++) {
            double latency_other;
            double *stats_other =
                (double *)((char *)seginfo[i].addr + MAX_MSG_SIZE);
            gasnet_get(&latency_other, i, stats_other, sizeof(latency_other));
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n",
                stats[0]/(num_nodes));
    }
}

void run_getget_latency_test(sync_type_t sync)
{
    int *origin, *target;
    int i;
    int num_active_nodes;
    double t1, t2;
    double *stats;

    num_active_nodes = (num_nodes % 2) ? num_nodes - 1 : num_nodes;
    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    stats = (double*)((char *)seginfo[my_node].addr + MAX_MSG_SIZE);

    if (my_node == 0) {
        printf("\n\nGet-Get Latency: (%d active pairs, %s)\n",
                (num_nodes/2), (sync == BARRIER) ? "barrier" : "p2p");
    }

    if (my_node < partner) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            gasnet_get(origin, partner, target, sizeof(*target));
            do_sync(sync);
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    } else if (my_node < num_active_nodes) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            do_sync(sync);
            gasnet_get(origin, partner, target, sizeof(*target));
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    gasnet_barrier_notify(0,0);
    gasnet_barrier_wait(0,0);

    if (my_node == 0) {
        /* collect stats from other nodes */
        for (i = 1; i < num_nodes; i++) {
            double latency_other;
            double *stats_other =
                (double *)((char *)seginfo[i].addr + MAX_MSG_SIZE);
            gasnet_get(&latency_other, i, stats_other, sizeof(latency_other));
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n",
                stats[0]/(num_nodes));
    }
}

void run_put_bw_test(int do_bulk)
{
    int *origin, *target;
    double t1, t2;
    int num_stats;
    size_t msg_size;
    double *stats;

    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    stats = (double *)((char *)seginfo[my_node].addr + MAX_MSG_SIZE);

    if (my_node == 0) {
        printf("\n\n1-way %s Put Bandwidth: (%d pairs)\n",
               do_bulk ? "(bulk)": "", (num_nodes/2));
        printf("%20s %20s %20s\n", "msg_size", "count", "bandwidth");
    }
    num_stats = 0;
    for (msg_size = sizeof(int); msg_size <= MAX_MSG_SIZE;
         msg_size *= 2) {
        int i;
        int count = BW_NITER;

        if (my_node < partner) {
            t1 = MPI_Wtime();
            if (do_bulk) {
                for (i = 0; i < count; i++) {
                    gasnet_put_bulk(partner, target, origin, msg_size);
                }
            } else {
                for (i = 0; i < count; i++) {
                    gasnet_put(partner, target, origin, msg_size);
                }
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*count/(1024*1024*(t2-t1));
        }

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_nodes/2; i++) {
                double bw_other;
                double *stats_other =
                    (double *)((char *)seginfo[i].addr + MAX_MSG_SIZE);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)msg_size, (long)count,
                    stats[num_stats]/(num_nodes/2));
        }
        num_stats++;
    }
}

void run_get_bw_test(do_bulk)
{
    int *origin, *target;
    double t1, t2;
    int num_stats;
    size_t msg_size;
    double *stats;

    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    stats = (double *)((char *)seginfo[my_node].addr + MAX_MSG_SIZE);

    if (my_node == 0) {
        printf("\n\n1-way %s Get Bandwidth (%d pairs):\n",
               do_bulk ? "(bulk)":"", (num_nodes/2));
        printf("%20s %20s %20s\n", "msg_size", "count", "bandwidth");
    }
    num_stats = 0;
    for (msg_size = sizeof(int); msg_size <= MAX_MSG_SIZE;
         msg_size *= 2) {
        int i;
        int count = BW_NITER;

        if (my_node < partner) {
            t1 = MPI_Wtime();
            if (do_bulk) {
                for (i = 0; i < count; i++) {
                    gasnet_get_bulk(origin, partner, target, msg_size);
                }
            } else {
                for (i = 0; i < count; i++) {
                    gasnet_get(origin, partner, target, msg_size);
                }
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*count/(1024*1024*(t2-t1));
        }

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_nodes/2; i++) {
                double bw_other;
                double *stats_other =
                    (double *)((char *)seginfo[i].addr + MAX_MSG_SIZE);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)msg_size, (long)count,
                    stats[num_stats]/(num_nodes/2));
        }

        num_stats++;
    }
}

void run_put_duplex_bw_test(int do_bulk)
{
    int *origin, *target;
    double t1, t2;
    int num_stats;
    size_t msg_size;
    double *stats;

    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    stats = (double *)((char *)seginfo[my_node].addr + MAX_MSG_SIZE);

    if (my_node == 0) {
        printf("\n\n2-way %s Put Bandwidth: (%d pairs)\n",
               do_bulk ? "(bulk)": "", (num_nodes/2));
        printf("%20s %20s %20s\n", "msg_size", "count", "bandwidth");
    }
    num_stats = 0;
    for (msg_size = sizeof(int); msg_size <= MAX_MSG_SIZE;
         msg_size *= 2) {
        int i;
        int count = BW_NITER;

        t1 = MPI_Wtime();
        if (do_bulk) {
            for (i = 0; i < count; i++) {
                gasnet_put_bulk(partner, target, origin, msg_size);
            }
        } else {
            for (i = 0; i < count; i++) {
                gasnet_put(partner, target, origin, msg_size);
            }
        }
        t2 = MPI_Wtime();

        stats[num_stats] = msg_size*count/(1024*1024*(t2-t1));

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            int num_active_nodes;
            if (num_nodes % 2) {
                num_active_nodes = num_nodes - 1;
            } else {
                num_active_nodes = num_nodes;
            }
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other =
                    (double *)((char *)seginfo[i].addr + MAX_MSG_SIZE);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)msg_size, (long)count,
                    stats[num_stats]/num_active_nodes);
        }
        num_stats++;
    }
}

void run_get_duplex_bw_test(int do_bulk)
{
    int *origin, *target;
    double t1, t2;
    int num_stats;
    size_t msg_size;
    double *stats;

    origin = seginfo[my_node].addr;
    target = seginfo[partner].addr;

    stats = (double *)((char *)seginfo[my_node].addr + MAX_MSG_SIZE);

    if (my_node == 0) {
        printf("\n\n2-way %s Get Bandwidth (%d pairs):\n",
               do_bulk ? "(bulk)":"", (num_nodes/2));
        printf("%20s %20s %20s\n", "msg_size", "count", "bandwidth");
    }
    num_stats = 0;
    for (msg_size = sizeof(int); msg_size <= MAX_MSG_SIZE;
         msg_size *= 2) {
        int i;
        int count = BW_NITER;

        t1 = MPI_Wtime();
        if (do_bulk) {
            for (i = 0; i < count; i++) {
                gasnet_get_bulk(origin, partner, target, msg_size);
            }
        } else {
            for (i = 0; i < count; i++) {
                gasnet_get(origin, partner, target, msg_size);
            }
        }
        t2 = MPI_Wtime();

        stats[num_stats] = msg_size*count/(1024*1024*(t2-t1));

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            int num_active_nodes;

            if (num_nodes % 2) {
                num_active_nodes = num_nodes - 1;
            } else {
                num_active_nodes = num_nodes;
            }
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other =
                    (double *)((char *)seginfo[i].addr + MAX_MSG_SIZE);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)msg_size, (long)count,
                    stats[num_stats]/num_active_nodes);
        }

        num_stats++;
    }
}

void do_sync(sync_type_t sync)
{
    if (sync == BARRIER) {
        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);
    } else if (sync == P2P) {
        /* notify partner */
        do_sync_notify(partner);

        /* wait on partner */
        do_sync_wait(partner);
    }
}


void handler_sync_notify(gasnet_token_t token, gasnet_handlerarg_t partner)
{
    int *partner_sync = &sync_notify_buffer[(int)partner];

    /* increment counter */
    __sync_fetch_and_add(partner_sync, 1);
}


void do_sync_notify(int partner)
{
    gasnet_AMRequestShort1(partner, GASNET_HANDLER_SYNC_NOTIFY_REQUEST,
                           my_node);
}

void do_sync_wait(int partner)
{
    int *partner_sync = &sync_notify_buffer[partner];

    /* wait on partner */
    GASNET_BLOCKUNTIL(*partner_sync);

    /* decrement counter */
    __sync_fetch_and_add(partner_sync, -1);
}
