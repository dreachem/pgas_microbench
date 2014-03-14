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

void print_sendrecv_address();
void p2psync_test();

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
void run_strided_put_bw_test();
void run_strided_get_bw_test();

void run_put_bidir_bw_test(int do_bulk);
void run_get_bidir_bw_test(int do_bulk);
void run_strided_put_bidir_bw_test();
void run_strided_get_bidir_bw_test();


const size_t SEGMENT_SIZE = 30*1024*1024;
const size_t MAX_MSG_SIZE = 4*1024*1024;
const long long LAT_NITER = 1000000;
const long long BW_NITER = 10000;

int my_node;
int num_nodes;
int num_active_nodes;
int partner;
gasnet_seginfo_t *seginfo;

static void *segment_start;
static int *send_buffer;
static int *recv_buffer;
static double *stats_buffer;
static int *sync_notify_buffer;

#define REMOTE_ADDRESS(a,p) \
    (a) + ((char *)seginfo[(p)].addr - (char*)segment_start)/(sizeof *(a))


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
        num_active_nodes = num_nodes;
    } else {
        num_active_nodes = num_nodes - 1;
    }
    partner = (my_node + num_active_nodes/2) % num_active_nodes;

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

    segment_start = seginfo[my_node].addr;

    /* initialize address of send buffer */
    send_buffer = segment_start;

    /* initialize address of receive buffer */
    recv_buffer =
        &send_buffer[MAX_MSG_SIZE/(sizeof send_buffer[0]) + 1];

    /* initialize address of stats buffer */
    stats_buffer = (double *)
        &recv_buffer[MAX_MSG_SIZE/(sizeof send_buffer[0]) + 1];

    /* initialize address of sync notify buffer */
    sync_notify_buffer = (int *) &stats_buffer[num_nodes + 1];

    /* run tests */
    run_putget_latency_test();
    run_putput_latency_test(BARRIER);
    run_putput_latency_test(P2P);
    run_getget_latency_test(BARRIER);
    run_getget_latency_test(P2P);

    run_put_bw_test(0);
    run_get_bw_test(0);
    run_strided_put_bw_test();
    run_strided_get_bw_test();

    run_put_bidir_bw_test(0);
    run_get_bidir_bw_test(0);
    run_strided_put_bidir_bw_test();
    run_strided_get_bidir_bw_test();

    gasnet_exit(0);
}

#ifdef _DEBUG
void print_sendrecv_address()
{
    int *origin_send, *target_recv;
    int *origin_recv, *target_send;

    origin_send = send_buffer;
    target_recv = REMOTE_ADDRESS(recv_buffer, partner);
    origin_recv = recv_buffer;
    target_send = REMOTE_ADDRESS(send_buffer, partner);

    printf("%d: osend: %p, trecv: %p, orecv: %p, tsend: %p\n",
            my_node, origin_send, target_recv, origin_recv, target_send);
}

void p2psync_test()
{
    const int delay = 5;
    if (my_node < partner) {
        printf("%d doing work\n", my_node);
        sleep(delay);
        printf("%d sending sync to %d\n", my_node, partner);
        do_sync_notify(partner);
        printf("%d waiting on sync from %d\n", my_node, partner);
        do_sync_wait(partner);
        printf("%d received sync from %d\n", my_node, partner);
    } else if (my_node < num_active_nodes) {
        printf("%d waiting on sync from %d\n", my_node, partner);
        do_sync_wait(partner);
        printf("%d received sync from %d\n", my_node, partner);
        printf("%d doing work\n", my_node);
        sleep(delay);
        printf("%d sending sync to %d\n", my_node, partner);
        do_sync_notify(partner);
    }
}
#endif

/********************************************************************
 *                      SYNCHRONIZION ROUTINES
 ********************************************************************/

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

/********************************************************************
 *                      LATENCY TESTS
 ********************************************************************/

void run_putget_latency_test()
{
    int *origin_send, *target_recv;
    int *origin_recv, *target_send;
    int i;
    int num_pairs;
    double t1, t2;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = REMOTE_ADDRESS(recv_buffer, partner);
    origin_recv = recv_buffer;
    target_send = REMOTE_ADDRESS(send_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\nPut-Get Latency: (%d active pairs)\n", num_pairs);
    }

    if (my_node < partner) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            gasnet_put(partner, target_recv, origin_send,
                       sizeof(*target_recv));
            gasnet_get(origin_recv, partner, target_send,
                       sizeof(*target_recv));
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    gasnet_barrier_notify(0,0);
    gasnet_barrier_wait(0,0);

    if (my_node == 0) {
        /* collect stats from other active nodes */
        for (i = 1; i < num_pairs; i++) {
            double latency_other;
            double *stats_other = REMOTE_ADDRESS(stats, i);
            gasnet_get(&latency_other, i, stats_other, sizeof(latency_other));
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n", stats[0]/num_pairs);
    }
}

void run_putput_latency_test(sync_type_t sync)
{
    int *origin_send, *target_recv;
    int i;
    int num_pairs;
    double t1, t2;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = REMOTE_ADDRESS(recv_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\nPut-Put Latency: (%d active pairs, %s)\n",
                num_pairs, (sync == BARRIER) ? "barrier" : "p2p");
    }

    if (my_node < partner) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            gasnet_put(partner, target_recv, origin_send,
                       sizeof(*target_recv));
            do_sync(sync);
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    } else if (my_node < num_active_nodes) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            do_sync(sync);
            gasnet_put(partner, target_recv, origin_send,
                       sizeof(*target_recv));
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    gasnet_barrier_notify(0,0);
    gasnet_barrier_wait(0,0);

    if (my_node == 0) {
        /* collect stats from other nodes */
        for (i = 1; i < num_active_nodes; i++) {
            double latency_other;
            double *stats_other = REMOTE_ADDRESS(stats, i);
            gasnet_get(&latency_other, i, stats_other, sizeof(latency_other));
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n",
                stats[0]/(num_active_nodes));
    }
}

void run_getget_latency_test(sync_type_t sync)
{
    int *origin_recv, *target_send;
    int num_pairs;
    int i;
    double t1, t2;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = REMOTE_ADDRESS(send_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\nGet-Get Latency: (%d active pairs, %s)\n",
                num_pairs, (sync == BARRIER) ? "barrier" : "p2p");
    }

    if (my_node < partner) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            gasnet_get(origin_recv, partner, target_send,
                       sizeof(*target_send));
            do_sync(sync);
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    } else if (my_node < num_active_nodes) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            do_sync(sync);
            gasnet_get(origin_recv, partner, target_send,
                       sizeof(*target_send));
            do_sync(sync);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    gasnet_barrier_notify(0,0);
    gasnet_barrier_wait(0,0);

    if (my_node == 0) {
        /* collect stats from other nodes */
        for (i = 1; i < num_active_nodes; i++) {
            double latency_other;
            double *stats_other = REMOTE_ADDRESS(stats, i);
            gasnet_get(&latency_other, i, stats_other, sizeof(latency_other));
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n", stats[0]/num_active_nodes);
    }
}

/********************************************************************
 *                      1-Way Bandwidth Tests
 ********************************************************************/

void run_put_bw_test(int do_bulk)
{
    int *origin_send, *target_recv;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = REMOTE_ADDRESS(recv_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n1-way %s Put Bandwidth: (%d pairs)\n",
               do_bulk ? "(bulk)": "", num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            size_t msg_size = blksize * (sizeof *origin_send);
            t1 = MPI_Wtime();
            if (do_bulk) {
                for (i = 0; i < nrep; i++) {
                    gasnet_put_bulk(partner, target_recv, origin_send, msg_size);
                }
            } else {
                for (i = 0; i < nrep; i++) {
                    gasnet_put(partner, target_recv, origin_send, msg_size);
                }
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_pairs);
        }
        num_stats++;
    }
}

void run_get_bw_test(do_bulk)
{
    int *origin_recv, *target_send;
    double t1, t2;
    int num_pairs;
    int num_stats;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = REMOTE_ADDRESS(send_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n1-way %s Get Bandwidth (%d pairs):\n",
               do_bulk ? "(bulk)":"", num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            size_t msg_size = blksize * (sizeof *target_send);
            t1 = MPI_Wtime();
            if (do_bulk) {
                for (i = 0; i < nrep; i++) {
                    gasnet_get_bulk(origin_recv, partner, target_send,
                                    msg_size);
                }
            } else {
                for (i = 0; i < nrep; i++) {
                    gasnet_get(origin_recv, partner, target_send, msg_size);
                }
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_pairs);
        }

        num_stats++;
    }
}

void run_strided_put_bw_test()
{
    int *origin_send, *target_recv;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    size_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = REMOTE_ADDRESS(recv_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n1-way Strided Put Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            size_t origin_strides[1], target_strides[1];
            size_t count[2];
            size_t msg_size = MAX_COUNT * (sizeof *origin_send);

            target_strides[0] = stride * (sizeof *origin_send);
            origin_strides[0] = sizeof *origin_send;
            count[0] = sizeof *origin_send;
            count[1] = MAX_COUNT;

            t1 = MPI_Wtime();
            for (i = 0; i < nrep; i++) {
                gasnet_puts_bulk(partner, target_recv, target_strides,
                                 origin_send, origin_strides, count, 1);
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %20ld %17.3f MB/s\n",
                    (long)MAX_COUNT, (long)stride,
                    (long)nrep,
                    stats[num_stats]/num_pairs);
        }
        num_stats++;
    }
}

void run_strided_get_bw_test()
{
    int *origin_recv, *target_send;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    size_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = REMOTE_ADDRESS(send_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n1-way Strided Get Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            size_t origin_strides[1], target_strides[1];
            size_t count[2];
            size_t msg_size = MAX_COUNT * (sizeof *target_send);

            origin_strides[0] = stride * (sizeof *target_send);
            target_strides[0] = sizeof *target_send;
            count[0] = sizeof *target_send;
            count[1] = MAX_COUNT;

            t1 = MPI_Wtime();
            for (i = 0; i < nrep; i++) {
                gasnet_gets_bulk(origin_recv, origin_strides,
                        partner, target_send, target_strides,
                        count, 1);
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %20ld %17.3f MB/s\n",
                    (long)MAX_COUNT, (long)stride,
                    (long)nrep,
                    stats[num_stats]/num_pairs);
        }
        num_stats++;
    }
}

/********************************************************************
 *                      2-Way Bandwidth Tests
 ********************************************************************/

void run_put_bidir_bw_test(int do_bulk)
{
    int *origin_send, *target_recv;
    double t1, t2;
    int num_pairs;
    int num_stats;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = REMOTE_ADDRESS(recv_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n2-way %s Put Bandwidth: (%d pairs)\n",
               do_bulk ? "(bulk)": "", num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;
        size_t msg_size = blksize * (sizeof *origin_send);

        t1 = MPI_Wtime();
        if (do_bulk) {
            for (i = 0; i < nrep; i++) {
                gasnet_put_bulk(partner, target_recv, origin_send, msg_size);
            }
        } else {
            for (i = 0; i < nrep; i++) {
                gasnet_put(partner, target_recv, origin_send, msg_size);
            }
        }
        t2 = MPI_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_active_nodes);
        }
        num_stats++;
    }
}

void run_get_bidir_bw_test(int do_bulk)
{
    int *origin_recv, *target_send;
    double t1, t2;
    int num_pairs;
    int num_stats;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = REMOTE_ADDRESS(send_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n2-way %s Get Bandwidth (%d pairs):\n",
               do_bulk ? "(bulk)":"", num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;
        size_t msg_size = blksize * (sizeof *target_send);

        t1 = MPI_Wtime();
        if (do_bulk) {
            for (i = 0; i < nrep; i++) {
                gasnet_get_bulk(origin_recv, partner, target_send, msg_size);
            }
        } else {
            for (i = 0; i < nrep; i++) {
                gasnet_get(origin_recv, partner, target_send, msg_size);
            }
        }
        t2 = MPI_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_active_nodes);
        }

        num_stats++;
    }
}

void run_strided_put_bidir_bw_test()
{
    int *origin_send, *target_recv;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    size_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = REMOTE_ADDRESS(recv_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n2-way Strided Put Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        size_t origin_strides[1], target_strides[1];
        size_t count[2];
        int nrep = BW_NITER;
        size_t msg_size = MAX_COUNT * (sizeof *origin_send);

        target_strides[0] = stride * (sizeof *origin_send);
        origin_strides[0] = sizeof *origin_send;
        count[0] = sizeof *origin_send;
        count[1] = MAX_COUNT;

        t1 = MPI_Wtime();
        for (i = 0; i < nrep; i++) {
            gasnet_puts_bulk(partner, target_recv, target_strides,
                             origin_send, origin_strides, count, 1);
        }
        t2 = MPI_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %20ld %17.3f MB/s\n",
                    (long)MAX_COUNT, (long)stride,
                    (long)nrep,
                    stats[num_stats]/num_active_nodes);
        }
        num_stats++;
    }
}

void run_strided_get_bidir_bw_test()
{
    int *origin_recv, *target_send;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    size_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = REMOTE_ADDRESS(send_buffer, partner);

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\n2-way Strided Get Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        size_t origin_strides[1], target_strides[1];
        size_t count[2];
        int nrep = BW_NITER;
        size_t msg_size = MAX_COUNT * (sizeof *target_send);

        origin_strides[0] = stride * (sizeof *target_send);
        target_strides[0] = sizeof *target_send;
        count[0] = sizeof *target_send;
        count[1] = MAX_COUNT;

        t1 = MPI_Wtime();
        for (i = 0; i < nrep; i++) {
            gasnet_gets_bulk(origin_recv, origin_strides,
                    partner, target_send, target_strides,
                    count, 1);
        }
        t2 = MPI_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        gasnet_barrier_notify(0,0);
        gasnet_barrier_wait(0,0);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = REMOTE_ADDRESS(stats, i);
                gasnet_get(&bw_other, i, &stats_other[num_stats],
                           sizeof(bw_other));
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %20ld %17.3f MB/s\n",
                    (long)MAX_COUNT, (long)stride,
                    (long)nrep,
                    stats[num_stats]/num_active_nodes);
        }
        num_stats++;
    }
}
