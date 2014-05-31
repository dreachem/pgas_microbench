/*
 * Microbenchmarks for SHMEM
 *
 * (C) HPCTools Group, University of Houston
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <shmem.h>


typedef enum {
  BARRIER,
  P2P
} sync_type_t;

typedef enum {
  TARGET_STRIDED,
  ORIGIN_STRIDED,
  BOTH_STRIDED
} strided_type_t;

void print_sendrecv_address();
void p2psync_test();

void do_sync_notify(int partner);
void do_sync_wait(int partner);
void do_sync(sync_type_t sync);

void run_putget_latency_test();
void run_getput_latency_test(sync_type_t sync);
void run_putput_latency_test(sync_type_t sync);
void run_getget_latency_test(sync_type_t sync);

void run_put_bw_test();
void run_get_bw_test();
void run_strided_put_bw_test(strided_type_t strided);
void run_strided_get_bw_test(strided_type_t strided);

void run_put_bidir_bw_test();
void run_get_bidir_bw_test();
void run_strided_put_bidir_bw_test(strided_type_t strided);
void run_strided_get_bidir_bw_test(strided_type_t strided);

void run_reduce_test(int separate_target);


const int TIMEOUT = 5;
const size_t MAX_MSG_SIZE = 4*1024*1024;
const long long LAT_NITER = 10000;
const long long BW_NITER = 10000;
const long long RED_NITER = 100;

const int NUM_STATS = 32;

int my_node;
int num_nodes;
int num_active_nodes;
int partner;
static int *send_buffer;
static int *recv_buffer;
static double *stats_buffer;
static int *sync_notify_buffer;


int main(int argc, char **argv)
{
    int ret;
    double t1, t2, t3;

    /* start up SHMEM */
    start_pes(0);

    my_node       = _my_pe();
    num_nodes     = _num_pes();

    if (num_nodes % 2 != 0) {
        fprintf(stderr, "Number of PEs should be even.\n");
        exit(1);
    }

    num_active_nodes = num_nodes;
    partner = (my_node + num_active_nodes/2) % num_active_nodes;

    send_buffer         = shmalloc(MAX_MSG_SIZE);
    recv_buffer         = shmalloc(MAX_MSG_SIZE);
    stats_buffer        = shmalloc(NUM_STATS * sizeof(stats_buffer[0]));
    sync_notify_buffer  = shmalloc(num_nodes * sizeof(sync_notify_buffer[0]));

    /* run tests */
    run_putget_latency_test();
    run_putput_latency_test(BARRIER);
    run_putput_latency_test(P2P);
    run_getget_latency_test(BARRIER);
    run_getget_latency_test(P2P);

    run_put_bw_test();
    run_get_bw_test();
    run_put_bidir_bw_test();
    run_get_bidir_bw_test();

    run_strided_put_bw_test(TARGET_STRIDED);
    run_strided_put_bw_test(ORIGIN_STRIDED);
    run_strided_put_bw_test(BOTH_STRIDED);
    run_strided_get_bw_test(TARGET_STRIDED);
    run_strided_get_bw_test(ORIGIN_STRIDED);
    run_strided_get_bw_test(BOTH_STRIDED);

    run_strided_put_bidir_bw_test(TARGET_STRIDED);
    run_strided_put_bidir_bw_test(ORIGIN_STRIDED);
    run_strided_put_bidir_bw_test(BOTH_STRIDED);
    run_strided_get_bidir_bw_test(TARGET_STRIDED);
    run_strided_get_bidir_bw_test(ORIGIN_STRIDED);
    run_strided_get_bidir_bw_test(BOTH_STRIDED);

    run_reduce_test(0);
    run_reduce_test(1);
}

#ifdef _DEBUG
void print_sendrecv_address()
{
    int *origin_send, *target_recv;
    int *origin_recv, *target_send;

    origin_send = send_buffer;
    target_recv = shmem_ptr(recv_buffer, partner);
    origin_recv = recv_buffer;
    target_send = shmem_ptr(send_buffer, partner);

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
        shmem_barrier_all();
    } else if (sync == P2P) {
        /* notify partner */
        do_sync_notify(partner);

        /* wait on partner */
        do_sync_wait(partner);
    }
}


void do_sync_notify(int partner)
{
    shmem_int_inc(&sync_notify_buffer[my_node], partner);
}

void do_sync_wait(int partner)
{
    shmem_int_wait(&sync_notify_buffer[partner], 0);
    shmem_int_add(&sync_notify_buffer[partner], -1, my_node);
}

/********************************************************************
 *                      LATENCY TESTS
 ********************************************************************/

double get_Wtime()
{
    double t;
    struct timeval tv;

    gettimeofday(&tv, NULL);

    t = (tv.tv_sec*1000000LL + tv.tv_usec)/1000000.0;

    return t;
}

void run_putget_latency_test()
{
    int *origin_send, *target_recv, *target_recv_r;
    int *origin_recv, *target_send, *target_send_r;
    int i;
    int num_pairs;
    double t1, t2;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = recv_buffer;
    origin_recv = recv_buffer;
    target_send = send_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_recv_r = shmem_ptr(target_recv, partner);
    target_send_r = shmem_ptr(target_send, partner);
#endif

    if (my_node == 0) {
        printf("\n\nPut-Get Latency: (%d active pairs)\n", num_pairs);
    }

    if (my_node < partner) {
        t1 = get_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
#ifdef USE_SHMEM_PTR
            if (target_recv_r) {
                memcpy(target_recv_r, origin_send, sizeof(*target_recv));
            } else
#endif
            {
                shmem_putmem(target_recv, origin_send, sizeof(*target_recv),
                        partner);
                shmem_quiet();
            }
#ifdef USE_SHMEM_PTR
            if (target_send_r) {
                memcpy(origin_recv, target_send_r, sizeof(*target_send));
            } else
#endif
            {
                shmem_getmem(origin_recv, target_send, sizeof(*target_send),
                        partner);
            }
        }
        t2 = get_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    shmem_barrier_all();

    if (my_node == 0) {
        /* collect stats from other active nodes */
        for (i = 1; i < num_pairs; i++) {
            double latency_other;
            double *stats_other = stats;
            shmem_getmem(&latency_other, stats_other, sizeof(latency_other),
                         i);
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n", stats[0]/num_pairs);
    }
}

void run_putput_latency_test(sync_type_t sync)
{
    int *origin_send, *target_recv, *target_recv_r;
    int i;
    int num_pairs;
    double t1, t2;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = recv_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_recv_r = shmem_ptr(target_recv, partner);
#endif

    if (my_node == 0) {
        printf("\n\nPut-Put Latency: (%d active pairs, %s)\n",
                num_pairs, (sync == BARRIER) ? "barrier" : "p2p");
    }

    if (my_node < partner) {
        t1 = get_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
#ifdef USE_SHMEM_PTR
            if (target_recv_r) {
                memcpy(target_recv_r, origin_send, sizeof(*target_recv));
            } else
#endif
            {
                shmem_putmem(target_recv, origin_send, sizeof(*target_recv),
                        partner);
            }
            do_sync(sync);
            do_sync(sync);
        }
        t2 = get_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    } else if (my_node < num_active_nodes) {
        t1 = get_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            do_sync(sync);
#ifdef USE_SHMEM_PTR
            if (target_recv_r) {
                memcpy(target_recv_r, origin_send, sizeof(*target_recv));
            } else
#endif
            {
                shmem_putmem(target_recv, origin_send, sizeof(*target_recv),
                        partner);
            }
            do_sync(sync);
        }
        t2 = get_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    shmem_barrier_all();

    if (my_node == 0) {
        /* collect stats from other nodes */
        for (i = 1; i < num_active_nodes; i++) {
            double latency_other;
            double *stats_other = stats;
            shmem_getmem(&latency_other, stats_other, sizeof(latency_other),
                         i);
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n",
                stats[0]/(num_active_nodes));
    }
}

void run_getget_latency_test(sync_type_t sync)
{
    int *origin_recv, *target_send, *target_send_r;
    int num_pairs;
    int i;
    double t1, t2;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = send_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_send_r = shmem_ptr(target_send, partner);
#endif

    if (my_node == 0) {
        printf("\n\nGet-Get Latency: (%d active pairs, %s)\n",
                num_pairs, (sync == BARRIER) ? "barrier" : "p2p");
    }

    if (my_node < partner) {
        t1 = get_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
#ifdef USE_SHMEM_PTR
            if (target_send_r) {
                memcpy(origin_recv, target_send_r, sizeof(*target_send));
            } else
#endif
            {
                shmem_getmem(origin_recv, target_send, sizeof(*target_send),
                        partner);
            }
            do_sync(sync);
            do_sync(sync);
        }
        t2 = get_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    } else if (my_node < num_active_nodes) {
        t1 = get_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            do_sync(sync);
#ifdef USE_SHMEM_PTR
            if (target_send_r) {
                memcpy(origin_recv, target_send_r, sizeof(*target_send));
            } else
#endif
            {
                shmem_getmem(origin_recv, target_send, sizeof(*target_send),
                        partner);
            }
            do_sync(sync);
        }
        t2 = get_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    }

    shmem_barrier_all();

    if (my_node == 0) {
        /* collect stats from other nodes */
        for (i = 1; i < num_active_nodes; i++) {
            double latency_other;
            double *stats_other = stats;
            shmem_getmem(&latency_other, stats_other, sizeof(latency_other),
                         i);
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n", stats[0]/num_active_nodes);
    }
}

/********************************************************************
 *                      1-Way Bandwidth Tests
 ********************************************************************/

void run_put_bw_test()
{
    int *origin_send, *target_recv, *target_recv_r;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = recv_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_recv_r = shmem_ptr(target_recv, partner);
#endif

    if (my_node == 0) {
        printf("\n\n1-way Put Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            size_t msg_size = blksize * (sizeof *origin_send);
            t1 = get_Wtime();
            for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
                if (target_recv_r) {
                    memcpy(target_recv_r, origin_send, msg_size);
                }
                else
#endif
                {
                    shmem_putmem(target_recv, origin_send, msg_size, partner);
                    shmem_fence();
                }

                if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
                  nrep = i;
                }
            }
            t2 = get_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other),
                             i);
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_pairs);
        }
        num_stats++;
    }
}

void run_get_bw_test()
{
    int *origin_recv, *target_send, *target_send_r;
    double t1, t2;
    int num_pairs;
    int num_stats;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = send_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_send_r = shmem_ptr(target_send, partner);
#endif

    if (my_node == 0) {
        printf("\n\n1-way Get Bandwidth (%d pairs):\n",
               num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            size_t msg_size = blksize * (sizeof *target_send);
            t1 = get_Wtime();
            for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
                if (target_send_r) {
                    memcpy(origin_recv, target_send_r, msg_size);
                }
                else
#endif
                {
                    shmem_getmem(origin_recv, target_send, msg_size, partner);
                }
                if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
                  nrep = i;
                }
            }
            t2 = get_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other), i);
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_pairs);
        }

        num_stats++;
    }
}

void run_strided_put_bw_test(strided_type_t strided)
{
    int *origin_send, *target_recv, *target_recv_r;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    ptrdiff_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = recv_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_recv_r = shmem_ptr(target_recv, partner);
#endif

    if (my_node == 0) {
        printf("\n\n1-way %s Put Bandwidth: (%d pairs)\n",
               (strided == TARGET_STRIDED) ? "Target Strided" :
               (strided == ORIGIN_STRIDED) ? "Origin Strided" :
               "Both Strided",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            ptrdiff_t origin_stride, target_stride;
            size_t msg_size = MAX_COUNT * (sizeof *origin_send);

            target_stride = 1;
            origin_stride = 1;

            if (strided == TARGET_STRIDED)
                target_stride = stride;

            if (strided == ORIGIN_STRIDED)
                origin_stride = stride;

            if (strided == BOTH_STRIDED) {
                target_stride = stride;
                origin_stride = stride;
            }

            t1 = get_Wtime();
            for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
                if (target_recv_r) {
                    int j, k1, k2;
                    k1 = 0;
                    k2 = 0;
                    for (j = 0; j < MAX_COUNT; j++) {
                        target_recv_r[k1] = origin_send[k2];
                        k1 += target_stride;
                        k2 += origin_stride;
                    }
                } else
#endif
                {
                    shmem_int_iput(target_recv, origin_send, target_stride,
                                   origin_stride, MAX_COUNT, partner);
                    shmem_fence();
                }
                if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
                  nrep = i;
                }
            }
            t2 = get_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other),
                             i);
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

void run_strided_get_bw_test(strided_type_t strided)
{
    int *origin_recv, *target_send, *target_send_r;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    ptrdiff_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = send_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_send_r = shmem_ptr(target_send, partner);
#endif

    if (my_node == 0) {
        printf("\n\n1-way %s Get Bandwidth: (%d pairs)\n",
               (strided == TARGET_STRIDED) ? "Target Strided" :
               (strided == ORIGIN_STRIDED) ? "Origin Strided" :
               "Both Strided",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            ptrdiff_t origin_stride, target_stride;
            size_t msg_size = MAX_COUNT * (sizeof *target_send);

            target_stride = 1;
            origin_stride = 1;

            if (strided == TARGET_STRIDED)
                target_stride = stride;

            if (strided == ORIGIN_STRIDED)
                origin_stride = stride;

            if (strided == BOTH_STRIDED) {
                target_stride = stride;
                origin_stride = stride;
            }

            t1 = get_Wtime();
            for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
                if (target_send_r) {
                    int j, k1, k2;
                    k1 = 0;
                    k2 = 0;
                    for (j = 0; j < MAX_COUNT; j++) {
                        origin_recv[k1] = target_send_r[k2];
                        k1 += target_stride;
                        k2 += origin_stride;
                    }
                } else
#endif
                {
                    shmem_int_iget(origin_recv, target_send, origin_stride,
                            target_stride, MAX_COUNT, partner);
                }
                if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
                  nrep = i;
                }
            }
            t2 = get_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        }

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other), i);
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

void run_put_bidir_bw_test()
{
    int *origin_send, *target_recv, *target_recv_r;
    double t1, t2;
    int num_pairs;
    int num_stats;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = recv_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_recv_r = shmem_ptr(target_recv, partner);
#endif

    if (my_node == 0) {
        printf("\n\n2-way Put Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;
        size_t msg_size = blksize * (sizeof *origin_send);

        t1 = get_Wtime();
        for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
            if (target_recv_r) {
                memcpy(target_recv_r, origin_send, msg_size);
            }
            else
#endif
            {
                shmem_putmem(target_recv, origin_send, msg_size, partner);
                shmem_fence();
            }
            if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
              nrep = i;
            }
        }
        t2 = get_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other),
                             i);
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_active_nodes);
        }
        num_stats++;
    }
}

void run_get_bidir_bw_test()
{
    int *origin_recv, *target_send, *target_send_r;
    double t1, t2;
    int num_pairs;
    int num_stats;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    size_t blksize;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = send_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_send_r = shmem_ptr(target_send, partner);
#endif

    if (my_node == 0) {
        printf("\n\n2-way Get Bandwidth (%d pairs):\n",
               num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;
        size_t msg_size = blksize * (sizeof *target_send);

        t1 = get_Wtime();
        for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
            if (target_send_r) {
                memcpy(origin_recv, target_send_r, msg_size);
            }
            else
#endif
            {
                shmem_getmem(origin_recv, target_send, msg_size, partner);
            }
            if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
                nrep = i;
            }
        }
        t2 = get_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other), i);
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_active_nodes);
        }

        num_stats++;
    }
}

void run_strided_put_bidir_bw_test(strided_type_t strided)
{
    int *origin_send, *target_recv, *target_recv_r;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    ptrdiff_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_send = send_buffer;
    target_recv = recv_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_recv_r = shmem_ptr(target_recv, partner);
#endif

    if (my_node == 0) {
        printf("\n\n2-way %s Put Bandwidth: (%d pairs)\n",
               (strided == TARGET_STRIDED) ? "Target Strided" :
               (strided == ORIGIN_STRIDED) ? "Origin Strided" :
               "Both Strided",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        ptrdiff_t origin_stride, target_stride;
        int nrep = BW_NITER;
        size_t msg_size = MAX_COUNT * (sizeof *origin_send);

        target_stride = 1;
        origin_stride = 1;

        if (strided == TARGET_STRIDED)
            target_stride = stride;

        if (strided == ORIGIN_STRIDED)
            origin_stride = stride;

        if (strided == BOTH_STRIDED) {
            target_stride = stride;
            origin_stride = stride;
        }

        t1 = get_Wtime();
        for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
            if (target_recv_r) {
                int j, k1, k2;
                k1 = 0;
                k2 = 0;
                for (j = 0; j < MAX_COUNT; j++) {
                    target_recv_r[k1] = origin_send[k2];
                    k1 += target_stride;
                    k2 += origin_stride;
                }
            } else
#endif
            {
                shmem_int_iput(target_recv, origin_send, target_stride,
                        origin_stride, MAX_COUNT, partner);
                shmem_fence();
            }
            if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
              nrep = i;
            }
        }
        t2 = get_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other), i);
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

void run_strided_get_bidir_bw_test(strided_type_t strided)
{
    int *origin_recv, *target_send, *target_send_r;
    double t1, t2;
    int num_stats;
    int num_pairs;
    const size_t MAX_COUNT = 32*1024;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *target_send);
    const size_t MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT;
    ptrdiff_t stride;
    double *stats;

    num_pairs = num_active_nodes / 2;
    origin_recv = recv_buffer;
    target_send = send_buffer;

    stats = stats_buffer;
#ifdef USE_SHMEM_PTR
    target_send_r = shmem_ptr(target_send, partner);
#endif

    if (my_node == 0) {
        printf("\n\n2-way %s Get Bandwidth: (%d pairs)\n",
               (strided == TARGET_STRIDED) ? "Target Strided" :
               (strided == ORIGIN_STRIDED) ? "Origin Strided" :
               "Both Strided",
               num_pairs);
        printf("%20s %20s %20s %20s\n", "count", "stride", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (stride = 1; stride <= MAX_STRIDE; stride *= 2) {
        int i;
        int nrep = BW_NITER;
        ptrdiff_t origin_stride, target_stride;
        size_t msg_size = MAX_COUNT * (sizeof *target_send);

        target_stride = 1;
        origin_stride = 1;

        if (strided == TARGET_STRIDED)
            target_stride = stride;

        if (strided == ORIGIN_STRIDED)
            origin_stride = stride;

        if (strided == BOTH_STRIDED) {
            target_stride = stride;
            origin_stride = stride;
        }

        t1 = get_Wtime();
        for (i = 0; i < nrep; i++) {
#ifdef USE_SHMEM_PTR
            if (target_send_r) {
                int j, k1, k2;
                k1 = 0;
                k2 = 0;
                for (j = 0; j < MAX_COUNT; j++) {
                    origin_recv[k1] = target_send_r[k2];
                    k1 += target_stride;
                    k2 += origin_stride;
                }
            } else
#endif
            {
                shmem_int_iget(origin_recv, target_send, origin_stride,
                        target_stride, MAX_COUNT, partner);
            }
            if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
              nrep = i;
            }
        }
        t2 = get_Wtime();

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                double *stats_other = stats;
                shmem_getmem(&bw_other, stats_other, sizeof(bw_other), i);
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

/********************************************************************
 *                      REDUCTION TESTS
 ********************************************************************/

void run_reduce_test(int separate_target)
{
    int *origin_send, *target_recv;
    int *pWrk;
    long *pSync;
    int pWrk_size;
    double t1, t2;
    int num_stats;
    const size_t MAX_BLKSIZE = MAX_MSG_SIZE / (sizeof *origin_send);
    size_t blksize;
    double *stats;
    int i;

    pSync = shmalloc(_SHMEM_REDUCE_SYNC_SIZE * sizeof (*pSync));
    for (i = 0; i < _SHMEM_REDUCE_SYNC_SIZE; i++) {
        pSync[i] = _SHMEM_SYNC_VALUE;
    }

    origin_send = send_buffer;
    if (separate_target) {
        target_recv = recv_buffer;
    } else {
        target_recv = origin_send;
    }

    stats = stats_buffer;

    if (my_node == 0) {
        printf("\n\nReduction (src %s target): \n",
                separate_target ? "!=" : "==");
        printf("%20s %20s %20s\n", "blksize", "nrep", "latency");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = RED_NITER;
        size_t msg_size = blksize * (sizeof *origin_send);
        int x1, x2;

        x1 = blksize/2+1;
        x2 = _SHMEM_REDUCE_MIN_WRKDATA_SIZE;
        pWrk_size = (x1 > x2) ? x1 : x2;
        pWrk = shmalloc(pWrk_size * sizeof(*pWrk));

        t1 = get_Wtime();
        for (i = 0; i < nrep; i++) {
            shmem_int_sum_to_all(target_recv, origin_send, blksize,
                     0, 0, num_active_nodes, pWrk, pSync);
            //shmem_quiet();
            shmem_barrier_all();
            if (i % 10 == 0 && (get_Wtime() - t1) > TIMEOUT) {
              nrep = i;
            }
        }
        t2 = get_Wtime();

        shfree(pWrk);
        stats[num_stats] = 1000000*(t2-t1)/(RED_NITER);

        shmem_barrier_all();

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double lat_other;
                double *stats_other = stats;
                shmem_getmem(&lat_other, &stats_other[num_stats],
                             sizeof(lat_other), i);
                stats[num_stats] += lat_other;
            }

            printf("%20ld %20ld %17.3f us\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_active_nodes);
        }
        num_stats++;
    }

    shfree(pSync);
}
