/*
 * Microbenchmarks for MPI
 *
 * (C) HPCTools Group, University of Houston
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>


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

void run_latency_test();

void  run_bw_test();
void  run_bidir_bw_test();
void  run_reduce_test(int separate_target);


const int TIMEOUT = 5;
const size_t SEGMENT_SIZE = 30*1024*1024;
const size_t MAX_MSG_SIZE = 4*1024*1024;
const long long LAT_NITER = 10000;
const long long BW_NITER = 1000;
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

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &my_node);
    MPI_Comm_size(MPI_COMM_WORLD, &num_nodes);

    if (num_nodes % 2 != 0) {
        fprintf(stderr, "Number of PEs should be even.\n");
        exit(1);
    }

    num_active_nodes = num_nodes;
    partner = (my_node + num_active_nodes/2) % num_active_nodes;

    send_buffer         = malloc(MAX_MSG_SIZE);
    recv_buffer         = malloc(MAX_MSG_SIZE);
    stats_buffer        = malloc(NUM_STATS * sizeof(stats_buffer[0]));
    sync_notify_buffer  = malloc(num_nodes * sizeof(sync_notify_buffer[0]));

    /* run tests */

    run_latency_test();

    /*
    run_bw_test();
    run_bidir_bw_test();

    run_reduce_test(0);
    run_reduce_test(1);
    */

    MPI_Finalize();
}

/********************************************************************
 *                      LATENCY TESTS
 ********************************************************************/

void run_latency_test()
{
    MPI_Status status;
    int *origin_send, *target_recv;
    int *origin_recv, *target_send;
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

    if (my_node == 0) {
        printf("\n\nLatency: (%d active pairs)\n", num_pairs);
    }

    if (my_node < partner) {
        t1 = MPI_Wtime();
        for (i = 0; i < LAT_NITER; i++) {
            MPI_Send(origin_send, 1, MPI_INT, partner, 0, MPI_COMM_WORLD);
            MPI_Recv(origin_recv, 1, MPI_INT, partner, 0, MPI_COMM_WORLD, &status);
        }
        t2 = MPI_Wtime();

        stats[0] = 1000000*(t2-t1)/(LAT_NITER);
    } else {
        for (i = 0; i < LAT_NITER; i++) {
            MPI_Recv(origin_recv, 1, MPI_INT, partner, 0, MPI_COMM_WORLD, &status);
            MPI_Send(origin_send, 1, MPI_INT, partner, 0, MPI_COMM_WORLD);
        }
    }


    if (my_node == 0) {
        /* collect stats from other active nodes */
        for (i = 1; i < num_pairs; i++) {
            double latency_other;
            MPI_Recv(&latency_other, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &status);
            stats[0] += latency_other;
        }
        printf("%20.8lf us\n", stats[0]/num_pairs);
    } else if (my_node < num_pairs) {
        MPI_Send(stats, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    }
}


/********************************************************************
 *                      1-Way Bandwidth Tests
 ********************************************************************/

void run_bw_test()
{
    MPI_Status status;
    int *origin_send, *target_recv;
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

    if (my_node == 0) {
        printf("\n\n1-way Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;

        if (my_node < partner) {
            size_t msg_size = blksize * (sizeof *origin_send);
            t1 = MPI_Wtime();
            for (i = 0; i < nrep; i++) {
                MPI_Recv(recv_buffer, blksize, MPI_INT, partner, i, MPI_COMM_WORLD,
                         &status);
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        } else {
            size_t msg_size = blksize * (sizeof *origin_send);
            for (i = 0; i < nrep; i++) {
                MPI_Send(send_buffer, blksize, MPI_INT, partner, i, MPI_COMM_WORLD);
            }
        }

        MPI_Barrier(MPI_COMM_WORLD);


        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_pairs; i++) {
                double bw_other;
                MPI_Recv(&bw_other, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &status);
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_pairs);
        } else if (my_node < num_pairs) {
            MPI_Send(&stats[num_stats], 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        }
        num_stats++;
    }
}


/********************************************************************
 *                      2-Way Bandwidth Tests
 ********************************************************************/

void run_bidir_bw_test()
{
    MPI_Status status;
    MPI_Request req;
    int *origin_send, *target_recv;
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

    if (my_node == 0) {
        printf("\n\n2-way Bandwidth: (%d pairs)\n",
               num_pairs);
        printf("%20s %20s %20s\n", "blksize", "nrep", "bandwidth");
    }
    num_stats = 0;
    for (blksize = 1; blksize <= MAX_BLKSIZE; blksize *= 2) {
        int i;
        int nrep = BW_NITER;
        size_t msg_size = blksize * (sizeof *origin_send);

        if (my_node < partner) {
            size_t msg_size = blksize * (sizeof *origin_send);
            t1 = MPI_Wtime();
            for (i = 0; i < nrep; i++) {
                MPI_Irecv(recv_buffer, blksize, MPI_INT, partner, i, MPI_COMM_WORLD,
                          &req);
                MPI_Send(recv_buffer, blksize, MPI_INT, partner, i, MPI_COMM_WORLD);
                MPI_Wait(&req, &status);
            }
            t2 = MPI_Wtime();

            stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));
        } else {
            size_t msg_size = blksize * (sizeof *origin_send);
            t1 = MPI_Wtime();
            for (i = 0; i < nrep; i++) {
                MPI_Irecv(recv_buffer, blksize, MPI_INT, partner, i, MPI_COMM_WORLD,
                          &req);
                MPI_Send(recv_buffer, blksize, MPI_INT, partner, i, MPI_COMM_WORLD);
                MPI_Wait(&req, &status);
            }
            t2 = MPI_Wtime();
        }

        stats[num_stats] = msg_size*nrep/(1024*1024*(t2-t1));

        MPI_Barrier(MPI_COMM_WORLD);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double bw_other;
                MPI_Recv(&bw_other, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &status);
                stats[num_stats] += bw_other;
            }

            printf("%20ld %20ld %17.3f MB/s\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_active_nodes);
        } else if (my_node < num_active_nodes) {
            MPI_Send(&stats[num_stats], 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        }
        num_stats++;
    }
}


/********************************************************************
 *                      REDUCTION TESTS
 ********************************************************************/

void run_reduce_test(int separate_target)
{
    MPI_Status status;
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

        t1 = MPI_Wtime();
        for (i = 0; i < nrep; i++) {
            if (separate_target) {
                MPI_Allreduce(origin_send, target_recv, blksize, MPI_INT, MPI_SUM,
                              MPI_COMM_WORLD);
            } else {
                MPI_Allreduce(MPI_IN_PLACE, target_recv, blksize, MPI_INT, MPI_SUM,
                              MPI_COMM_WORLD);
            }
            if (i % 10 == 0 && (MPI_Wtime() - t1) > TIMEOUT) {
              nrep = i;
            }
        }
        t2 = MPI_Wtime();

        stats[num_stats] = 1000000*(t2-t1)/(RED_NITER);

        if (my_node == 0) {
            /* collect stats from other nodes */
            for (i = 1; i < num_active_nodes; i++) {
                double lat_other;
                MPI_Recv(&lat_other, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &status);
                stats[num_stats] += lat_other;
            }

            printf("%20ld %20ld %17.3f us\n",
                    (long)blksize, (long)nrep,
                    stats[num_stats]/num_active_nodes);
        } else if (my_node < num_active_nodes) {
            MPI_Send(&stats[num_stats], 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        }
        num_stats++;
    }
}
