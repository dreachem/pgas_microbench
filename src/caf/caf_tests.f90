!
! Microbenchmarks for CAF
!
! (C) HPCTools Group, University of Houston
!

module caf_microbenchmarks
    implicit none

    include 'mpif.h'

    integer, parameter :: BARRIER = 1
    integer, parameter :: P2P = 2

    integer, parameter :: BUFFER_SIZE = 1024*1024
    integer, parameter :: LAT_NITER = 1000000
    integer, parameter :: BW_NITER = 10000

    integer, parameter :: ELEM_SIZE = 4

    integer, allocatable :: send_buffer(:)[:]
    integer, allocatable :: recv_buffer(:)[:]
    double precision, allocatable :: stats_buffer(:)[:]

    integer :: partner
    integer :: num_active_images

    contains


    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    !                 SYNCHRONIZATION ROUTINES
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    subroutine do_sync(sync)
        integer, intent(in) :: sync

        if (sync == BARRIER) then
            sync all
        else if (sync == P2P) then
            sync images (partner)
        end if
    end subroutine

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    !                       LATENCY TESTS
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    subroutine run_putget_latency_test()
        implicit none
        integer :: num_pairs
        double precision :: t1, t2
        double precision :: latency_other

        integer :: ti, ni, i

        ti = this_image()
        ni = num_images()

        num_pairs = num_active_images / 2

        if (ti == 1) then
            write (*,'("Put-Get Latency: (",I3," active pairs)")') &
                 num_pairs
        end if

        if (ti < partner) then
            t1 = MPI_WTIME()
            do i = 1, LAT_NITER
              recv_buffer(1)[partner] = send_buffer(1)
              recv_buffer(1) = send_buffer(1)[partner]
            end do
            t2 = MPI_WTIME()

            stats_buffer(1) = 1000000*(t2-t1)/(LAT_NITER)
        end if

        sync all

        if (ti == 1) then
            ! collect stats from other active nodes
            do i = 2, num_pairs
              latency_other = stats_buffer(1)[i]
              stats_buffer(1) = stats_buffer(1) + latency_other
            end do
            write (*, '(F20.8, " us")') stats_buffer(1)/num_pairs
        end if

    end subroutine run_putget_latency_test

    subroutine run_putput_latency_test(sync)
        implicit none
        integer :: sync

        integer :: num_pairs
        double precision :: t1, t2
        double precision :: latency_other

        integer :: ti, ni, i

        ti = this_image()
        ni = num_images()

        num_pairs = num_active_images / 2

        if (ti == 1) then
            if (sync == BARRIER) then
                write (*,'("Put-Put Latency: (",I3," active pairs, ",A7,")")') &
                    num_pairs, "barrier"
            else
                write (*,'("Put-Put Latency: (",I3," active pairs, ",A3,")")') &
                    num_pairs, "p2p"
            end if
        end if

        if (ti < partner) then
            t1 = MPI_WTIME()
            do i = 1, LAT_NITER
              recv_buffer(1)[partner] = send_buffer(1)
              call do_sync(sync)
              call do_sync(sync)
            end do
            t2 = MPI_WTIME()

            stats_buffer(1) = 1000000*(t2-t1)/(LAT_NITER)
        else if (ti <= num_active_images) then
            t1 = MPI_WTIME()
            do i = 1, LAT_NITER
              call do_sync(sync)
              recv_buffer(1)[partner] = send_buffer(1)
              call do_sync(sync)
            end do
            t2 = MPI_WTIME()

            stats_buffer(1) = 1000000*(t2-t1)/(LAT_NITER)
        end if

        sync all

        if (ti == 1) then
            ! collect stats from other active nodes
            do i = 2, num_active_images
              latency_other = stats_buffer(1)[i]
              stats_buffer(1) = stats_buffer(1) + latency_other
            end do
            write (*, '(F20.8, " us")') stats_buffer(1)/num_active_images
        end if

    end subroutine run_putput_latency_test

    subroutine run_getget_latency_test(sync)
        implicit none
        integer :: sync

        integer :: num_pairs
        double precision :: t1, t2
        double precision :: latency_other

        integer :: ti, ni, i

        ti = this_image()
        ni = num_images()

        num_pairs = num_active_images / 2

        if (ti == 1) then
            if (sync == BARRIER) then
                write (*,'("Get-Get Latency: (",I3," active pairs, ",A7,")")') &
                    num_pairs, "barrier"
            else
                write (*,'("Get-Get Latency: (",I3," active pairs, ",A3,")")') &
                    num_pairs, "p2p"
            end if
        end if

        if (ti < partner) then
            t1 = MPI_WTIME()
            do i = 1, LAT_NITER
              recv_buffer(1) = send_buffer(1)[partner]
              call do_sync(sync)
              call do_sync(sync)
            end do
            t2 = MPI_WTIME()

            stats_buffer(1) = 1000000*(t2-t1)/(LAT_NITER)
        else if (ti <= num_active_images) then
            t1 = MPI_WTIME()
            do i = 1, LAT_NITER
              call do_sync(sync)
              recv_buffer(1) = send_buffer(1)[partner]
              call do_sync(sync)
            end do
            t2 = MPI_WTIME()

            stats_buffer(1) = 1000000*(t2-t1)/(LAT_NITER)
        end if

        sync all

        if (ti == 1) then
            ! collect stats from other active nodes
            do i = 2, num_active_images
              latency_other = stats_buffer(1)[i]
              stats_buffer(1) = stats_buffer(1) + latency_other
            end do
            write (*, '(F20.8, " us")') stats_buffer(1)/num_active_images
        end if

    end subroutine run_getget_latency_test

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    !                   1-WAY BANDWIDTH TESTS
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    subroutine run_put_bw_test()
        implicit none
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer :: blksize
        integer :: i

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            write (*,'("1-Way Put Bandwith: (",I3," active pairs)")') &
                num_pairs
            write (*,'(A20, A20, A20)') "blksize", "nrep", "bandwidth"
        end if

        num_stats = 1
        blksize = 1
        do while (blksize <= BUFFER_SIZE)
          nrep = BW_NITER

          if (ti < partner) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                recv_buffer(1:blksize)[partner] = send_buffer(1:blksize)
              end do
              t2 = MPI_WTIME()

              stats_buffer(num_stats) = &
                  dble(blksize)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))
          end if

          sync all

          if (ti == 1) then
              do i = 2, num_pairs
                  stats_buffer(num_stats) = stats_buffer(num_stats) + stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,F17.3, " MB/S")') &
                     blksize, nrep, stats_buffer(num_stats)/num_pairs
          end if

          num_stats = num_stats + 1
          blksize = blksize * 2
        end do

    end subroutine run_put_bw_test

    subroutine run_get_bw_test()
    end subroutine run_get_bw_test

    subroutine run_strided_put_bw_test()
    end subroutine run_strided_put_bw_test

    subroutine run_strided_get_bw_test()
    end subroutine run_strided_get_bw_test

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    !                   2-WAY BANDWIDTH TESTS
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

end module caf_microbenchmarks

program main
    use caf_microbenchmarks

    implicit none

    if (num_images() < 2) then
        error stop "not enough images running"
    end if

    if (mod(num_images(), 2) == 0) then
        num_active_images = num_images()
    else
        num_active_images = num_images() - 1
    end if

    partner = 1 + mod(this_image()-1+num_active_images/2, num_active_images)

    allocate ( send_buffer(BUFFER_SIZE)[*] )
    allocate ( recv_buffer(BUFFER_SIZE)[*] )
    allocate ( stats_buffer(num_images())[*] )

!     call run_putget_latency_test()
!     call run_putput_latency_test(BARRIER)
!     call run_putput_latency_test(P2P)
!     call run_getget_latency_test(BARRIER)
!     call run_getget_latency_test(P2P)

    call run_put_bw_test()

end program
