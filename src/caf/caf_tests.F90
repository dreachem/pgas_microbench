!
! Microbenchmarks for CAF
!
! (C) HPCTools Group, University of Houston
!

module caf_microbenchmarks
    use, intrinsic :: iso_fortran_env
    implicit none

    include 'mpif.h'

    integer, parameter :: BARRIER = 1
    integer, parameter :: P2P = 2

    integer, parameter :: TARGET_STRIDED = 1
    integer, parameter :: ORIGIN_STRIDED = 2
    integer, parameter :: BOTH_STRIDED = 3

    integer, parameter :: BUFFER_SIZE = 1024*1024
    integer, parameter :: LAT_NITER = 10000
    integer, parameter :: BW_NITER = 1000

    integer, parameter :: TIMEOUT = 5

    integer, parameter :: ELEM_SIZE = 4

    integer, parameter :: NUM_STATS = 32

    type(lock_type) :: image_lock[*]
    integer :: send_buffer(BUFFER_SIZE)[*]
    integer :: recv_buffer(BUFFER_SIZE)[*]
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
            write (*,'(//,"Put-Get Latency: (",I0," pairs)")') &
                 num_pairs
        end if

        if (ti < partner) then
            t1 = MPI_WTIME()
            do i = 1, LAT_NITER
              recv_buffer(1)[partner] = send_buffer(1)
              recv_buffer(1) = recv_buffer(1)[partner]
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
                write (*,'(//,"Put-Put Latency: (",I0," pairs, ",A0,")")') &
                    num_pairs, "barrier"
            else
                write (*,'(//, "Put-Put Latency: (",I0," pairs, ",A0,")")') &
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
                write (*,'(//,"Get-Get Latency: (",I0," pairs, ",A0,")")') &
                    num_pairs, "barrier"
            else
                write (*,'(//,"Get-Get Latency: (",I0," pairs, ",A0,")")') &
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
            write (*,'(//,"1-Way Put Bandwith: (",I0," pairs)")') &
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
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()

              stats_buffer(num_stats) = &
                  dble(blksize)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))
          end if

          sync all

          if (ti == 1) then
              do i = 2, num_pairs
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,F17.3, " MB/s")') &
                     blksize, nrep, stats_buffer(num_stats)/num_pairs
          end if

          num_stats = num_stats + 1
          blksize = blksize * 2
        end do

    end subroutine run_put_bw_test

    subroutine run_get_bw_test()
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
            write (*,'(//,"1-Way Get Bandwith: (",I0," pairs)")') &
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
                recv_buffer(1:blksize) = send_buffer(1:blksize)[partner]
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()

              stats_buffer(num_stats) = &
                  dble(blksize)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))
          end if

          sync all

          if (ti == 1) then
              do i = 2, num_pairs
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,F17.3, " MB/s")') &
                      blksize, nrep, stats_buffer(num_stats)/num_pairs
          end if

          num_stats = num_stats + 1
          blksize = blksize * 2
        end do
    end subroutine run_get_bw_test

    subroutine run_rand_put_bw_test()
        implicit none
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer :: blksize
        integer :: i
        integer :: rand_seed, rand_index, rand_image
        real    :: real_rand_num

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            write (*,'(//,"Random Put Bandwith")')
            write (*,'(A20, A20, A20)') "blksize", "nrep", "bandwidth"
        end if

        num_stats = 1
        blksize = 1
        do while (blksize <= BUFFER_SIZE/2)
          nrep = BW_NITER

          t1 = MPI_WTIME()
          do i = 1, nrep
            rand_seed = i*this_image()
            call random_seed(rand_seed)
            call random_number(real_rand_num)
            rand_index = INT(real_rand_num*BUFFER_SIZE/2)
            call random_number(real_rand_num)
            rand_image = INT(real_rand_num*num_images())+1
            lock (image_lock[rand_image])
            recv_buffer(rand_index:rand_index+blksize-1)[rand_image] = &
                send_buffer(1:blksize)
            unlock (image_lock[rand_image])
            if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
              nrep = i
              exit
            end if
          end do
          t2 = MPI_WTIME()

          stats_buffer(num_stats) = &
              dble(blksize)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_images()
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,F17.3, " MB/s")') &
                     blksize, nrep, stats_buffer(num_stats)/num_images()
          end if

          num_stats = num_stats + 1
          blksize = blksize * 2
        end do

    end subroutine run_rand_put_bw_test

    subroutine run_rand_get_bw_test()
        implicit none
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer :: blksize
        integer :: i
        integer :: rand_seed, rand_index, rand_image
        real    :: real_rand_num

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            write (*,'(//,"Random Get Bandwith")')
            write (*,'(A20, A20, A20)') "blksize", "nrep", "bandwidth"
        end if

        num_stats = 1
        blksize = 1
        do while (blksize <= BUFFER_SIZE)
          nrep = BW_NITER

          t1 = MPI_WTIME()
          do i = 1, nrep
            rand_seed = i*this_image()
            call random_seed(rand_seed)
            call random_number(real_rand_num)
            rand_index = INT(real_rand_num*BUFFER_SIZE/2)
            call random_number(real_rand_num)
            rand_image = INT(real_rand_num*num_images())+1
            lock (image_lock[rand_image])
            recv_buffer(1:blksize) = &
                 send_buffer(rand_index:rand_index+blksize-1)[rand_image]
            unlock (image_lock[rand_image])
            if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
              nrep = i
              exit
            end if
          end do
          t2 = MPI_WTIME()

          stats_buffer(num_stats) = &
              dble(blksize)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_images()
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,F17.3, " MB/s")') &
                     blksize, nrep, stats_buffer(num_stats)/num_images()
          end if

          num_stats = num_stats + 1
          blksize = blksize * 2
        end do
    end subroutine run_rand_get_bw_test

    subroutine run_strided_put_bw_test(strided)
        implicit none

        integer, intent(in) :: strided

        character (len=15) :: strided_label(3)
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer, parameter :: MAX_COUNT = 32*1024
        integer, parameter :: MAX_BLKSIZE = BUFFER_SIZE
        integer, parameter :: MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT
        integer :: stride,extent
        integer :: i

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            strided_label(1) = "Target Strided"
            strided_label(2) = "Origin Strided"
            strided_label(3) = "Both Strided"

            write (*,'(//,"1-Way ",A0," Put Bandwith: (",I0," pairs)")') &
                strided_label(strided), num_pairs
            write (*,'(A20, A20, A20, A20)') "count", "stride", "nrep", "bandwidth"
        end if

        num_stats = 1
        stride = 1
        do while (stride <= MAX_STRIDE)
          nrep = BW_NITER

          if (ti < partner) then
              if (strided == TARGET_STRIDED) then
                  t1 = MPI_WTIME()
                  do i = 1, nrep
                    extent = MAX_COUNT*stride
                    recv_buffer(1:extent:stride)[partner] = send_buffer(1:MAX_COUNT)
                    if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                      nrep = i
                      exit
                    end if
                  end do
                  t2 = MPI_WTIME()
              else if (strided == ORIGIN_STRIDED) then
                  t1 = MPI_WTIME()
                  do i = 1, nrep
                    extent = MAX_COUNT*stride
                    recv_buffer(1:MAX_COUNT)[partner] = send_buffer(1:extent:stride)
                    if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                      nrep = i
                      exit
                    end if
                  end do
                  t2 = MPI_WTIME()
              else
                  t1 = MPI_WTIME()
                  do i = 1, nrep
                    extent = MAX_COUNT*stride
                    recv_buffer(1:extent:stride)[partner] = &
                                 send_buffer(1:extent:stride)
                    if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                      nrep = i
                      exit
                    end if
                  end do
                  t2 = MPI_WTIME()
              end if

              stats_buffer(num_stats) = &
                  dble(MAX_COUNT)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))
          end if

          sync all

          if (ti == 1) then
              do i = 2, num_pairs
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,I20,F17.3, " MB/s")') &
                     MAX_COUNT, stride, nrep, stats_buffer(num_stats)/num_pairs
          end if

          num_stats = num_stats + 1
          stride = stride * 2
        end do
    end subroutine run_strided_put_bw_test

    subroutine run_strided_get_bw_test(strided)
        implicit none

        integer, intent(in) :: strided

        character (len=15) :: strided_label(3)
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer, parameter :: MAX_COUNT = 32*1024
        integer, parameter :: MAX_BLKSIZE = BUFFER_SIZE
        integer, parameter :: MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT
        integer :: stride,extent
        integer :: i

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            strided_label(1) = "Target Strided"
            strided_label(2) = "Origin Strided"
            strided_label(3) = "Both Strided"

            write (*,'(//,"1-Way ",A0," Get Bandwith: (",I0," pairs)")') &
                strided_label(strided), num_pairs
            write (*,'(A20, A20, A20, A20)') "count", "stride", "nrep", "bandwidth"
        end if

        num_stats = 1
        stride = 1
        do while (stride <= MAX_STRIDE)
          nrep = BW_NITER

          if (ti < partner) then
              if (strided == TARGET_STRIDED) then
                  t1 = MPI_WTIME()
                  do i = 1, nrep
                    extent = MAX_COUNT*stride
                    recv_buffer(1:MAX_COUNT) = send_buffer(1:extent:stride)[partner]
                    if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                      nrep = i
                      exit
                    end if
                  end do
                  t2 = MPI_WTIME()
              else if (strided == ORIGIN_STRIDED) then
                  t1 = MPI_WTIME()
                  do i = 1, nrep
                    extent = MAX_COUNT*stride
                    recv_buffer(1:extent:stride) = send_buffer(1:MAX_COUNT)[partner]
                    if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                      nrep = i
                      exit
                    end if
                  end do
                  t2 = MPI_WTIME()
              else
                  t1 = MPI_WTIME()
                  do i = 1, nrep
                    extent = MAX_COUNT*stride
                    recv_buffer(1:extent:stride) = &
                                 send_buffer(1:extent:stride)[partner]
                    if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                      nrep = i
                      exit
                    end if
                  end do
                  t2 = MPI_WTIME()
              end if

              stats_buffer(num_stats) = &
                  dble(MAX_COUNT)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))
          end if

          sync all

          if (ti == 1) then
              do i = 2, num_pairs
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,I20,F17.3, " MB/s")') &
                     MAX_COUNT, stride, nrep, stats_buffer(num_stats)/num_pairs
          end if

          num_stats = num_stats + 1
          stride = stride * 2
        end do
    end subroutine run_strided_get_bw_test


    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    !                   2-WAY BANDWIDTH TESTS
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    subroutine run_put_bw_bidir_test()
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
            write (*,'(//,"2-Way Put Bandwith: (",I0," pairs)")') &
                num_pairs
            write (*,'(A20, A20, A20)') "blksize", "nrep", "bandwidth"
        end if

        num_stats = 1
        blksize = 1
        do while (blksize <= BUFFER_SIZE)
          nrep = BW_NITER

          t1 = MPI_WTIME()
          do i = 1, nrep
            recv_buffer(1:blksize)[partner] = send_buffer(1:blksize)
            if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
              nrep = i
              exit
            end if
          end do
          t2 = MPI_WTIME()

          stats_buffer(num_stats) = &
              dble(blksize)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_active_images
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,F17.3, " MB/s")') &
                     blksize, nrep, stats_buffer(num_stats)/num_active_images
          end if

          num_stats = num_stats + 1
          blksize = blksize * 2
        end do
    end subroutine run_put_bw_bidir_test

    subroutine run_get_bw_bidir_test()
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
            write (*,'(//,"2-Way Get Bandwith: (",I0," pairs)")') &
                num_pairs
            write (*,'(A20, A20, A20)') "blksize", "nrep", "bandwidth"
        end if

        num_stats = 1
        blksize = 1
        do while (blksize <= BUFFER_SIZE)
          nrep = BW_NITER

          t1 = MPI_WTIME()
          do i = 1, nrep
            recv_buffer(1:blksize) = send_buffer(1:blksize)[partner]
            if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
              nrep = i
              exit
            end if
          end do
          t2 = MPI_WTIME()

          stats_buffer(num_stats) = &
              dble(blksize)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_active_images
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,F17.3, " MB/s")') &
                     blksize, nrep, stats_buffer(num_stats)/num_active_images
          end if

          num_stats = num_stats + 1
          blksize = blksize * 2
        end do
    end subroutine run_get_bw_bidir_test

    subroutine run_strided_put_bidir_bw_test(strided)
        implicit none

        integer, intent(in) :: strided

        character (len=15) :: strided_label(3)
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer, parameter :: MAX_COUNT = 32*1024
        integer, parameter :: MAX_BLKSIZE = BUFFER_SIZE
        integer, parameter :: MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT
        integer :: stride,extent
        integer :: i

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            strided_label(1) = "Target Strided"
            strided_label(2) = "Origin Strided"
            strided_label(3) = "Both Strided"

            write (*,'(//,"2-Way ",A0," Put Bandwith: (",I0," pairs)")') &
                strided_label(strided), num_pairs
            write (*,'(A20, A20, A20, A20)') "count", "stride", "nrep", "bandwidth"
        end if

        num_stats = 1
        stride = 1
        do while (stride <= MAX_STRIDE)
          nrep = BW_NITER

          if (strided == TARGET_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:extent:stride)[partner] = send_buffer(1:MAX_COUNT)
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else if (strided == ORIGIN_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:MAX_COUNT)[partner] = send_buffer(1:extent:stride)
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:extent:stride)[partner] = &
                             send_buffer(1:extent:stride)
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          end if

          stats_buffer(num_stats) = &
              dble(MAX_COUNT)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_active_images
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,I20,F17.3, " MB/s")') &
                     MAX_COUNT, stride, nrep, &
                     stats_buffer(num_stats)/num_active_images
          end if

          num_stats = num_stats + 1
          stride = stride * 2
        end do
    end subroutine run_strided_put_bidir_bw_test

    subroutine run_strided_get_bidir_bw_test(strided)
        implicit none

        integer, intent(in) :: strided

        character (len=15) :: strided_label(3)
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer, parameter :: MAX_COUNT = 32*1024
        integer, parameter :: MAX_BLKSIZE = BUFFER_SIZE
        integer, parameter :: MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT
        integer :: stride,extent
        integer :: i

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            strided_label(1) = "Target Strided"
            strided_label(2) = "Origin Strided"
            strided_label(3) = "Both Strided"

            write (*,'(//,"2-Way ",A0, " Get Bandwith: (",I0," pairs)")') &
                strided_label(strided), num_pairs
            write (*,'(A20, A20, A20, A20)') "count", "stride", "nrep", "bandwidth"
        end if

        num_stats = 1
        stride = 1
        do while (stride <= MAX_STRIDE)
          nrep = BW_NITER

          if (strided == TARGET_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:MAX_COUNT) = send_buffer(1:extent:stride)[partner]
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else if (strided == ORIGIN_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:extent:stride) = send_buffer(1:MAX_COUNT)[partner]
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:extent:stride) = &
                             send_buffer(1:extent:stride)[partner]
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          end if

          stats_buffer(num_stats) = &
              dble(MAX_COUNT)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_active_images
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,I20,F17.3, " MB/s")') &
                     MAX_COUNT, stride, nrep, &
                     stats_buffer(num_stats)/num_active_images
          end if

          num_stats = num_stats + 1
          stride = stride * 2
        end do
    end subroutine run_strided_get_bidir_bw_test

    subroutine run_rand_strided_put_bw_test(strided)
        implicit none

        integer, intent(in) :: strided

        character (len=15) :: strided_label(3)
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer, parameter :: MAX_COUNT = 32*1024
        integer, parameter :: MAX_BLKSIZE = BUFFER_SIZE
        integer, parameter :: MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT
        integer :: stride,extent
        integer :: i
        integer :: rand_seed, rand_image
        real    :: real_rand_num

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            strided_label(1) = "Target Strided"
            strided_label(2) = "Origin Strided"
            strided_label(3) = "Both Strided"

            write (*,'(//,A0," Random Put Bandwith")') &
                strided_label(strided)
            write (*,'(A20, A20, A20, A20)') "count", "stride", "nrep", "bandwidth"
        end if

        num_stats = 1
        stride = 1
        do while (stride <= MAX_STRIDE)
          nrep = BW_NITER

          if (strided == TARGET_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                rand_seed = i*this_image()
                call random_seed(rand_seed)
                call random_number(real_rand_num)
                rand_image = INT(real_rand_num*num_images())+1
                extent = MAX_COUNT*stride
                lock (image_lock[rand_image])
                recv_buffer(1:extent:stride)[partner] = send_buffer(1:MAX_COUNT)
                unlock (image_lock[rand_image])
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else if (strided == ORIGIN_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                rand_seed = i*this_image()
                call random_seed(rand_seed)
                call random_number(real_rand_num)
                rand_image = INT(real_rand_num*num_images())+1
                extent = MAX_COUNT*stride
                lock (image_lock[rand_image])
                recv_buffer(1:MAX_COUNT)[partner] = send_buffer(1:extent:stride)
                unlock (image_lock[rand_image])
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else
              t1 = MPI_WTIME()
              do i = 1, nrep
                rand_seed = i*this_image()
                call random_seed(rand_seed)
                call random_number(real_rand_num)
                rand_image = INT(real_rand_num*num_images())+1
                extent = MAX_COUNT*stride
                lock (image_lock[rand_image])
                recv_buffer(1:extent:stride)[partner] = &
                             send_buffer(1:extent:stride)
                unlock (image_lock[rand_image])
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          end if

          stats_buffer(num_stats) = &
              dble(MAX_COUNT)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_images()
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,I20,F17.3, " MB/s")') &
                     MAX_COUNT, stride, nrep, &
                     stats_buffer(num_stats)/num_images()
          end if

          num_stats = num_stats + 1
          stride = stride * 2
        end do
    end subroutine run_rand_strided_put_bw_test

    subroutine run_rand_strided_get_bw_test(strided)
        implicit none

        integer, intent(in) :: strided

        character (len=15) :: strided_label(3)
        double precision :: t1, t2
        integer :: ti, ni, nrep
        integer :: num_stats
        integer :: num_pairs
        integer, parameter :: MAX_COUNT = 32*1024
        integer, parameter :: MAX_BLKSIZE = BUFFER_SIZE
        integer, parameter :: MAX_STRIDE = MAX_BLKSIZE / MAX_COUNT
        integer :: stride,extent
        integer :: i
        integer :: rand_seed, rand_image
        real    :: real_rand_num

        ti = this_image()
        ni = num_images()
        num_pairs = num_active_images / 2

        if (ti == 1) then
            strided_label(1) = "Target Strided"
            strided_label(2) = "Origin Strided"
            strided_label(3) = "Both Strided"

            write (*,'(//,A0, " Random Get Bandwith ")') &
                strided_label(strided)
            write (*,'(A20, A20, A20, A20)') "count", "stride", "nrep", "bandwidth"
        end if

        num_stats = 1
        stride = 1
        do while (stride <= MAX_STRIDE)
          nrep = BW_NITER

          if (strided == TARGET_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                rand_seed = i*this_image()
                call random_seed(rand_seed)
                call random_number(real_rand_num)
                rand_image = INT(real_rand_num*num_images())+1
                extent = MAX_COUNT*stride
                lock (image_lock[rand_image])
                recv_buffer(1:MAX_COUNT) = send_buffer(1:extent:stride)[partner]
                unlock (image_lock[rand_image])
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else if (strided == ORIGIN_STRIDED) then
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:extent:stride) = send_buffer(1:MAX_COUNT)[partner]
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          else
              t1 = MPI_WTIME()
              do i = 1, nrep
                extent = MAX_COUNT*stride
                recv_buffer(1:extent:stride) = &
                             send_buffer(1:extent:stride)[partner]
                if (mod(i,10) == 0 .and. (MPI_WTIME()-t1) > TIMEOUT) then
                  nrep = i
                  exit
                end if
              end do
              t2 = MPI_WTIME()
          end if

          stats_buffer(num_stats) = &
              dble(MAX_COUNT)*ELEM_SIZE*nrep/(1024*1024*(t2-t1))

          sync all

          if (ti == 1) then
              do i = 2, num_images()
                  stats_buffer(num_stats) = stats_buffer(num_stats) + &
                                            stats_buffer(num_stats)[i]
              end do
              write (*, '(I20,I20,I20,F17.3, " MB/s")') &
                     MAX_COUNT, stride, nrep, &
                     stats_buffer(num_stats)/num_images()
          end if

          num_stats = num_stats + 1
          stride = stride * 2
        end do
    end subroutine run_rand_strided_get_bw_test
end module caf_microbenchmarks

program main
    use caf_microbenchmarks

    implicit none

    if (mod(num_images(), 2) /= 0) then
        error stop "use an even number of images"
    end if

    num_active_images = num_images()
    partner = 1 + mod(this_image()-1+num_active_images/2, num_active_images)

    allocate ( stats_buffer(NUM_STATS)[*] )

    call run_putget_latency_test()
    call run_putput_latency_test(BARRIER)
    call run_putput_latency_test(P2P)
    call run_getget_latency_test(BARRIER)
    call run_getget_latency_test(P2P)

    call run_put_bw_test()
    call run_get_bw_test()
    call run_put_bw_bidir_test()
    call run_get_bw_bidir_test()
    call run_rand_put_bw_test()
    call run_rand_get_bw_test()

    call run_strided_put_bw_test(TARGET_STRIDED)
    call run_strided_put_bw_test(ORIGIN_STRIDED)
    call run_strided_put_bw_test(BOTH_STRIDED)

    call run_strided_get_bw_test(TARGET_STRIDED)
    call run_strided_get_bw_test(ORIGIN_STRIDED)
    call run_strided_get_bw_test(BOTH_STRIDED)

    call run_strided_put_bidir_bw_test(TARGET_STRIDED)
    call run_strided_put_bidir_bw_test(ORIGIN_STRIDED)
    call run_strided_put_bidir_bw_test(BOTH_STRIDED)

    call run_strided_get_bidir_bw_test(TARGET_STRIDED)
    call run_strided_get_bidir_bw_test(ORIGIN_STRIDED)
    call run_strided_get_bidir_bw_test(BOTH_STRIDED)

    call run_rand_strided_put_bw_test(TARGET_STRIDED)
    call run_rand_strided_put_bw_test(ORIGIN_STRIDED)
    call run_rand_strided_put_bw_test(BOTH_STRIDED)

    call run_rand_strided_get_bw_test(TARGET_STRIDED)
    call run_rand_strided_get_bw_test(ORIGIN_STRIDED)
    call run_rand_strided_get_bw_test(BOTH_STRIDED)
end program
