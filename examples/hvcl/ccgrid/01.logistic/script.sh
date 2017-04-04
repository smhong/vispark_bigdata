#!/bin/bash

RUN_COMMAND="/home/whchoi/Project/vispark/python/pyspark/vislib/gpu_manager.py"
KILL_COMMAND="/home/whchoi/Project/vispark/python/pyspark/vislib/kill_manager.py"


CUDA_PROFILE_RESULT='cuda_%q{OMPI_COMM_WORLD_RANK}.prof'

MPI_PROFILE_SCRIPT="mpirun --mca oob_tcp_if_include ib0 -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 nvprof -f -o $CUDA_PROFILE_RESULT python $RUN_COMMAND"

MPI_RUN_SCRIPT="mpirun --mca oob_tcp_if_include ib0 -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 python $RUN_COMMAND"
MPI_KILL_SCRIPT="mpirun --mca oob_tcp_if_include ib0 -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 python $KILL_COMMAND"


SPARK_RUN="spark-submit spark_lr.py"
VISPARK_RUN="vispark vispark_lr.py"


#Elems=( 4000000 8000000 16000000 32000000 )
Elems=( 32000000 )
#Elems=( 4000000 )
#Elems=( 10000000 )
GPUpersist=( 0 1 )
#Dims=( 40 80 160 320 )
Dims=( 40 )

for t in "${Dims[@]}"
do
    for p in "${GPUpersist[@]}"
    do
        for k in "${Elems[@]}"
        do
            for ((i=0;i<1;i+=1)) 
            do 
                #$MPI_RUN_SCRIPT &
                #$MPI_PROFILE_SCRIPT &
                $VISPARK_RUN $k $p $t
                #$MPI_KILL_SCRIPT
                $SPARK_RUN $k $p $t
            done
        done
    done
done
