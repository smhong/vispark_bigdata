#!/bin/bash

RUN_COMMAND="/home/whchoi/Project/vispark/python/pyspark/vislib/gpu_manager.py"
KILL_COMMAND="/home/whchoi/Project/vispark/python/pyspark/vislib/kill_manager.py"


CUDA_PROFILE_RESULT='cuda_%q{OMPI_COMM_WORLD_RANK}.prof'

MPI_PROFILE_SCRIPT="mpirun --mca oob_tcp_if_include ib0 -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 nvprof -f -o $CUDA_PROFILE_RESULT python $RUN_COMMAND"

MPI_RUN_SCRIPT="mpirun --mca oob_tcp_if_include ib0 -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 python $RUN_COMMAND"
MPI_KILL_SCRIPT="mpirun --mca oob_tcp_if_include ib0 -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 python $KILL_COMMAND"


SPARK_RUN="spark-submit HF_Spark_Patch.py"
SPARK_MPI_RUN="spark-submit HF_Spark_MPI.py"
GPU_MPI_RUN="spark-submit HF_GPU_MPI.py"
VISPARK_RUN="vispark HF_Vispark.py"

Types=( "$SPARK_RUN" "$SPARK_MPI_RUN" "$GPU_MPI_RUN" "$VISPARK_RUN" )
#Types=("$VISPARK_RUN" )
#Types=("$VISPARK_RUN" )

for solver in "${Types[@]}"
do
    for ((i=0;i<1;i+=1)) 
    do 
        #$MPI_RUN_SCRIPT &
        #echo -e $solver 
        $solver 
        #$MPI_KILL_SCRIPT
    done
done

#
