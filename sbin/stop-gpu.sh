#!/bin/bash
#CUDA_PROFILE_RESULT='cuda_%q{OMPI_COMM_WORLD_RANK}.prof'
#mpirun -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 nvprof -f -o $CUDA_PROFILE_RESULT python gpu_manager.py
#mpirun --mca oob_tcp_if_include ib0 -np 8 -host ib1,ib2,ib3,ib4,ib5,ib6,ib7,ib8 python kill_manager.py
mpirun -np 8 -host dumbo001,dumbo002,dumbo003,dumbo004,dumbo005,dumbo006,dumbo007,dumbo008 python $SPARK_HOME/python/pyspark/vislib/kill_manager.py
#mpirun -np 8 -host emerald1,emerald2,emerald3,emerald4,emerald5,emerald6,emerald7,emerald8 python gpu_manager.py
