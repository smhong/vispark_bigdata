#!/bin/bash
#mpirun --mca oob_tcp_if_include ib0 -np 4 -host ib1,ib2,ib3,ib4 python gpu_manager.py
#mpirun -np 8 dumb python gpu_manager.py
#mpirun --mca oob_tcp_if_include ib0 -np 4 -host ib1,ib2,ib3,ib4 python gpu_manager.py
#mpirun --mca oob_tcp_if_include ib0 -np 1 -host ib1 python gpu_manager.py

mpirun -np 8 -host dumbo001,dumbo002,dumbo003,dumbo004,dumbo005,dumbo006,dumbo007,dumbo008 python $SPARK_HOME/python/pyspark/vislib/gpu_manager.py
#mpirun --mca oob_tcp_if_include dumbo000 -np 8 -host dumbo001,dumbo002,dumbo003,dumbo004,dumbo005,dumbo006,dumbo007,dumbo008 python $SPARK_HOME/python/pyspark/vislib/gpu_manager.py
