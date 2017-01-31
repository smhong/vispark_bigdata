#!/bin/bash
#mpirun --mca oob_tcp_if_include ib0 -np 4 -host ib1,ib2,ib3,ib4 python gpu_manager.py

PYGPU_MANAGER=$SPARK_HOME/python/pyspark/vislib/gpu_manager.py
SLAVES_FILE=$SPARK_HOME/conf/slaves

NP=0
HOST=""

while read line
do 
    if [[ $line != *"#"* ]]; then 
        if [[ ${#line} > 1 ]]; then 
            #echo $line
            NP=$(( NP+1 ))
            HOST=$HOST$line, 
        fi
    fi
done < $SLAVES_FILE

#echo $NP
#echo $HOST

mpirun -np $NP -host $HOST python $PYGPU_MANAGER
