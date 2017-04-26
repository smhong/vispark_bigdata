#!/bin/bash
#mpirun --mca oob_tcp_if_include ib0 -np 4 -host ib1,ib2,ib3,ib4 python gpu_manager.py

PYGPU_MANAGER=$SPARK_HOME/python/pyspark/vislib/gpu_manager.py
SLAVES_FILE=$SPARK_HOME/conf/slaves
#PREFIX="--prefix $LOCAL_HOME"
MPIRUN=`which mpirun`


echo $MPIRUN 

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

echo $NP
echo $HOST

MPI_OPTION="--mca oob_tcp_if_include ib0 --mca orte_base_help_aggregate 0"

#mpirun $MPI_OPTION -np $NP -host $HOST python $PYGPU_MANAGER $SLAVES_FILE
$MPIRUN $MPI_OPTION -np $NP -host $HOST python $PYGPU_MANAGER $SLAVES_FILE
