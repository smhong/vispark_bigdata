#!/bin/bash

PYGPU_KILLER=$SPARK_HOME/python/pyspark/vislib/kill_manager.py
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

mpirun -np $NP -host $HOST python $PYGPU_KILLER
#mpirun -np 8 -host dumbo001,dumbo002,dumbo003,dumbo004,dumbo005,dumbo006,dumbo007,dumbo008 python $SPARK_HOME/python/pyspark/vislib/kill_manager.py
