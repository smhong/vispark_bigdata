#!/bin/bash
#mpirun --mca oob_tcp_if_include ib0 -np 4 -host ib1,ib2,ib3,ib4 python gpu_manager.py

#PYGPU_MANAGER=$SPARK_HOME/python/pyspark/vislib/gpu_manager.py
PYGPU_KILL=$SPARK_HOME/python/pyspark/vislib/gpu_kill.py
SLAVES_FILE=$SPARK_HOME/conf/slaves
#PREFIX="--prefix $LOCAL_HOME"
MPIRUN=`which mpirun`
pid=/tmp/vispark_worker.pid

#echo $MPIRUN 

NP=0
#HOST=""
declare -a HOST


while read line
do 
    if [[ $line != *"#"* ]]; then 
        if [[ ${#line} > 1 ]]; then 
            #echo $line
            NP=$(( NP+1 ))
            HOST+=("$line")
            #HOST=$HOST$line, 
        fi
    fi
done < $SLAVES_FILE

#echo $NP
#echo ${HOST[2]}


for x in ${HOST[@]}
do
    #if [[ $(ssh -q $x [[ -f "$pid" ]]) ]]; then
    ssh $x "test -e $pid"
    if [ $? -eq 0 ]; then
        TARGET_ID="$(ssh $x cat "$pid")"

        if [[ $(ssh $x ps -p "$TARGET_ID" -o comm=) =~ "python" ]]; then
            python $PYGPU_KILL $x 4949 2048
            ssh -q $x killall python
            echo "Killed GPU worker running as process $TARGET_ID."  
            continue
        fi
    fi

    #ssh $x python $PYGPU_MANAGER $SLAVES_FILE $x $pid &
    echo "No GPU server on $x"
done


#MPI_OPTION="--mca oob_tcp_if_include ib0 --mca orte_base_help_aggregate 0"
#MPI_OPTION="--mca oob_tcp_if_include ib0"

#mpirun $MPI_OPTION -np $NP -host $HOST python $PYGPU_MANAGER $SLAVES_FILE
#$MPIRUN $MPI_OPTION -np $NP -host $HOST python $PYGPU_MANAGER $SLAVES_FILE
