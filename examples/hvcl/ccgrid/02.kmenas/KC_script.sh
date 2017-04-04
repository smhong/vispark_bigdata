#!/bin/bash

testcases=( 20 40 80 160 320 )

bold='\033[1m'
endc='\033[0m'
red='\033[91m'
blue='\033[94m'

echo    '============================================'
echo -e ${bold}'     Start Kmeans Clustering [Vispark]'${endc}
echo    '============================================'

k=$1
echo -e ${bold}'  Kmeans Clustering'${endc}' < k =' ${red}${k}${endc}' > is started'
for ((i=0;i<1;i++))
do
    #echo -e $bold$blue'                TRIAL' $((i+1)) $endc
    vispark KC_Vispark.py $k 2> error.log
done

#for k in "${testcases[@]}"
#do
    #echo -e ${bold}'  Kmeans Clustering'${endc}' < k =' ${red}${k}${endc}' > is started'
    #for ((i=0;i<1;i++))
    #do
        #echo -e $bold$blue'                TRIAL' $((i+1)) $endc
        #vispark KC_Vispark.py $k 2> error.log
    #done
#done


#for k in "${testcases[@]}"
#do
    #for ((i=0;i<3;i++))
    #do
        #spark KC_Spark.py $k
    #done
#done
