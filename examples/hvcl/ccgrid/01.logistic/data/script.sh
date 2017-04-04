#!/bin/bash

Dims=( 40 )
Size=10000000
for d in "${Dims[@]}"
do
    ./a.out $Size $d
    filename=$(printf 'lr_%d_%d.txt' $Size $d)
    python lr_uploader.py $filename $Size 64

done
#
