#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from collections import namedtuple
from math import exp
from os.path import realpath
import sys

import os
import time
import numpy
from pyspark import SparkContext


D = 40  # Number of dimensions


# Read a batch of points from the input file into a NumPy matrix object. We operate on batches to
# make further computations faster.
# The data file contains lines of the form <label> <x1> <x2> ... <xD>. We load each block of these
# into a NumPy array of size numLines * (D + 1) and pull out column 0 vs the others in gradient().
def readPointBatch(iterator):
    strs = list(iterator)
    matrix = numpy.zeros((len(strs), D + 1)).astype(numpy.float32)
    for i in xrange(len(strs)):
        matrix[i] = numpy.fromstring(strs[i].replace(',', ' '), dtype=numpy.float32, sep=' ')

    import random


    return (str(int(random.random()*255)),[matrix])

def readPoint(key,value,N):
    key = key[-6:]
    key = key[:2]
    print key 

    with open("/home/whchoi/profile/%s.txt"%(key), "a") as myfile:
        myfile.write("%0.3f\n"%(time.time()))

    data = [elem for elem in value.replace('\n', '').split(' ')]
    data = numpy.array([elem if elem is not '' else elem for elem in data])


    #print len(data)
    if len(data) > N*(D+1):
        data = data[:N*(D+1)]
    
    
    data = data.reshape(N,D+1).astype(numpy.float32)
    data = data * 255

    data = data.astype(numpy.uint8)

    #print data 

    #return key,data.astype(numpy.uint8)

    with open("/home/whchoi/profile/%s.txt"%(key), "a") as myfile:
        myfile.write("%0.3f\n"%(time.time()))

    return key,data

if __name__ == "__main__":

    #if len(sys.argv) != 3:
    #    print >> sys.stderr, "Usage: logistic_regression <file> <iterations>"
    #    exit(-1)

    try :
        os.system("rm -rf /home/whchoi/profile/*")
    except:
        pass


    elems = int(sys.argv[1]) if len(sys.argv)>=2 else 2000000
    isPersist = int(sys.argv[2]) if len(sys.argv)>=3 else 1

    with open("/home/whchoi/profile/%s.txt"%("init"), "a") as myfile:
        myfile.write("%0.3f\n"%(time.time()))

    DataPath='hdfs://emerald1:9000/user/whchoi/lr_%s_40'%elems
    sc = SparkContext(appName="VisparkLR_for_%d"%elems)

    with open("/home/whchoi/profile/%s.txt"%("init"), "a") as myfile:
        myfile.write("%0.3f\n"%(time.time()))

    #points = sc.textFile(DataPath, minPartitions = 64).mapPartitions(readPointBatch).cache()
    #iterations = int(sys.argv[2])
    iterations = 5

    #
    N = elems / 64 

    #points = sc.wholeTextFiles(DataPath, minPartitions =128, tag="VISPARK").map(lambda (key,value):readPoint(key,value,N)).persist()
    points = sc.wholeTextFiles(DataPath, minPartitions =128, tag="VISPARK").map(lambda (key,value):readPoint(key,value,N))

    if isPersist == 0 : 
        points = points.persist()
    else : 
        points = points.persist("GPU")

    #print points.collect()

    # Initialize w to a random value
    w = 2 * numpy.random.ranf(size=D).astype(numpy.float32) - 1
    print "Initial w: " + str(w)

    cuda_code = open('lr.cu','r').read()

    for i in range(iterations):
        print "On iteration %i" % (i + 1)
        if   elems == 2000000:
            new_m = points.vmap(transpose(data,w,D,N).range(x=0:31250).output(float,40).extern_code(cuda_code))
        elif elems == 4000000:
            new_m = points.vmap(transpose(data,w,D,N).range(x=0:62500).output(float,40).extern_code(cuda_code))
        elif elems == 8000000:
            new_m = points.vmap(transpose(data,w,D,N).range(x=0:125000).output(float,40).extern_code(cuda_code))
        elif elems == 16000000:
            new_m = points.vmap(transpose(data,w,D,N).range(x=0:250000).output(float,40).extern_code(cuda_code))
        elif elems == 32000000:
            new_m = points.vmap(transpose(data,w,D,N).range(x=0:500000).output(float,40).extern_code(cuda_code))
        elif elems == 64000000:
            new_m = points.vmap(transpose(data,w,D,N).range(x=0:1000000).output(float,40).extern_code(cuda_code))
        elif elems == 10000:
            new_m = points.vmap(transpose(data,w,D,N).range(x=0:10000).output(float,40).extern_code(cuda_code))

        new_w = new_m.vmap(local_reduce(data,D,N).range(x=0:40).output(float,1).extern_code(cuda_code))
        w -= new_w.map(lambda (key,value):numpy.array(value).astype(numpy.float32)).reduce(lambda a,b:a+b)
        
        print w 

    print "Final w: " + str(w)



    with open("/home/whchoi/profile/%s.txt"%("close"), "a") as myfile:
        myfile.write("%0.3f\n"%(time.time()))

    sc.stop()
    with open("/home/whchoi/profile/%s.txt"%("close"), "a") as myfile:
        myfile.write("%0.3f\n"%(time.time()))


