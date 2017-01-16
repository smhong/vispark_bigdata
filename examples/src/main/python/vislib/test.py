
from pyspark import SparkContext
import numpy, math
import sys


if __name__ == "__main__":

    sc = SparkContext()

    N = 1000

    isPersist = 1

    cuda_code = open('test.cu', 'r').read()

    #data  = numpy.zeros(N).astype(numpy.float32)
    #rdd = sc.parallelize(data,minPartitions = 2, tag = "VISPARK")

    DataPath='hdfs://dumbo009:9000/user/smhong/test_1000'
    rdd = sc.wholeTextFiles(DataPath,minPartitions = 2, tag = "VISPARK")

    rdd = rdd.map(lambda (key,value) : (key,numpy.fromstring(value,dtype=numpy.float32)))

    print rdd.collect()[0]

    if isPersist > 0 :
        rdd = rdd.persist("GPU")

    for n in range(3):
        print rdd

        rdd = rdd.vmap(adder(rdd,N,1.0).range(x=0:1000).output("float",1).extern_code(cuda_code))
        
        rdd = rdd.map(lambda (key,value) : (key,numpy.array(value).astype(numpy.float32).reshape(N)))

        print rdd.collect()

