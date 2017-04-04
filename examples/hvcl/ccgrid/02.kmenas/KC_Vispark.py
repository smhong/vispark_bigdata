import sys

import time
import numpy
from pyspark import SparkContext
from pyspark.storagelevel import StorageLevel
from PIL import Image

class bcolors:
    HEADER  = '\033[95m' 
    OKBLUE  = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL    = '\033[91m'
    ENDC    = '\033[0m'
    BOLD    = '\033[1m'
    UNDERLINE = '\033[4m'

def print_green(source):                                                                                              
    #return 
    print '%s%s%s'%( bcolors.OKGREEN,source,  bcolors.ENDC )

def print_red(source):
    print '%s%s%s'%( bcolors.FAIL,source, bcolors.ENDC)

def print_blue(source):
    print '%s%s%s'%( bcolors.OKBLUE,source, bcolors.ENDC)

def print_bblue(source):
    print '%s%s%s'%( bcolors.BOLD, bcolors.OKBLUE,source, bcolors.ENDC )

def print_yellow(source):
    print '%s%s%s'%( bcolors.WARNING,source, bcolors.ENDC)

def print_purple(source):
    print '%s%s%s'%( bcolors.HEADER,source, bcolors.ENDC)

def print_bold(source):                                                                                               
    #return
    print '%s%s%s'%(bcolors.BOLD,source, bcolors.ENDC)

def print_bred(source):                                                                                               
    #return
    print '%s%s%s'%(bcolors.BOLD, bcolors.FAIL, source, bcolors.ENDC)
    

def add(data, x):
    ret = point_query_2d(data, x, 1)
    return ret


def parseVector(key, x, elems, dim):
   
    a = [elem for elem in x.replace('\n', '').split(' ')][:-1]
    a = numpy.array([int(elem) if elem is not '' else elem for elem in a])

    a = a.reshape(elems, dim+1).astype(numpy.uint8)
    a = a[:,1:]

    return key, a


def compose_local(data1, data2, k):
    if len(data1.shape) == 1:
        nearest_list = data1
        points_list  = data2
    else:
        nearest_list = data2
        points_list  = data1

    #1000x784
    #shape     = list(points_list.shape)
    #shape[1] += 2

    #1000x786

    #result = numpy.ndarray((shape))
   
    #result[:,1:-1] = points_list
    #result[:,-1]   = 1
    #result[:,0]   = nearest_list

     

    #print "NEAREST LIST", nearest_list.shape
    #print "POINTS LIST ", points_list.shape

    result_buffer = []
    for elem in range(k):
        result_buffer.append([0, numpy.zeros(points_list[0].shape, dtype=numpy.float32)])

    
    #for elem in range(len(nearest_list)):
        #nearest = nearest_list[elem]
        #points  = points_list[elem]

        #result_buffer[nearest][0] += 1
        #result_buffer[nearest][1] += points
    
    def summation(result_buffer, nearest, points):
        try:
            result_buffer[nearest][0] += 1
            result_buffer[nearest][1] += points
        except:
            print nearest, k

    #print result_buffer

    map(lambda x, y: summation(result_buffer, x, y), nearest_list, points_list)

    

    return_buffer = []
    for elem in range(len(result_buffer)):
        target_buffer = result_buffer[elem]
        if target_buffer[0] > 0:
            return_buffer.append([elem, target_buffer])

    return return_buffer
    #return (data1.shape, data2.shape)

def compose_global(data1, data2):
    data1[0] += data2[0]
    data1[1] += data2[1]

    return data1
        

def closestPointVis(data, x, dims, centers, size_of_centers):
    bestIndex = 1.0
    
    closest = 9999999999.0
    for elem in range(size_of_centers):
        tmpDist = 0

        for i in range(dims):
            tmpDist += (data[x*dims+i]-centers[elem*dims+i])*(data[x*dims+i]-centers[elem*dims+i])

        if tmpDist < closest:
            closest = tmpDist
            bestIndex = elem

    
    return bestIndex

if __name__ == "__main__": 
    #k = 10
    k = int(sys.argv[1]) if len(sys.argv)>=2 else 20


    #textPath = 'hdfs://dumbo009:9000/user/smhong/km_3000000_784_%s/'%k
    textPath = 'hdfs://dumbo009:9000/user/smhong/km_1024_784_%s/'%k
    sc = SparkContext(appName="Vis_Kmeans_Clustering_for_k=%d"%k)

    first_iter = True
    start = time.time()

    numPartitions = 8

    lines = sc.wholeTextFiles(textPath, minPartitions=numPartitions,tag="VISPARK")

    elements = 1024 / numPartitions
    dims     = 784



    data = lines.map(lambda (key,value):parseVector(key,value,elements, dims)).persist()

    kPoints = numpy.zeros((k,dims)).astype(numpy.float32)



    ori_data_rdd = data.rdd()
    
    sampler = ori_data_rdd.map(lambda (key, value): value[:k]).collect()
    for elem in range(len(sampler)):
        kPoints += sampler[elem]

    kPoints /= len(sampler)

    #print kPoints
        

    maxcnt = 1
    
    cnt = 0
    while cnt < maxcnt:
        if first_iter == False:
            start = time.time()
        cnt += 1
        closest = data.vmap(closestPointVis(data, y, dims, kPoints, k))

        #print (closest.collect()[1][1][:100])
        print closest.collect()

        #tmp_rdd = ori_data_rdd.union(closest.rdd())
        #tmp_rdd = tmp_rdd.reduceByKey(lambda x, y:compose_local(x, y, k))
        #tmp_rdd = tmp_rdd.flatMap(lambda (key, data): data)


        # count
        #tmp_rdd = tmp_rdd.reduceByKey(lambda x, y:compose_global(x, y))
        #tmp_rdd = tmp_rdd.map(lambda (key, value): value)
        #kPoints = tmp_rdd.map(lambda (count, summation): summation / count).collect()
        if first_iter == True:
            print "%sFIRST ITER%s"%(bcolors.BOLD, bcolors.ENDC)
            first_iter = False
        if cnt == 2:
            print "%sREST ITER%s"%(bcolors.BOLD, bcolors.ENDC)

        print "  %sElapsed Time%s : %.04f seconds"%(bcolors.BOLD, bcolors.ENDC, time.time() - start)
    
        #kPoints = numpy.array(kPoints, dtype=numpy.float32)
        #print kPoints

        #sys.stdout.write("\033[F")

    sc.stop()
    #tmp_rdd = closest.map(lambda (key,data):(key, data[0]))
    #print tmp_rdd
