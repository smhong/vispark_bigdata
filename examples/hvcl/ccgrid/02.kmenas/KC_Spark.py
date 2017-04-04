from pyspark import SparkContext
import numpy
from PIL import Image
from pyspark.storagelevel import StorageLevel

import sys
import time

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
 

def parseVector(key, x, elems, dim, dType):
   
    a = [elem for elem in x.replace('\n', '').split(' ')][:-1]
    a = numpy.array([int(elem) if elem is not '' else elem for elem in a])

    if dType == 0:
        a = a.reshape(elems, dim+1).astype(numpy.uint8)
    else :
        a = a.reshape(elems, dim+1).astype(numpy.float32)
 
#    a = a.reshape(elems, dim+1).astype(numpy.float32)
    a = a[:,1:]

    #return a
    return [elem for elem in a]



def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")


    for i in range(len(centers)):
            
        tempDist = numpy.sum((p - centers[i]) ** 2)

        if tempDist < closest:
            closest = tempDist
            bestIndex = i

    return bestIndex


def composite(px, cx, py, cy):
    array1 = numpy.zeros(784)
    array2 = numpy.zeros(784)

    for elem in px:
        array1[elem[0]] = elem[1]
    for elem in py:
        array2[elem[0]] = elem[1]

    arr = array1+array2
    return ([(int(elem), float(arr[elem])) for elem in range(784) if arr[elem] > 0], cx+cy)

def averaging(val, cnt):
    return [[int(elem[0]),float(elem[1]/cnt)] for elem in val]


if __name__ == "__main__":

    k = int(sys.argv[1]) if len(sys.argv)>=2 else 40
    isPersist = int(sys.argv[2]) if len(sys.argv)>=3 else 1
    dType = int(sys.argv[3]) if len(sys.argv)>=4 else 1


    #textPath = 'hdfs://dumbo009:9000/user/smhong/Randpoints_3000000_784_%s/'%k
    #textPath = 'hdfs://dumbo009:9000/user/smhong/km_1024_784_%s/'%k
    #DataPath='hdfs://dumbo009:9000/user/smhong/km_3000000_784_%s'%k
    DataPath='hdfs://emerald1:9000/user/whchoi/km_3000000_784_%s'%k
    sc = SparkContext(appName="Kmeans_Clustering(SPARK)_for_k=%d"%k)

 
    start = time.time()

    NumPart = 64
    elements = 3000000/ NumPart
    dims     =  784

    lines = sc.wholeTextFiles(DataPath, minPartitions=NumPart*2)

    data = lines.flatMap(lambda (key, value):parseVector(key, value, elements, dims, dType)).persist()
 
    
    #kPoints = ori_data_rdd.map(lambda (key, value): value[:k]).reduce(lambda a,b : a+b).astype(numpy.float32)/NumPart
    kPoints = numpy.array(data.takeSample(False, k, 1)).astype(numpy.float32)

    print kPoints

    maxcnt = 1
    
    for n in range(5): 

        closest = data.map(
            lambda p: (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        newPoints = pointStats.map(
            lambda (x, (y, z)): (x, 1.0*y / z)).collect()

        tempDist = sum(numpy.sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)

        for (x, y) in newPoints:
            kPoints[x] = y
        
        kPoints = numpy.array(kPoints).astype(numpy.float32)
        print kPoints


    sc.stop()

