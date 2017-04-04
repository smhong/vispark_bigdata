# VISPARK

from pyspark import SparkContext
import numpy, math
from PIL import Image
import collections
from socket import *



def printDebugInfo(rdd, numElem=10):
    info = rdd.take(numElem);
    print rdd.count()

    print info

def transfer(data, x, y):
    val = point_query_2d(data, x, y)
    return val


def heatflow(data, x, y, z):

    a = point_query_3d(data, x+1, y+0, z+0)
    b = point_query_3d(data, x-1, y+0, z+0)
    c = point_query_3d(data, x+0, y+1, z+0)
    d = point_query_3d(data, x+0, y-1, z+0)
    e = point_query_3d(data, x+0, y+0, z+1)
    f = point_query_3d(data, x+0, y+0, z-1)

    center =  point_query_3d(data, x+0, y+0, z+0)
    
    dt =  0.25
    result = center +  dt*(a+b+c+d+e+f - 6.0 * center)   
    
    return result

def read_data(key, data, dim, halo):
    ori_data = numpy.fromstring(data, dtype=numpy.float32).reshape(dim[2], dim[1], dim[0])
    data = numpy.ndarray((dim[2]+halo*2, dim[1]+halo*2, dim[0]+halo*2), dtype=numpy.float32)
    data[1:-1,1:-1,1:-1] = ori_data

    return key, data


if __name__ == "__main__":

    import sys

    sc = SparkContext(appName="HeatFlow_Vispark")
    print"pyspark version:" + str(sc.version)

    sigma = 1.0

    #pixels = sc.binaryFiles('hdfs://10.20.17.242:9000/user/whchoi/CThead',tag="VISPARK")
    # 32GB (4096 x 4096 x 2048) (512MB per block (512x512x512x4, float), automatic distribution)
    #ImageName = 'Heatflow_16G'
    #Ori_Shape = [512, 512, 512]
    #Ori_Size  = 512 + 1 * 2


    # 4GB (1024 x 1024 x 1024) (512MB per block (512x512x512x4, float), automatic distribution)
    #ImageName = 'Heatflow_4G'
    ImageName = 'Heatflow_8G'
    Ori_Shape = [512, 256, 256]
    Ori_Size_X  = 512 + 1 * 2
    Ori_Size_Y  = 256 + 1 * 2
    Ori_Size_Z  = 256 + 1 * 2

    #ImageName = 'Heatflow_16G'
    #Ori_Shape = [512, 512, 256]
    #Ori_Size_X  = 512 + 1 * 2
    #Ori_Size_Y  = 512 + 1 * 2
    #Ori_Size_Z  = 256 + 1 * 2


    # 8MB (128 x 128 x 128) (1MB per block (64x64x64x4, float), automatic distribution)
    #ImageName = 'Heatflow_8M'
    #Ori_Shape = [64, 64, 64]
    #Ori_Size  = 64 + 1 * 2

    #external CUDA Code
    cuda_code = open('hf.cu').read()

    #Read Data from HDFS
    pixels = sc.binaryFiles('hdfs://emerald1:9000/user/whchoi/%s'%ImageName,tag="VISPARK")

    #Data passing and Save in GPU 
    data = pixels.map(lambda (key, val): read_data(key, val, Ori_Shape, 1)).persist("GPU")

    for elem in range(5):
        #Halo communication using GPU data
        data = data.halo(1)

        #Heat flow 
        data = data.vmap(heatf(data, Ori_Size_X, Ori_Size_Y, Ori_Size_Z, Ori_Size_X, Ori_Size_Y).range(x=0:514,y=0:258,z=0:258).output(float, 1).extern_code(cuda_code))
        #data = data.vmap(heatf(data, Ori_Size_X, Ori_Size_Y, Ori_Size_Z, Ori_Size_X).range(x=0:514,y=0:514,z=0:258).output(float, 68162568).extern_code(cuda_code))
        
        #Save result on GPU 
        data = data.persist("GPU")
       
    #Save result on HDFS 
    data.saveAsRaw('Heatflow_result',False)
