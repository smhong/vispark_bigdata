import numpy
import sys
import os

FilePath="test_1000"
N = 1000
NumFiles = 2

data = numpy.zeros(N).astype(numpy.float32)

os.system("hdfs dfs -rm -r -f %s"%FilePath)
os.system("hdfs dfs -mkdir %s"%FilePath)

for i in range(NumFiles):
    FileName = "%s_%02d"%(FilePath,i)
    fp = open("%s"%FileName,"w")
    fp.write(data)
    fp.close()

    #f_size = os.path.getsize(FileName)

    os.system("hdfs dfs -put %s %s"%(FileName, FilePath))


