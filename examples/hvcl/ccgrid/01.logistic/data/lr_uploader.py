import numpy
import sys
import os


if len(sys.argv) < 4:
    print "USAGE : python %s Filename TheNumberOfLines [Splits, default=8]"%sys.argv[0]
    exit(0)


# Argument Token
FileName = sys.argv[1]
NumLines = int(sys.argv[2])
NumSplites = int(sys.argv[3])



# Prepare Filepointer
FilePointer = open(FileName, "r")
ReadingUnit = NumLines / NumSplites


# Prepare tmp_directory
os.system("mkdir -p tmp_directory")


filePrefix = "tmp_directory/%s"%(FileName.replace('.txt', '_%02d.txt'))




data = []
for elem in range(NumSplites):
#for elem in range(1):
    for i in range(ReadingUnit):
        data.append(FilePointer.readline())
    #for i in range(len(data)):
        #data[i] = [int(values) for values in data[i].split(' ')]

    #print len(data)
    #data = numpy.array(data, dtype=numpy.uint8)
    #print filePrefix
    print "Data gen %d/%d"%(elem,NumSplites)
    with open(filePrefix%elem, "w") as fp:
        for i in range(len(data)):
            #print i, len(data[i])
            fp.write(data[i])


    data = []

current_path = os.path.abspath(os.getcwd())
target_prefix = "%s/%s"%(current_path, filePrefix)

HDFS_filepath = FileName[:FileName.rfind(".")]

os.system("hdfs dfs -rm -r -f %s"%HDFS_filepath)
os.system("hdfs dfs -mkdir %s"%HDFS_filepath)

server_list = ["ib1", "ib2", "ib3", "ib4", "ib5", "ib6", "ib7", "ib8"]
#server_list = ["dumbo003", "dumbo004", "dumbo007", "dumbo008"]

for elem in range(NumSplites):

    print "Data upload %d/%d"%(elem,NumSplites)
    f_size = os.path.getsize(target_prefix%elem)
    
    if f_size < 1024*1014:
        f_size = 1024*1024
    
    if f_size % 512 != 0:
        f_size = f_size + 512 - (f_size % 512)
    #print f_size

    #print "ssh %s hdfs dfs -D fs.local.block.size=%d -put %s"%(server_list[elem%len(server_list)], f_size,target_prefix%elem+" "+HDFS_filepath)
    os.system("ssh %s hdfs dfs -D dfs.block.size=%d -put %s"%(server_list[elem%len(server_list)], f_size,target_prefix%elem+" "+HDFS_filepath))
    #print "ssh %s hdfs dfs -D fs.local.block.size=%d -put "%(server_list[elem%len(server_list)]+" "+target_prefix%elem+" "+HDFS_filepath,f_size)
    #os.system("ssh %s hdfs dfs -D fs.local.block.size=%d -put "%(server_list[elem%len(server_list)]+" "+target_prefix%elem+" "+HDFS_filepath,f_size))

os.system("rm -rf tmp_directory")
