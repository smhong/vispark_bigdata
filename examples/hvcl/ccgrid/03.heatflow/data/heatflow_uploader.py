import numpy
import sys
import os

if len(sys.argv) < 3:
    print "USAGE : python %s Filename numofblocks"%sys.argv[0]
    exit(0)

# Argument Token
FileName = sys.argv[1]
FileCnt  = int(sys.argv[2])


HDFS_filepath   = FileName[:FileName.rfind(".")]
HDFS_fileprefix = HDFS_filepath+'/data_%03d'

current_path = os.path.abspath(os.getcwd())
FileName = current_path + '/' + FileName

os.system("hdfs dfs -rm -r -f %s"%HDFS_filepath)
os.system("hdfs dfs -mkdir %s"%HDFS_filepath)

server_list = ["emerald1", "emerald2", "emerald3", "emerald4", "emerald5", "emerald6", "emerald7", "emerald8"]

for elem in range(FileCnt):
    f_size = os.path.getsize(FileName)
    
    if f_size < 1024*1014:
        f_size = 1024*1024
    
    if f_size % 512 != 0:
        f_size = f_size + 512 - (f_size % 512)
    #print f_size

    os.system("ssh %s hdfs dfs -D dfs.block.size=%d -put %s"%(server_list[elem%len(server_list)], f_size, FileName+" "+HDFS_fileprefix%elem))
    #print ("ssh %s hdfs dfs -D dfs.block.size=%d -put %s"%(server_list[elem%len(server_list)], f_size, FileName+" "+HDFS_fileprefix%elem))
